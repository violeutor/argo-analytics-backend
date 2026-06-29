"""
SE-01–SE-13 · signal_engine.py
Pfad: argo-analytics-backend/src/services/signal_engine.py

Signal-Engine — täglich via Cron (~06:00 UTC).

Quellen:
  - SEC EDGAR full-text search  → S-1 (IPO), 10-K/10-Q (Earnings), 8-K (M&A/Events)
  - Google News RSS              → Funding, M&A, allg. News
  - TechCrunch RSS               → Funding-Runden EU/US
  - Interner Ownership-Vergleich → Ownership-Changes (DB-Diff)
  - DB-Zustand                   → Abweichungs-Signale (SE-13)

Pipeline je Company:
  1. EDGAR-Parser         → strukturierte Events (S-1, 10-K, 8-K)
  2. News-Parser          → NER via Keyword-Matching (SE-08: \b-Wortgrenzen)
  3. Claude-NER Pass 2    → direction + signal_category (SE-09/SE-11/SE-12)
  4. Ownership-Diff       → Vergleich mit letztem DB-Stand
  5. Abweichungs-Check    → Risiken aus fehlendem DB-Zustand (SE-13)
  6. upsert_signals       → Duplikat-sicher via UNIQUE CONSTRAINT

Signal-Richtungen (SE-09):
  positive  → Potenzial-Signal (Funding, Partnerschaft, positives Sentiment)
  negative  → Risiko-Signal (Regulatorik, Insider-Verkauf, negatives Earnings)
  neutral   → Informativ (Filing, Ownership-Eintrag ohne klare Richtung)

Signal-Kategorien (SE-11/SE-12):
  Potenzial: funding | partnership | ipo_progress | market_growth | patent | investor_entry
  Risiko:    regulatory | negative_earnings | supply_chain | insider_selling | customer_concentration
  Neutral:   filing | ownership_entry | general_news
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, date, timezone, timedelta
from typing import Literal
from xml.etree import ElementTree as ET

import httpx

# LISTED-STATUS-REVIEW-01 (S75): kanonische Listed-Erkennung statt eigener
# ticker-only-Shortcuts (vorher zwei Stellen, siehe check_absence_signals +
# run_signal_engine). Kein zirkulärer Import — score_calculator.py importiert
# nichts aus signal_engine.py.
from src.services.score_calculator import _is_listed

logger = logging.getLogger(__name__)

# ── Typen ─────────────────────────────────────────────────────────────────────

EventType = Literal[
    "funding_round", "ipo_status_change", "m_and_a_event",
    "ownership_change", "kpi_breakout", "earnings", "news"
]

Severity = Literal["high", "medium", "low"]

# SE-09: Signal-Richtung
Direction = Literal["positive", "negative", "neutral"]

# SE-11/SE-12: Signal-Kategorie (Potenzial / Risiko / Neutral)
SignalCategory = Literal[
    # Potenzial-Signale
    "funding", "partnership", "ipo_progress", "market_growth", "patent", "investor_entry",
    # Risiko-Signale
    "regulatory", "negative_earnings", "supply_chain", "insider_selling", "customer_concentration",
    # SE-18: Insider-Käufe (positives Signal — eigene Kategorie)
    "insider_buying",
    # Neutral / Informativ
    "filing", "ownership_entry", "general_news",
]


class SignalEvent:
    __slots__ = (
        "company_id", "company_name", "event_type", "event_date",
        "summary", "source", "source_url", "severity", "raw_title",
        # SE-09/SE-11/SE-12: Richtung + Kategorie
        "direction", "signal_category",
        # Session 10: Qualität + Deduplizierung + B-05
        "source_domain", "relevance_score", "funding_amount_usd_mn",
        # SE-17: Multi-Source Aggregation
        "source_count", "source_names",
    )

    def __init__(
        self,
        company_id: str,
        company_name: str,
        event_type: EventType,
        event_date: date,
        summary: str,
        source: str,
        source_url: str | None = None,
        severity: Severity = "medium",
        raw_title: str | None = None,
        direction: Direction = "neutral",
        signal_category: SignalCategory = "general_news",
        source_domain: str | None = None,
        relevance_score: float | None = None,
        funding_amount_usd_mn: float | None = None,
        source_count: int = 1,
        source_names: list[str] | None = None,
    ):
        self.company_id            = company_id
        self.company_name          = company_name
        self.event_type            = event_type
        self.event_date            = event_date
        self.summary               = summary
        self.source                = source
        self.source_url            = source_url
        self.severity              = severity
        self.raw_title             = raw_title
        self.direction             = direction
        self.signal_category       = signal_category
        self.source_domain         = source_domain or _extract_domain(source_url)
        self.relevance_score       = relevance_score
        self.funding_amount_usd_mn = funding_amount_usd_mn
        self.source_count          = source_count
        self.source_names          = source_names or [source]

    def to_dict(self) -> dict:
        return {
            "company_id":            self.company_id,
            "event_type":            self.event_type,
            "event_date":            self.event_date.isoformat(),
            "summary":               self.summary,
            "source":                self.source,
            "source_url":            self.source_url,
            "severity":              self.severity,
            "raw_title":             self.raw_title,
            "direction":             self.direction,
            "signal_category":       self.signal_category,
            "source_domain":         self.source_domain,
            "relevance_score":       self.relevance_score,
            "funding_amount_usd_mn": self.funding_amount_usd_mn,
            "source_count":          self.source_count,
            "source_names":          self.source_names,
        }


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _extract_domain(url: str | None) -> str | None:
    """Extrahiert Hostname aus URL für Deduplizierung — z.B. 'techcrunch.com'."""
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        return urlparse(url).hostname or None
    except Exception:
        return None


_FUNDING_AMOUNT_RE = re.compile(
    r'\$\s*([\d,]+(?:\.\d+)?)\s*(billion|million|bn|mn|b|m)\b'
    r'|'
    r'([\d,]+(?:\.\d+)?)\s*(billion|million|bn|mn|b|m)\s*(USD|Dollar|dollars)',
    re.IGNORECASE
)


def _extract_funding_amount(text: str) -> float | None:
    """
    B-05: Extrahiert Funding-Betrag aus Freitext → USD Mio.
    Beispiele: '$50 million' → 50.0 | '$1.2 billion' → 1200.0 | '€80 Mio.' → None (EUR skip)

    EUR-Beträge bewusst weiterhin None hier — siehe _extract_eur_funding_amount()
    direkt darunter. Keine Erweiterung dieser Funktion um EUR-Pattern: sie wird
    an drei Stellen aufgerufen (Google News, TechCrunch, Claude-NER-Pass) und
    überall direkt in funding_amount_usd_mn geschrieben — ein EUR-Treffer ohne
    FX-Konvertierung würde dort stillschweigend als USD-Wert fehletikettiert
    (Daten-Fabrikation, kein Fix). Eigenständige Funktion statt Erweiterung
    hält den Blast-Radius auf den neuen Discovery-EU-News-Pfad beschränkt, der
    seinen Floor ohnehin in EUR definiert (≥€5M, DISCOVERY-PREIPO-01) und daher
    unkonvertiert bleiben soll.
    """
    m = _FUNDING_AMOUNT_RE.search(text)
    if not m:
        return None
    raw_num = (m.group(1) or m.group(3) or "").replace(",", "")
    unit    = (m.group(2) or m.group(4) or "").lower()
    try:
        val = float(raw_num)
    except ValueError:
        return None
    if unit in ("billion", "bn", "b"):
        return round(val * 1000, 2)
    if unit in ("million", "mn", "m"):
        return round(val, 2)
    return None


# DISCOVERY-PREIPO-01 — EUR-Pendant, additiv und eigenständig (s. Docstring oben).
# Unit-Präfixe: Mio./Million(en) = ×1, Mrd./Milliarde(n) = ×1000. Deutsches
# Dezimalkomma wird vor float() in einen Punkt übersetzt; Tausendertrennzeichen
# werden bei Funding-Beträgen dieser Größenordnung nicht erwartet (Scope-Grenze,
# kein vollständiger DE-Zahlenparser).
_EUR_FUNDING_AMOUNT_RE = re.compile(
    r'€\s*([\d]+(?:[.,]\d+)?)\s*(milliarden|mrd\.?|millionen|mio\.?|m)\b'
    r'|'
    r'([\d]+(?:[.,]\d+)?)\s*(milliarden|mrd\.?|millionen|mio\.?)\s*(?:€|euro|eur)\b',
    re.IGNORECASE
)


def _extract_eur_funding_amount(text: str) -> float | None:
    """
    DISCOVERY-PREIPO-01: EUR-Pendant zu _extract_funding_amount() — separate
    Funktion statt Erweiterung der USD-Funktion (Begründung im Docstring dort).
    Gibt den Betrag in EUR Mio. zurück, UNKONVERTIERT — der Discovery-Floor für
    den kuratierten EU-News-Pfad ist in EUR definiert (≥€5M), keine FX-Quelle
    im Scope dieses Builds.

    Beispiele: '€80 Mio.' → 80.0 | '80 Millionen Euro' → 80.0 | '€1,2 Mrd.' → 1200.0
    """
    m = _EUR_FUNDING_AMOUNT_RE.search(text)
    if not m:
        return None
    raw_num = (m.group(1) or m.group(3) or "").replace(",", ".")
    unit    = (m.group(2) or m.group(4) or "").lower()
    try:
        val = float(raw_num)
    except ValueError:
        return None
    if unit.startswith("mrd") or unit.startswith("milliarden"):
        return round(val * 1000, 2)
    if unit.startswith("mio") or unit.startswith("million"):
        return round(val, 2)
    return None


def _normalize_name(name: str) -> str:
    """Vereinfacht Company-Namen für Keyword-Matching."""
    stop = {"AG", "GmbH", "SE", "Inc", "Corp", "Ltd", "LLC", "PLC", "NV", "BV", "SAS", "S.A."}
    parts = name.split()
    return " ".join(p for p in parts if p not in stop).lower().strip()


def _name_in_text(company_name: str, text: str) -> bool:
    """
    SE-08: Prüft ob Company-Name (normalisiert) im Text vorkommt.
    Verwendet \b-Wortgrenzen — eliminiert False-Positives wie
    'Moment Energy' matcht auf 'SpaceX'-Artikel der 'momentum' enthält.
    """
    normalized = _normalize_name(company_name)
    # Vollständiger normalisierter Name mit Wortgrenzen
    if re.search(r'\b' + re.escape(normalized) + r'\b', text.lower()):
        return True
    # Hauptwort match (erstes Wort ≥ 5 Zeichen) — ebenfalls mit \b
    main_word = normalized.split()[0] if normalized else ""
    if len(main_word) >= 5 and re.search(r'\b' + re.escape(main_word) + r'\b', text.lower()):
        return True
    return False


def _parse_rss_date(pub_date: str | None) -> date:
    """
    Parst RSS pubDate-Strings zu einem date-Objekt.
    Unterstützt RFC-2822 ("Mon, 25 May 2026 04:00:00 +0000") und ISO-8601.
    Gibt date.today() zurück wenn pub_date None oder nicht parsebar ist.

    BUG-55: Funktion war versehentlich aus signal_engine.py entfernt worden.
    Wird intern (Google News, TechCrunch) und von funding_enrichment.py genutzt.
    """
    if not pub_date:
        return date.today()
    _RFC_FORMATS = (
        "%a, %d %b %Y %H:%M:%S %z",   # RFC-2822 mit Timezone
        "%a, %d %b %Y %H:%M:%S",       # RFC-2822 ohne Timezone
        "%Y-%m-%dT%H:%M:%S%z",         # ISO-8601 mit Timezone
        "%Y-%m-%dT%H:%M:%S",           # ISO-8601 ohne Timezone
        "%Y-%m-%d",                     # Einfaches Datum
    )
    for fmt in _RFC_FORMATS:
        try:
            return datetime.strptime(pub_date.strip(), fmt).date()
        except ValueError:
            continue
    logger.debug("_parse_rss_date: konnte '%s' nicht parsen — verwende today()", pub_date)
    return date.today()


def _extract_date_from_text(text: str | None) -> date | None:
    """
    BUG-04: Extrahiert das Datum aus dem Artikel-Body/Titel wenn Feed-Datum
    nicht zuverlässig ist (Re-Published alte Artikel).

    Sucht nach ISO-Datumsangaben und deutschen/englischen Monatsnamen.
    Gibt None zurück wenn kein plausibles Datum gefunden.
    """
    if not text:
        return None
    # ISO-Datum im Text (z.B. "2025-04-15" oder "15.04.2025")
    iso_pattern = re.compile(r'\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b')
    de_pattern  = re.compile(
        r'\b(0?[1-9]|[12]\d|3[01])\s*\.?\s*'
        r'(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember|'
        r'Jan|Feb|Mär|Apr|Jun|Jul|Aug|Sep|Okt|Nov|Dez)'
        r'\.?\s*(20\d{2})\b', re.IGNORECASE
    )
    _DE_MONTHS = {
        "januar":1,"jan":1,"februar":2,"feb":2,"märz":3,"mär":3,
        "april":4,"apr":4,"mai":5,"juni":6,"jun":6,"juli":7,"jul":7,
        "august":8,"aug":8,"september":9,"sep":9,"oktober":10,"okt":10,
        "november":11,"nov":11,"dezember":12,"dez":12,
    }
    # ISO zuerst
    m = iso_pattern.search(text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    # Deutsches Format
    m = de_pattern.search(text)
    if m:
        try:
            month = _DE_MONTHS.get(m.group(2).lower().rstrip("."))
            if month:
                return date(int(m.group(3)), month, int(m.group(1)))
        except (ValueError, AttributeError):
            pass
    return None


def _aggregate_events(events: list[SignalEvent]) -> list[SignalEvent]:
    """
    SE-17: Multi-Source Signal Aggregation.
    Gruppiert Events nach (company_id, event_type, 3-Tage-Bucket) und
    merged Duplikate aus verschiedenen Quellen zu einem aggregierten Signal.

    Trigger: Siemens-Italtech-Akquisition kam via EDGAR + Google News + TechCrunch
    dreifach rein — jetzt ein Event mit source_count=3 + gewichtetem relevance_score.

    Formel: relevance_score = base × min(2.0, 1.0 + source_count × 0.33)
      source_count=1 → ×1.33  |  source_count=2 → ×1.66
      source_count=3 → ×1.99  |  source_count=4+ → ×2.0 (Cap)
    """
    from collections import defaultdict

    def _bucket(d: date) -> int:
        """3-Tage-Bucket — Events innerhalb von ±3 Tagen landen im gleichen Bucket."""
        return (d - date(2020, 1, 1)).days // 3

    groups: dict[tuple, list[SignalEvent]] = defaultdict(list)
    for ev in events:
        key = (ev.company_id, ev.event_type, _bucket(ev.event_date))
        groups[key].append(ev)

    merged: list[SignalEvent] = []
    for group in groups.values():
        if len(group) == 1:
            ev = group[0]
            if ev.relevance_score is not None:
                ev.relevance_score = round(min(ev.relevance_score * 1.33, 2.0), 3)
            merged.append(ev)
            continue

        # Bestes Event als Basis (höchster relevance_score)
        base = max(group, key=lambda e: e.relevance_score or 0.0)
        source_count  = len(group)
        base_score    = base.relevance_score or 0.6
        base.relevance_score = round(
            min(base_score * min(2.0, 1.0 + source_count * 0.33), 2.0), 3
        )
        base.source_count = source_count
        base.source_names = list(dict.fromkeys(
            s for ev in group for s in (ev.source_names or [ev.source])
        ))
        if source_count > 1 and base.summary:
            base.summary = f"[{source_count} Quellen] {base.summary}"
        merged.append(base)

    logger.info(
        "SE-17 Aggregation: %d events → %d aggregiert (%.0f%% Reduktion)",
        len(events), len(merged),
        (1 - len(merged) / len(events)) * 100 if events else 0,
    )
    return merged


# ── SE-09/SE-11/SE-12: Keyword-basierte Direction + Category (Fallback) ──────

# Potenzial-Signale: direction=positive
_POSITIVE_KW: list[tuple[str, SignalCategory]] = [
    # Funding / Investment
    ("series a",            "funding"),
    ("series b",            "funding"),
    ("series c",            "funding"),
    ("series d",            "funding"),
    ("funding round",       "funding"),
    ("raised",              "funding"),
    ("finanzierungsrunde",  "funding"),
    ("investment",          "funding"),
    ("new investor",        "investor_entry"),
    ("neuer investor",      "investor_entry"),
    # Partnership
    ("partnership",         "partnership"),
    ("partnerschaft",       "partnership"),
    ("joint venture",       "partnership"),
    ("kooperation",         "partnership"),
    ("collaboration",       "partnership"),
    ("strategic agreement", "partnership"),
    # IPO
    ("s-1",                 "ipo_progress"),
    ("ipo",                 "ipo_progress"),
    ("initial public offering", "ipo_progress"),
    ("börsengang",          "ipo_progress"),
    ("going public",        "ipo_progress"),
    # Market Growth
    ("market growth",       "market_growth"),
    ("wachstum",            "market_growth"),
    ("expansion",           "market_growth"),
    # Patent
    ("patent",              "patent"),
    ("patentiert",          "patent"),
]

# Risiko-Signale: direction=negative
_NEGATIVE_KW: list[tuple[str, SignalCategory]] = [
    # Regulatory
    ("regulatorisch",       "regulatory"),
    ("regulatory",          "regulatory"),
    ("sanktion",            "regulatory"),
    ("sanction",            "regulatory"),
    ("bußgeld",             "regulatory"),
    ("fine",                "regulatory"),
    ("strafe",              "regulatory"),
    ("klage",               "regulatory"),
    ("lawsuit",             "regulatory"),
    ("investigation",       "regulatory"),
    ("ermittlung",          "regulatory"),
    # Negative Earnings
    ("verlust",             "negative_earnings"),
    ("loss",                "negative_earnings"),
    ("gewinnwarnung",       "negative_earnings"),
    ("profit warning",      "negative_earnings"),
    ("umsatzrückgang",      "negative_earnings"),
    ("revenue decline",     "negative_earnings"),
    ("insolvenz",           "negative_earnings"),
    ("bankruptcy",          "negative_earnings"),
    ("restructuring",       "negative_earnings"),
    ("stellenabbau",        "negative_earnings"),
    ("layoffs",             "negative_earnings"),
    # Supply Chain
    ("supply chain",        "supply_chain"),
    ("lieferkette",         "supply_chain"),
    ("shortage",            "supply_chain"),
    ("engpass",             "supply_chain"),
    # Insider Selling
    ("insider selling",     "insider_selling"),
    ("insider verkauf",     "insider_selling"),
    ("direktor verkauft",   "insider_selling"),
    # Customer Concentration
    ("key customer",        "customer_concentration"),
    ("hauptkunde",          "customer_concentration"),
    ("customer dependency", "customer_concentration"),
]


def _keyword_direction(text: str) -> tuple[Direction, SignalCategory]:
    """
    SE-09 Fallback: Keyword-basierte direction + signal_category.
    Wird verwendet wenn Claude-API nicht verfügbar.
    Reihenfolge: Negativ vor Positiv (konservativer Ansatz bei Ambiguität).
    """
    text_lower = text.lower()
    for kw, cat in _NEGATIVE_KW:
        if kw in text_lower:
            return "negative", cat
    for kw, cat in _POSITIVE_KW:
        if kw in text_lower:
            return "positive", cat
    return "neutral", "general_news"


def _event_type_direction(event_type: EventType) -> tuple[Direction, SignalCategory]:
    """
    Strukturierte Events (EDGAR, Ownership) bekommen direction aus event_type —
    kein Claude-Pass nötig, da Quelle eindeutig ist.
    """
    mapping: dict[EventType, tuple[Direction, SignalCategory]] = {
        "funding_round":     ("positive", "funding"),
        "ipo_status_change": ("positive", "ipo_progress"),
        "m_and_a_event":     ("neutral",  "filing"),      # M&A kann + oder - sein → Claude klärt
        "ownership_change":  ("neutral",  "ownership_entry"),
        "kpi_breakout":      ("positive", "market_growth"),
        "earnings":          ("neutral",  "filing"),       # Earnings → Claude klärt Richtung
        "news":              ("neutral",  "general_news"),
    }
    return mapping.get(event_type, ("neutral", "general_news"))


# SE16-INTENT-UPGRADE-01: Geteilte Klassifikation IPO-Completion vs. IPO-Intent.
# Zwei Call-Sites: (1) run_signal_engine() unten — Bestands-Companies im
# 04:00-UTC-Cron, (2) discovery_engine.py — neu entdeckte Companies beim Anlegen.
# Eine Funktion, damit die Phrase-Listen nicht an zwei Stellen auseinanderlaufen.
_IPO_COMPLETION_KW = (
    "began trading", "started trading", "listed on", "debut", "ipo priced",
    "börsengang abgeschlossen", "erstmals gehandelt",
)
# Intent: konservative Phrase-Liste analog zur Completion-Liste — bewusst kein
# generisches "ipo"-Keyword-Match (das würde auch Completion-/Spekulations-News
# treffen). Form-Präfix-Check (raw_title beginnt mit "S-1"/"S-11") ist das
# präziseste Signal für EDGAR-Quellen; die Phrasen darunter greifen für
# Nicht-EDGAR-Quellen (z.B. News-Pfad), wo raw_title kein Form-Präfix trägt.
_IPO_INTENT_KW = (
    "files for ipo", "filed for ipo", "s-1 filing", "s-1-antrag",
    "s-11-antrag", "ipo-prozess gestartet", "ipo-antrag eingereicht",
)


def _classify_ipo_signal(raw_title: str | None) -> Literal["completion", "intent", "none"]:
    """
    SE16-INTENT-UPGRADE-01: Unterscheidet IPO-Completion (Listing abgeschlossen)
    von IPO-Intent (S-1/S-11 eingereicht, Prozess gestartet, noch nicht gelistet).
    Completion hat Priorität — ein abgeschlossenes Listing ist nie gleichzeitig
    nur "Intent", auch falls beide Phrase-Sets zufällig im selben Titel matchen.
    """
    title = (raw_title or "").lower()
    if any(k in title for k in _IPO_COMPLETION_KW):
        return "completion"
    # EDGAR-Form-Präfix: raw_title-Format ist "{form_type} — {entity}" (s. parse_edgar)
    form_prefix = title.split("—")[0].strip()
    if form_prefix in ("s-1", "s-11"):
        return "intent"
    if any(k in title for k in _IPO_INTENT_KW):
        return "intent"
    return "none"


def _classify_event(title: str, description: str) -> EventType:
    """Klassifiziert News-Artikel in Event-Typ via Keyword-Matching."""
    text = (title + " " + description).lower()

    ipo_kw    = ["s-1", "ipo", "initial public offering", "börsengang", "going public", "listing"]
    fund_kw   = ["series a", "series b", "series c", "series d", "funding round",
                 "raised", "investment", "venture", "finanzierungsrunde", "millionen euro"]
    ma_kw     = ["acquisition", "merger", "acquires", "acquired", "takeover", "übernahme",
                 "fusioniert", "buys", "deal", "m&a"]
    earn_kw   = ["earnings", "quarterly results", "q1", "q2", "q3", "q4",
                 "jahresbericht", "umsatz", "revenue report", "10-k", "10-q"]

    if any(k in text for k in ipo_kw):     return "ipo_status_change"
    if any(k in text for k in fund_kw):    return "funding_round"
    if any(k in text for k in ma_kw):      return "m_and_a_event"
    if any(k in text for k in earn_kw):    return "earnings"
    return "news"


def _severity_for_event(event_type: EventType) -> Severity:
    return {
        "ipo_status_change": "high",
        "m_and_a_event":     "high",
        "funding_round":     "medium",
        "earnings":          "medium",
        "ownership_change":  "medium",
        "kpi_breakout":      "medium",
        "news":              "low",
    }.get(event_type, "medium")


# ── SE-09: Claude-NER zweiter Pass ───────────────────────────────────────────

_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL   = "claude-haiku-4-5-20251001"   # schnell + günstig für NER-Batch

# ── SE-14: EPO OPS Konstanten ─────────────────────────────────────────────────
_EPO_AUTH_URL   = "https://ops.epo.org/3.2/auth/access_token"
_EPO_SEARCH_URL = "https://ops.epo.org/3.2/rest-services/published-data/search"
_EPO_TOKEN_CACHE: dict = {}   # {"token": str, "expires_at": datetime}

# XML-Namespaces EPO OPS 3.2
_NS_OPS = "http://ops.epo.org/3.2"
_NS_EPO = "http://www.epo.org/exchange"

# Kind-Codes gültig erteilter Patente (B1/B2 = EP erteilt, C = korrigiert, W = PCT granted)
_GRANTED_KINDS: frozenset[str] = frozenset({"B1", "B2", "B3", "C", "C1", "C2"})

# PATENT_SCORING_SECTORS + TRENDS_RELEVANT_SECTORS → SSOT: src/taxonomy.py
# Importierbar von score_calculator.py + assessments.py:
#   from src.taxonomy import PATENT_SCORING_SECTORS
from src.taxonomy import (
    PATENT_SCORING_SECTORS,
    TRENDS_RELEVANT_SECTORS,
    is_patent_relevant as _is_patent_relevant_fn,
    is_trends_relevant as _is_trends_relevant_fn,
)

_NER_SYSTEM = """\
Du bist ein präziser Signal-Klassifikator für M&A- und Investment-Screening.
Analysiere Unternehmens-News und klassifiziere das Signal.

Antworte NUR mit einem JSON-Objekt, keine Erklärung, keine Markdown-Backticks:
{
  "direction": "positive" | "negative" | "neutral",
  "signal_category": "<kategorie>",
  "confidence": "high" | "medium" | "low"
}

Kategorien:
  Positiv (direction=positive):
    funding           – neue Finanzierungsrunde, Kapitalerhöhung
    partnership       – Partnerschaft, JV, Kooperation, strategisches Abkommen
    ipo_progress      – S-1-Filing, IPO-Ankündigung, Börsengang-Pläne
    market_growth     – Marktwachstum, Expansion, neue Märkte
    patent            – neues Patent, IP-Schutz, Innovation
    investor_entry    – neuer namhafter Investor steigt ein

  Negativ (direction=negative):
    regulatory        – Regulierung, Klage, Bußgeld, Ermittlung, Sanktion
    negative_earnings – Verlust, Umsatzrückgang, Gewinnwarnung, Insolvenz, Layoffs
    supply_chain      – Lieferkettenprobleme, Engpässe, Abhängigkeiten
    insider_selling   – Insider-Verkäufe, Direktorenverkäufe
    customer_concentration – hohe Kundenabhängigkeit, Verlust eines Hauptkunden

  Neutral (direction=neutral):
    filing            – reguläres Filing (10-K, 10-Q, 8-K) ohne klares Signal
    ownership_entry   – Ownership-Änderung ohne klare Richtung
    general_news      – allgemeine News ohne Potenzial- oder Risikosignal

Regeln:
- Bei Ambiguität: konservativ klassifizieren (neutral > positiv > negativ)
- M&A-Events: positiv wenn Übernahme durch starken Käufer, negativ wenn Verkauf unter Druck
- Earnings: positiv wenn Beat, negativ wenn Miss/Verlust, neutral wenn gemischt
- confidence=low wenn wenig Information im Text
"""


async def _claude_ner_pass(
    events: list[SignalEvent],
    client: httpx.AsyncClient,
) -> list[SignalEvent]:
    """
    SE-09: Claude-NER zweiter Pass.
    Klassifiziert direction + signal_category für News-basierte Events.
    EDGAR-strukturierte Events (filing, ownership_entry) werden übersprungen.
    Batch-Verarbeitung: max 10 Events pro API-Call.
    Fallback auf Keyword-Matching bei API-Fehler.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("SE-09: ANTHROPIC_API_KEY fehlt — Keyword-Fallback für alle Events")
        for ev in events:
            if ev.direction == "neutral" and ev.signal_category == "general_news":
                ev.direction, ev.signal_category = _keyword_direction(
                    f"{ev.raw_title or ''} {ev.summary}"
                )
        return events

    # Events die Claude-Pass brauchen: News + ambige Events (m_and_a, earnings)
    needs_ner = [
        ev for ev in events
        if ev.event_type in ("news", "m_and_a_event", "earnings")
        or (ev.direction == "neutral" and ev.signal_category == "general_news")
    ]

    if not needs_ner:
        return events

    # Batch-Verarbeitung in Chunks von 10
    for i in range(0, len(needs_ner), 10):
        batch = needs_ner[i:i+10]
        items_text = "\n".join(
            f"{j+1}. Titel: {ev.raw_title or '—'}\n   Summary: {ev.summary[:200]}"
            for j, ev in enumerate(batch)
        )
        prompt = (
            f"Klassifiziere diese {len(batch)} Unternehmens-Signale.\n"
            f"Antworte mit einem JSON-Array der Länge {len(batch)}:\n"
            f"[{{\"direction\":...,\"signal_category\":...,\"confidence\":...}}, ...]\n\n"
            f"{items_text}"
        )

        try:
            resp = await client.post(
                _CLAUDE_API_URL,
                headers={
                    "x-api-key":         api_key,
                    "anthropic-version":  "2023-06-01",
                    "content-type":       "application/json",
                },
                json={
                    "model":      _CLAUDE_MODEL,
                    "max_tokens": 512,
                    "system":     _NER_SYSTEM,
                    "messages":   [{"role": "user", "content": prompt}],
                },
                timeout=20.0,
            )

            if resp.status_code != 200:
                logger.warning("SE-09 Claude API %s — Keyword-Fallback", resp.status_code)
                _apply_keyword_fallback(batch)
                continue

            raw = resp.json()["content"][0]["text"].strip()
            # JSON-Fences entfernen falls Claude sie doch schreibt
            raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
            results = json.loads(raw)

            if not isinstance(results, list) or len(results) != len(batch):
                logger.warning("SE-09 Claude Antwort hat falsche Länge — Keyword-Fallback")
                _apply_keyword_fallback(batch)
                continue

            for ev, res in zip(batch, results):
                direction = res.get("direction", "neutral")
                category  = res.get("signal_category", "general_news")
                confidence = res.get("confidence", "medium")
                # Validierung gegen erlaubte Werte
                if direction in ("positive", "negative", "neutral"):
                    ev.direction = direction
                if category in (
                    "funding", "partnership", "ipo_progress", "market_growth",
                    "patent", "investor_entry", "regulatory", "negative_earnings",
                    "supply_chain", "insider_selling", "customer_concentration",
                    "filing", "ownership_entry", "general_news"
                ):
                    ev.signal_category = category
                # relevance_score aus Confidence
                ev.relevance_score = {"high": 0.9, "medium": 0.6, "low": 0.3}.get(confidence, 0.6)
                # B-05: Funding-Betrag aus raw_title + summary extrahieren
                if ev.event_type == "funding_round" and not ev.funding_amount_usd_mn:
                    ev.funding_amount_usd_mn = _extract_funding_amount(
                        f"{ev.raw_title or ''} {ev.summary}"
                    )

        except (json.JSONDecodeError, KeyError, Exception) as e:
            logger.warning("SE-09 Claude NER Fehler: %s — Keyword-Fallback", e)
            _apply_keyword_fallback(batch)

        await asyncio.sleep(0.2)   # Rate-Limit

    return events


def _apply_keyword_fallback(events: list[SignalEvent]) -> None:
    """Wendet Keyword-Fallback auf eine Liste von Events an (in-place)."""
    for ev in events:
        ev.direction, ev.signal_category = _keyword_direction(
            f"{ev.raw_title or ''} {ev.summary}"
        )


# ── SE-14: EPO OPS Patent Parser ─────────────────────────────────────────────

async def _epo_get_token(client: httpx.AsyncClient) -> str | None:
    """
    OAuth2 client_credentials flow für EPO OPS API.
    Token wird 19 Minuten gecacht (EPO-Standard: 20 Min TTL).

    Umgebungsvariablen:
      EPO_OPS_KEY    — Consumer Key (EPO Developer Portal, kostenlos)
      EPO_OPS_SECRET — Consumer Secret
    """
    now = datetime.now(timezone.utc)
    if _EPO_TOKEN_CACHE.get("token") and _EPO_TOKEN_CACHE.get("expires_at", now) > now:
        return _EPO_TOKEN_CACHE["token"]

    consumer_key    = os.environ.get("EPO_OPS_KEY")
    consumer_secret = os.environ.get("EPO_OPS_SECRET")
    if not consumer_key or not consumer_secret:
        logger.warning("SE-14: EPO_OPS_KEY / EPO_OPS_SECRET nicht gesetzt — Patent-Signal übersprungen")
        return None

    try:
        resp = await client.post(
            _EPO_AUTH_URL,
            data={"grant_type": "client_credentials"},
            auth=(consumer_key, consumer_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10.0,
        )
        if resp.status_code != 200:
            logger.warning("SE-14: EPO Auth HTTP %s", resp.status_code)
            return None
        data = resp.json()
        token = data.get("access_token")
        if not token:
            return None
        expires_in = int(data.get("expires_in", 1200))
        _EPO_TOKEN_CACHE["token"] = token
        _EPO_TOKEN_CACHE["expires_at"] = now + timedelta(seconds=expires_in - 60)
        logger.info("SE-14: EPO OPS Token erneuert (gültig %ds)", expires_in)
        return token
    except Exception as e:
        logger.warning("SE-14: EPO Auth fehlgeschlagen: %s", e)
        return None


def _epo_kind_to_status(kind: str) -> str:
    """Kind-Code → Patentstatus ('granted' | 'filed' | 'withdrawn')."""
    if kind in _GRANTED_KINDS:
        return "granted"
    if kind.upper() in ("D1", "D2"):
        return "withdrawn"
    return "filed"   # A1/A2/A3 = publizierte Anmeldung; W = PCT


def _epo_parse_date(date_str: str | None) -> date | None:
    """Parst EPO-Datumsformat YYYYMMDD → date. Gibt None bei ungültigem Input."""
    if not date_str or len(date_str) < 8:
        return None
    try:
        return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
    except (ValueError, TypeError):
        return None


def _epo_extract_ipc(ex_doc: "ET.Element") -> list[str]:
    """
    Extrahiert IPC-Codes aus exchange-document.
    Format im XML: 'Y02E 10/50 20130101' → normalisiert zu 'Y02E10/50'.
    """
    codes: list[str] = []
    for cl in ex_doc.findall(
        f".//{{{_NS_EPO}}}classifications-ipcr/{{{_NS_EPO}}}classification-ipcr"
    ):
        text_el = cl.find(f"{{{_NS_EPO}}}text")
        if text_el is not None and text_el.text:
            parts = text_el.text.strip().split()
            if len(parts) >= 2:
                codes.append(f"{parts[0]}{parts[1]}")
    return list(dict.fromkeys(codes))   # dedupliziert, Reihenfolge erhalten


async def parse_epo(
    company_id: str,
    company_name: str,
    client: httpx.AsyncClient,
    lookback_days: int = 90,
) -> tuple[list[SignalEvent], list[dict]]:
    """
    SE-14: EPO OPS Patent-Parser.

    Sucht Patente nach Anmelder-Name via CQL (alle Regionen: EP, WO, US, DE, …).
    Gibt zurück:
      signals        — SignalEvents für Patente mit signal_date ≤ lookback_days
      patent_records — Alle gefundenen Patente für company_patents-Tabelle (Bulk-Upsert)

    Datensammlung universal (Chemie, Pharma, Deep Tech, alle Sektoren).
    Scoring-Einfluss auf tech_readiness nur für PATENT_SCORING_SECTORS —
    gesteuert in score_calculator.py via `from src.taxonomy import PATENT_SCORING_SECTORS`.

    Rate-Limit Free Tier: 4 req/s, 10k req/Woche.
    Bei 43 Companies à 1 Call ≈ 43 req/Tag — unkritisch.
    """
    signals:        list[SignalEvent] = []
    patent_records: list[dict]        = []

    token = await _epo_get_token(client)
    if not token:
        return signals, patent_records

    cutoff = date.today() - timedelta(days=lookback_days)
    norm   = _normalize_name(company_name)
    cql    = f'pa all "{norm}"'   # CQL: Anmelder (pa) enthält normalisierten Namen

    try:
        resp = await client.get(
            _EPO_SEARCH_URL,
            params={"q": cql, "Range": "1-25"},
            headers={
                "Authorization": f"Bearer {token}",
                "Accept":        "application/xml",
                "X-OPS-Range":   "1-25",
            },
            timeout=15.0,
        )
    except Exception as e:
        logger.warning("SE-14: EPO Netzwerkfehler für %s: %s", company_name, e)
        return signals, patent_records

    if resp.status_code == 404:
        logger.info("SE-14: Keine Patente für %s (CQL: %s)", company_name, cql)
        return signals, patent_records

    if resp.status_code == 403:
        # Rate-Limit oder Token abgelaufen → Cache leeren, nächster Company-Call holt neuen Token
        logger.warning("SE-14: EPO OPS 403 für %s — Token-Cache geleert", company_name)
        _EPO_TOKEN_CACHE.clear()
        return signals, patent_records

    if resp.status_code != 200:
        logger.warning("SE-14: EPO OPS HTTP %s für %s", resp.status_code, company_name)
        return signals, patent_records

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        logger.warning("SE-14: XML-Parse-Fehler für %s: %s", company_name, e)
        return signals, patent_records

    for ex_doc in root.findall(f".//{{{_NS_EPO}}}exchange-document"):
        country = ex_doc.get("country", "")
        doc_num = ex_doc.get("doc-number", "")
        kind    = ex_doc.get("kind", "")
        pat_num = f"{country}{doc_num}" if country and doc_num else doc_num
        if not pat_num:
            continue

        # Publikationsdatum (= Erteilungsdatum bei granted)
        pub_date_str: str | None = None
        for doc_id in ex_doc.findall(
            f".//{{{_NS_EPO}}}publication-reference/{{{_NS_EPO}}}document-id"
        ):
            d = doc_id.find(f"{{{_NS_EPO}}}date")
            if d is not None and d.text:
                pub_date_str = d.text.strip()
                break

        # Anmeldedatum
        filing_date_str: str | None = None
        for doc_id in ex_doc.findall(
            f".//{{{_NS_EPO}}}application-reference/{{{_NS_EPO}}}document-id"
        ):
            d = doc_id.find(f"{{{_NS_EPO}}}date")
            if d is not None and d.text:
                filing_date_str = d.text.strip()
                break

        filing_date = _epo_parse_date(filing_date_str) or _epo_parse_date(pub_date_str)
        status      = _epo_kind_to_status(kind)
        grant_date  = _epo_parse_date(pub_date_str) if status == "granted" else None

        # Titel — Englisch bevorzugt, Fallback auf ersten verfügbaren
        title = ""
        for t in ex_doc.findall(f".//{{{_NS_EPO}}}invention-title"):
            if t.get("lang", "") == "en" and t.text:
                title = t.text.strip()
                break
        if not title:
            t_el = ex_doc.find(f".//{{{_NS_EPO}}}invention-title")
            if t_el is not None and t_el.text:
                title = t_el.text.strip()

        ipc_codes    = _epo_extract_ipc(ex_doc)
        geo_coverage = [country] if country else []

        patent_records.append({
            "company_id":     company_id,
            "patent_number":  pat_num,
            "title":          title or None,
            "filing_date":    filing_date.isoformat() if filing_date else None,
            "grant_date":     grant_date.isoformat() if grant_date else None,
            "status":         status,
            "ipc_codes":      ipc_codes or None,
            "citation_count": None,   # Phase 2: separater Citation-Call je Patent
            "geo_coverage":   geo_coverage or None,
            "source":         "epo_ops",
        })

        # SignalEvent nur für Patente innerhalb des Lookback-Fensters
        signal_date = grant_date or filing_date
        if not signal_date or signal_date < cutoff:
            continue

        ipc_display = ", ".join(ipc_codes[:2]) if ipc_codes else "IPC n/v"
        if status == "granted":
            summary   = (
                f"{company_name} — Patent erteilt: \"{title[:100]}\" "
                f"({pat_num} · {ipc_display} · erteilt {grant_date})."
            )
            severity  = "medium"
            relevance = 0.85
            raw_title = f"Patent erteilt: {title[:80]}"
        else:
            summary   = (
                f"{company_name} — Neue Patentanmeldung: \"{title[:100]}\" "
                f"({pat_num} · {ipc_display} · angemeldet {filing_date})."
            )
            severity  = "low"
            relevance = 0.50
            raw_title = f"Patentanmeldung: {title[:80]}"

        signals.append(SignalEvent(
            company_id=company_id,
            company_name=company_name,
            event_type="news",
            event_date=signal_date,
            summary=summary,
            source="epo_ops",
            source_url=(
                f"https://worldwide.espacenet.com/publicationDetails/biblio"
                f"?CC={country}&NR={doc_num}"
                if country and doc_num else None
            ),
            severity=severity,
            raw_title=raw_title,
            direction="positive",
            signal_category="patent",
            source_domain="epo.org",
            relevance_score=relevance,
        ))

    logger.info(
        "SE-14: %s → %d Patente (%d neu im Lookback, %d granted gesamt)",
        company_name,
        len(patent_records),
        len(signals),
        sum(1 for r in patent_records if r["status"] == "granted"),
    )
    return signals, patent_records


# ── SE-15: Google Trends Signal ───────────────────────────────────────────────

_TRENDS_URL = "https://trends.google.com/trends/api/explore"
_TRENDS_WIDGET_URL = "https://trends.google.com/trends/api/widgetdata/multiline"


def _is_trends_relevant(category: str, industry: str) -> bool:
    """Prüft ob Google Trends für diese Company ein sinnvolles Signal liefert."""
    return _is_trends_relevant_fn(category, industry)


async def _fetch_pytrends(
    company_name: str,
    client: httpx.AsyncClient,
    lookback_days: int = 90,
) -> tuple[str, float]:
    """
    SE-15: Google Trends Daten via pytrends (lazy import).
    Gibt (direction, change_pct) zurück: 'rising' | 'falling' | 'stable', Δ%.

    Fallback: ('stable', 0.0) bei Import-Fehler oder Rate-Limit.
    pytrends braucht keine eigene httpx-Session — nutzt requests intern.
    """
    try:
        from pytrends.request import TrendReq   # optionale Abhängigkeit
    except ImportError:
        logger.debug("SE-15: pytrends nicht installiert — Signal übersprungen")
        return "stable", 0.0

    try:
        pt = TrendReq(hl="en-US", tz=0, timeout=(4, 8), retries=2, backoff_factor=0.5)
        pt.build_payload([company_name], timeframe="today 3-m", geo="")
        df = pt.interest_over_time()

        if df is None or df.empty or company_name not in df.columns:
            return "stable", 0.0

        series = df[company_name].astype(float)
        if len(series) < 8:   # zu wenig Datenpunkte
            return "stable", 0.0

        # Letztes Drittel vs. erstes Drittel → Trendrichtung
        n       = len(series)
        third   = max(n // 3, 4)
        recent  = series.iloc[-third:].mean()
        earlier = series.iloc[:third].mean()

        if earlier < 1:   # kein Suchvolumen → kein Signal
            return "stable", 0.0

        change = (recent - earlier) / earlier
        if change > 0.25:
            return "rising", round(change * 100, 1)
        elif change < -0.25:
            return "falling", round(change * 100, 1)
        return "stable", round(change * 100, 1)

    except Exception as e:
        # 429 Rate-Limit oder IP-Block auf Cloud-Servern — graceful degradation
        logger.debug("SE-15: pytrends fehlgeschlagen für '%s': %s", company_name, e)
        return "stable", 0.0


async def parse_google_trends(
    company_id: str,
    company_name: str,
    category: str,
    industry: str,
    client: httpx.AsyncClient,
) -> list[SignalEvent]:
    """
    SE-15: Google Trends Signal für SaaS / AI / Software Companies.

    Suchvolumen-Trend über 90 Tage → direction (rising/falling/stable).
    Nur aktiv für TRENDS_RELEVANT_SECTORS — Deep Tech / Industrial übersprungen.

    Gibt leere Liste zurück wenn:
      - Sektor nicht relevant
      - pytrends nicht installiert
      - Rate-Limit / IP-Block (Render-Cloud)
      - Zu wenig Suchvolumen

    Kein Crash bei Fehler — immer graceful degradation.
    """
    if not _is_trends_relevant(category, industry):
        return []

    direction, change_pct = await _fetch_pytrends(company_name, client)

    if direction == "stable":
        return []   # stable ist kein actionables Signal

    today = date.today()

    if direction == "rising":
        summary = (
            f"{company_name} — Google Trends: Suchvolumen +{change_pct:.0f}% "
            f"in den letzten 90 Tagen. Wachsendes Marktinteresse."
        )
        signal_direction: Direction = "positive"
        severity: Severity          = "low"
        relevance                   = 0.45
        raw_title                   = f"Google Trends: steigend (+{change_pct:.0f}%)"
    else:
        summary = (
            f"{company_name} — Google Trends: Suchvolumen {change_pct:.0f}% "
            f"in den letzten 90 Tagen. Rückläufiges Marktinteresse."
        )
        signal_direction = "negative"
        severity         = "low"
        relevance        = 0.40
        raw_title        = f"Google Trends: fallend ({change_pct:.0f}%)"

    logger.info(
        "SE-15: %s → Trend %s (%.0f%%) — Kategorie: %s",
        company_name, direction, change_pct, category,
    )

    return [SignalEvent(
        company_id=company_id,
        company_name=company_name,
        event_type="news",
        event_date=today,
        summary=summary,
        source="google_trends",
        source_url=f"https://trends.google.com/trends/explore?q={company_name.replace(' ', '+')}",
        severity=severity,
        raw_title=raw_title,
        direction=signal_direction,
        signal_category="market_growth" if direction == "rising" else "general_news",
        source_domain="trends.google.com",
        relevance_score=relevance,
    )]

_EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

# Form-Typen → Event-Mapping
_EDGAR_FORM_MAP: dict[str, EventType] = {
    "S-1":    "ipo_status_change",
    "S-11":   "ipo_status_change",
    "10-K":   "earnings",
    "10-Q":   "earnings",
    "8-K":    "m_and_a_event",    # 8-K kann viel sein — NER klärt ob M&A
    "D":      "funding_round",    # SEC Form D = private Placement / Funding
    "D/A":    "funding_round",    # Amendment zu Form D
    "SC 13D": "ownership_change",
    "SC 13G": "ownership_change",
}

# 8-K Item-Nummern die echte M&A / wesentliche Events indizieren
_8K_MA_ITEMS = {"1.01", "2.01", "2.02", "5.02", "8.01"}


async def parse_edgar(
    company_id: str,
    company_name: str,
    ticker: str | None,
    client: httpx.AsyncClient,
    lookback_days: int = 30,
) -> list[SignalEvent]:
    """
    SE-10: Robuster SEC EDGAR Parser.
    - Saubere URL-Konstruktion via httpx params (kein manueller String-Bau)
    - Form D (funding_round) für private Companies
    - 8-K: nur Items 1.01/2.01/2.02 (echte M&A/Events), nicht jedes Filing
    - Korrekter filing_url via accession number
    - source_domain gesetzt für Deduplizierung
    """
    events: list[SignalEvent] = []
    start_date = (date.today() - timedelta(days=lookback_days)).isoformat()

    # Ticker-Prefix bereinigen (kann "LNZA · Nasdaq" sein)
    clean_ticker = ticker.split("·")[0].strip() if ticker else None

    # Query: Ticker hat Prio (EDGAR kennt Ticker), sonst Company-Name
    query = f'"{clean_ticker}"' if clean_ticker else f'"{company_name}"'
    forms = "S-1,S-11,10-K,10-Q,8-K,D,D/A,SC 13D,SC 13G"

    try:
        resp = await client.get(
            _EDGAR_SEARCH_URL,
            params={
                "q":         query,
                "dateRange": "custom",
                "startdt":   start_date,
                "forms":     forms,
                "_source":   "period_of_report,file_date,display_names,entity_name,file_num,form_type,items",
            },
            timeout=12.0,
        )
        if resp.status_code != 200:
            logger.warning("EDGAR HTTP %s for %s (query=%s)", resp.status_code, company_name, query)
            # Fallback: Namens-Suche wenn Ticker-Suche scheitert
            if clean_ticker and resp.status_code in (400, 404):
                return await _edgar_name_fallback(company_id, company_name, client, start_date, forms)
            return events

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])

        if not hits and clean_ticker:
            # Kein Ergebnis mit Ticker → Name-Fallback
            logger.info("EDGAR: kein Treffer für Ticker %s — versuche Name-Suche", clean_ticker)
            return await _edgar_name_fallback(company_id, company_name, client, start_date, forms)

        for hit in hits[:8]:   # max 8 Filings pro Company
            src       = hit.get("_source", {})
            # form_type kann in '_source.form_type' oder '_source.forms[0]' stehen
            form_type = src.get("form_type") or (src.get("forms") or [None])[0]
            file_date = src.get("file_date") or src.get("period_of_report")
            entity    = src.get("entity_name", company_name)
            accession = hit.get("_id", "")

            if not form_type or not file_date:
                continue

            # 8-K: nur wenn echte M&A/Event-Items → kein Rauschen
            items = src.get("items", [])
            if form_type == "8-K" and items:
                if not any(item in _8K_MA_ITEMS for item in items):
                    continue   # 8-K ohne relevante Items überspringen

            event_type = _EDGAR_FORM_MAP.get(form_type, "news")
            try:
                ev_date = date.fromisoformat(file_date[:10])
            except ValueError:
                ev_date = date.today()

            # Korrekter Filing-Link via accession number
            acc_clean   = accession.replace("-", "")
            filing_url  = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{src.get('file_num', '').replace('-', '')}/{acc_clean}/{accession}-index.htm"
                if accession else None
            )
            # Fallback: EDGAR-Suche nach file_num
            if not filing_url and src.get("file_num"):
                filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum={src['file_num']}"

            summary = _edgar_summary(form_type, entity, ev_date)

            # B-05: Form D → Funding-Betrag aus total_offering falls vorhanden
            funding_amount = None
            if form_type in ("D", "D/A"):
                total_offering = src.get("total_offering_amount")
                if total_offering:
                    try:
                        funding_amount = round(float(total_offering) / 1_000_000, 2)
                    except (ValueError, TypeError):
                        pass

            events.append(SignalEvent(
                company_id=company_id,
                company_name=company_name,
                event_type=event_type,
                event_date=ev_date,
                summary=summary,
                source="edgar",
                source_url=filing_url,
                severity=_severity_for_event(event_type),
                raw_title=f"{form_type} — {entity}",
                source_domain="sec.gov",
                funding_amount_usd_mn=funding_amount,
            ))
            logger.info("EDGAR signal: %s %s %s → %s", company_name, form_type, ev_date, event_type)

    except Exception as e:
        logger.warning("EDGAR parse failed for %s: %s", company_name, e)

    return events


async def _edgar_name_fallback(
    company_id: str,
    company_name: str,
    client: httpx.AsyncClient,
    start_date: str,
    forms: str,
) -> list[SignalEvent]:
    """
    SE-10: Fallback-Suche via Company-Name wenn Ticker-Suche kein Ergebnis liefert.
    Wird nur aufgerufen wenn Ticker-Suche fehlschlägt.
    """
    events: list[SignalEvent] = []
    # Normalisierter Name ohne Rechtssuffixe
    normalized = _normalize_name(company_name)
    try:
        resp = await client.get(
            _EDGAR_SEARCH_URL,
            params={
                "q":         f'"{normalized}"',
                "dateRange": "custom",
                "startdt":   start_date,
                "forms":     forms,
                "_source":   "period_of_report,file_date,entity_name,file_num,form_type,items",
            },
            timeout=10.0,
        )
        if resp.status_code != 200:
            return events
        hits = resp.json().get("hits", {}).get("hits", [])
        for hit in hits[:5]:
            src       = hit.get("_source", {})
            form_type = src.get("form_type") or (src.get("forms") or [None])[0]
            file_date = src.get("file_date") or src.get("period_of_report")
            entity    = src.get("entity_name", company_name)
            if not form_type or not file_date:
                continue
            items = src.get("items", [])
            if form_type == "8-K" and items and not any(i in _8K_MA_ITEMS for i in items):
                continue
            event_type = _EDGAR_FORM_MAP.get(form_type, "news")
            try:
                ev_date = date.fromisoformat(file_date[:10])
            except ValueError:
                ev_date = date.today()
            events.append(SignalEvent(
                company_id=company_id,
                company_name=company_name,
                event_type=event_type,
                event_date=ev_date,
                summary=_edgar_summary(form_type, entity, ev_date),
                source="edgar",
                source_url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum={src.get('file_num', '')}",
                severity=_severity_for_event(event_type),
                raw_title=f"{form_type} — {entity}",
                source_domain="sec.gov",
            ))
    except Exception as e:
        logger.warning("EDGAR name_fallback failed for %s: %s", company_name, e)
    return events


def _edgar_summary(form_type: str, entity: str, ev_date: date) -> str:
    summaries = {
        "S-1":    f"{entity} hat einen S-1-Antrag bei der SEC eingereicht — IPO-Prozess gestartet ({ev_date}).",
        "S-11":   f"{entity} hat einen S-11-Antrag bei der SEC eingereicht — IPO-Prozess gestartet ({ev_date}).",
        "10-K":   f"{entity} hat den Jahresbericht (10-K) bei der SEC eingereicht ({ev_date}).",
        "10-Q":   f"{entity} hat den Quartalsbericht (10-Q) bei der SEC eingereicht ({ev_date}).",
        "8-K":    f"{entity} hat ein 8-K-Formular eingereicht — wesentliches Unternehmens-Event oder M&A ({ev_date}).",
        "D":      f"{entity} hat ein Form D bei der SEC eingereicht — private Kapitalaufnahme gemeldet ({ev_date}).",
        "D/A":    f"{entity} hat eine Form-D-Änderung (D/A) bei der SEC eingereicht — Kapitalrunde aktualisiert ({ev_date}).",
        "SC 13D": f"Neuer Großaktionär (>5%) bei {entity} gemeldet via SC 13D ({ev_date}).",
        "SC 13G": f"Institutioneller Investor hat Position bei {entity} via SC 13G gemeldet ({ev_date}).",
    }
    return summaries.get(form_type, f"{entity}: SEC-Filing {form_type} am {ev_date}.")


# ── Google News RSS Parser ────────────────────────────────────────────────────

_GNEWS_BASE = "https://news.google.com/rss/search"

async def parse_google_news(
    company_id: str,
    company_name: str,
    client: httpx.AsyncClient,
    lookback_days: int = 14,
) -> list[SignalEvent]:
    """
    Parsed Google News RSS für eine Company.
    Kein API-Key nötig. Rate-Limit: ~100 req/h — bei 43 Companies unkritisch.
    """
    events: list[SignalEvent] = []
    cutoff = date.today() - timedelta(days=lookback_days)

    # Zwei Queries: Deutsch + Englisch
    queries = [
        f"{company_name} Finanzierung OR Übernahme OR Börsengang OR IPO",
        f"{company_name} funding OR acquisition OR IPO OR merger OR earnings",
    ]

    for query in queries:
        try:
            resp = await client.get(
                _GNEWS_BASE,
                params={"q": query, "hl": "de", "gl": "DE", "ceid": "DE:de"},
                timeout=8.0,
            )
            if resp.status_code != 200:
                continue

            root = ET.fromstring(resp.text)
            items = root.findall(".//item")

            for item in items[:8]:   # max 8 Artikel pro Query
                title       = (item.findtext("title") or "").strip()
                description = (item.findtext("description") or "").strip()
                link        = (item.findtext("link") or "").strip()
                pub_date    = item.findtext("pubDate")

                ev_date = _parse_rss_date(pub_date)
                if ev_date < cutoff:
                    continue

                # Name-Match prüfen
                if not _name_in_text(company_name, title + " " + description):
                    continue

                event_type = _classify_event(title, description)
                severity   = _severity_for_event(event_type)

                # Kurze Summary aus Titel
                summary = _news_summary(event_type, company_name, title, ev_date)

                # B-05: Funding-Betrag direkt aus Titel/Description extrahieren
                funding_amount = None
                if event_type == "funding_round":
                    funding_amount = _extract_funding_amount(f"{title} {description[:300]}")

                events.append(SignalEvent(
                    company_id=company_id,
                    company_name=company_name,
                    event_type=event_type,
                    event_date=ev_date,
                    summary=summary,
                    source="google_news",
                    source_url=link or None,
                    severity=severity,
                    raw_title=title,
                    funding_amount_usd_mn=funding_amount,
                ))

        except ET.ParseError as e:
            logger.warning("Google News XML parse error for %s: %s", company_name, e)
        except Exception as e:
            logger.warning("Google News failed for %s: %s", company_name, e)

    # Deduplizieren: gleicher event_type + event_date → nur erster behalten
    seen: set[tuple] = set()
    deduped: list[SignalEvent] = []
    for ev in events:
        key = (ev.event_type, ev.event_date)
        if key not in seen:
            seen.add(key)
            deduped.append(ev)

    return deduped


def _news_summary(event_type: EventType, company: str, title: str, ev_date: date) -> str:
    prefix = {
        "ipo_status_change": f"{company} — IPO-relevante News",
        "funding_round":     f"{company} — Finanzierungsrunde",
        "m_and_a_event":     f"{company} — M&A-Event",
        "earnings":          f"{company} — Earnings/Finanzergebnis",
        "ownership_change":  f"{company} — Ownership-Änderung",
        "news":              f"{company} — News",
    }.get(event_type, company)
    # Titel kürzen auf 120 Zeichen
    short_title = title[:120] + "…" if len(title) > 120 else title
    return f"{prefix} ({ev_date}): {short_title}"


# ── TechCrunch RSS Parser ─────────────────────────────────────────────────────

_TC_FEED = "https://techcrunch.com/feed/"

async def parse_techcrunch(
    company_id: str,
    company_name: str,
    client: httpx.AsyncClient,
    lookback_days: int = 14,
) -> list[SignalEvent]:
    """
    Parsed TechCrunch RSS-Feed für Funding-Events.
    TechCrunch hat keinen company-spezifischen RSS — wir filtern aus dem globalen Feed.
    Sinnvoll für US/EU Startups die TechCrunch-Coverage haben.
    """
    events: list[SignalEvent] = []
    cutoff = date.today() - timedelta(days=lookback_days)

    try:
        resp = await client.get(_TC_FEED, timeout=8.0)
        if resp.status_code != 200:
            return events

        root = ET.fromstring(resp.text)
        for item in root.findall(".//item"):
            title       = (item.findtext("title") or "").strip()
            description = (item.findtext("description") or "").strip()
            link        = (item.findtext("link") or "").strip()
            pub_date    = item.findtext("pubDate")

            ev_date = _parse_rss_date(pub_date)
            if ev_date < cutoff:
                continue

            if not _name_in_text(company_name, title + " " + description):
                continue

            event_type = _classify_event(title, description)
            if event_type not in ("funding_round", "m_and_a_event", "ipo_status_change"):
                continue
            funding_amount = None
            if event_type == "funding_round":
                funding_amount = _extract_funding_amount(f"{title} {description[:300]}")
            events.append(SignalEvent(
                company_id=company_id,
                company_name=company_name,
                event_type=event_type,
                event_date=ev_date,
                summary=_news_summary(event_type, company_name, title, ev_date),
                source="techcrunch",
                source_url=link or None,
                severity=_severity_for_event(event_type),
                raw_title=title,
                source_domain=_extract_domain(link) or "techcrunch.com",
                funding_amount_usd_mn=funding_amount,
            ))

    except Exception as e:
        logger.warning("TechCrunch parse failed for %s: %s", company_name, e)

    return events


# ── Ownership-Watcher (interner DB-Diff) ─────────────────────────────────────

async def watch_ownership_changes(
    company_id: str,
    company_name: str,
    current_entries: list[dict],
    previous_signal_date: date | None,
) -> list[SignalEvent]:
    """
    Vergleicht aktuelle Ownership-Einträge mit letztem bekannten Stand.
    Trigger: neuer Eintrag mit as_of_date > letztem Signal-Datum.
    """
    events: list[SignalEvent] = []
    if not current_entries:
        return events

    for entry in current_entries:
        as_of = entry.get("as_of_date")
        if not as_of:
            continue
        try:
            entry_date = date.fromisoformat(as_of[:10])
        except ValueError:
            continue

        # Nur neue Einträge seit letztem Signal
        if previous_signal_date and entry_date <= previous_signal_date:
            continue

        name      = entry.get("name", "Unbekannt")
        role      = entry.get("role", "Investor")
        share_pct = entry.get("share_pct")
        share_str = f" ({share_pct:.1f}%)" if share_pct else ""

        events.append(SignalEvent(
            company_id=company_id,
            company_name=company_name,
            event_type="ownership_change",
            event_date=entry_date,
            summary=f"{company_name} — Ownership-Änderung: {name}{share_str} als {role} eingetragen ({as_of[:10]}).",
            source="internal",
            source_url=entry.get("source"),
            severity="medium",
            raw_title=f"Neuer Investor: {name}",
            direction="neutral",
            signal_category="ownership_entry",
        ))

    return events


# ── SE-13: Abweichungs-Signale (Risiko aus fehlendem DB-Zustand) ─────────────

def check_absence_signals(
    company_id: str,
    company_name: str,
    company: dict,
    ownership_entries: list[dict],
    signals_count: int,
    existing_absence_categories: set[str] | None = None,
) -> list[SignalEvent]:
    """
    SE-13: Erkennt Risiken aus dem was fehlt — kein Scraping, kein externer API-Call.
    Ausgewertet werden ausschließlich Felder die bereits in der DB stehen.

    Checks:
      1. Ownership undurchsichtig → kein einziger Eintrag in ownership_entries
      2. Kein Ticker in VC-getriebenem Markt (private + kein Funding) → fehlende Transparenz
      3. Keine Signals in letzten 90 Tagen → Signal-Stille als Risiko
      4. Kein Headcount → fehlende Transparenz bei privaten Companies
      5. Kein Revenue in patentlastigem / VC-dominiertem Sektor (Tech, Biotech)

    BUG-01: existing_absence_categories verhindert Doppel-Emission pro 30 Tage.
    Jede Absence-Kategorie wird max. 1× pro 30 Tage emittiert.
    Caller (run_signal_engine) befüllt aus DB-Query der letzten 30 Tage.
    """
    events: list[SignalEvent] = []
    today = date.today()
    industry = (company.get("industry") or "").lower()
    is_listed = _is_listed(company)
    region    = company.get("region", "")

    # BUG-01: Cooldown-Set — überspringe Kategorien die in letzten 30d bereits emittiert
    cooldown = existing_absence_categories or set()

    # 1. Ownership-Transparenz: keine Einträge
    if not ownership_entries and not is_listed and "ownership" not in cooldown:
        events.append(SignalEvent(
            company_id=company_id,
            company_name=company_name,
            event_type="ownership_change",
            event_date=today,
            summary=(
                f"{company_name} — Keine Ownership-Daten verfügbar. "
                f"Für private Companies ohne öffentliche Register-Einträge erhöht dies das Governance-Risiko."
            ),
            source="internal_absence",
            source_url=None,
            severity="medium",
            raw_title="Ownership-Transparenz: keine Daten",
            direction="negative",
            signal_category="regulatory",
        ))

    # 2. Signal-Stille: kein Signal in letzten 90 Tagen (nur wenn Company ≥ 90 Tage alt)
    last_signal_raw = company.get("last_signal_date")
    if last_signal_raw and "signal_stille" not in cooldown:
        try:
            last_signal_date = date.fromisoformat(last_signal_raw[:10])
            days_silent = (today - last_signal_date).days
            if days_silent > 90:
                events.append(SignalEvent(
                    company_id=company_id,
                    company_name=company_name,
                    event_type="news",
                    event_date=today,
                    summary=(
                        f"{company_name} — Keine öffentlichen Signale in den letzten {days_silent} Tagen. "
                        f"Signal-Stille kann auf Kommunikationsprobleme oder stagnierende Aktivität hinweisen."
                    ),
                    source="internal_absence",
                    source_url=None,
                    severity="low",
                    raw_title="Signal-Stille: keine öffentlichen Signale",   # statisch — kein {days_silent}
                    direction="negative",
                    signal_category="general_news",
                ))
        except ValueError:
            pass

    # 3. Kein Headcount bei privater Company (fehlende Transparenz)
    headcount = company.get("headcount")
    if not headcount and not is_listed and "headcount" not in cooldown:
        events.append(SignalEvent(
            company_id=company_id,
            company_name=company_name,
            event_type="news",
            event_date=today,
            summary=(
                f"{company_name} — Keine Headcount-Daten verfügbar. "
                f"Für private Companies ohne öffentliche Mitarbeiterzahl ist die Skalierungseinschätzung eingeschränkt."
            ),
            source="internal_absence",
            source_url=None,
            severity="low",
            raw_title="Headcount-Transparenz: keine Daten",
            direction="negative",
            signal_category="filing",
        ))

    # 4. Kein Revenue in tech-nahen Sektoren (höhere Erwartung an Transparenz)
    tech_sectors = ("software", "ai", "biotech", "medtech", "semiconductor", "cloud", "saas")
    revenue = company.get("revenue_usd_mn")
    if not revenue and any(s in industry for s in tech_sectors) and not is_listed and "revenue" not in cooldown:
        events.append(SignalEvent(
            company_id=company_id,
            company_name=company_name,
            event_type="earnings",
            event_date=today,
            summary=(
                f"{company_name} — Keine Umsatzdaten verfügbar. "
                f"Im {industry}-Sektor ist Umsatz-Transparenz typischerweise höher — "
                f"fehlende Daten erhöhen das Bewertungsrisiko."
            ),
            source="internal_absence",
            source_url=None,
            severity="low",
            raw_title=f"Revenue-Transparenz: keine Daten ({industry})",
            direction="negative",
            signal_category="negative_earnings",
        ))

    return events


# ── Haupt-Orchestrator ────────────────────────────────────────────────────────

async def run_signal_engine(
    companies: list[dict],
    ownership_map: dict[str, list[dict]],
    absence_cooldown_map: dict[str, set[str]] | None = None,
) -> tuple[list[SignalEvent], list[dict]]:
    """
    SE-01 — Haupt-Pipeline.
    Läuft täglich via Cron. Gibt zurück:
      all_events     — alle gesammelten SignalEvents (für signals-Tabelle)
      patent_records — alle EPO-Patentdaten (für company_patents-Tabelle, Bulk-Upsert)

    Aufgerufen von main.py _cron_signal_engine().

    Args:
        companies:            Liste von Company-Dicts — main.py's _cron_signal_engine()
                              nutzt fetch_companies(limit=500), denselben vollen Fetch wie
                              der Scoring-Cron. KEINE enge Projektion — ipo_status/
                              ipo_potential/is_listed sind verfügbar (LISTED-STATUS-REVIEW-01,
                              S75: diese Liste war vorher unvollständig dokumentiert als nur
                              id/name/ticker/exchange/last_signal_date/industry/region/
                              headcount/revenue_usd_mn — das sah wie eine bewusste enge
                              Projektion aus, war aber nur eine illustrative Teilliste).
        ownership_map:        {company_id: [ownership_entries]} — aktueller DB-Stand
        absence_cooldown_map: {company_id: {'ownership', 'headcount', ...}} —
                              BUG-01: Absence-Kategorien die in letzten 30d bereits emittiert
                              wurden. Verhindert täglich neue negative Absence-Signale.
                              Caller (main.py) befüllt via fetch_recent_absence_categories().

    main.py muss Rückgabe entpacken:
        all_events, patent_records = await run_signal_engine(companies, ownership_map, ...)
        # → signals-Tabelle: upsert_signals(all_events)
        # → company_patents:  bulk_upsert_patents(patent_records)
        # → companies:        update_patent_aggregates(patent_records)
    """
    all_events:     list[SignalEvent] = []
    all_patents:    list[dict]         = []
    timeout = httpx.Timeout(12.0, connect=4.0)

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": "ArgoAnalytics/1.0 (research; contact@argo-analytics.io)"},
        follow_redirects=True,
    ) as client:
        # TechCrunch einmal laden (globaler Feed) — nicht per Company
        tc_feed_cached: list[dict] | None = None
        try:
            tc_resp = await client.get(_TC_FEED, timeout=8.0)
            if tc_resp.status_code == 200:
                tc_feed_cached = tc_resp.text
        except Exception:
            pass

        for company in companies:
            cid    = company.get("id", "")
            cname  = company.get("name", "")
            ticker = company.get("ticker")
            is_listed = _is_listed(company)

            last_signal_raw = company.get("last_signal_date")
            last_signal_date: date | None = None
            if last_signal_raw:
                try:
                    last_signal_date = date.fromisoformat(last_signal_raw[:10])
                except ValueError:
                    pass

            logger.info("Signal-Engine: processing %s (listed=%s)", cname, is_listed)
            company_events: list[SignalEvent] = []

            # 1. EDGAR — nur für US-listed oder US-Unternehmen sinnvoll
            region = company.get("region", "")
            if is_listed or region in ("US", ""):
                edgar_events = await parse_edgar(cid, cname, ticker, client)
                # EDGAR-Events: direction direkt aus event_type ableiten (strukturierte Quelle)
                for ev in edgar_events:
                    ev.direction, ev.signal_category = _event_type_direction(ev.event_type)
                company_events.extend(edgar_events)
                if edgar_events:
                    await asyncio.sleep(0.5)   # EDGAR Rate-Limit respektieren

            # 2. Google News
            gnews_events = await parse_google_news(cid, cname, client)
            company_events.extend(gnews_events)
            await asyncio.sleep(0.3)

            # 3. TechCrunch (aus gecachtem Feed filtern)
            if tc_feed_cached:
                tc_events = await _filter_techcrunch_cached(
                    cid, cname, tc_feed_cached
                )
                company_events.extend(tc_events)

            # 4. Ownership-Watcher
            entries = ownership_map.get(cid, [])
            ow_events = await watch_ownership_changes(cid, cname, entries, last_signal_date)
            company_events.extend(ow_events)

            # 5. SE-09: Claude-NER zweiter Pass (direction + signal_category für News-Events)
            if company_events:
                company_events = await _claude_ner_pass(company_events, client)

            # 6. SE-13: Abweichungs-Signale aus DB-Zustand
            absence_events = check_absence_signals(
                cid, cname, company,
                ownership_entries=entries,
                signals_count=len(company_events),
                existing_absence_categories=(absence_cooldown_map or {}).get(cid, set()),
            )
            company_events.extend(absence_events)

            # 7. SE-14: EPO OPS Patent-Signal (universell — alle Sektoren)
            epo_signals, epo_patents = await parse_epo(cid, cname, client)
            company_events.extend(epo_signals)
            all_patents.extend(epo_patents)
            if epo_patents:
                await asyncio.sleep(0.25)   # EPO Rate-Limit: 4 req/s

            # 8. SE-15: Google Trends (nur TRENDS_RELEVANT_SECTORS)
            trends_signals = await parse_google_trends(
                cid, cname,
                company.get("category") or "",
                company.get("industry") or "",
                client,
            )
            company_events.extend(trends_signals)
            if trends_signals:
                await asyncio.sleep(1.0)   # Google Trends Rate-Limit: konservativ

            # 9. SE-18: BaFin Directors' Dealings — nur listed DE Companies
            # Bedingung: is_listed + DE-Börse (via exchange oder ISIN-Prefix)
            _de_exchanges = {"xetra", "frankfurt", "fse", "hamburg", "berlin", "dusseldorf", "stuttgart", "munich"}
            _exchange     = (company.get("exchange") or "").lower()
            _isin         = (company.get("isin") or "").strip()
            _is_de_listed = is_listed and (_exchange in _de_exchanges or _isin.startswith("DE"))

            if _is_de_listed:
                from src.services.bafin_insider import parse_bafin_insider
                # Bevorzuge ISIN (präziser), Fallback: Company-Name als Issuer-Query
                insider_signals = await parse_bafin_insider(
                    company_id=cid,
                    company_name=cname,
                    isin=_isin,
                    issuer_name=cname if not _isin else None,
                    client=client,
                )
                company_events.extend(insider_signals)
                if insider_signals:
                    await asyncio.sleep(0.5)

            logger.info(
                "Signal-Engine: %s → %d events "
                "(edgar=%d news=%d tc=%d ownership=%d absence=%d patent=%d trends=%d insider=%d) "
                "pos=%d neg=%d neu=%d",
                cname, len(company_events),
                sum(1 for e in company_events if e.source == "edgar"),
                sum(1 for e in company_events if e.source == "google_news"),
                sum(1 for e in company_events if e.source == "techcrunch"),
                sum(1 for e in company_events if e.source == "internal"),
                sum(1 for e in company_events if e.source == "internal_absence"),
                sum(1 for e in company_events if e.source == "epo_ops"),
                sum(1 for e in company_events if e.source == "google_trends"),
                sum(1 for e in company_events if e.source == "bafin_insider"),
                sum(1 for e in company_events if e.direction == "positive"),
                sum(1 for e in company_events if e.direction == "negative"),
                sum(1 for e in company_events if e.direction == "neutral"),
            )

            # SE-16: ipo_status_change → sofort companies.ipo_status aktualisieren
            # Keine Verzögerung — Status-Update hat immer Prio vor Signal-Persist.
            ipo_events = [
                e for e in company_events
                if e.event_type == "ipo_status_change" and e.direction == "positive"
            ]
            if ipo_events and cid:
                # Neueste Event bestimmen
                latest_ipo = max(ipo_events, key=lambda e: e.event_date)
                current_status = (company.get("ipo_status") or "").lower()
                # current_status == "listed" ist der einzige Zustand, aus dem es
                # kein Upgrade mehr geben soll (kein listed→listed, kein Downgrade).
                already_listed   = current_status == "listed"
                already_advanced = current_status.startswith("pre_ipo")

                signal_kind = _classify_ipo_signal(latest_ipo.raw_title)

                # BUGFIX (beim SE16-INTENT-UPGRADE-01-Umbau gefunden, nicht gesucht):
                # Der alte Guard prüfte `current_status == "private"`. Seit
                # IPO-STATUS-ENUM-01 (S69) ist "private" aber kein gültiger Wert
                # mehr in companies.ipo_status — normalize_ipo_status_for_db()
                # mappt "private" auf None. current_status ist für jede nicht-
                # gelistete Company also "" (aus `or ""`), nie "private" — der
                # Completion-Upgrade-Pfad konnte seit S69 nie mehr greifen, ohne
                # dass das aufgefallen wäre (Symptom wäre "kein Upgrade passiert",
                # leicht mit "kein IPO-Ereignis" zu verwechseln). Guard jetzt auf
                # "nicht bereits listed" — das war die eigentliche Absicht laut
                # Kommentar ("kein listed→listed oder listed→downgrade").
                if signal_kind == "completion" and not already_listed:
                    try:
                        from src.integrations.supabase import upsert_company_enrichment
                        upsert_company_enrichment(cid, {"ipo_status": "listed"})
                        logger.info(
                            "SE-16: %s ipo_status %s → listed (source=%s, title=%s)",
                            cname, current_status or "—", latest_ipo.source, latest_ipo.raw_title,
                        )
                    except Exception as e:
                        logger.warning("SE-16: ipo_status update failed for %s: %s", cname, e)
                elif signal_kind == "intent" and not already_listed and not already_advanced:
                    # SE16-INTENT-UPGRADE-01: S-1/S-11-Intent auf eine Bestands-
                    # Company → pre_ipo_high statt weiterhin NULL/unklar. Gleiche
                    # Klassifikation wie beim Discovery-Neuanlegen (discovery_engine.py).
                    try:
                        from src.integrations.supabase import upsert_company_enrichment
                        upsert_company_enrichment(cid, {"ipo_status": "pre_ipo_high"})
                        logger.info(
                            "SE16-INTENT-UPGRADE-01: %s ipo_status %s → pre_ipo_high (intent, title=%s)",
                            cname, current_status or "—", latest_ipo.raw_title,
                        )
                    except Exception as e:
                        logger.warning("SE16-INTENT-UPGRADE-01: ipo_status update failed for %s: %s", cname, e)
                else:
                    # Signal erkannt, aber kein Update nötig/sinnvoll — nur loggen
                    logger.info(
                        "SE-16: %s ipo_status-Signal erkannt, kein Update "
                        "(current=%s, kind=%s, title=%s)",
                        cname, current_status or "—", signal_kind, latest_ipo.raw_title,
                    )

            all_events.extend(company_events)

    # SE-17: Multi-Source Aggregation — vor dem Return deduplizieren
    all_events = _aggregate_events(all_events)

    # ── BUG-05: Staleness-Filter — Events >180d nicht schreiben ──────────────
    # Re-Published alte Artikel (Google News Aggregatoren) würden sonst als
    # neue Events erscheinen. 180d = sicherer Puffer für alle Signal-Typen.
    _today     = date.today()
    _cutoff_write  = _today - timedelta(days=180)
    _cutoff_decay  = _today - timedelta(days=90)

    _before_filter = len(all_events)
    filtered_events: list[SignalEvent] = []
    for ev in all_events:
        age_days = (_today - ev.event_date).days

        # BUG-05: älter als 180d → komplett verwerfen
        if ev.event_date < _cutoff_write:
            logger.debug(
                "Staleness-Filter: %s / %s (%s) — %dd alt, wird nicht geschrieben",
                ev.company_name, ev.event_type, ev.event_date.isoformat(), age_days,
            )
            continue

        # BUG-06: 90–180d → schreiben aber relevance_score = 0 (eingepreist)
        # Quartalszahlen etc. sind nach 90d im Kurs eingepreist — kein Scoring-Einfluss.
        if ev.event_date < _cutoff_decay:
            if ev.relevance_score != 0:
                logger.debug(
                    "Relevanz-Decay: %s / %s (%s) — %dd alt, relevance_score → 0",
                    ev.company_name, ev.event_type, ev.event_date.isoformat(), age_days,
                )
            ev.relevance_score = 0.0

        # BUG-04: Datum-Plausibilitäts-Check — Feed-Datum vs. extrahiertes Artikel-Datum
        # Wenn Feed-Datum = heute aber Artikel-Text enthält älteres Datum → Artikel-Datum nutzen
        if ev.event_date == _today and ev.summary:
            extracted = _extract_date_from_text(ev.summary)
            if extracted and extracted < _today and (_today - extracted).days <= 180:
                logger.info(
                    "BUG-04 Datum-Korrektur: %s / %s — Feed=%s → Artikel=%s",
                    ev.company_name, ev.event_type,
                    ev.event_date.isoformat(), extracted.isoformat(),
                )
                ev.event_date = extracted
                # Decay ggf. nachtragen nach Datum-Korrektur
                if ev.event_date < _cutoff_decay:
                    ev.relevance_score = 0.0

        filtered_events.append(ev)

    logger.info(
        "Signal-Filter: %d events → %d (-%d Staleness-Drop, Decay-Events behalten)",
        _before_filter, len(filtered_events), _before_filter - len(filtered_events),
    )

    return filtered_events, all_patents


async def _filter_techcrunch_cached(
    company_id: str,
    company_name: str,
    feed_text: str,
    lookback_days: int = 14,
) -> list[SignalEvent]:
    """Filtert gecachten TechCrunch-Feed für eine Company."""
    events: list[SignalEvent] = []
    cutoff = date.today() - timedelta(days=lookback_days)
    try:
        root = ET.fromstring(feed_text)
        for item in root.findall(".//item"):
            title       = (item.findtext("title") or "").strip()
            description = (item.findtext("description") or "").strip()
            link        = (item.findtext("link") or "").strip()
            pub_date    = item.findtext("pubDate")
            ev_date     = _parse_rss_date(pub_date)
            if ev_date < cutoff:
                continue
            if not _name_in_text(company_name, title + " " + description):
                continue
            event_type = _classify_event(title, description)
            if event_type not in ("funding_round", "m_and_a_event", "ipo_status_change"):
                continue
            events.append(SignalEvent(
                company_id=company_id,
                company_name=company_name,
                event_type=event_type,
                event_date=ev_date,
                summary=_news_summary(event_type, company_name, title, ev_date),
                source="techcrunch",
                source_url=link or None,
                severity=_severity_for_event(event_type),
                raw_title=title,
            ))
    except Exception as e:
        logger.warning("TechCrunch cached filter failed for %s: %s", company_name, e)
    return events
