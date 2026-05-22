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
    """Parst RSS pubDate zu date — Fallback: heute."""
    if not date_str:
        return date.today()
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return date.today()


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


# ── EDGAR Parser ─────────────────────────────────────────────────────────────

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
    """
    events: list[SignalEvent] = []
    today = date.today()
    industry = (company.get("industry") or "").lower()
    is_listed = bool(company.get("ticker"))
    region    = company.get("region", "")

    # 1. Ownership-Transparenz: keine Einträge
    if not ownership_entries and not is_listed:
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
    if last_signal_raw:
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
                    raw_title=f"Signal-Stille: {days_silent} Tage",
                    direction="negative",
                    signal_category="general_news",
                ))
        except ValueError:
            pass

    # 3. Kein Headcount bei privater Company (fehlende Transparenz)
    headcount = company.get("headcount")
    if not headcount and not is_listed:
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
    if not revenue and any(s in industry for s in tech_sectors) and not is_listed:
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

async def run_signal_engine(companies: list[dict], ownership_map: dict[str, list[dict]]) -> list[SignalEvent]:
    """
    SE-01 — Haupt-Pipeline.
    Läuft täglich via Cron. Gibt alle gesammelten Signals zurück.
    Aufgerufen von main.py _cron_signal_engine().

    Args:
        companies:     Liste von Company-Dicts (id, name, ticker, exchange,
                       last_signal_date, industry, region, headcount, revenue_usd_mn)
        ownership_map: {company_id: [ownership_entries]} — aktueller DB-Stand
    """
    all_events: list[SignalEvent] = []
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
            is_listed = bool(ticker)

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
            )
            company_events.extend(absence_events)

            logger.info(
                "Signal-Engine: %s → %d events (edgar=%d news=%d tc=%d ownership=%d absence=%d) "
                "pos=%d neg=%d neu=%d",
                cname, len(company_events),
                sum(1 for e in company_events if e.source == "edgar"),
                sum(1 for e in company_events if e.source == "google_news"),
                sum(1 for e in company_events if e.source == "techcrunch"),
                sum(1 for e in company_events if e.source == "internal"),
                sum(1 for e in company_events if e.source == "internal_absence"),
                sum(1 for e in company_events if e.direction == "positive"),
                sum(1 for e in company_events if e.direction == "negative"),
                sum(1 for e in company_events if e.direction == "neutral"),
            )
            all_events.extend(company_events)

    return all_events


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
