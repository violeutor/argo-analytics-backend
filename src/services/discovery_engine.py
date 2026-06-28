"""
DISCOVERY-PREIPO-01 · discovery_engine.py
Pfad: argo-analytics-backend/src/services/discovery_engine.py

Continuous-Discovery-Pipeline für Pre-IPO-/Funding-Kandidaten — findet NEUE,
der DB noch unbekannte Companies. Eigenständiges Modul, KEINE Erweiterung von
signal_engine.py: run_signal_engine() ist strikt auf bereits bekannte
company_name/ticker geschlüsselt (jede Quelle baut ihre Query AUS dem Namen,
statt einen Namen ERST aufzulösen) — Discovery braucht den umgekehrten Pfad.

DRY-RUN (Default, S72-Erstbau): Schreibt NICHTS in companies/signals — loggt
nur Kandidaten + Gate-Ergebnisse inkl. Roh-Antworten der noch nicht vollständig
verifizierten Quelle (ESMA PRIII-Feldnamen, s. unten). Scharfstellen erst nach
einem Render-Testlauf: DISCOVERY_DRY_RUN=false (Env-Var).

Vier Quellen, zwei Caps nach KONTEXT (nicht Quellen-Mechanismus), je 2/Tag
Decke, kein Soll — Qualität vor Quantität ist explizite Vorgabe, das Gate wird
nie gelockert um die Zahl zu erreichen:

  IPO-Intent-Topf:
    (a) EDGAR S-1/S-11   — volle Form-Indizes (/edgar/daily-index/.../form.idx),
                            NICHT efts.sec.gov (laut SEC-FAQ für Stichwortsuche
                            gedacht, kein "alle Filings eines Typs ohne Suchbegriff").
    (b) ESMA PRIII       — esma_registers_priii_documents Solr-Core (pan-EWR-
                            Pendant zu S-1). Core-Name live verifiziert (S71-
                            Recherche), EXAKTE Feldnamen NICHT — Sandbox kommt
                            zwar an registers.esma.europa.eu ran, aber das
                            Solr-Predefined-Query-Dokument war zu groß, um die
                            PRIII-spezifische Feldtabelle vollständig zu laden.
                            Dry-Run loggt deshalb die Roh-Keys der ersten Antwort
                            (_log_unknown_schema) — der erste echte Render-Lauf
                            liefert die fehlende Bestätigung quasi nebenbei.

  Funding-Topf:
    (c) EDGAR Form D/D-A — gleicher Index-Mechanismus wie (a). WICHTIG: die
                            Indexdatei selbst enthält KEINEN Betrag — total_
                            offering_amount kommt erst aus einem zweiten,
                            gezielten Call gegen efts.sec.gov (Name/CIK bereits
                            bekannt aus dem Index → das ist innerhalb der von
                            SEC dokumentierten Nutzung, anders als ein "alle
                            Filings"-Scan ohne Suchbegriff). Floor ≥$10M.
    (d) Kuratierte EU-News (Google News DE+EN, sektor- statt company-geschlüsselt,
                            + bestehender TechCrunch-Globalfeed), Domain-Whitelist,
                            Floor ≥€5M via _extract_eur_funding_amount() (NEU,
                            signal_engine.py, additiv — s. dortiger Patch).

Gates (alle vier Quellen, NACH dem jeweiligen Cap/Floor-Filter):
  1. DB-Dedupe       — normalisierter Namens-Check (_normalize_name, reused)
  2. Sektor-Gate     — Claude-Haiku-Klassifikation gegen die 5 Konviktion-
                        Sektoren (Climate Tech, Circular Economy, Modern
                        Agriculture, Biotech, Defense). Schreibt NICHT
                        category/industry — das übernimmt die bestehende
                        Rolling-Refresh-Pipeline beim ersten regulären
                        Enrichment-Pass (infer_category_industry, taxonomy.py-
                        SSOT). Sektor-Gate hier ist reiner Kosten-/Qualitäts-
                        Filter, kein Taxonomy-Autor.
  3. Identitäts-Gate — 1:1 Reuse aus PEER-IDENT-01: resolve_entity()
                        (wikidata_resolver.py, Auto-Pick, kein Modal) → bei
                        Leerlauf check_entity_existence() (market_data_
                        enrichment.py) → identity_confidence-Flag, KEIN Drop
                        bei doppeltem Leerlauf (exakt wie peers.py::
                        _resolve_or_create_peer).

Write (nur scharf, DISCOVERY_DRY_RUN=false):
  companies-Row — OHNE enrichment_status, OHNE category/industry. enriched_at
  bleibt NULL → main.py::_cron_rolling_refresh holt die neue Row beim nächsten
  stündlichen Lauf automatisch ab (sortiert enriched_at IS NULL zuerst) — KEIN
  Discovery-eigener Enrichment-Sonderpfad, exakt wie in der Spec verlangt
  ("danach normale Cold-Path-Enrichment").
  + EIN SignalEvent (signal_engine.SignalEvent) mit dem Filing/Artikel als
  sichtbare Begründung, über upsert_signals() — identischer Pfad wie
  signal_engine.py, damit Notifications/Signal-Feed nichts Neues lernen müssen.

Cron-Slot: main.py, 03:45 UTC, vor Signal-Engine (04:00) — s. main.py-Patch.

Offene Verifikation (bewusst nicht blockierend für den Dry-Run-Bau, s. MODUL06
S71/S72): ESMA-PRIII-Feldnamen final, EDGAR-form.idx-Spaltentrennung am echten
Live-File (Bot-Detection verhinderte den direkten Abruf aus der Sandbox; Doku-
basierte Spalten-Semantik ist verifiziert, das exakte Whitespace-Layout nicht).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Literal

import httpx

from src.services.signal_engine import (
    SignalEvent,
    _normalize_name,
    _extract_eur_funding_amount,
)

logger = logging.getLogger(__name__)

# ── Konfiguration ─────────────────────────────────────────────────────────────

# Default DRY: keine DB-Writes, nur Logging. Scharf erst nach Render-Testlauf.
DRY_RUN: bool = os.environ.get("DISCOVERY_DRY_RUN", "true").lower() != "false"

DAILY_CAP_PER_POT = 2   # Decke, kein Soll — pro Topf (IPO-Intent / Funding)

FORM_D_FLOOR_USD_MN = 10.0    # ≥$10M, strukturiert aus EDGAR vorhanden
EU_NEWS_FLOOR_EUR_MN = 5.0    # ≥€5M, aus Artikel-Freitext extrahiert

DOMAIN_WHITELIST = {"sifted.eu", "tech.eu", "techcrunch.com", "gruenderszene.de"}

CONVICTION_SECTORS = (
    "Climate Tech", "Circular Economy", "Modern Agriculture", "Biotech", "Defense",
)

# SEC verlangt einen deklarierten User-Agent (sonst Bot-Detection/403) UND
# dokumentiert ein hartes Rate-Limit von 10 req/s — kein "kein Limit gefunden"
# mehr (Korrektur ggü. der ursprünglichen S71-Spezifikation, per Web-Diagnose
# vor diesem Build verifiziert). Gleicher UA-String wie signal_engine.py, damit
# SEC nicht zwei unterschiedliche Absender für dasselbe Produkt sieht.
_SEC_USER_AGENT = "ArgoAnalytics/1.0 (research; contact@argo-analytics.io)"
_SEC_HEADERS = {"User-Agent": _SEC_USER_AGENT}
_SEC_MIN_DELAY_S = 0.15   # ≈6.6 req/s — sicher unter dem 10-req/s-Limit

_EDGAR_DAILY_INDEX_BASE = "https://www.sec.gov/Archives/edgar/daily-index"
_EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"   # NUR für (c) Form-D-Beträge, gezielt pro CIK/Name

_ESMA_PRIII_SELECT = "https://registers.esma.europa.eu/solr/esma_registers_priii_documents/select"

_TC_FEED = "https://techcrunch.com/feed/"
_GNEWS_BASE = "https://news.google.com/rss/search"

_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-haiku-4-5-20251001"


# ── Hilfsfunktionen: EDGAR Full-Index ────────────────────────────────────────

def _edgar_index_url(target_date: date) -> str:
    quarter = (target_date.month - 1) // 3 + 1
    return (
        f"{_EDGAR_DAILY_INDEX_BASE}/{target_date.year}/QTR{quarter}/"
        f"form.{target_date.strftime('%Y%m%d')}.idx"
    )


def _parse_form_idx(raw_text: str) -> list[dict]:
    """
    Parst form.idx (Plain-Text, Spalten via 2+ Leerzeichen getrennt — etablierter
    Standard für EDGAR-Idx-Dateien). Spalten gemäß offizieller SEC-Doku
    (Accessing-EDGAR-Data): Form Type, Company Name, CIK, Date Filed, File Name.

    Nicht aus einem Live-File verifiziert (Bot-Detection in der Sandbox, s.
    Moduldocstring) — defensiv: Zeilen, die nicht in genau 5 Felder zerfallen,
    werden geloggt statt stillschweigend verworfen (IMPORT-GUARD-01-Prinzip:
    Format-Drift soll laut auffallen, nicht als leere Liste maskiert werden).
    """
    lines = raw_text.splitlines()
    start_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("---"):
            start_idx = i + 1
            break
    if start_idx is None:
        logger.warning("form.idx: kein Dashes-Separator gefunden — leeres Ergebnis")
        return []

    rows: list[dict] = []
    malformed = 0
    for line in lines[start_idx:]:
        if not line.strip():
            continue
        fields = re.split(r"\s{2,}", line.strip())
        if len(fields) != 5:
            malformed += 1
            continue
        form_type, company_name, cik, date_filed, file_name = fields
        rows.append({
            "form_type": form_type.strip(),
            "company_name": company_name.strip(),
            "cik": cik.strip(),
            "date_filed": date_filed.strip(),
            "file_name": file_name.strip(),
        })

    if malformed:
        logger.warning(
            "form.idx: %d/%d Zeilen nicht in 5 Felder zerlegbar — geprüft werden "
            "nur die restlichen %d (Format-Drift möglich, nicht stillschweigend ignorieren)",
            malformed, len(lines) - start_idx, len(rows),
        )
    return rows


async def _fetch_edgar_daily_index(
    client: httpx.AsyncClient,
    forms: set[str],
    max_lookback_days: int = 5,
) -> list[dict]:
    """
    Holt die form.idx-Datei des letzten Handelstags (mit Rückwärts-Fallback für
    Wochenenden/Feiertage, max_lookback_days Versuche) und filtert auf `forms`.
    """
    today = date.today()
    for delta in range(1, max_lookback_days + 1):
        target = today - timedelta(days=delta)
        url = _edgar_index_url(target)
        try:
            resp = await client.get(url, headers=_SEC_HEADERS, timeout=15.0)
            await asyncio.sleep(_SEC_MIN_DELAY_S)
            if resp.status_code == 404:
                continue   # kein Handelstag — nächsten Tag zurück versuchen
            if resp.status_code != 200:
                logger.warning("EDGAR full-index HTTP %s für %s", resp.status_code, url)
                continue
            rows = _parse_form_idx(resp.text)
            filtered = [r for r in rows if r["form_type"] in forms]
            logger.info(
                "EDGAR full-index %s: %d Zeilen gesamt, %d nach Form-Filter %s",
                target.isoformat(), len(rows), len(filtered), sorted(forms),
            )
            return filtered
        except Exception as e:
            logger.warning("EDGAR full-index Fetch fehlgeschlagen für %s: %s", url, e)
    logger.warning("EDGAR full-index: kein Treffer in %d Tagen Rückwärtssuche", max_lookback_days)
    return []


async def _fetch_form_d_amount(
    client: httpx.AsyncClient,
    company_name: str,
) -> float | None:
    """
    total_offering_amount steht NICHT im form.idx (nur form_type/name/CIK/date/
    path) — gezielter Zweit-Call gegen efts.sec.gov, NAME bereits bekannt aus
    dem Index. Das ist die normale, von SEC dokumentierte Nutzung dieser API
    (Namens-/Ticker-Suche), kein "alle Filings ohne Suchbegriff"-Scan — der
    Unterschied, der efts.sec.gov für die Index-Phase selbst ungeeignet macht
    (s. Moduldocstring), gilt hier nicht.
    """
    try:
        resp = await client.get(
            _EDGAR_SEARCH_URL,
            params={
                "q": f'"{company_name}"',
                "forms": "D,D/A",
                "_source": "total_offering_amount",
            },
            headers=_SEC_HEADERS,
            timeout=12.0,
        )
        await asyncio.sleep(_SEC_MIN_DELAY_S)
        if resp.status_code != 200:
            return None
        hits = resp.json().get("hits", {}).get("hits", [])
        for hit in hits[:3]:
            total = hit.get("_source", {}).get("total_offering_amount")
            if total:
                try:
                    return round(float(total) / 1_000_000, 2)
                except (ValueError, TypeError):
                    continue
    except Exception as e:
        logger.debug("Form-D-Betrag-Lookup fehlgeschlagen für %s: %s", company_name, e)
    return None


# ── ESMA PRIII ────────────────────────────────────────────────────────────────

# Plausible Feldnamen-Kandidaten — ESMA-Register-Cores folgen durchgängig einem
# Präfix-Schema (ae_, sn_, shs_, bm_ ...), für PRIII vermutlich "pd_" o.ä.
# NICHT final verifiziert (s. Moduldocstring) — defensiv mehrere Kandidaten
# probieren statt einen einzigen zu raten und bei Fehltreffer leer zu laufen.
_ESMA_ISSUER_NAME_KEYS = (
    "pd_issuerName", "issuerName", "issuer_name", "pd_issuer_name", "name",
)
_ESMA_DATE_KEYS = ("pd_approvalDate", "approvalDate", "approval_date", "pd_publicationDate")
_ESMA_LOGGED_UNKNOWN_SCHEMA = False   # nur einmal pro Prozess loggen, nicht pro Kandidat


def _extract_esma_field(doc: dict, candidates: tuple[str, ...]) -> str | None:
    for key in candidates:
        if doc.get(key):
            return str(doc[key])
    return None


def _log_unknown_schema_once(doc: dict) -> None:
    global _ESMA_LOGGED_UNKNOWN_SCHEMA
    if _ESMA_LOGGED_UNKNOWN_SCHEMA:
        return
    _ESMA_LOGGED_UNKNOWN_SCHEMA = True
    logger.warning(
        "ESMA PRIII: keiner der bekannten Issuer-Name-Keys %s im Dokument gefunden. "
        "Roh-Keys der ersten Antwort (für Feld-Abgleich, einmalig geloggt): %s",
        _ESMA_ISSUER_NAME_KEYS, sorted(doc.keys()),
    )


async def _fetch_esma_priii_documents(
    client: httpx.AsyncClient,
    lookback_days: int = 3,
    rows: int = 50,
) -> list[dict]:
    """
    Holt aktuelle PRIII-Dokumente (Prospekt-Genehmigungen, pan-EWR). Kein API-
    Key, öffentlicher Solr-Select-Endpoint. Datumsfilter clientseitig statt via
    `fq`, da der exakte Datumsfeld-Name nicht verifiziert ist (s. oben) — robuster
    gegen einen falsch geratenen fq-Feldnamen, der sonst stillschweigend 0
    Treffer liefern würde, statt einen Fehler zu zeigen.
    """
    candidates: list[dict] = []
    try:
        resp = await client.get(
            _ESMA_PRIII_SELECT,
            params={"q": "*:*", "rows": rows, "wt": "json", "sort": "id desc"},
            timeout=15.0,
        )
        if resp.status_code != 200:
            logger.warning("ESMA PRIII HTTP %s", resp.status_code)
            return []
        data = resp.json()
        docs = data.get("response", {}).get("docs", [])
        logger.info("ESMA PRIII: %d Dokumente in Rohantwort", len(docs))
        for doc in docs:
            name = _extract_esma_field(doc, _ESMA_ISSUER_NAME_KEYS)
            if not name:
                _log_unknown_schema_once(doc)
                continue
            date_str = _extract_esma_field(doc, _ESMA_DATE_KEYS)
            candidates.append({
                "company_name": name,
                "date_filed": (date_str or "")[:10],
                "raw": doc,
            })
    except Exception as e:
        logger.warning("ESMA PRIII Fetch fehlgeschlagen: %s", e)
    return candidates


# ── Kuratierte EU-News ────────────────────────────────────────────────────────

async def _fetch_curated_eu_news(client: httpx.AsyncClient, lookback_days: int = 3) -> list[dict]:
    """
    Sektor-geschlüsselte Google-News-Queries (DE+EN, je Konviktion-Sektor) +
    bestehender TechCrunch-Globalfeed — gefiltert auf Domain-Whitelist. Anders
    als parse_google_news()/parse_techcrunch() in signal_engine.py (company-
    geschlüsselt, kein NER nötig) braucht dieser Pfad eine echte Entitäts-
    Extraktion aus Freitext — genau die Lücke, die laut MODUL06 (S71) in der
    bestehenden Signal-Engine fehlt. Claude-Haiku-Extraktion s. _extract_company_from_news.
    """
    cutoff = date.today() - timedelta(days=lookback_days)
    raw_items: list[dict] = []

    # (1) Sektor-geschlüsselte Google-News-Queries
    for sector in CONVICTION_SECTORS:
        queries = [
            f"{sector} Finanzierungsrunde OR Millionen Euro",
            f"{sector} funding round OR raises million",
        ]
        for q in queries:
            try:
                resp = await client.get(
                    _GNEWS_BASE, params={"q": q, "hl": "de", "gl": "DE", "ceid": "DE:de"}, timeout=8.0,
                )
                if resp.status_code != 200:
                    continue
                from xml.etree import ElementTree as ET
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item")[:6]:
                    raw_items.append({
                        "title": (item.findtext("title") or "").strip(),
                        "description": (item.findtext("description") or "").strip(),
                        "link": (item.findtext("link") or "").strip(),
                        "pub_date": item.findtext("pubDate"),
                        "sector_hint": sector,
                    })
            except Exception as e:
                logger.debug("EU-News Google-Query fehlgeschlagen (%s): %s", sector, e)
            await asyncio.sleep(0.3)

    # (2) TechCrunch-Globalfeed (eigener Fetch — eigener Cron-Slot, kein
    # In-Memory-Cache aus run_signal_engine() verfügbar)
    try:
        resp = await client.get(_TC_FEED, timeout=8.0)
        if resp.status_code == 200:
            from xml.etree import ElementTree as ET
            root = ET.fromstring(resp.text)
            for item in root.findall(".//item"):
                raw_items.append({
                    "title": (item.findtext("title") or "").strip(),
                    "description": (item.findtext("description") or "").strip(),
                    "link": (item.findtext("link") or "").strip(),
                    "pub_date": item.findtext("pubDate"),
                    "sector_hint": None,
                })
    except Exception as e:
        logger.debug("EU-News TechCrunch-Fetch fehlgeschlagen: %s", e)

    # Domain-Whitelist + Lookback-Filter
    from src.services.signal_engine import _extract_domain, _parse_rss_date
    filtered: list[dict] = []
    for it in raw_items:
        domain = _extract_domain(it["link"]) or ""
        in_whitelist = any(domain == d or domain.endswith("." + d) for d in DOMAIN_WHITELIST)
        if not in_whitelist:
            continue
        ev_date = _parse_rss_date(it["pub_date"])
        if ev_date < cutoff:
            continue
        it["event_date"] = ev_date
        it["domain"] = domain
        filtered.append(it)

    logger.info("EU-News: %d Roh-Items, %d nach Domain-Whitelist+Lookback", len(raw_items), len(filtered))
    return filtered


# ── Claude Haiku: Sektor-Gate + Entitäts-Extraktion (News-Pfad) ──────────────

def _claude_headers() -> dict | None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    return {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}


async def _classify_sector(client: httpx.AsyncClient, name: str, context: str) -> str | None:
    """Sektor-Gate — gegen die 5 Konviktion-Sektoren, sonst None (kein Fit)."""
    headers = _claude_headers()
    if not headers:
        logger.warning("Sektor-Gate: ANTHROPIC_API_KEY fehlt — Kandidat '%s' ohne Gate-Klassifikation, wird NICHT verworfen (Fail-Open wäre hier riskanter als geloggter Fail-Closed) — siehe Rückgabewert None", name)
        return None
    sectors = ", ".join(CONVICTION_SECTORS)
    prompt = (
        f"Firma: {name}\nKontext: {context[:300]}\n\n"
        f"Passt diese Firma in genau einen dieser Sektoren: {sectors}? "
        f"Antworte NUR mit einem JSON-Objekt: {{\"sector\": \"<einer der 5 Namen>\"}} "
        f"oder {{\"sector\": null}} falls kein Fit. Kein Fließtext."
    )
    try:
        resp = await client.post(
            _CLAUDE_API_URL, headers=headers,
            json={"model": _CLAUDE_MODEL, "max_tokens": 64, "messages": [{"role": "user", "content": prompt}]},
            timeout=15.0,
        )
        if resp.status_code != 200:
            logger.warning("Sektor-Gate Claude HTTP %s für '%s'", resp.status_code, name)
            return None
        import json as _json
        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = _json.loads(raw)
        sector = parsed.get("sector")
        return sector if sector in CONVICTION_SECTORS else None
    except Exception as e:
        logger.warning("Sektor-Gate fehlgeschlagen für '%s': %s", name, e)
        return None


async def _extract_company_from_news(client: httpx.AsyncClient, title: str, description: str) -> dict | None:
    """
    NEU (anders als alles in signal_engine.py): extrahiert eine Firma + Fund-
    ing-Signal aus Freitext, statt einen bereits bekannten Namen nur zu klassi-
    fizieren. Genau die Lücke, die laut MODUL06 (S71) in run_signal_engine()/
    _claude_ner_pass() fehlt ("klassifiziert nur, extrahiert keine neuen Entitäten").
    """
    headers = _claude_headers()
    if not headers:
        return None
    prompt = (
        f"Titel: {title}\nBeschreibung: {description[:300]}\n\n"
        f"Ist das eine Meldung über eine Finanzierungsrunde/Funding-Round einer "
        f"einzelnen, konkret benannten Firma? Falls ja, antworte NUR mit JSON: "
        f"{{\"is_funding\": true, \"company_name\": \"<Firmenname>\"}}. "
        f"Falls nein oder keine klar benannte Firma: {{\"is_funding\": false}}."
    )
    try:
        resp = await client.post(
            _CLAUDE_API_URL, headers=headers,
            json={"model": _CLAUDE_MODEL, "max_tokens": 100, "messages": [{"role": "user", "content": prompt}]},
            timeout=15.0,
        )
        if resp.status_code != 200:
            return None
        import json as _json
        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = _json.loads(raw)
        if not parsed.get("is_funding") or not parsed.get("company_name"):
            return None
        return {"company_name": parsed["company_name"]}
    except Exception as e:
        logger.debug("Entitäts-Extraktion fehlgeschlagen für '%s': %s", title[:60], e)
        return None


# ── Identitäts-Gate (1:1 Reuse aus PEER-IDENT-01) ────────────────────────────

def _autopick(result) -> "object | None":
    """
    Wortgleiche Logik zu peers.py::_autopick_peer — bewusst hier dupliziert
    statt aus src.routes.peers importiert: services importieren nicht aus
    routes (Layering), und die Funktion ist 4 Zeilen lang. Kein Modal, kein
    Listed-Zwang (anders als bei Buyern) — Discovery-Kandidaten sind über-
    wiegend pre-IPO/privat, ein Hard-Drop bei fehlendem Wikidata-Hit wäre
    verlustreich.
    """
    resolved = getattr(result, "resolved", None)
    if resolved is not None:
        return resolved
    candidates = getattr(result, "candidates", None) or []
    return candidates[0] if candidates else None


async def _identity_gate(client: httpx.AsyncClient, name: str) -> tuple[str, dict]:
    """
    Gibt (identity_confidence, resolved_fields) zurück. resolved_fields ist
    leer falls nichts aufgelöst werden konnte — identity_confidence ist dann
    'unverified', die Company wird TROTZDEM angelegt (kein Drop), exakt wie
    in peers.py::_resolve_or_create_peer (PEER-IDENT-01).
    """
    from src.services.wikidata_resolver import resolve_entity
    from src.services.market_data_enrichment import check_entity_existence

    resolved_fields: dict = {}
    identity_confidence = "unverified"

    try:
        wd_result = await resolve_entity(name, client=client)
        picked = _autopick(wd_result)
        if picked is not None:
            resolved_fields["name"] = getattr(picked, "display_name", None) or getattr(picked, "name", None) or name
            resolved_fields["ticker"] = getattr(picked, "ticker", None)
            resolved_fields["exchange"] = getattr(picked, "display_exchange", None)
            resolved_fields["headquarters"] = getattr(picked, "headquarters", None)
            identity_confidence = "verified_wikidata"
    except Exception as e:
        logger.debug("Identitäts-Gate Wikidata-Resolve failed für '%s': %s", name, e)

    if identity_confidence == "unverified":
        try:
            found, source = await check_entity_existence(name)
            if found:
                identity_confidence = source
        except Exception as e:
            logger.debug("Identitäts-Gate DDG-Check failed für '%s': %s", name, e)

    return identity_confidence, resolved_fields


# ── DB-Dedupe ─────────────────────────────────────────────────────────────────

def _is_known(name: str, known_normalized: set[str]) -> bool:
    norm = _normalize_name(name)
    if norm in known_normalized:
        return True
    return any(norm in k or k in norm for k in known_normalized if norm and k)


# ── Write-Pfad (DB) ───────────────────────────────────────────────────────────

def _insert_discovery_company(db, name: str, discovery_source: str, identity_confidence: str, resolved_fields: dict) -> str | None:
    """
    Insert mit Enum-Retry-Fallback — exakt das in peers.py::_resolve_or_create_peer
    etablierte Muster (S66/BUYER-IDENT-02-Trap: company_source-Enum braucht u.U.
    eine Migration für den neuen Wert; bis die läuft, fällt der Insert auf
    'manual' zurück statt zu crashen — LAUT geloggt, nicht stillschweigend).
    """
    payload = {
        "name": resolved_fields.get("name") or name,
        "discovery_source": discovery_source,
        "identity_confidence": identity_confidence,
    }
    if resolved_fields.get("ticker"):
        payload["ticker"] = resolved_fields["ticker"]
    if resolved_fields.get("exchange"):
        payload["exchange"] = resolved_fields["exchange"]
    if resolved_fields.get("headquarters"):
        payload["headquarters"] = resolved_fields["headquarters"]

    for source_val in ("discovery_engine", "manual"):
        try:
            payload["source"] = source_val
            result = db.table("companies").insert(payload).execute()
            if result.data:
                new_id = result.data[0]["id"]
                logger.info(
                    "Discovery: '%s' angelegt → %s (discovery_source=%s, source=%s, identity_confidence=%s)",
                    payload["name"], new_id, discovery_source, source_val, identity_confidence,
                )
                return new_id
            break
        except Exception as e:
            if source_val == "discovery_engine" and "22P02" in str(e):
                logger.warning(
                    "Enum-Wert 'discovery_engine' auf company_source fehlt (Migration "
                    "schema_patch_discovery_source_enum.sql noch nicht gelaufen?) — "
                    "Fallback auf 'manual' für '%s'", name,
                )
                continue
            logger.warning("Discovery: Insert fehlgeschlagen für '%s': %s", name, e)
            break
    return None


# ── Orchestrator ──────────────────────────────────────────────────────────────

async def run_discovery_pipeline() -> dict:
    """
    Haupteinstieg, aufgerufen von main.py::_cron_discovery() um 03:45 UTC.
    Gibt eine Zusammenfassung zurück (für Logging/Monitoring).
    """
    from src.integrations.supabase import get_supabase, fetch_companies, upsert_signals

    db = get_supabase()
    existing = fetch_companies(limit=500)
    known_normalized = {_normalize_name(c.get("name", "")) for c in existing if c.get("name")}

    stats = {
        "dry_run": DRY_RUN,
        "ipo_intent_written": 0, "funding_written": 0,
        "ipo_intent_seen": 0, "funding_seen": 0,
        "rejected_sector": 0, "rejected_dedupe": 0,
    }
    new_signals: list[SignalEvent] = []

    timeout = httpx.Timeout(15.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:

        # ── IPO-Intent-Topf: EDGAR S-1/S-11 + ESMA PRIII ─────────────────────
        ipo_intent_candidates: list[dict] = []

        edgar_s1 = await _fetch_edgar_daily_index(client, forms={"S-1", "S-11"})
        for row in edgar_s1:
            ipo_intent_candidates.append({
                "name": row["company_name"], "source": "edgar_s1",
                "context": f"{row['form_type']} Filing, CIK {row['cik']}, {row['date_filed']}",
                "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={row['cik']}",
                "raw_title": f"{row['form_type']} — {row['company_name']}",
                "event_date": row["date_filed"],
            })

        esma_docs = await _fetch_esma_priii_documents(client)
        for doc in esma_docs:
            ipo_intent_candidates.append({
                "name": doc["company_name"], "source": "esma_prospectus",
                "context": f"ESMA PRIII Prospektus, {doc.get('date_filed') or 'Datum unbekannt'}",
                "source_url": None,
                "raw_title": f"PRIII — {doc['company_name']}",
                "event_date": doc.get("date_filed") or date.today().isoformat(),
            })

        stats["ipo_intent_seen"] = len(ipo_intent_candidates)

        for cand in ipo_intent_candidates:
            if stats["ipo_intent_written"] >= DAILY_CAP_PER_POT:
                break
            if _is_known(cand["name"], known_normalized):
                stats["rejected_dedupe"] += 1
                continue
            sector = await _classify_sector(client, cand["name"], cand["context"])
            if sector is None:
                stats["rejected_sector"] += 1
                continue
            identity_confidence, resolved = await _identity_gate(client, cand["name"])

            if DRY_RUN:
                logger.info(
                    "[DRY-RUN] IPO-Intent-Kandidat akzeptiert: '%s' (Quelle=%s, Sektor=%s, "
                    "Identität=%s) — würde geschrieben mit ipo_status=pre_ipo_high",
                    cand["name"], cand["source"], sector, identity_confidence,
                )
            else:
                new_id = _insert_discovery_company(db, cand["name"], cand["source"], identity_confidence, resolved)
                if new_id:
                    from src.integrations.supabase import upsert_company_enrichment
                    upsert_company_enrichment(new_id, {"ipo_status": "pre_ipo_high"})
                    new_signals.append(SignalEvent(
                        company_id=new_id, company_name=resolved.get("name") or cand["name"],
                        event_type="ipo_status_change",
                        event_date=date.fromisoformat(cand["event_date"][:10]) if cand["event_date"] else date.today(),
                        summary=f"Neu entdeckt via {cand['source']}: {cand['context']}",
                        source=cand["source"], source_url=cand["source_url"],
                        severity="high", raw_title=cand["raw_title"],
                        direction="positive", signal_category="ipo_progress",
                    ))
            stats["ipo_intent_written"] += 1

        # ── Funding-Topf: EDGAR Form D/D-A + kuratierte EU-News ──────────────
        funding_candidates: list[dict] = []

        edgar_formd = await _fetch_edgar_daily_index(client, forms={"D", "D/A"})
        for row in edgar_formd:
            amount = await _fetch_form_d_amount(client, row["company_name"])
            if amount is None or amount < FORM_D_FLOOR_USD_MN:
                continue
            funding_candidates.append({
                "name": row["company_name"], "source": "edgar_formd",
                "context": f"Form {row['form_type']}, ${amount:.1f}M, CIK {row['cik']}",
                "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={row['cik']}",
                "raw_title": f"{row['form_type']} — {row['company_name']}",
                "event_date": row["date_filed"],
                "funding_amount_usd_mn": amount,
            })

        eu_news = await _fetch_curated_eu_news(client)
        for item in eu_news:
            text = f"{item['title']} {item['description']}"
            eur_amount = _extract_eur_funding_amount(text)
            if eur_amount is None or eur_amount < EU_NEWS_FLOOR_EUR_MN:
                continue
            extracted = await _extract_company_from_news(client, item["title"], item["description"])
            if not extracted:
                continue
            funding_candidates.append({
                "name": extracted["company_name"], "source": "news_curated_eu",
                "context": f"{item['domain']}, €{eur_amount:.1f}M — {item['title'][:120]}",
                "source_url": item["link"] or None,
                "raw_title": item["title"],
                "event_date": item["event_date"].isoformat(),
                "funding_amount_usd_mn": None,   # bewusst nicht in USD-Feld — EUR-Wert separat
            })

        stats["funding_seen"] = len(funding_candidates)

        for cand in funding_candidates:
            if stats["funding_written"] >= DAILY_CAP_PER_POT:
                break
            if _is_known(cand["name"], known_normalized):
                stats["rejected_dedupe"] += 1
                continue
            sector = await _classify_sector(client, cand["name"], cand["context"])
            if sector is None:
                stats["rejected_sector"] += 1
                continue
            identity_confidence, resolved = await _identity_gate(client, cand["name"])

            if DRY_RUN:
                logger.info(
                    "[DRY-RUN] Funding-Kandidat akzeptiert: '%s' (Quelle=%s, Sektor=%s, "
                    "Identität=%s) — %s",
                    cand["name"], cand["source"], sector, identity_confidence, cand["context"],
                )
            else:
                new_id = _insert_discovery_company(db, cand["name"], cand["source"], identity_confidence, resolved)
                if new_id:
                    new_signals.append(SignalEvent(
                        company_id=new_id, company_name=resolved.get("name") or cand["name"],
                        event_type="funding_round",
                        event_date=date.fromisoformat(cand["event_date"][:10]) if cand["event_date"] else date.today(),
                        summary=f"Neu entdeckt via {cand['source']}: {cand['context']}",
                        source=cand["source"], source_url=cand["source_url"],
                        severity="medium", raw_title=cand["raw_title"],
                        direction="positive", signal_category="funding",
                        funding_amount_usd_mn=cand.get("funding_amount_usd_mn"),
                    ))
            stats["funding_written"] += 1

    if new_signals and not DRY_RUN:
        written = upsert_signals([e.to_dict() for e in new_signals])
        stats["signals_written"] = written

    logger.info("Discovery-Cron fertig: %s", stats)
    return stats
