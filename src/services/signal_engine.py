"""
SE-01–SE-03 · signal_engine.py
Pfad: argo-analytics-backend/src/services/signal_engine.py

Signal-Engine — täglich via Cron (~06:00 UTC).

Quellen:
  - SEC EDGAR full-text search  → S-1 (IPO), 10-K/10-Q (Earnings), 8-K (M&A/Events)
  - Google News RSS              → Funding, M&A, allg. News
  - TechCrunch RSS               → Funding-Runden EU/US
  - Interner Ownership-Vergleich → Ownership-Changes (DB-Diff)

Pipeline je Company:
  1. EDGAR-Parser   → strukturierte Events (S-1, 10-K, 8-K)
  2. News-Parser    → NER via Keyword-Matching + Claude-Klassifikation
  3. Ownership-Diff → Vergleich mit letztem DB-Stand
  4. upsert_signals → Duplikat-sicher via UNIQUE INDEX
"""

import asyncio
import logging
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


class SignalEvent:
    __slots__ = ("company_id", "company_name", "event_type", "event_date",
                 "summary", "source", "source_url", "severity", "raw_title")

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
    ):
        self.company_id   = company_id
        self.company_name = company_name
        self.event_type   = event_type
        self.event_date   = event_date
        self.summary      = summary
        self.source       = source
        self.source_url   = source_url
        self.severity     = severity
        self.raw_title    = raw_title

    def to_dict(self) -> dict:
        return {
            "company_id":   self.company_id,
            "event_type":   self.event_type,
            "event_date":   self.event_date.isoformat(),
            "summary":      self.summary,
            "source":       self.source,
            "source_url":   self.source_url,
            "severity":     self.severity,
            "raw_title":    self.raw_title,
        }


# ── Hilfsfunktionen ──────────────────────────────────────────────────────────

def _parse_rss_date(date_str: str | None) -> date:
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
    """Prüft ob Company-Name (normalisiert) im Text vorkommt."""
    normalized = _normalize_name(company_name)
    text_lower = text.lower()
    # Direktmatch
    if normalized in text_lower:
        return True
    # Hauptwort match (erstes Wort ≥ 5 Zeichen)
    main_word = normalized.split()[0] if normalized else ""
    if len(main_word) >= 5 and main_word in text_lower:
        return True
    return False


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


# ── EDGAR Parser ─────────────────────────────────────────────────────────────

_EDGAR_BASE = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index?q=%22{query}%22&dateRange=custom&startdt={start}&forms={forms}&hits.hits._source=period_of_report,file_date,display_names,period_of_report,entity_name,file_num"

# Form-Typen → Event-Mapping
_EDGAR_FORM_MAP: dict[str, EventType] = {
    "S-1":  "ipo_status_change",
    "S-11": "ipo_status_change",
    "10-K": "earnings",
    "10-Q": "earnings",
    "8-K":  "m_and_a_event",   # 8-K kann viel sein — NER klärt ob M&A
    "SC 13D": "ownership_change",
    "SC 13G": "ownership_change",
}


async def parse_edgar(
    company_id: str,
    company_name: str,
    ticker: str | None,
    client: httpx.AsyncClient,
    lookback_days: int = 30,
) -> list[SignalEvent]:
    """
    Sucht SEC EDGAR nach Filings der letzten `lookback_days` Tage.
    Unterstützt US-listed Companies (Ticker vorhanden) und Namens-Suche.
    """
    events: list[SignalEvent] = []
    start_date = (date.today() - timedelta(days=lookback_days)).isoformat()

    # Query: Ticker hat Prio (exakter Match), sonst Company-Name
    query = ticker.split("·")[0].strip() if ticker else company_name
    forms = "S-1,S-11,10-K,10-Q,8-K,SC 13D,SC 13G"

    url = (
        f"https://efts.sec.gov/LATEST/search-index"
        f"?q=%22{httpx.QueryParams({'': query}).value if False else query.replace(' ', '+')}"
        f"%22&dateRange=custom&startdt={start_date}&forms={forms}"
    )

    try:
        resp = await client.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={
                "q":         f'"{query}"',
                "dateRange": "custom",
                "startdt":   start_date,
                "forms":     forms,
            },
            timeout=10.0,
        )
        if resp.status_code != 200:
            logger.warning("EDGAR HTTP %s for %s", resp.status_code, company_name)
            return events

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])

        for hit in hits[:5]:   # max 5 Filings pro Company
            src = hit.get("_source", {})
            form_type  = src.get("forms", [None])[0] if src.get("forms") else None
            file_date  = src.get("file_date") or src.get("period_of_report")
            entity     = src.get("entity_name", company_name)
            accession  = hit.get("_id", "").replace("-", "")
            filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum={src.get('file_num', '')}" if src.get("file_num") else None

            if not form_type or not file_date:
                continue

            event_type = _EDGAR_FORM_MAP.get(form_type, "news")
            try:
                ev_date = date.fromisoformat(file_date[:10])
            except ValueError:
                ev_date = date.today()

            summary = _edgar_summary(form_type, entity, ev_date)

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
            ))
            logger.info("EDGAR signal: %s %s %s → %s", company_name, form_type, ev_date, event_type)

    except Exception as e:
        logger.warning("EDGAR parse failed for %s: %s", company_name, e)

    return events


def _edgar_summary(form_type: str, entity: str, ev_date: date) -> str:
    summaries = {
        "S-1":   f"{entity} hat einen S-1-Antrag bei der SEC eingereicht — IPO-Prozess gestartet ({ev_date}).",
        "S-11":  f"{entity} hat einen S-11-Antrag bei der SEC eingereicht — IPO-Prozess gestartet ({ev_date}).",
        "10-K":  f"{entity} hat den Jahresbericht (10-K) bei der SEC eingereicht ({ev_date}).",
        "10-Q":  f"{entity} hat den Quartalsbericht (10-Q) bei der SEC eingereicht ({ev_date}).",
        "8-K":   f"{entity} hat ein 8-K-Formular eingereicht — mögliche M&A- oder wesentliche Unternehmens-Events ({ev_date}).",
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
                continue   # TechCrunch nur für Investment-Events nutzen

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
        ))

    return events


# ── Haupt-Orchestrator ────────────────────────────────────────────────────────

async def run_signal_engine(companies: list[dict], ownership_map: dict[str, list[dict]]) -> list[SignalEvent]:
    """
    SE-01 — Haupt-Pipeline.
    Läuft täglich via Cron. Gibt alle gesammelten Signals zurück.
    Aufgerufen von main.py _cron_signal_engine().

    Args:
        companies:     Liste von Company-Dicts (id, name, ticker, exchange, last_signal_date)
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

            logger.info(
                "Signal-Engine: %s → %d events (edgar=%d news=%d tc=%d ownership=%d)",
                cname, len(company_events),
                sum(1 for e in company_events if e.source == "edgar"),
                sum(1 for e in company_events if e.source == "google_news"),
                sum(1 for e in company_events if e.source == "techcrunch"),
                sum(1 for e in company_events if e.source == "internal"),
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
