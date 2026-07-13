"""
B-05 · funding_enrichment.py
Pfad: argo-analytics-backend/src/services/funding_enrichment.py

Automatische Befüllung von funding_rounds via:
  1. SEC EDGAR Form D   → US-Companies (private placement filings)
  2. TechCrunch RSS     → EU/US Startups mit TC-Coverage
  3. Google News NER    → Fallback für alle Companies

Pipeline-Logik:
  - Läuft täglich via Cron (main.py) NACH signal_engine
  - Nutzt signals-Tabelle als vorqualifizierte Quelle (funding_amount_usd_mn bereits extrahiert)
  - Schreibt in funding_rounds (Duplikat-sicher via UNIQUE company_id+date+type)
  - Setzt companies.last_funding_enriched_at

Abhängigkeiten:
  - signal_engine.py: _extract_funding_amount(), parse_edgar()
  - supabase.py: upsert_funding_round(), update_last_funding_enriched_at(),
                 fetch_funding_signals(), fetch_companies_for_funding_enrichment()
"""

import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import httpx

from src.services.signal_engine import (
    _normalize_name,
    _name_in_text,
    _parse_rss_date,
    _extract_funding_amount,
    _extract_domain,
    _EDGAR_SEARCH_URL,
    _EDGAR_FORM_MAP,
)
from src.integrations.supabase import (
    upsert_funding_round,
    update_last_funding_enriched_at,
    fetch_funding_signals,
    fetch_companies_for_funding_enrichment,
    fetch_funding_rounds,
)

logger = logging.getLogger(__name__)

# ── Konstanten ────────────────────────────────────────────────────────────────

_TC_FEED      = "https://techcrunch.com/feed/"
_GNEWS_BASE   = "https://news.google.com/rss/search"
_CLAUDE_URL   = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-haiku-4-5-20251001"

# Funding-Stage-Normalisierung (EDGAR form_type → Argo stage)
_STAGE_FROM_FORM: dict[str, str] = {
    "D":   "venture",   # Form D = private placement, meist Venture
    "D/A": "venture",
}

# Keyword → Stage-Inferenz aus Freitext
_STAGE_KW: list[tuple[str, str]] = [
    ("seed",        "Seed"),
    ("series a",    "Series A"),
    ("series b",    "Series B"),
    ("series c",    "Series C"),
    ("series d",    "Series D"),
    ("series e",    "Series E"),
    ("pre-ipo",     "Pre-IPO"),
    ("pre-seed",    "Pre-Seed"),
    ("bridge",      "Bridge"),
    ("convertible", "Convertible Note"),
    ("grant",       "Grant"),
    ("debt",        "Debt"),
    ("venture",     "Venture"),
]


def _infer_stage(text: str) -> str:
    """Leitet Funding-Stage aus Freitext ab."""
    t = text.lower()
    for kw, stage in _STAGE_KW:
        if kw in t:
            return stage
    return "Venture"   # Fallback


def _infer_date_from_text(text: str) -> date | None:
    """
    Extrahiert Datum aus Freitext — z.B. 'May 2025', 'Q1 2026', '2024-03-15'.
    Gibt None zurück wenn kein Datum erkennbar.
    """
    # ISO-Format
    m = re.search(r'\b(\d{4}-\d{2}-\d{2})\b', text)
    if m:
        try:
            return date.fromisoformat(m.group(1))
        except ValueError:
            pass
    # Monat + Jahr
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    m2 = re.search(r'\b(' + '|'.join(months.keys()) + r')\s+(\d{4})\b', text, re.IGNORECASE)
    if m2:
        return date(int(m2.group(2)), months[m2.group(1).lower()], 1)
    # Jahr allein
    m3 = re.search(r'\b(202[3-9]|203\d)\b', text)
    if m3:
        return date(int(m3.group(1)), 1, 1)
    return None


# ── EDGAR Form D Parser ───────────────────────────────────────────────────────

async def enrich_from_edgar_form_d(
    company_id: str,
    company_name: str,
    ticker: str | None,
    client: httpx.AsyncClient,
    lookback_days: int = 90,
) -> list[dict]:
    """
    B-05: Sucht SEC EDGAR Form D für private Funding-Rounds.
    Form D = private placement notice — meldepflichtig für US-Startups.
    Gibt strukturierte funding_round-Dicts zurück (bereit für upsert_funding_round).
    """
    rounds: list[dict] = []
    start_date = (date.today() - timedelta(days=lookback_days)).isoformat()

    clean_ticker = ticker.split("·")[0].strip() if ticker else None
    query = f'"{clean_ticker}"' if clean_ticker else f'"{company_name}"'

    try:
        resp = await client.get(
            _EDGAR_SEARCH_URL,
            params={
                "q":         query,
                "dateRange": "custom",
                "startdt":   start_date,
                "forms":     "D,D/A",
                "_source":   "file_date,entity_name,total_offering_amount,form_type,file_num",
            },
            timeout=12.0,
        )
        if resp.status_code != 200:
            logger.warning("EDGAR Form D HTTP %s for %s", resp.status_code, company_name)
            return rounds

        hits = resp.json().get("hits", {}).get("hits", [])

        for hit in hits[:5]:
            src       = hit.get("_source", {})
            form_type = src.get("form_type", "D")
            file_date = src.get("file_date")
            entity    = src.get("entity_name", company_name)
            offering  = src.get("total_offering_amount")

            if not file_date:
                continue

            try:
                ev_date = date.fromisoformat(file_date[:10])
            except ValueError:
                ev_date = date.today()

            amount_usd_mn = None
            if offering:
                try:
                    amount_usd_mn = round(float(offering) / 1_000_000, 2)
                except (ValueError, TypeError):
                    pass

            # Valide Runde: Betrag ≥ 0.1M oder kein Betrag aber Filing vorhanden
            filing_url = (
                f"https://www.sec.gov/cgi-bin/browse-edgar"
                f"?action=getcompany&filenum={src['file_num']}"
                if src.get("file_num") else None
            )

            rounds.append({
                "date":              ev_date.isoformat(),
                "type":              "Venture",   # Form D = Venture by default
                "amount_usd_mn":     amount_usd_mn,
                "lead_investor":     None,        # Form D enthält keine Investor-Namen
                "source_url":        filing_url,
                "raw_text":          f"{form_type} — {entity} — {file_date}",
                "enrichment_source": "edgar_form_d",
            })
            logger.info(
                "Form D: %s %s %.1fM",
                company_name, ev_date,
                amount_usd_mn or 0,
            )

    except Exception as e:
        logger.warning("EDGAR Form D failed for %s: %s", company_name, e)

    return rounds


# ── TechCrunch Funding NER ────────────────────────────────────────────────────

async def enrich_from_techcrunch(
    company_id: str,
    company_name: str,
    feed_text: str,
    client: httpx.AsyncClient,
    lookback_days: int = 30,
) -> list[dict]:
    """
    B-05: Extrahiert Funding-Runden aus TechCrunch RSS.
    Nutzt gecachten Feed — kein zusätzlicher HTTP-Call.
    Claude-NER für Lead-Investor-Extraktion wenn API-Key vorhanden.
    """
    rounds: list[dict] = []
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
            if not _name_in_text(company_name, f"{title} {description}"):
                continue

            # Nur Funding-Artikel
            text = f"{title} {description}".lower()
            funding_kw = ["raises", "raised", "funding", "series", "seed", "round", "investment"]
            if not any(kw in text for kw in funding_kw):
                continue

            amount = _extract_funding_amount(f"{title} {description[:400]}")
            stage  = _infer_stage(f"{title} {description}")

            # Lead-Investor via Claude extrahieren (wenn API-Key vorhanden)
            lead_investor = await _extract_lead_investor_claude(
                f"{title}\n{description[:300]}", client
            )

            if amount is None and lead_investor is None:
                continue   # Zu wenig Signal — überspringen

            rounds.append({
                "date":              ev_date.isoformat(),
                "type":              stage,
                "amount_usd_mn":     amount,
                "lead_investor":     lead_investor,
                "source_url":        link or None,
                "raw_text":          f"{title[:500]}",
                "enrichment_source": "techcrunch",
            })
            logger.info(
                "TechCrunch Funding: %s %s %s %.1fM lead=%s",
                company_name, ev_date, stage, amount or 0, lead_investor or "—",
            )

    except Exception as e:
        logger.warning("TechCrunch funding NER failed for %s: %s", company_name, e)

    return rounds


async def _extract_lead_investor_claude(text: str, client: httpx.AsyncClient) -> str | None:
    """
    Claude-NER: Extrahiert Lead-Investor aus Funding-Artikel.
    Gibt None zurück wenn API-Key fehlt oder kein Investor erkennbar.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        resp = await client.post(
            _CLAUDE_URL,
            headers={
                "x-api-key":        api_key,
                "anthropic-version": "2023-06-01",
                "content-type":     "application/json",
            },
            json={
                "model":      _CLAUDE_MODEL,
                "max_tokens": 64,
                "system": (
                    "Du extrahierst den Lead-Investor aus einem Funding-Artikel. "
                    "Antworte NUR mit dem Investorennamen oder 'null' wenn nicht erkennbar. "
                    "Kein JSON, kein Markdown, nur der Name."
                ),
                "messages": [{"role": "user", "content": text}],
            },
            timeout=8.0,
        )
        if resp.status_code != 200:
            return None
        raw = resp.json()["content"][0]["text"].strip()
        return None if raw.lower() in ("null", "—", "", "none") else raw[:100]
    except Exception:
        return None


# ── Google News Funding NER ───────────────────────────────────────────────────

async def enrich_from_google_news(
    company_id: str,
    company_name: str,
    client: httpx.AsyncClient,
    lookback_days: int = 30,
) -> list[dict]:
    """
    B-05: Fallback-Quelle für Companies ohne EDGAR/TC-Daten.
    Fokussiert auf Funding-Keywords in zwei Sprachen.
    """
    rounds: list[dict] = []
    cutoff = date.today() - timedelta(days=lookback_days)

    queries = [
        f"{company_name} funding round raised million",
        f"{company_name} Finanzierungsrunde Millionen Euro",
    ]

    for query in queries:
        try:
            resp = await client.get(
                _GNEWS_BASE,
                params={"q": query, "hl": "en", "gl": "US", "ceid": "US:en"},
                timeout=8.0,
            )
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.text)
            for item in root.findall(".//item")[:6]:
                title    = (item.findtext("title") or "").strip()
                desc     = (item.findtext("description") or "").strip()
                link     = (item.findtext("link") or "").strip()
                pub_date = item.findtext("pubDate")
                ev_date  = _parse_rss_date(pub_date)

                if ev_date < cutoff:
                    continue
                if not _name_in_text(company_name, f"{title} {desc}"):
                    continue

                amount = _extract_funding_amount(f"{title} {desc[:300]}")
                if not amount:
                    continue   # Google News: nur mit extrahiertem Betrag

                stage = _infer_stage(f"{title} {desc}")
                rounds.append({
                    "date":              ev_date.isoformat(),
                    "type":              stage,
                    "amount_usd_mn":     amount,
                    "source_url":        link or None,
                    "raw_text":          f"{title[:500]}",
                    "enrichment_source": "google_news",
                })
                logger.info(
                    "Google News Funding: %s %s %.1fM", company_name, ev_date, amount
                )
        except Exception as e:
            logger.warning("Google News funding NER failed for %s: %s", company_name, e)

    # Deduplizieren: gleicher Monat + ähnlicher Betrag → nur erster
    seen: set[tuple] = set()
    deduped = []
    for r in rounds:
        key = (r["date"][:7], round((r["amount_usd_mn"] or 0) / 10) * 10)
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    return deduped


# ── Haupt-Pipeline ────────────────────────────────────────────────────────────

async def run_funding_enrichment(
    companies: list[dict] | None = None,
    days_since_last: int = 7,
) -> dict[str, int]:
    """
    B-05: Haupt-Pipeline für Funding Enrichment.
    Läuft täglich via Cron NACH signal_engine (nutzt bereits extrahierte Signale).

    Args:
        companies:       Optional — Liste von Company-Dicts. None → aus DB laden.
        days_since_last: Nur Companies enrichen die seit N Tagen kein Update hatten.

    Returns:
        dict mit Statistiken: {companies_processed, rounds_written, rounds_skipped}
    """
    stats = {"companies_processed": 0, "rounds_written": 0, "rounds_skipped": 0}

    if companies is None:
        companies = fetch_companies_for_funding_enrichment(days_since_last=days_since_last)

    if not companies:
        logger.info("B-05: Keine Companies für Funding-Enrichment.")
        return stats

    logger.info("B-05: Funding-Enrichment für %d Companies", len(companies))

    timeout = httpx.Timeout(12.0, connect=4.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": "ArgoAnalytics/1.0 (research; contact@argo-analytics.io)"},
        follow_redirects=True,
    ) as client:

        # TechCrunch Feed einmal laden (Cache)
        tc_feed: str | None = None
        try:
            tc_resp = await client.get(_TC_FEED, timeout=8.0)
            if tc_resp.status_code == 200:
                tc_feed = tc_resp.text
                logger.info("B-05: TechCrunch Feed geladen (%d bytes)", len(tc_feed))
        except Exception as e:
            logger.warning("B-05: TechCrunch Feed fehlgeschlagen: %s", e)

        for company in companies:
            cid    = company.get("id", "")
            cname  = company.get("name", "")
            ticker = company.get("ticker")
            region = company.get("region", "")

            logger.info("B-05: Enriching %s (region=%s)", cname, region)
            new_rounds: list[dict] = []

            # 1. EDGAR Form D — US-Companies oder ohne klare Region
            if region in ("US", "", None) or ticker:
                edgar_rounds = await enrich_from_edgar_form_d(
                    cid, cname, ticker, client, lookback_days=90
                )
                new_rounds.extend(edgar_rounds)
                if edgar_rounds:
                    await asyncio.sleep(0.5)   # EDGAR Rate-Limit

            # 2. TechCrunch (aus Cache)
            if tc_feed:
                tc_rounds = await enrich_from_techcrunch(
                    cid, cname, tc_feed, client, lookback_days=30
                )
                new_rounds.extend(tc_rounds)

            # 3. Google News — Fallback wenn keine anderen Ergebnisse
            if not new_rounds:
                gn_rounds = await enrich_from_google_news(
                    cid, cname, client, lookback_days=30
                )
                new_rounds.extend(gn_rounds)
                if gn_rounds:
                    await asyncio.sleep(0.3)

            # 4. Signals-Tabelle als zusätzliche Quelle nutzen
            #    (funding_amount bereits extrahiert von signal_engine)
            sig_rounds = _rounds_from_signals(cid, cname)
            new_rounds.extend(sig_rounds)

            # 5. In DB schreiben
            for r in new_rounds:
                r["company_id"] = cid
                ok = upsert_funding_round(cid, r)
                if ok:
                    stats["rounds_written"] += 1
                else:
                    stats["rounds_skipped"] += 1

            # Timestamp aktualisieren
            update_last_funding_enriched_at(cid)
            stats["companies_processed"] += 1

            logger.info(
                "B-05: %s → %d neue Runden (%d geschrieben, %d skip)",
                cname, len(new_rounds),
                stats["rounds_written"], stats["rounds_skipped"],
            )

    logger.info(
        "B-05: Fertig — %d Companies, %d Runden geschrieben, %d Skip",
        stats["companies_processed"], stats["rounds_written"], stats["rounds_skipped"],
    )
    return stats


async def enrich_one_company_funding(
    company_id: str,
    company_name: str,
    ticker: str | None = None,
    region: str | None = None,
) -> dict:
    """
    FUNDING-OD-01: On-Demand Funding-Enrichment für eine EINZELNE Company.

    Schließt die Cold-Path-Lücke: funding_rounds wurde bisher NUR vom 04:30-Cron
    befüllt. Eine frische private Company hatte beim ersten Load leere Runden →
    kein FundingMomentum-Score, kein Stage-Badge, keine Funding-History.

    Thin Wrapper um run_funding_enrichment — übergibt die eine Company als Liste,
    damit die gesamte Pipeline-Logik (EDGAR Form D → TechCrunch → Google News →
    Signals) unberührt bleibt. Kein Code-Duplikat, kein Drift.

    Gate: company_id + company_name Pflicht. Idempotent: upsert_funding_round
    nutzt UNIQUE(company_id, date, type) → Re-Trigger schreibt nichts Doppeltes.
    Caller-Gate in company_detail.py: feuert nur wenn db_rounds leer (bereits geladen).
    """
    if not company_id or not company_name:
        return {"rounds_written": 0, "rounds_skipped": 0, "companies_processed": 0}

    company_dict = {
        "id":     company_id,
        "name":   company_name,
        "ticker": ticker,
        "region": region or "",
    }
    try:
        stats = await run_funding_enrichment(companies=[company_dict], days_since_last=0)
        logger.info(
            "FUNDING-OD-01: '%s' — %d Runden geschrieben, %d skip",
            company_name, stats.get("rounds_written", 0), stats.get("rounds_skipped", 0),
        )
        return stats
    except Exception as e:
        logger.warning("FUNDING-OD-01: failed für '%s': %s", company_name, e)
        return {"rounds_written": 0, "rounds_skipped": 0, "companies_processed": 0}


def _rounds_from_signals(company_id: str, company_name: str) -> list[dict]:
    """
    B-05: Konvertiert funding_signals (aus signals-Tabelle) in funding_round-Dicts.
    Nutzt bereits extrahierte funding_amount_usd_mn aus signal_engine.
    """
    rounds = []
    try:
        signals = fetch_funding_signals(company_id, limit=10)
        for sig in signals:
            amount = sig.get("funding_amount_usd_mn")
            if not amount:
                continue
            ev_date_raw = sig.get("event_date", "")
            raw_text    = f"{sig.get('raw_title', '')} {sig.get('summary', '')}"
            stage       = _infer_stage(raw_text)
            # FUNDING-DATE-FALLBACK-01: date.today() als Fallback brach die
            # Idempotenz (UNIQUE(company_id, date, type) greift nie, wenn date
            # bei jedem Cron-Lauf neu = heute ist — führte zu wiederkehrenden
            # Dubletten, s. Fervo-Debt-Fall). Jetzt: erst event_date, sonst
            # Datum aus Freitext inferieren (Monat/Jahr, Jahr allein reicht für
            # die UNIQUE-Dedup), nur wenn wirklich nichts erkennbar ist: Signal
            # überspringen statt ein Datum zu erfinden — no data > wrong data.
            if ev_date_raw:
                round_date = ev_date_raw[:10]
            else:
                inferred = _infer_date_from_text(raw_text)
                if not inferred:
                    continue
                round_date = inferred.isoformat()
            rounds.append({
                "date":              round_date,
                "type":              stage,
                "amount_usd_mn":     amount,
                "source_url":        sig.get("source_url"),
                "raw_text":          sig.get("summary", "")[:500],
                "enrichment_source": sig.get("source", "google_news"),
            })
    except Exception as e:
        logger.warning("_rounds_from_signals failed for %s: %s", company_id, e)
    return rounds
