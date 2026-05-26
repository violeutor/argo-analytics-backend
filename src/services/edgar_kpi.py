"""
edgar_kpi.py — EDGAR XBRL KPI Pipeline
========================================
KPI-01/02/03: Finanzkennzahlen für US Companies aus EDGAR Company Facts API.

Endpunkt: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
Nur 10-K annual filings, neueste 5 Jahre.
Max 50 Companies/Run, EDGAR erlaubt 10 Req/s → 0.15s Sleep zwischen Calls.

Metriken (currency-neutral, analog BA-Bridge):
  revenue_mn, ebitda_mn, net_income_mn, equity_mn, total_assets_mn, headcount
  currency='USD' für Monetary-Felder.

CIK-Lookup: Ticker-Match via company_tickers.json → Fallback EDGAR Search.
Schreibt direkt in Supabase kpi_timeseries (selbe Logik wie kpi_timeseries.py Route).
"""

import asyncio
import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

# ── Konstanten ────────────────────────────────────────────────────────────────

EDGAR_COMPANY_FACTS = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
EDGAR_TICKERS_JSON  = "https://www.sec.gov/files/company_tickers.json"
EDGAR_SEARCH_URL    = "https://efts.sec.gov/LATEST/search-index"

HEADERS = {
    "User-Agent":      "ArgoAnalytics research@argo-analytics.com",
    "Accept-Encoding": "gzip",
}

# XBRL us-gaap Tag → currency-neutraler Metric-Key (analog BA-Bridge METRIC_MAP)
_XBRL_MAP: dict[str, str] = {
    # Revenue (verschiedene GAAP-Tags je Branche/Jahr)
    "Revenues":                                                          "revenue_mn",
    "RevenueFromContractWithCustomerExcludingAssessedTax":               "revenue_mn",
    "SalesRevenueNet":                                                   "revenue_mn",
    "SalesRevenueGoodsNet":                                              "revenue_mn",
    # Net Income
    "NetIncomeLoss":                                                     "net_income_mn",
    # Operating Income (für EBITDA-Annäherung)
    "OperatingIncomeLoss":                                               "operating_income_mn",
    # D&A (für EBITDA)
    "DepreciationDepletionAndAmortization":                              "depreciation_mn",
    "DepreciationAndAmortization":                                       "depreciation_mn",
    # Equity
    "StockholdersEquity":                                                "equity_mn",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "equity_mn",
    # Total Assets
    "Assets":                                                            "total_assets_mn",
}

# DEI-Namespace (separate von us-gaap)
_DEI_MAP: dict[str, str] = {
    "EntityNumberOfEmployees": "headcount",
}

# Monetary-Metriken → durch 1_000_000 dividieren (XBRL liefert volle USD)
_MONETARY: frozenset[str] = frozenset({
    "revenue_mn", "net_income_mn", "operating_income_mn",
    "depreciation_mn", "equity_mn", "total_assets_mn",
})

# Helper-Metriken die nur für EBITDA-Berechnung gebraucht werden
_HELPER_METRICS: frozenset[str] = frozenset({"operating_income_mn", "depreciation_mn"})


# ── CIK Lookup ────────────────────────────────────────────────────────────────

async def _fetch_tickers_map(client: httpx.AsyncClient) -> dict[str, str]:
    """
    Lädt company_tickers.json einmalig pro Pipeline-Run.
    Gibt ticker.upper() -> cik_str (10-stellig) zurück.
    Wird von run_edgar_kpi_pipeline gecacht und weitergereicht.
    """
    try:
        resp = await client.get(EDGAR_TICKERS_JSON, timeout=15)
        if resp.status_code != 200:
            logger.warning("EDGAR tickers.json HTTP %s", resp.status_code)
            return {}
        return {
            (entry.get("ticker") or "").upper(): str(entry["cik_str"]).zfill(10)
            for entry in resp.json().values()
            if entry.get("ticker") and entry.get("cik_str")
        }
    except Exception as e:
        logger.warning("EDGAR tickers.json fetch failed: %s", e)
        return {}


def _lookup_cik_by_ticker(ticker: str, tickers_map: dict[str, str]) -> str | None:
    """
    CIK-Lookup aus gecachtem tickers_map.
    Stripped Exchange-Suffix (SIE.DE -> SIE, H2O.DE -> H2O) vor dem Match.
    """
    ticker_clean = ticker.upper().split(".")[0]
    return tickers_map.get(ticker_clean)


async def _lookup_cik_by_name(company_name: str, client: httpx.AsyncClient) -> str | None:
    """
    Fallback CIK-Lookup via EDGAR Full-Text-Search (10-K Filings).
    Gibt die entity_id des ersten Treffers zurück.
    """
    try:
        resp = await client.get(
            EDGAR_SEARCH_URL,
            params={"q": f'"{company_name}"', "forms": "10-K"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        hits = resp.json().get("hits", {}).get("hits", [])
        if not hits:
            return None
        entity_id = hits[0].get("_source", {}).get("entity_id", "")
        return str(entity_id).zfill(10) if entity_id else None
    except Exception as e:
        logger.debug("CIK name lookup failed for '%s': %s", company_name, e)
    return None


# ── XBRL Extraktion ───────────────────────────────────────────────────────────

def _extract_xbrl_values(
    facts: dict,
    tag_map: dict[str, str],
    namespace: str,
    cutoff_year: int,
) -> list[dict]:
    """
    Extrahiert Werte aus einem XBRL-Namespace (us-gaap oder dei).
    Nur 10-K/10-K/A, nur fiscal_year >= cutoff_year.
    Bei mehreren Einträgen für (metric, fy): neuesten nehmen (letzter in Liste = neueste Einreichung).
    """
    ns_data = facts.get(namespace, {})
    # (metric, fy) → value — letzter Eintrag gewinnt (chronologisch in EDGAR sortiert)
    best: dict[tuple[str, int], dict] = {}

    for xbrl_tag, metric_key in tag_map.items():
        tag_data = ns_data.get(xbrl_tag, {})
        if not tag_data:
            continue

        unit_key = "USD" if metric_key in _MONETARY else "pure"
        for entry in tag_data.get("units", {}).get(unit_key, []):
            if entry.get("form") not in ("10-K", "10-K/A"):
                continue
            fy = entry.get("fy")
            if not fy or fy < cutoff_year:
                continue
            val = entry.get("val")
            if val is None:
                continue

            # Monetary: in Millionen umrechnen
            value = round(val / 1_000_000, 2) if metric_key in _MONETARY else float(val)
            key = (metric_key, fy)
            best[key] = {
                "metric":      metric_key,
                "fiscal_year": fy,
                "value":       value,
                "currency":    "USD" if metric_key in _MONETARY else None,
                "source":      "edgar_xbrl",
                "confidence":  "high",
            }

    return list(best.values())


def _derive_ebitda(rows: list[dict]) -> list[dict]:
    """
    Berechnet ebitda_mn = operating_income_mn + depreciation_mn wo beide vorhanden.
    Entfernt Helper-Metriken (operating_income_mn, depreciation_mn) aus Output.
    """
    op_by_fy  = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "operating_income_mn"}
    dep_by_fy = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "depreciation_mn"}

    # Helper-Metriken rausfiltern
    clean = [r for r in rows if r["metric"] not in _HELPER_METRICS]

    # EBITDA-Rows wo beide vorhanden
    for fy in sorted(set(op_by_fy) & set(dep_by_fy)):
        clean.append({
            "metric":      "ebitda_mn",
            "fiscal_year": fy,
            "value":       round(op_by_fy[fy] + dep_by_fy[fy], 2),
            "currency":    "USD",
            "source":      "edgar_xbrl",
            "confidence":  "medium",   # Approximation: OperatingIncome + D&A ≠ exaktes EBITDA
        })

    return clean


# ── Haupt-Fetch ───────────────────────────────────────────────────────────────

async def fetch_edgar_kpis(
    company_name: str,
    ticker: str | None = None,
    tickers_map: dict[str, str] | None = None,
) -> list[dict]:
    """
    Holt KPI-Rows für eine Company aus der EDGAR Company Facts API.
    Gibt Liste von KPIRow-kompatiblen Dicts zurück (leer wenn kein EDGAR-Treffer).

    CIK-Lookup: tickers_map (gecacht) → Fallback company name search.
    tickers_map wird von run_edgar_kpi_pipeline einmalig pro Run geladen.
    """
    cutoff_year = datetime.now(timezone.utc).year - 5

    async with httpx.AsyncClient(headers=HEADERS, timeout=12) as client:
        # CIK Lookup — gecachter Map bevorzugt (kein extra HTTP-Call pro Company)
        cik = None
        if ticker and tickers_map is not None:
            cik = _lookup_cik_by_ticker(ticker, tickers_map)
        if not cik:
            cik = await _lookup_cik_by_name(company_name, client)
        if not cik:
            logger.debug("EDGAR KPI: kein CIK für '%s' — übersprungen", company_name)
            return []

        # Company Facts API
        try:
            resp = await client.get(EDGAR_COMPANY_FACTS.format(cik=cik), timeout=15)
        except Exception as e:
            logger.warning("EDGAR Company Facts fetch failed für '%s': %s", company_name, e)
            return []

        if resp.status_code != 200:
            logger.warning(
                "EDGAR Company Facts HTTP %s für '%s' (CIK %s)",
                resp.status_code, company_name, cik,
            )
            return []

        facts = resp.json().get("facts", {})

    # XBRL extrahieren
    rows  = _extract_xbrl_values(facts, _XBRL_MAP, "us-gaap", cutoff_year)
    rows += _extract_xbrl_values(facts, _DEI_MAP,  "dei",     cutoff_year)

    # EBITDA ableiten + Helper-Metriken entfernen
    rows = _derive_ebitda(rows)

    logger.info(
        "EDGAR KPI: %d rows für '%s' (CIK %s, cutoff %d)",
        len(rows), company_name, cik, cutoff_year,
    )
    return rows


# ── Pipeline (KPI-03) ─────────────────────────────────────────────────────────

async def run_edgar_kpi_pipeline(companies: list[dict]) -> dict:
    """
    KPI-03: Läuft täglich 05:15 UTC für alle Companies in der DB.
    EDGAR liefert nur US-Companies — CIK-Lookup schlägt für EU-Companies still fehl.
    Keine manuelle Filterung nötig: Non-US → kein CIK → übersprungen.
    Max 50 Companies/Run. EDGAR erlaubt 10 Req/s → 0.15s Sleep.
    Schreibt direkt in Supabase kpi_timeseries (Upsert, kein Überschreiben).
    """
    from src.integrations.supabase import get_supabase, fetch_company_by_name

    stats = {"companies_processed": 0, "rows_written": 0, "rows_skipped": 0, "errors": 0}
    db = get_supabase()

    # tickers_map einmalig laden — verhindert 50x Download von company_tickers.json (~7MB)
    async with httpx.AsyncClient(headers=HEADERS, timeout=15) as _client:
        tickers_map = await _fetch_tickers_map(_client)
    logger.info("EDGAR KPI: tickers_map geladen — %d Einträge", len(tickers_map))

    for company in companies[:50]:
        name   = company.get("name", "")
        ticker = company.get("ticker_yf") or company.get("ticker") or None
        if not name:
            continue

        try:
            kpi_rows = await fetch_edgar_kpis(name, ticker, tickers_map=tickers_map)
            if not kpi_rows:
                await asyncio.sleep(0.15)
                continue

            # Company-ID aus Supabase
            co = fetch_company_by_name(name)
            if not co:
                logger.debug("EDGAR KPI: Company '%s' nicht in DB", name)
                continue

            company_id = co["id"]
            written = skipped = 0

            for row in kpi_rows:
                payload = {
                    "company_id":  company_id,
                    "metric":      row["metric"],
                    "fiscal_year": row["fiscal_year"],
                    "value":       row["value"],
                    "source":      row["source"],
                }
                if row.get("currency"):
                    payload["currency"] = row["currency"]
                if row.get("confidence"):
                    payload["confidence"] = row["confidence"]

                try:
                    result = (
                        db.table("kpi_timeseries")
                        .upsert(
                            payload,
                            on_conflict="company_id,metric,fiscal_year,source",
                            ignore_duplicates=True,
                        )
                        .execute()
                    )
                    if result.data:
                        written += 1
                    else:
                        skipped += 1
                except Exception as e:
                    logger.debug("KPI upsert skip für '%s' %s FY%s: %s",
                                 name, row["metric"], row["fiscal_year"], e)
                    skipped += 1

            stats["companies_processed"] += 1
            stats["rows_written"]  += written
            stats["rows_skipped"]  += skipped
            logger.info(
                "EDGAR KPI: '%s' — %d rows written, %d skipped",
                name, written, skipped,
            )

        except Exception as e:
            logger.warning("EDGAR KPI pipeline failed für '%s': %s", name, e)
            stats["errors"] += 1

        await asyncio.sleep(0.15)   # 6–7 req/s — unter EDGAR-Limit von 10/s

    logger.info(
        "EDGAR KPI Pipeline fertig — %d Companies, %d rows written, %d skipped, %d errors",
        stats["companies_processed"], stats["rows_written"],
        stats["rows_skipped"], stats["errors"],
    )
    return stats
