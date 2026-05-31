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
    # D&A (für EBITDA) — Income- UND Cashflow-Statement-Varianten.
    # Viele Filer führen D&A NICHT im Income Statement, sondern nur im Cashflow
    # Statement unter abweichenden Tags → ohne diese Varianten fehlt EBITDA
    # systematisch (LanzaTech-Fall). Reihenfolge unkritisch: pro FY gewinnt der
    # letzte Eintrag, alle mappen auf denselben metric-Key.
    "DepreciationDepletionAndAmortization":                              "depreciation_mn",
    "DepreciationAndAmortization":                                       "depreciation_mn",
    "DepreciationAmortizationAndAccretionNet":                           "depreciation_mn",
    "DepreciationDepletionAndAmortizationNonproductionExpense":          "depreciation_mn",
    "Depreciation":                                                      "depreciation_mn",
    "AmortizationOfIntangibleAssets":                                    "depreciation_mn",
    # Equity
    "StockholdersEquity":                                                "equity_mn",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "equity_mn",
    # Total Assets
    "Assets":                                                            "total_assets_mn",
    # ── KPI-06: Neue Metriken ─────────────────────────────────────────────
    # Gross Profit → gross_margin_pct
    "GrossProfit":                                                       "gross_profit_mn",
    # Debt (Long + Short) → reales EV = Mktcap + Debt - Cash
    "LongTermDebt":                                                      "long_term_debt_mn",
    "LongTermDebtNoncurrent":                                            "long_term_debt_mn",
    "ShortTermBorrowings":                                               "short_term_debt_mn",
    "DebtCurrent":                                                       "short_term_debt_mn",
    # Cash → reales EV
    "CashAndCashEquivalentsAtCarryingValue":                             "cash_mn",
    "CashCashEquivalentsAndShortTermInvestments":                        "cash_mn",
    # Operating Cashflow
    "NetCashProvidedByUsedInOperatingActivities":                        "operating_cashflow_mn",
    # CapEx → FCF = OpCF - CapEx
    "PaymentsToAcquirePropertyPlantAndEquipment":                        "capex_mn",
    "PaymentsForCapitalImprovements":                                    "capex_mn",
}

# DEI-Namespace (separate von us-gaap)
_DEI_MAP: dict[str, str] = {
    "EntityNumberOfEmployees":               "headcount",
    "EntityCommonStockSharesOutstanding":    "shares_outstanding",   # KPI-06: für market_cap_bn Derivation
}

# Monetary-Metriken → durch 1_000_000 dividieren (XBRL liefert volle USD)
_MONETARY: frozenset[str] = frozenset({
    "revenue_mn", "net_income_mn", "operating_income_mn",
    "depreciation_mn", "equity_mn", "total_assets_mn",
    # KPI-06
    "gross_profit_mn", "long_term_debt_mn", "short_term_debt_mn",
    "cash_mn", "operating_cashflow_mn", "capex_mn",
})

# Shares-Metriken → XBRL unit key "shares", Wert als absolute Zahl (kein Divider)
_SHARES_UNIT: frozenset[str] = frozenset({"shares_outstanding"})

# Helper-Metriken die nur für EBITDA-Berechnung gebraucht werden.
# operating_income_mn wird NICHT mehr verworfen — es ist EBIT, eine eigene Kennzahl
# (EBIT-Marge). _derive_ebitda gibt es zusätzlich als ebit_mn aus. Nur depreciation_mn
# bleibt reiner Helper (D&A wird nicht separat angezeigt).
_HELPER_METRICS: frozenset[str] = frozenset({"depreciation_mn"})

# Debt-Komponenten: werden zu total_debt_mn summiert, dann aus Output entfernt
_DEBT_COMPONENTS: frozenset[str] = frozenset({"long_term_debt_mn", "short_term_debt_mn"})


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

        unit_key = (
            "USD"    if metric_key in _MONETARY    else
            "shares" if metric_key in _SHARES_UNIT else
            "pure"
        )
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
    operating_income_mn = EBIT → wird als ebit_mn ausgegeben (eigene Kennzahl für
    EBIT-Marge in company_detail). Nur depreciation_mn bleibt reiner Helper.
    """
    op_by_fy  = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "operating_income_mn"}
    dep_by_fy = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "depreciation_mn"}

    # Helper (D&A) rausfiltern. operating_income_mn → ebit_mn umbenennen (behalten!).
    clean = []
    for r in rows:
        if r["metric"] in _HELPER_METRICS:
            continue
        if r["metric"] == "operating_income_mn":
            r = {**r, "metric": "ebit_mn"}
        clean.append(r)

    # EBITDA-Rows wo operating_income UND D&A vorhanden
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


def _derive_debt_and_fcf(rows: list[dict]) -> list[dict]:
    """
    KPI-06:
      total_debt_mn  = long_term_debt_mn + short_term_debt_mn  (pro FY summiert)
      free_cashflow_mn = operating_cashflow_mn - capex_mn       (pro FY)
    Entfernt Debt-Komponenten (long/short) aus Output — nur total_debt_mn bleibt.
    CapEx bleibt als Einzelmetrik erhalten (nützlich für CapEx-Intensitäts-Score).
    """
    lt_by_fy  = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "long_term_debt_mn"}
    st_by_fy  = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "short_term_debt_mn"}
    ocf_by_fy = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "operating_cashflow_mn"}
    cx_by_fy  = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "capex_mn"}

    # Debt-Komponenten rausfiltern
    clean = [r for r in rows if r["metric"] not in _DEBT_COMPONENTS]

    # total_debt_mn: LT + ST (mind. LT muss vorhanden sein)
    all_debt_fys = set(lt_by_fy) | set(st_by_fy)
    for fy in sorted(all_debt_fys):
        lt  = lt_by_fy.get(fy, 0.0)
        st  = st_by_fy.get(fy, 0.0)
        total = round(lt + st, 2)
        if total >= 0:
            clean.append({
                "metric":      "total_debt_mn",
                "fiscal_year": fy,
                "value":       total,
                "currency":    "USD",
                "source":      "edgar_xbrl",
                "confidence":  "high",
            })

    # free_cashflow_mn = OpCF - CapEx (wo beide vorhanden)
    for fy in sorted(set(ocf_by_fy) & set(cx_by_fy)):
        fcf = round(ocf_by_fy[fy] - cx_by_fy[fy], 2)
        clean.append({
            "metric":      "free_cashflow_mn",
            "fiscal_year": fy,
            "value":       fcf,
            "currency":    "USD",
            "source":      "edgar_xbrl",
            "confidence":  "high",
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
    # KPI-06: total_debt_mn + free_cashflow_mn ableiten + Debt-Komponenten entfernen
    rows = _derive_debt_and_fcf(rows)

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
