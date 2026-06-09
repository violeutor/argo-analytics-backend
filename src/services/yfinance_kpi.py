"""
yfinance_kpi.py — yfinance KPI-Zeitreihe für EU/DE/nicht-US-Listed Companies
==============================================================================
YH-KPI-TS-01: Füllt kpi_timeseries für Companies die EDGAR nicht abdeckt.

Quelle: yfinance t.income_stmt / t.balance_sheet / t.cashflow (annual)
Analogon zu edgar_kpi.py — selbe Metric-Keys, selbe _write_kpi_rows-Logik,
selber Upsert-Conflict: (company_id, metric, fiscal_year, source).

Source-Label: "yfinance"
Currency: aus t.fast_info.currency (EUR/GBP/CHF etc. je Listing)

Gate (On-Demand + Cron):
  - is_listed = True
  - NOT _looks_us_listed(ticker, exchange) — US-Companies bleiben bei EDGAR
  - Hat ticker_yf oder ticker (mit Exchange-Suffix, z.B. "SIE.DE")

Trigger:
  - On-Demand:  Cold-Path EU-Listed in company_detail.py → BackgroundTask
                → _trigger_yf_kpi_ondemand → enrich_one_company_yf()
  - Cron:       05:20 UTC täglich (nach EDGAR 05:15 UTC)
                → run_yfinance_kpi_pipeline(companies)

Metrik-Keys (identisch edgar_kpi.py / kpi_timeseries Route):
  revenue_mn, gross_profit_mn, ebit_mn, ebitda_mn, net_income_mn
  equity_mn, total_assets_mn, cash_mn, total_debt_mn
  operating_cashflow_mn, capex_mn, free_cashflow_mn

Abgeleitete Metriken (on-demand im GET-Endpoint via _add_derived_metrics):
  ebitda_margin_pct, equity_ratio_pct, revenue_per_fte_k, revenue_cagr_pct
  → werden NICHT hier geschrieben, sondern live in kpi_timeseries.py berechnet.

Suffix-Fallback: .DE → .F (Frankfurt) — analog _fetch_yf_fundamentals.
"""

import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ── Metric Maps ───────────────────────────────────────────────────────────────

# yfinance income_stmt Zeilennamen → metric_key.
# Mehrere Zeilennamen können auf denselben Key mappen — _YF_PRIORITY entscheidet.
_YF_INCOME_MAP: dict[str, str] = {
    "Total Revenue":                        "revenue_mn",
    "Gross Profit":                         "gross_profit_mn",
    "EBIT":                                 "ebit_mn",
    "Operating Income":                     "ebit_mn",             # Alias
    "EBITDA":                               "ebitda_mn",           # Direkt wenn vorhanden
    "Net Income":                           "net_income_mn",
    "Net Income Common Stockholders":       "net_income_mn",       # Alias
    "Reconciled Depreciation":              "depreciation_mn",     # Für EBITDA-Derivation
    "Depreciation Amortization Depletion":  "depreciation_mn",     # Alias
    "Depreciation And Amortization":        "depreciation_mn",     # Alias
}

# yfinance balance_sheet Zeilennamen → metric_key.
_YF_BALANCE_MAP: dict[str, str] = {
    "Stockholders Equity":                              "equity_mn",
    "Common Stock Equity":                              "equity_mn",        # Alias
    "Total Equity Gross Minority Interest":             "equity_mn",        # Niedrigste Prio
    "Total Assets":                                     "total_assets_mn",
    "Cash And Cash Equivalents":                        "cash_mn",
    "Cash Cash Equivalents And Short Term Investments": "cash_mn",          # Niedrigere Prio
    "Total Debt":                                       "total_debt_mn",    # Direkt — bevorzugt
    "Long Term Debt":                                   "long_term_debt_mn",
    "Current Debt":                                     "short_term_debt_mn",
    "Short Term Debt":                                  "short_term_debt_mn",  # Alias
}

# yfinance cashflow Zeilennamen → metric_key.
_YF_CASHFLOW_MAP: dict[str, str] = {
    "Operating Cash Flow": "operating_cashflow_mn",
    "Capital Expenditure": "capex_mn",          # yfinance: negativ → abs() gesetzt
    "Free Cash Flow":      "free_cashflow_mn",  # Direkt wenn vorhanden
}

# Zeilennamen-Priorität bei Kollision auf denselben metric_key (höher = bevorzugt).
# Analog _TAG_PRIORITY in edgar_kpi.py.
_YF_PRIORITY: dict[str, int] = {
    # equity: ohne Minderheitsanteile ist sauberer
    "Stockholders Equity":                              2,
    "Common Stock Equity":                              1,
    "Total Equity Gross Minority Interest":             0,
    # cash: reines Cash bevorzugt (analog edgar_kpi.py _TAG_PRIORITY)
    "Cash And Cash Equivalents":                        2,
    "Cash Cash Equivalents And Short Term Investments": 1,
    # net_income: Total-NetIncome schlägt per-Stammaktionäre
    "Net Income":                                       2,
    "Net Income Common Stockholders":                   1,
    # ebit: direkte EBIT-Zeile schlägt Operating Income Alias
    "EBIT":                                             2,
    "Operating Income":                                 1,
    # depreciation: Reconciled ist buchhalterisch exakter
    "Reconciled Depreciation":                          2,
    "Depreciation Amortization Depletion":              1,
    "Depreciation And Amortization":                    0,
}

# capex: yfinance liefert negativen Cashflow-Abfluss → als positiven Betrag speichern.
_NEGATE_KEYS: frozenset[str] = frozenset({"capex_mn"})

# Helper-Metriken: nur für EBITDA-Derivation gebraucht — nicht im finalen Output.
_HELPER_METRICS: frozenset[str] = frozenset({"depreciation_mn"})

# Debt-Komponenten: → total_debt_mn summiert, Einzelwerte aus Output entfernt.
_DEBT_COMPONENTS: frozenset[str] = frozenset({"long_term_debt_mn", "short_term_debt_mn"})


# ── DataFrame Extraktion ──────────────────────────────────────────────────────

def _extract_statement(
    df,                      # pd.DataFrame (import pandas inside _fetch_sync)
    metric_map: dict[str, str],
    currency: str,
    cutoff_year: int,
) -> list[dict]:
    """
    Extrahiert KPI-Rows aus einem yfinance Statement-DataFrame.

    Columns = Timestamps (Periodenende), Index = Zeilennamen.
    Werte in nativer Währung, absolute Zahlen → durch 1_000_000 dividieren.
    Bei mehreren Zeilennamen auf denselben (metric_key, fiscal_year):
    _YF_PRIORITY entscheidet (höher = bevorzugt).
    """
    import pandas as pd

    # (metric_key, fiscal_year) → beste Row
    best: dict[tuple[str, int], dict] = {}

    for col in df.columns:
        try:
            fiscal_year = col.year   # Timestamp → Jahreszahl des Periodenendstichpunkts
        except AttributeError:
            continue
        if fiscal_year < cutoff_year:
            continue

        for row_name, metric_key in metric_map.items():
            if row_name not in df.index:
                continue
            val = df.loc[row_name, col]
            if pd.isna(val):
                continue

            try:
                value_mn = round(float(val) / 1_000_000, 2)
            except (TypeError, ValueError):
                continue

            # capex: yfinance liefert negativen Cashflow-Abfluss → positiv speichern
            if metric_key in _NEGATE_KEYS:
                value_mn = abs(value_mn)

            key  = (metric_key, fiscal_year)
            prio = _YF_PRIORITY.get(row_name, 0)
            prev = best.get(key)

            if prev is None or prio > prev["_prio"]:
                best[key] = {
                    "metric":      metric_key,
                    "fiscal_year": fiscal_year,
                    "value":       value_mn,
                    "currency":    currency,
                    "source":      "yfinance",
                    "confidence":  "high",
                    "_prio":       prio,
                }

    # _prio ist ein internes Feld — vor Rückgabe entfernen
    return [{k: v for k, v in row.items() if not k.startswith("_")} for row in best.values()]


# ── EBITDA / Debt / FCF Derivation (analog edgar_kpi.py) ─────────────────────

def _derive_ebitda(rows: list[dict]) -> list[dict]:
    """
    ebitda_mn = ebit_mn + depreciation_mn, wo beide vorhanden UND kein direktes
    ebitda_mn gesetzt (direkter Wert aus income_stmt hat Vorrang).
    depreciation_mn (Helper) danach aus Output entfernt.
    confidence="medium" — EBIT + D&A ist Approximation (Lease-Corrections etc. fehlen).
    """
    ebit_by_fy   = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "ebit_mn"}
    dep_by_fy    = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "depreciation_mn"}
    ebitda_by_fy = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "ebitda_mn"}
    currency     = next((r["currency"] for r in rows if r.get("currency")), "EUR")

    clean = [r for r in rows if r["metric"] not in _HELPER_METRICS]

    for fy in sorted(set(ebit_by_fy) & set(dep_by_fy)):
        if fy in ebitda_by_fy:
            continue   # direktes EBITDA hat Vorrang — nicht überschreiben
        clean.append({
            "metric":      "ebitda_mn",
            "fiscal_year": fy,
            "value":       round(ebit_by_fy[fy] + dep_by_fy[fy], 2),
            "currency":    currency,
            "source":      "yfinance",
            "confidence":  "medium",
        })

    return clean


def _derive_debt_and_fcf(rows: list[dict]) -> list[dict]:
    """
    total_debt_mn:
      Direkt aus Bilanz (Total Debt) → bevorzugt, kein Ableiten nötig.
      Fallback: long_term_debt_mn + short_term_debt_mn (analog edgar_kpi.py).
      Debt-Komponenten immer aus Output entfernen.

    free_cashflow_mn:
      Direkt aus Cashflow-Statement → bevorzugt.
      Fallback: operating_cashflow_mn − capex_mn.
    """
    lt_by_fy     = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "long_term_debt_mn"}
    st_by_fy     = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "short_term_debt_mn"}
    total_by_fy  = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "total_debt_mn"}
    ocf_by_fy    = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "operating_cashflow_mn"}
    cx_by_fy     = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "capex_mn"}
    fcf_by_fy    = {r["fiscal_year"]: r["value"] for r in rows if r["metric"] == "free_cashflow_mn"}
    currency     = next((r["currency"] for r in rows if r.get("currency")), "EUR")

    clean = [r for r in rows if r["metric"] not in _DEBT_COMPONENTS]

    # total_debt_mn: aus LT+ST ableiten, nur wo kein direktes total_debt_mn vorhanden
    for fy in sorted(set(lt_by_fy) | set(st_by_fy)):
        if fy in total_by_fy:
            continue
        total = round(lt_by_fy.get(fy, 0.0) + st_by_fy.get(fy, 0.0), 2)
        if total >= 0:
            clean.append({
                "metric":      "total_debt_mn",
                "fiscal_year": fy,
                "value":       total,
                "currency":    currency,
                "source":      "yfinance",
                "confidence":  "high",
            })

    # free_cashflow_mn: ableiten wo nicht direkt vorhanden
    for fy in sorted(set(ocf_by_fy) & set(cx_by_fy)):
        if fy in fcf_by_fy:
            continue
        clean.append({
            "metric":      "free_cashflow_mn",
            "fiscal_year": fy,
            "value":       round(ocf_by_fy[fy] - cx_by_fy[fy], 2),
            "currency":    currency,
            "source":      "yfinance",
            "confidence":  "high",
        })

    return clean


# ── Synchroner Fetch (für asyncio.to_thread) ──────────────────────────────────

def _fetch_yf_kpis_sync(ticker: str, cutoff_year: int) -> list[dict]:
    """
    Synchroner yfinance-Abruf — wird via asyncio.to_thread ausgeführt.

    Suffix-Fallback: .DE → .F (Frankfurt) analog _fetch_yf_fundamentals.
    Sanity-Check via fast_info.last_price — unbekannte Ticker haben keinen Preis.
    Gibt leere Liste zurück wenn Ticker unbekannt oder Statements fehlen.
    """
    import yfinance as yf
    import pandas as pd

    candidates = [ticker]
    if ticker.upper().endswith(".DE"):
        candidates.append(ticker[:-3] + ".F")
    elif ticker.upper().endswith(".F"):
        candidates.append(ticker[:-2] + ".DE")

    for sym in candidates:
        try:
            t        = yf.Ticker(sym)
            currency = getattr(t.fast_info, "currency", None) or "EUR"

            # Sanity-Check: Ticker mit Preis → bekannt und handelbar
            price = getattr(t.fast_info, "last_price", None)
            if not price:
                logger.debug("YF KPI: kein Preis für %s — Fallback oder skip", sym)
                continue

            all_rows: list[dict] = []

            inc = t.income_stmt
            if inc is not None and not inc.empty:
                all_rows += _extract_statement(inc, _YF_INCOME_MAP, currency, cutoff_year)

            bs = t.balance_sheet
            if bs is not None and not bs.empty:
                all_rows += _extract_statement(bs, _YF_BALANCE_MAP, currency, cutoff_year)

            cf = t.cashflow
            if cf is not None and not cf.empty:
                all_rows += _extract_statement(cf, _YF_CASHFLOW_MAP, currency, cutoff_year)

            if not all_rows:
                logger.debug("YF KPI: keine Statement-Rows für %s", sym)
                continue

            all_rows = _derive_ebitda(all_rows)
            all_rows = _derive_debt_and_fcf(all_rows)

            logger.info(
                "YF KPI: %d rows für %s (currency=%s, cutoff=%d)",
                len(all_rows), sym, currency, cutoff_year,
            )
            return all_rows

        except Exception as e:
            logger.debug("YF KPI sync failed für %s: %s", sym, e)
            continue

    return []


async def fetch_yf_kpis(ticker: str) -> list[dict]:
    """
    Async Wrapper um _fetch_yf_kpis_sync (via asyncio.to_thread).
    Timeout: 12s — yfinance kann bei .DE-Symbolen langsam sein.
    """
    cutoff_year = datetime.now(timezone.utc).year - 5
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_fetch_yf_kpis_sync, ticker, cutoff_year),
            timeout=12.0,
        )
    except asyncio.TimeoutError:
        logger.warning("YF KPI: Timeout für %s", ticker)
        return []
    except Exception as e:
        logger.warning("YF KPI: fetch_yf_kpis failed für %s: %s", ticker, e)
        return []


# ── On-Demand (YH-KPI-OD-01) ─────────────────────────────────────────────────

async def enrich_one_company_yf(
    company_id: str,
    name: str,
    ticker: str | None = None,
) -> dict:
    """
    YH-KPI-OD-01: On-Demand-Enrichment einer EINZELNEN EU/DE-Listed Company.

    Aufgerufen aus _trigger_yf_kpi_ondemand in company_detail.py als
    Fire-and-Forget BackgroundTask beim Cold-Load.

    ticker muss bereits ein gültiges yfinance-Symbol sein (z.B. "SIE.DE") —
    Auflösung via _resolve_yf_symbol() geschieht im Caller.

    Idempotent: upsert mit ignore_duplicates → Re-Trigger schreibt nichts.
    Gibt {rows_written, rows_skipped, found} zurück.
    """
    from src.integrations.supabase import get_supabase
    from src.services.edgar_kpi import _write_kpi_rows   # SSOT für Write-Logik

    if not ticker or not name:
        return {"rows_written": 0, "rows_skipped": 0, "found": False}

    try:
        kpi_rows = await fetch_yf_kpis(ticker)
    except Exception as e:
        logger.warning("YH-KPI-OD-01: fetch failed für '%s' (%s): %s", name, ticker, e)
        return {"rows_written": 0, "rows_skipped": 0, "found": False}

    if not kpi_rows:
        logger.info("YH-KPI-OD-01: keine Rows für '%s' (ticker=%s)", name, ticker)
        return {"rows_written": 0, "rows_skipped": 0, "found": False}

    db = get_supabase()
    written, skipped = _write_kpi_rows(db, company_id, name, kpi_rows)
    logger.info(
        "YH-KPI-OD-01: '%s' — %d rows written, %d skipped (on-demand)",
        name, written, skipped,
    )
    return {"rows_written": written, "rows_skipped": skipped, "found": True}


# ── Cron-Pipeline (05:20 UTC) ─────────────────────────────────────────────────

async def run_yfinance_kpi_pipeline(companies: list[dict]) -> dict:
    """
    Cron-Pipeline für EU/DE/nicht-US-Listed Companies.
    Läuft täglich 05:20 UTC — nach EDGAR-Cron (05:15 UTC).

    Filterung: is_listed=True + region != 'us' (EDGAR deckt US ab).
    Max 60 Companies/Run. Sleep 1.5s zwischen Calls.

    companies: vollständige companies-Liste aus main.py-Cron
               (braucht: name, ticker_yf, ticker, exchange, region, is_listed)
    Schreibt direkt in kpi_timeseries (Upsert, ignore_duplicates).
    """
    from src.integrations.supabase import get_supabase, fetch_company_by_name
    from src.services.edgar_kpi import _write_kpi_rows

    stats = {"companies_processed": 0, "rows_written": 0, "rows_skipped": 0, "errors": 0}
    db    = get_supabase()

    # Nur EU/DE-Listed — region='us' wird von EDGAR abgedeckt.
    # region-Feld statt _looks_us_listed (liegt in src/routes/) — kein Service→Route-Import.
    eu_companies = [
        c for c in companies
        if c.get("is_listed")
        and (c.get("region") or "").lower() not in ("us", "usa", "united states")
    ]

    logger.info(
        "YF KPI Pipeline: %d EU-Listed aus %d total, max 60/Run",
        len(eu_companies), len(companies),
    )

    for company in eu_companies[:60]:
        name   = company.get("name", "")
        ticker = company.get("ticker_yf") or company.get("ticker") or None
        if not name or not ticker:
            continue

        try:
            kpi_rows = await fetch_yf_kpis(ticker)
            if not kpi_rows:
                await asyncio.sleep(1.5)
                continue

            co = fetch_company_by_name(name)
            if not co:
                logger.debug("YF KPI: Company '%s' nicht in DB", name)
                continue

            written, skipped = _write_kpi_rows(db, co["id"], name, kpi_rows)
            stats["companies_processed"] += 1
            stats["rows_written"]  += written
            stats["rows_skipped"]  += skipped
            logger.info("YF KPI: '%s' — %d rows written, %d skipped", name, written, skipped)

        except Exception as e:
            logger.warning("YF KPI Pipeline: failed für '%s': %s", name, e)
            stats["errors"] += 1

        await asyncio.sleep(1.5)   # yfinance-Rate-Limit: konservativer als EDGAR (0.15s)

    logger.info(
        "YF KPI Pipeline fertig — %d Companies, %d rows written, %d skipped, %d errors",
        stats["companies_processed"], stats["rows_written"],
        stats["rows_skipped"], stats["errors"],
    )
    return stats
