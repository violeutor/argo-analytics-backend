"""
Route: POST /api/v1/company/{name}/kpi-timeseries
===================================================
Pfad: argo-analytics-backend/src/routes/kpi_timeseries.py

Empfängt KPI-Zeitreihen-Rows von der BA-Bridge und schreibt sie
in die Supabase kpi_timeseries-Tabelle (upsert — kein Überschreiben).

Wird aufgerufen von:
  - BA-Bridge Cron (03:00 UTC) nach parse_pending
  - On-demand via _push_kpi_to_argo() nach /ba/company/{name}

GET /api/v1/company/{name}/kpi-timeseries
  → Gibt Zeitreihe für eine Company zurück (für Frontend-Modal)
"""
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.integrations.supabase import get_supabase, fetch_company_by_name

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["kpi_timeseries"])


# ── Models ────────────────────────────────────────────────────────────────────

class KPIRow(BaseModel):
    metric:      str
    fiscal_year: int
    value:       float
    currency:    str | None = None
    source:      str = "ba_bridge"
    confidence:  str | None = None


class KPIWriteRequest(BaseModel):
    rows: list[KPIRow]


class KPITimeseriesResponse(BaseModel):
    company_name: str
    metrics:      dict[str, list[dict]]   # metric → [{fiscal_year, value, currency, source}]
    years:        list[int]               # alle vorhandenen Jahrgänge sortiert


# ── Write Endpoint (BA-Bridge → Argo) ────────────────────────────────────────

@router.post("/company/{name}/kpi-timeseries")
async def write_kpi_timeseries(name: str, body: KPIWriteRequest):
    """
    Schreibt KPI-Zeitreihen-Rows für eine Company.
    Upsert via Unique Constraint (company_id, metric, fiscal_year, source).
    Kein Überschreiben bestehender Werte — fortschreiben only.
    """
    db = get_supabase()

    company = fetch_company_by_name(name)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' nicht gefunden.")

    company_id = company["id"]
    written = 0
    skipped = 0

    for row in body.rows:
        try:
            payload = {
                "company_id":  company_id,
                "metric":      row.metric,
                "fiscal_year": row.fiscal_year,
                "value":       row.value,
                "source":      row.source,
            }
            if row.currency:
                payload["currency"] = row.currency
            if row.confidence:
                payload["confidence"] = row.confidence

            # Upsert: bei Konflikt (unique) nichts tun — nie überschreiben
            result = (
                db.table("kpi_timeseries")
                .upsert(payload, on_conflict="company_id,metric,fiscal_year,source", ignore_duplicates=True)
                .execute()
            )
            if result.data:
                written += 1
            else:
                skipped += 1
        except Exception as e:
            logger.warning("kpi_timeseries upsert failed: %s / %s FY%s: %s",
                           name, row.metric, row.fiscal_year, e)
            skipped += 1

    logger.info("kpi_timeseries write '%s': %d written, %d skipped", name, written, skipped)
    return {"status": "ok", "written": written, "skipped": skipped}


# ── Read Endpoint (Frontend-Modal) ────────────────────────────────────────────

@router.get("/company/{name}/kpi-timeseries", response_model=KPITimeseriesResponse)
async def get_kpi_timeseries(name: str):
    """
    Gibt alle KPI-Zeitreihen für eine Company zurück.
    Gruppiert nach metric — für Chart-Modal im Frontend.
    """
    db = get_supabase()

    company = fetch_company_by_name(name)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' nicht gefunden.")

    company_id = company["id"]

    try:
        result = (
            db.table("kpi_timeseries")
            .select("metric, fiscal_year, value, currency, source, confidence")
            .eq("company_id", company_id)
            .order("fiscal_year", desc=False)
            .execute()
        )
        rows = result.data or []
    except Exception as e:
        logger.warning("kpi_timeseries fetch failed für '%s': %s", name, e)
        rows = []

    # Gruppieren nach metric
    metrics: dict[str, list[dict]] = {}
    years_set: set[int] = set()

    for row in rows:
        m = row["metric"]
        fy = row["fiscal_year"]
        if m not in metrics:
            metrics[m] = []
        metrics[m].append({
            "fiscal_year": fy,
            "value":       row["value"],
            "currency":    row.get("currency"),
            "source":      row.get("source"),
            "confidence":  row.get("confidence"),
        })
        if fy:
            years_set.add(fy)

    # Abgeleitete Metriken on-demand berechnen (wenn Basis-Metriken vorhanden)
    _add_derived_metrics(metrics)

    return KPITimeseriesResponse(
        company_name=name,
        metrics=metrics,
        years=sorted(years_set),
    )


def _add_derived_metrics(metrics: dict[str, list[dict]]) -> None:
    """
    Berechnet abgeleitete Metriken wenn Basis-Daten vorhanden:
      - ebitda_margin_pct  = ebitda_eur_mn / revenue_eur_mn × 100
      - revenue_per_fte    = revenue_eur_mn / headcount × 1000 (EUR k/Kopf)
      - equity_ratio_pct   = equity_eur_mn / total_assets_eur_mn × 100
      - revenue_cagr_pct   = CAGR über alle verfügbaren Jahre (≥2 Jahre)
    """
    # EBITDA-Marge
    if "ebitda_eur_mn" in metrics and "revenue_eur_mn" in metrics:
        rev_by_fy  = {r["fiscal_year"]: r["value"] for r in metrics["revenue_eur_mn"]}
        ebit_by_fy = {r["fiscal_year"]: r["value"] for r in metrics["ebitda_eur_mn"]}
        derived = []
        for fy, ebitda in sorted(ebit_by_fy.items()):
            rev = rev_by_fy.get(fy)
            if rev and rev > 0:
                derived.append({"fiscal_year": fy, "value": round(ebitda / rev * 100, 1),
                                 "currency": None, "source": "derived", "confidence": "high"})
        if derived:
            metrics["ebitda_margin_pct"] = derived

    # Revenue per FTE
    if "revenue_eur_mn" in metrics and "headcount" in metrics:
        rev_by_fy = {r["fiscal_year"]: r["value"] for r in metrics["revenue_eur_mn"]}
        hc_by_fy  = {r["fiscal_year"]: r["value"] for r in metrics["headcount"]}
        derived = []
        for fy, hc in sorted(hc_by_fy.items()):
            rev = rev_by_fy.get(fy)
            if rev and hc and hc > 0:
                derived.append({"fiscal_year": fy, "value": round(rev * 1000 / hc, 1),
                                 "currency": "EUR_k", "source": "derived", "confidence": "high"})
        if derived:
            metrics["revenue_per_fte_eur_k"] = derived

    # Equity Ratio
    if "equity_eur_mn" in metrics and "total_assets_eur_mn" in metrics:
        eq_by_fy = {r["fiscal_year"]: r["value"] for r in metrics["equity_eur_mn"]}
        ta_by_fy = {r["fiscal_year"]: r["value"] for r in metrics["total_assets_eur_mn"]}
        derived = []
        for fy, eq in sorted(eq_by_fy.items()):
            ta = ta_by_fy.get(fy)
            if ta and ta > 0:
                derived.append({"fiscal_year": fy, "value": round(eq / ta * 100, 1),
                                 "currency": None, "source": "derived", "confidence": "high"})
        if derived:
            metrics["equity_ratio_pct"] = derived

    # Revenue CAGR (braucht ≥2 Datenpunkte)
    if "revenue_eur_mn" in metrics:
        rev_rows = sorted(metrics["revenue_eur_mn"], key=lambda r: r["fiscal_year"])
        if len(rev_rows) >= 2:
            first = rev_rows[0]
            last  = rev_rows[-1]
            n = last["fiscal_year"] - first["fiscal_year"]
            if n > 0 and first["value"] and first["value"] > 0 and last["value"]:
                cagr = ((last["value"] / first["value"]) ** (1 / n) - 1) * 100
                metrics["revenue_cagr_pct"] = [{
                    "fiscal_year": last["fiscal_year"],
                    "value":       round(cagr, 1),
                    "currency":    None,
                    "source":      "derived",
                    "confidence":  "high" if n >= 3 else "medium",
                    "note":        f"CAGR {first['fiscal_year']}–{last['fiscal_year']} ({n}J)",
                }]


# ── Argo Backend main.py — Router registrieren ────────────────────────────────
# Pfad: argo-analytics-backend/src/main.py
# Folgende zwei Zeilen ergänzen:
#
# from src.routes.kpi_timeseries import router as kpi_router
# app.include_router(kpi_router)
