"""
GET /api/v1/company/{name}/market  —  MD-B08
=============================================
Eigener Endpunkt für Tab-Markt-Daten.

Gibt market_data-Row aus DB zurück (gecacht).
404 wenn noch nicht angereichert → Frontend zeigt Skeleton/Ladeindikator.
202 wenn Enrichment läuft (enrichment_status = running | pending).

Trigger: wenn market_data fehlt oder unvollständig → Background-Enrichment anstoßen.
"""

import logging
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from src.integrations.supabase import (
    fetch_companies,
    fetch_market_data,
    fetch_tam_cache,
    fetch_all_funding_rounds,
    upsert_market_data,
    set_enrichment_status,
    get_supabase,
)
from src.services.market_data_enrichment import (
    enrich_market_data,
    enrich_market_data_sync_wrapper,
)

logger = logging.getLogger(__name__)

# R10: verwaiste running-Locks nach dieser Zeit neu triggerbar (Crash/Neustart-Schutz)
_ENRICHMENT_LOCK_TTL_S = 600   # 10 Minuten


def _fetch_live_enrichment_status(company_id: str) -> tuple[str | None, bool]:
    """R10: frischen enrichment_status + Stale-Flag direkt aus der DB lesen.

    Returns (status, is_stale_running). Nötig weil der company-Snapshot aus
    fetch_companies(500) am Seitenanfang veraltet sein kann — ein paralleler
    Request (z.B. /company) kann den Status zwischenzeitlich auf "running" gesetzt
    haben. is_stale_running=True wenn der Lock älter als TTL ist (verwaist).
    """
    try:
        r = (get_supabase().table("companies")
             .select("enrichment_status, enrichment_started_at")
             .eq("id", company_id).limit(1).execute())
        if not r.data:
            return None, False
        row = r.data[0]
        status = row.get("enrichment_status")
        is_stale = False
        if status == "running" and row.get("enrichment_started_at"):
            from datetime import datetime, timezone
            try:
                started = datetime.fromisoformat(
                    str(row["enrichment_started_at"]).replace("Z", "+00:00"))
                is_stale = (datetime.now(timezone.utc) - started).total_seconds() > _ENRICHMENT_LOCK_TTL_S
            except Exception:
                is_stale = True
        return status, is_stale
    except Exception:
        return None, False
router = APIRouter(prefix="/api/v1", tags=["market"])


# ── Response model ────────────────────────────────────────────────────────────

class MarketSegment(BaseModel):
    name: str
    share_pct: float | None = None
    note: str | None = None


class RegionalBreakdownItem(BaseModel):
    region: str
    share_pct: float


class MarketDataResponse(BaseModel):
    # Status
    status: str                          # "ready" | "pending" | "running" | "error" | "empty"
    enriched_at: str | None = None

    # Marktgröße
    tam_2035_usd_bn: float | None = None
    cagr_pct: float | None = None
    tam_source: str | None = None

    # SAM
    sam_usd_bn: float | None = None
    sam_confidence: str | None = None   # low | medium | high
    sam_note: str | None = None
    sam_geo_factor: float | None = None
    sam_tech_filter: float | None = None

    # Segmente + Wachstumstreiber
    tam_segments: list[MarketSegment] = []
    growth_drivers: list[str] = []

    # Regionale Verteilung
    regional_breakdown: list[RegionalBreakdownItem] = []
    regional_sources: list[str] = []

    # Wettbewerb + Marktzyklus
    competition_score: str | None = None   # low | medium | high
    competition_note: str | None = None
    market_cycle: str | None = None        # early | growth | mature | consolidation
    market_cycle_note: str | None = None


def _market_data_valid(row: dict | None) -> bool:
    """Prüft ob market_data Row als vollständig gilt."""
    return bool(
        row
        and row.get("enriched_at")
        and row.get("sam_usd_bn")
    )


def _build_response(row: dict, status: str) -> MarketDataResponse:
    """Baut MarketDataResponse aus DB-Row."""
    # tam_segments: DB gibt JSONB zurück → list[dict]
    raw_segments = row.get("tam_segments") or []
    segments = []
    if isinstance(raw_segments, list):
        for s in raw_segments:
            if isinstance(s, dict):
                segments.append(MarketSegment(
                    name=s.get("name", ""),
                    share_pct=s.get("share_pct"),
                    note=s.get("note"),
                ))

    # regional_breakdown: JSONB → list[dict]
    raw_regional = row.get("regional_breakdown") or []
    regional = []
    if isinstance(raw_regional, list):
        for r in raw_regional:
            if isinstance(r, dict) and r.get("region"):
                regional.append(RegionalBreakdownItem(
                    region=r["region"],
                    share_pct=r.get("share_pct", 0),
                ))

    return MarketDataResponse(
        status=status,
        enriched_at=str(row.get("enriched_at")) if row.get("enriched_at") else None,
        tam_2035_usd_bn=row.get("tam_2035_usd_bn"),
        cagr_pct=row.get("cagr_pct"),
        tam_source=row.get("tam_source"),
        sam_usd_bn=row.get("sam_usd_bn"),
        sam_confidence=row.get("sam_confidence"),
        sam_note=row.get("sam_note"),
        sam_geo_factor=row.get("sam_geo_factor"),
        sam_tech_filter=row.get("sam_tech_filter"),
        tam_segments=segments,
        growth_drivers=row.get("growth_drivers") or [],
        regional_breakdown=regional,
        regional_sources=row.get("regional_sources") or [],
        competition_score=row.get("competition_score"),
        competition_note=row.get("competition_note"),
        market_cycle=row.get("market_cycle"),
        market_cycle_note=row.get("market_cycle_note"),
    )


# ── Route ─────────────────────────────────────────────────────────────────────

@router.get("/company/{name}/market", response_model=MarketDataResponse)
async def get_company_market(name: str, background_tasks: BackgroundTasks) -> MarketDataResponse:
    """
    Gibt Market Data für eine Company zurück.

    Status-Logik:
    - "ready"   → Daten vollständig, direkt rendern
    - "pending" → Enrichment noch nicht gestartet, wird jetzt angestoßen → 202
    - "running" → Enrichment läuft gerade → 202
    - "error"   → Enrichment fehlgeschlagen, wird nochmal versucht
    - "empty"   → Company nicht in DB
    """
    # 1. Company lookup
    companies = fetch_companies(limit=500)
    q = name.lower().replace("-", " ").replace("_", " ")
    company = next(
        (c for c in companies if
         c.get("name", "").lower() == q or
         q in c.get("name", "").lower() or
         c.get("name", "").lower().replace(" ", "-") == name.lower()),
        None,
    )

    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found.")

    company_id = company.get("id")
    company_name = company["name"]
    enrichment_status = company.get("enrichment_status", "pending")

    # 2. Market Data aus Cache holen
    market_row = fetch_market_data(company_id) if company_id else None

    # 3. Vollständig → direkt zurückgeben
    if _market_data_valid(market_row):
        return _build_response(market_row, status="ready")

    # 4. Noch nicht angereichert oder unvollständig → Background-Task anstoßen
    # R10: frischen Status + Stale-Flag aus DB lesen (nicht den veralteten Snapshot
    # aus fetch_companies am Seitenanfang) — company_detail.py kann zwischenzeitlich
    # "running" gesetzt haben. Verwaiste Locks (>TTL) werden neu getriggert.
    _live_status, _lock_stale = _fetch_live_enrichment_status(company_id) if company_id else (None, False)
    _is_running = (_live_status == "running") and not _lock_stale
    if company_id and not _is_running:
        if _live_status == "running" and _lock_stale:
            logger.warning("R10: verwaister running-Lock für %s (>%ds) — neu getriggert (market route)", company_name, _ENRICHMENT_LOCK_TTL_S)
        # R10 (Teil B): Lock VOR dem add_task setzen, nicht erst im Task. Schließt das
        # Race-Fenster zwischen "queued" und "running" — ein zweiter Request (anderer
        # Codepfad, z.B. /company) sieht dann bereits "running" und triggert nicht erneut.
        set_enrichment_status(company_id, "running")
        tam_cached = fetch_tam_cache(company_id)
        tam_usd_bn = float(tam_cached["tam_2035_usd_bn"]) if tam_cached and tam_cached.get("tam_2035_usd_bn") else None

        async def _market_enrichment_bg():
            try:
                async_result = await enrich_market_data(
                    company_id=company_id,
                    company_name=company_name,
                    category=company.get("category"),
                    sector_tag=None,
                    tam_usd_bn=tam_usd_bn,
                    tech_readiness=None,  # kein TR-Kontext hier verfügbar
                )
                all_companies = fetch_companies(limit=500)
                all_rounds = fetch_all_funding_rounds()
                sync_result = enrich_market_data_sync_wrapper(
                    company_id=company_id,
                    company_name=company_name,
                    category=company.get("category"),
                    sector_tag=None,
                    tam_usd_bn=tam_usd_bn,
                    all_companies=all_companies,
                    all_funding_rounds=all_rounds,
                )
                upsert_market_data(company_id, {**async_result, **sync_result})
                set_enrichment_status(company_id, "done")
                logger.info("Market enrichment done (market route) for %s", company_name)
            except Exception:
                set_enrichment_status(company_id, "error")
                logger.exception("Market enrichment TRACEBACK (market route) for %s", company_name)

        background_tasks.add_task(_market_enrichment_bg)
        logger.info("Market enrichment queued via /market route for %s", company_name)

    # 5. Teilweise befüllte Row zurückgeben falls vorhanden, sonst pending
    if market_row:
        return _build_response(market_row, status=enrichment_status or "pending")

    # Leere Response mit Status pending → Frontend zeigt Skeleton
    return MarketDataResponse(status=enrichment_status or "pending")
