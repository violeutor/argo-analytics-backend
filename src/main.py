from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from src.routes.analyze import router as analyze_router
from src.routes.companies import router as companies_router
from src.routes.search import router as search_router
from src.routes.company_detail import router as detail_router
from src.routes.market import router as market_router
from src.routes.ownership import router as ownership_router
from src.routes.value_drivers import router as value_drivers_router
from src.routes.signals import router as signals_router
from src.routes.peers import router as peers_router
from src.routes.kpi_timeseries import router as kpi_router
from src.routes.assessments import router as assessments_router
import os
import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

async def _cron_signal_engine():
    """SE-01 — Signal-Engine Cron, täglich 06:00 UTC."""
    try:
        from src.integrations.supabase import fetch_companies, upsert_signals, fetch_recent_absence_categories
        from src.services.signal_engine import run_signal_engine

        companies = fetch_companies(limit=500)
        ownership_map: dict = {}

        # BUG-01: Absence-Cooldown — verhindert täglich neue negative Absence-Signale
        company_ids = [c["id"] for c in companies if c.get("id")]
        absence_cooldown_map = fetch_recent_absence_categories(company_ids, days=30)

        logger.info("Signal-Engine Cron gestartet — %d Companies", len(companies))
        events = await run_signal_engine(companies, ownership_map, absence_cooldown_map)

        if events:
            dicts = [e.to_dict() for e in events]
            written = upsert_signals(dicts)
            logger.info("Signal-Engine Cron fertig — %d events, %d geschrieben", len(events), written)
        else:
            logger.info("Signal-Engine Cron — keine neuen Events")
    except Exception as e:
        logger.exception("Signal-Engine Cron FEHLER: %s", e)


async def _cron_scoring():
    """SC-01–SC-13 — Scoring-Engine Cron, täglich 07:30 UTC (nach Signal + Funding)."""
    try:
        from src.integrations.supabase import (
            fetch_companies, fetch_company_scores, upsert_company_scores,
            fetch_signals, fetch_value_drivers,
        )
        from src.services.score_calculator import compute_all_scores

        companies = fetch_companies(limit=500)
        logger.info("Scoring Cron gestartet — %d Companies", len(companies))

        written = 0
        for company in companies:
            cid = company.get("id")
            if not cid:
                continue
            try:
                signals_raw   = fetch_signals(cid, limit=50)
                vd_cached     = fetch_value_drivers(cid)
                vd_list: list[dict] = []
                if vd_cached:
                    for key in ("enablers", "contributors", "buyers"):
                        vd_list.extend(vd_cached.get(key) or [])

                result = compute_all_scores(
                    company=company,
                    signals=signals_raw,
                    value_drivers=vd_list,
                )
                if upsert_company_scores(cid, result.to_dict()):
                    written += 1
            except Exception as ce:
                logger.warning("Scoring Cron: %s failed — %s", company.get("name"), ce)

        logger.info("Scoring Cron fertig — %d/%d Companies gecacht", written, len(companies))
    except Exception as e:
        logger.exception("Scoring Cron FEHLER: %s", e)


async def _cron_funding_enrichment():
    """B-05 — Funding Enrichment Cron, täglich 06:30 UTC (nach Signal-Engine)."""
    try:
        from src.services.funding_enrichment import run_funding_enrichment

        logger.info("Funding-Enrichment Cron gestartet")
        stats = await run_funding_enrichment(days_since_last=7)
        logger.info(
            "Funding-Enrichment Cron fertig — %d Companies, %d Runden geschrieben, %d Skip",
            stats["companies_processed"], stats["rounds_written"], stats["rounds_skipped"],
        )
    except Exception as e:
        logger.exception("Funding-Enrichment Cron FEHLER: %s", e)


@asynccontextmanager
async def lifespan(app):
    """FastAPI Lifespan — Cron-Jobs starten."""
    import asyncio

    async def _schedule_cron():
        while True:
            now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
            # Nächste 06:00 UTC berechnen
            target = now.replace(hour=6, minute=0, second=0, microsecond=0)
            if target <= now:
                target = target + __import__("datetime").timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            logger.info("Signal-Engine Cron: nächster Run in %.0f Minuten", wait_seconds / 60)
            await asyncio.sleep(wait_seconds)
            await _cron_signal_engine()
            # B-05: Funding Enrichment 30 Min nach Signal-Engine (Signals als Quelle nutzen)
            await asyncio.sleep(30 * 60)
            await _cron_funding_enrichment()
            # SC-01–SC-13: Scoring 30 Min nach Funding Enrichment (frische Signals + Funding)
            await asyncio.sleep(30 * 60)
            await _cron_scoring()

    cron_task = asyncio.create_task(_schedule_cron())
    yield
    cron_task.cancel()


app = FastAPI(
    title="Argo Analytics API",
    description="M&A Deal Scoring Engine — SRR × MFR × TechReadiness + Company Enrichment",
    version="0.5.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class APIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Health check + CORS Preflight durchlassen
        if request.url.path == "/health" or request.method == "OPTIONS":
            return await call_next(request)

        api_key = request.headers.get("X-API-Key")
        expected = os.getenv("API_KEY")

        if not expected:
            raise RuntimeError("API_KEY environment variable not set")

        if api_key != expected:
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

        return await call_next(request)


app.add_middleware(APIKeyMiddleware)

app.include_router(analyze_router)
app.include_router(companies_router)
app.include_router(search_router)
app.include_router(detail_router)
app.include_router(market_router)
app.include_router(ownership_router)
app.include_router(value_drivers_router)
app.include_router(signals_router)
app.include_router(peers_router)
app.include_router(kpi_router)
app.include_router(assessments_router)


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.5.0"}
