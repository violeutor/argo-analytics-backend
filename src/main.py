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
import os
import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

async def _cron_signal_engine():
    """SE-01 — Signal-Engine Cron, täglich 06:00 UTC."""
    try:
        from src.integrations.supabase import fetch_companies, fetch_signals, upsert_signals
        from src.services.signal_engine import run_signal_engine

        companies = fetch_companies(limit=500)
        # Ownership-Map aus signals nicht nötig — watch_ownership_changes liest aus DB direkt
        # Wir übergeben leere Map; Ownership-Diff läuft intern via as_of_date
        ownership_map: dict = {}

        logger.info("Signal-Engine Cron gestartet — %d Companies", len(companies))
        events = await run_signal_engine(companies, ownership_map)

        if events:
            dicts = [e.to_dict() for e in events]
            written = upsert_signals(dicts)
            logger.info("Signal-Engine Cron fertig — %d events, %d geschrieben", len(events), written)
        else:
            logger.info("Signal-Engine Cron — keine neuen Events")
    except Exception as e:
        logger.exception("Signal-Engine Cron FEHLER: %s", e)


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


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.5.0"}
