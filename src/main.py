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
from src.routes.debug import router as debug_router
import os
import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

async def _cron_rolling_refresh():
    """
    ARCH-02: Rolling Background Refresh — strukturelle Daten monatlich rotierend.
    Läuft stündlich, 55 Companies pro Run → ~1.333/Tag → alle 30 Tage vollständig.
    Sortierung: enriched_at IS NULL zuerst, dann älteste zuerst.
    Signal Engine bleibt für zeitkritische Events (Funding, News, Patents, IPO-Status).
    """
    try:
        from src.integrations.supabase import (
            fetch_companies_for_rolling_refresh,
            upsert_company_enrichment,
            set_enrichment_status,
            get_supabase,
        )
        from src.services.enrichment import enrich_company

        companies = fetch_companies_for_rolling_refresh(batch_size=55)
        if not companies:
            logger.debug("Rolling Refresh: Queue leer — alle Companies frisch genug")
            return

        logger.info("Rolling Refresh gestartet — %d Companies", len(companies))
        refreshed = 0

        for company in companies:
            cid  = company.get("id")
            name = company.get("name", "")
            if not cid or not name:
                continue
            try:
                enrichment = await enrich_company(
                    company_name=name,
                    company_record=company,
                )
                payload = {
                    "founding_year": enrichment.founded_year,
                    "headquarters":  enrichment.headquarters or None,
                    "headcount":     enrichment.employee_count or None,
                    "description":   enrichment.description or None,
                    "website":       enrichment.website or None,
                }
                upsert_company_enrichment(cid, payload)   # schreibt auch enriched_at wenn payload nicht leer
                # ARCH-02: enriched_at immer schreiben — auch wenn Enrichment leer war
                # verhindert stündliche Retry-Loops für Companies ohne Wikipedia-Artikel
                from datetime import datetime, timezone as tz
                get_supabase().table("companies").update(
                    {"enriched_at": datetime.now(tz.utc).isoformat()}
                ).eq("id", cid).execute()
                refreshed += 1
                await asyncio.sleep(1.0)   # sanftes Rate-Limiting — kein Wikipedia-Burst
            except Exception as ce:
                logger.warning("Rolling Refresh: '%s' failed — %s", name, ce)

        logger.info("Rolling Refresh fertig — %d/%d Companies aktualisiert", refreshed, len(companies))
    except Exception as e:
        logger.exception("Rolling Refresh FEHLER: %s", e)


async def _cron_signal_engine():
    """SE-01 + SE-14 — Signal-Engine Cron, täglich 04:00 UTC."""
    try:
        from src.integrations.supabase import (
            fetch_companies, upsert_signals, fetch_recent_absence_categories,
            bulk_upsert_patents, update_patent_aggregates,
        )
        from src.services.signal_engine import run_signal_engine

        companies = fetch_companies(limit=500)
        ownership_map: dict = {}

        # BUG-01: Absence-Cooldown — verhindert täglich neue negative Absence-Signale
        company_ids = [c["id"] for c in companies if c.get("id")]
        absence_cooldown_map = fetch_recent_absence_categories(company_ids, days=30)

        logger.info("Signal-Engine Cron gestartet — %d Companies", len(companies))

        # SE-14: run_signal_engine gibt jetzt Tuple zurück (events, patent_records)
        events, patent_records = await run_signal_engine(companies, ownership_map, absence_cooldown_map)

        if events:
            dicts   = [e.to_dict() for e in events]
            written = upsert_signals(dicts)
            logger.info("Signal-Engine Cron fertig — %d events, %d geschrieben", len(events), written)
        else:
            logger.info("Signal-Engine Cron — keine neuen Events")

        # SE-14: Patent-Records in company_patents-Tabelle upserten
        if patent_records:
            pat_written = bulk_upsert_patents(patent_records)
            logger.info("SE-14: %d Patent-Records upserted (%d Companies)", pat_written, len({r["company_id"] for r in patent_records}))
            # Aggregate (patent_count, granted_ratio, ipc_codes) in companies zurückschreiben
            update_patent_aggregates(patent_records)
        else:
            logger.info("SE-14: Keine Patent-Records (EPO_OPS_KEY fehlt oder keine Patente gefunden)")

    except Exception as e:
        logger.exception("Signal-Engine Cron FEHLER: %s", e)


async def _cron_buyer_enrichment():
    """R-23 — Buyer-Enrichment Cron, täglich 07:30 UTC (nach Scoring-Cron)."""
    try:
        from src.integrations.supabase import fetch_companies, fetch_potential_buyers
        from src.services.buyer_enrichment import is_cache_valid, enrich_buyers_for_company

        companies = fetch_companies(limit=500)
        logger.info("Buyer-Enrichment Cron gestartet — %d Companies", len(companies))

        enriched = 0
        skipped  = 0
        for company in companies:
            cid = company.get("id")
            if not cid:
                continue
            try:
                existing = fetch_potential_buyers(cid)
                if is_cache_valid(existing):
                    skipped += 1
                    continue
                buyers = await enrich_buyers_for_company(company, cid)
                if buyers:
                    enriched += 1
                # Rate-Limit: kurze Pause zwischen Companies
                import asyncio as _asyncio
                await _asyncio.sleep(1.5)
            except Exception as ce:
                logger.warning("Buyer-Enrichment Cron: %s failed — %s", company.get("name"), ce)

        logger.info(
            "Buyer-Enrichment Cron fertig — %d enriched, %d skipped (cached)",
            enriched, skipped,
        )
    except Exception as e:
        logger.exception("Buyer-Enrichment Cron FEHLER: %s", e)


async def _cron_ticker_yf():
    """
    BUG-42: ticker_yf-Enrichment — täglich 04:45 UTC (zwischen Funding + Buyer).
    Berechnet yfinance-Ticker (z.B. SIE.DE) für alle listed Companies ohne ticker_yf
    und schreibt ihn direkt in companies.ticker_yf.
    Trennung: Argo-Backend schreibt ticker_yf, BA-Bridge liest nur noch.
    """
    # TICKER_SUFFIX_MAP — gleiche Logik wie price_fetcher.py in BA-Bridge
    _SUFFIX_MAP: dict[str, str] = {
        "xetra":              ".DE",
        "frankfurt":          ".DE",
        "fse":                ".F",
        "euronext paris":     ".PA",
        "euronext amsterdam": ".AS",
        "euronext":           ".PA",
        "london":             ".L",
        "lse":                ".L",
        "swiss":              ".SW",
        "six":                ".SW",
        "milan":              ".MI",
        "bmv":                ".MX",
        "tsx":                ".TO",
        "asx":                ".AX",
        "hkex":               ".HK",
        "tokyo":              ".T",
    }
    try:
        from src.integrations.supabase import (
            fetch_listed_companies_missing_ticker_yf, upsert_ticker_yf,
        )

        companies = fetch_listed_companies_missing_ticker_yf()
        logger.info("ticker_yf Cron: %d listed Companies ohne ticker_yf", len(companies))

        written = 0
        for c in companies:
            ticker   = (c.get("ticker") or "").strip().upper()
            exchange = (c.get("exchange") or "").lower()
            cid      = c.get("id") or ""
            if not ticker or not cid:
                continue
            suffix   = _SUFFIX_MAP.get(exchange, "")
            yf_ticker = ticker + suffix if suffix and "." not in ticker else ticker
            if yf_ticker == ticker and not suffix:
                # US-Ticker — kein Suffix nötig, trotzdem schreiben damit Feld befüllt ist
                yf_ticker = ticker
            upsert_ticker_yf(cid, yf_ticker)
            logger.info("ticker_yf: %s → %s (exchange=%s)", ticker, yf_ticker, exchange or "US")
            written += 1

        logger.info("ticker_yf Cron fertig — %d geschrieben", written)
    except Exception as e:
        logger.exception("ticker_yf Cron FEHLER: %s", e)


async def _cron_scoring():
    """SC-01–SC-13 — Scoring-Engine Cron, täglich 07:30 UTC (nach Signal + Funding)."""
    try:
        from src.integrations.supabase import (
            fetch_companies, fetch_company_scores, upsert_company_scores,
            fetch_signals, fetch_value_drivers, fetch_market_data,
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
                market_data   = fetch_market_data(cid)  # BUG-40: market_data fehlte im Cron
                vd_list: list[dict] = []
                if vd_cached:
                    for key in ("enablers", "contributors", "buyers"):
                        vd_list.extend(vd_cached.get(key) or [])

                result = compute_all_scores(
                    company=company,
                    signals=signals_raw,
                    value_drivers=vd_list,
                    market_data=market_data,  # BUG-40
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
            # Nächste 04:00 UTC berechnen (vorher: 06:00 UTC)
            target = now.replace(hour=4, minute=0, second=0, microsecond=0)
            if target <= now:
                target = target + __import__("datetime").timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            logger.info("Signal-Engine Cron: nächster Run in %.0f Minuten", wait_seconds / 60)
            await asyncio.sleep(wait_seconds)
            await _cron_signal_engine()          # 04:00 UTC — Signal + Patents
            await asyncio.sleep(30 * 60)
            await _cron_funding_enrichment()     # 04:30 UTC — Funding
            await asyncio.sleep(15 * 60)
            await _cron_ticker_yf()              # 04:45 UTC — ticker_yf (BUG-42)
            await asyncio.sleep(15 * 60)
            await _cron_buyer_enrichment()       # 05:00 UTC — Buyer
            await asyncio.sleep(30 * 60)
            await _cron_scoring()                # 05:30 UTC — Scoring

    async def _schedule_rolling_refresh():
        """ARCH-02: Stündlicher Rolling Refresh — unabhängig vom täglichen Cron."""
        await asyncio.sleep(5 * 60)   # 5min nach Start warten — DB-Verbindung sicher offen
        while True:
            await _cron_rolling_refresh()
            await asyncio.sleep(60 * 60)   # 1h warten bis zum nächsten Run

    cron_task    = asyncio.create_task(_schedule_cron())
    rolling_task = asyncio.create_task(_schedule_rolling_refresh())
    yield
    cron_task.cancel()
    rolling_task.cancel()


app = FastAPI(
    title="Argo Analytics API",
    description="M&A Deal Scoring Engine — SRR × MFR × TechReadiness + Company Enrichment",
    version="0.7.0",
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
app.include_router(debug_router)


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.7.0"}
