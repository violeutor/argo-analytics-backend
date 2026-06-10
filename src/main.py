from fastapi import FastAPI, Request, BackgroundTasks
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
from src.routes.notifications import router as notifications_router
from src.routes.watchlist import router as watchlist_router
from src.routes.explore import router as explore_router
from src.routes.access_request import router as access_request_router
from src.routes.user_profile import router as user_profile_router
from src.routes.admin import router as admin_router
import os
import asyncio
import logging
from contextlib import asynccontextmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
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
        filtered = 0
        for company in companies:
            cid = company.get("id")
            if not cid:
                continue
            # BUYER-AS-COMPANY-01 Entry-Point: Buyer-Gen NUR für Screening-Targets,
            # nicht für Buyer-Origin-Companies (source='buyer_gen'). Sonst kaskadiert
            # die Buyer-Generierung über das gesamte Acquirer-Universum (Veolia wird
            # als Company eingefügt → bekäme selbst Käufer → deren Käufer …).
            # On-demand-Loads einer Buyer-Company triggern weiterhin ihr Enrichment;
            # nur der proaktive Cron lässt sie aus.
            if company.get("source") == "buyer_gen":
                filtered += 1
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
            "Buyer-Enrichment Cron fertig — %d enriched, %d skipped (cached), %d filtered (buyer_gen)",
            enriched, skipped, filtered,
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
            fetch_potential_buyers,
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
                buyers        = fetch_potential_buyers(cid)  # BUYER-CRON-WIRE-01
                vd_list: list[dict] = []
                if vd_cached:
                    # value_drivers hat NUR enablers/contributors/etfs — kein "buyers".
                    # Akquisiteure kommen aus potential_buyers (separate Tabelle).
                    for key in ("enablers", "contributors"):
                        vd_list.extend(vd_cached.get(key) or [])

                result = compute_all_scores(
                    company=company,
                    signals=signals_raw,
                    value_drivers=vd_list,
                    market_data=market_data,  # BUG-40
                    buyers=buyers,            # BUYER-CRON-WIRE-01 — MFR-Annotation in compute_all_scores
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


async def _cron_edgar_kpi():
    """KPI-03 — EDGAR XBRL KPI Pipeline, täglich 05:15 UTC (zwischen ticker_yf + Buyer)."""
    try:
        from src.integrations.supabase import fetch_companies
        from src.services.edgar_kpi import run_edgar_kpi_pipeline

        companies = fetch_companies(limit=500)
        logger.info("EDGAR KPI Cron gestartet — %d Companies", len(companies))
        stats = await run_edgar_kpi_pipeline(companies)
        logger.info(
            "EDGAR KPI Cron fertig — %d Companies, %d rows written, %d skipped, %d errors",
            stats["companies_processed"], stats["rows_written"],
            stats["rows_skipped"], stats["errors"],
        )
    except Exception as e:
        logger.exception("EDGAR KPI Cron FEHLER: %s", e)


async def _cron_yf_kpi():
    """YH-KPI-TS-01 — yfinance KPI Pipeline für EU/DE-Listed, täglich 05:20 UTC."""
    try:
        from src.integrations.supabase import fetch_companies
        from src.services.yfinance_kpi import run_yfinance_kpi_pipeline

        companies = fetch_companies(limit=500)
        logger.info("YF KPI Cron gestartet — %d Companies", len(companies))
        stats = await run_yfinance_kpi_pipeline(companies)
        logger.info(
            "YF KPI Cron fertig — %d Companies, %d rows written, %d skipped, %d errors",
            stats["companies_processed"], stats["rows_written"],
            stats["rows_skipped"], stats["errors"],
        )
    except Exception as e:
        logger.exception("YF KPI Cron FEHLER: %s", e)


async def _cron_bafin_ownership(company_name: str | None = None) -> dict:
    """
    BaFin Stimmrechtsmitteilungen — direkt im Argo Backend (kein Bridge-Hop nötig).

    Zwei Modi:
      company_name=None  → alle listed DE Companies (30d Rolling Cron)
      company_name=str   → eine Company on-demand (One-Click-Trigger aus company_detail.py)

    BaFin = öffentlicher GET-Endpoint, kein CAPTCHA, kein Session-Management
    → kein BA-Bridge nötig. Rate-Limit (65s) nur im Cron-Modus relevant.
    """
    import os
    from src.services.bafin_ownership import (
        run_bafin_ownership_cron,
        run_bafin_on_demand,
    )

    supabase_url = os.getenv("SUPABASE_URL", "")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY", "")

    if not supabase_url or not supabase_key:
        logger.error("BaFin Cron: SUPABASE_URL/KEY fehlt — übersprungen")
        return {"error": "missing_credentials"}

    try:
        if company_name:
            logger.info("BaFin on-demand: '%s'", company_name)
            # Sync-Funktion im Thread-Pool — blockiert nicht den Event-Loop
            stats = await asyncio.to_thread(
                run_bafin_on_demand, company_name, supabase_url, supabase_key
            )
            logger.info("BaFin on-demand '%s' fertig — %s", company_name, stats)
        else:
            logger.info("BaFin Ownership Cron gestartet (30d Rolling)")
            stats = await asyncio.to_thread(
                run_bafin_ownership_cron, supabase_url, supabase_key
            )
            logger.info(
                "BaFin Cron fertig — processed=%d with_data=%d written=%d errors=%d",
                stats.get("companies_processed", 0),
                stats.get("companies_with_data", 0),
                stats.get("entries_written", 0),
                stats.get("errors", 0),
            )
        return stats
    except Exception as e:
        logger.exception("BaFin Cron FEHLER: %s", e)
        return {"error": str(e)}


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
            await asyncio.sleep(30 * 60)
            await _cron_edgar_kpi()              # 05:15 UTC — EDGAR XBRL KPIs (KPI-03)
            await asyncio.sleep(5 * 60)
            await _cron_yf_kpi()                 # 05:20 UTC — yfinance KPIs EU/DE (YH-KPI-TS-01)
            await asyncio.sleep(10 * 60)
            await _cron_buyer_enrichment()       # 05:30 UTC — Buyer
            await asyncio.sleep(30 * 60)
            await _cron_scoring()                # 06:00 UTC — Scoring

    async def _schedule_rolling_refresh():
        """ARCH-02: Stündlicher Rolling Refresh — unabhängig vom täglichen Cron."""
        await asyncio.sleep(5 * 60)   # 5min nach Start warten — DB-Verbindung sicher offen
        while True:
            await _cron_rolling_refresh()
            await asyncio.sleep(60 * 60)   # 1h warten bis zum nächsten Run

    async def _schedule_bafin_refresh():
        """BaFin 30d Rolling Refresh — unabhängig vom täglichen Cron."""
        await asyncio.sleep(10 * 60)   # 10min nach Start — hinter Rolling Refresh
        while True:
            await _cron_bafin_ownership()
            await asyncio.sleep(30 * 24 * 60 * 60)   # 30 Tage

    cron_task    = asyncio.create_task(_schedule_cron())
    rolling_task = asyncio.create_task(_schedule_rolling_refresh())
    bafin_task   = asyncio.create_task(_schedule_bafin_refresh())
    yield
    cron_task.cancel()
    rolling_task.cancel()
    bafin_task.cancel()


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

# Explizite /api/v1-Routen mit festen Pfaden ZUERST — verhindert Shadowing
# durch spätere Router mit Pfad-Parametern (z.B. /api/v1/{slug}).
app.include_router(admin_router)                              # /api/v1/admin/...
app.include_router(user_profile_router)                       # /api/v1/user-profile, /api/v1/user-preferences
app.include_router(access_request_router, prefix="/api/v1")   # /api/v1/access-requests
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
app.include_router(notifications_router)
app.include_router(watchlist_router)
app.include_router(explore_router)


@app.post("/internal/bafin/trigger")
async def trigger_bafin_cron(background_tasks: BackgroundTasks):
    """Manueller Trigger für BaFin 30d Cron — alle listed DE Companies."""
    background_tasks.add_task(_cron_bafin_ownership)
    return {"status": "triggered", "job": "_cron_bafin_ownership"}


@app.post("/internal/bafin/enrich/{company_name}")
async def trigger_bafin_ondemand(company_name: str, background_tasks: BackgroundTasks):
    """On-Demand BaFin-Fetch für eine einzelne Company (One-Click-Trigger)."""
    background_tasks.add_task(_cron_bafin_ownership, company_name)
    return {"status": "triggered", "company": company_name}


@app.post("/internal/bafin-insider/trigger")
async def trigger_bafin_insider_cron(background_tasks: BackgroundTasks):
    """
    SE-18: Manueller Trigger für BaFin Directors' Dealings — alle listed DE Companies.
    Nützlich für Testing außerhalb des 04:00 UTC Crons.
    """
    async def _run_se18():
        try:
            from src.integrations.supabase import fetch_companies
            from src.services.bafin_insider import parse_bafin_insider
            import httpx

            companies = fetch_companies(limit=500)
            de_exchanges = {"xetra", "frankfurt", "fse", "hamburg", "berlin", "dusseldorf", "stuttgart", "munich"}
            de_listed = [
                c for c in companies
                if c.get("ticker") and (
                    (c.get("exchange") or "").lower() in de_exchanges
                    or (c.get("isin") or "").startswith("DE")
                )
            ]
            logger.info("SE-18 Trigger: %d listed DE Companies", len(de_listed))

            from src.integrations.supabase import upsert_signals
            timeout = httpx.Timeout(12.0, connect=4.0)
            async with httpx.AsyncClient(
                timeout=timeout,
                headers={"User-Agent": "ArgoAnalytics/1.0 (research; contact@argo-analytics.io)"},
                follow_redirects=True,
            ) as client:
                all_signals = []
                for c in de_listed:
                    events = await parse_bafin_insider(
                        company_id=c["id"],
                        company_name=c.get("name", ""),
                        isin=(c.get("isin") or "").strip(),
                        issuer_name=c.get("name") if not c.get("isin") else None,
                        client=client,
                    )
                    all_signals.extend(events)
                    await asyncio.sleep(0.5)

            if all_signals:
                written = upsert_signals([e.to_dict() for e in all_signals])
                logger.info("SE-18 Trigger fertig — %d Signale, %d geschrieben", len(all_signals), written)
            else:
                logger.info("SE-18 Trigger fertig — keine neuen Insider-Signale")
        except Exception as e:
            logger.exception("SE-18 Trigger FEHLER: %s", e)

    background_tasks.add_task(_run_se18)
    return {"status": "triggered", "job": "se18_bafin_insider"}



@app.post("/internal/edgar-kpi/trigger")
async def trigger_edgar_kpi(background_tasks: BackgroundTasks):
    """Manueller Trigger für _cron_edgar_kpi (Testing/Debugging)."""
    background_tasks.add_task(_cron_edgar_kpi)
    return {"status": "triggered", "job": "_cron_edgar_kpi"}


@app.post("/internal/yf-kpi/trigger")
async def trigger_yf_kpi(background_tasks: BackgroundTasks):
    """Manueller Trigger für _cron_yf_kpi — alle EU/DE-Listed Companies (YH-KPI-TS-01)."""
    background_tasks.add_task(_cron_yf_kpi)
    return {"status": "triggered", "job": "_cron_yf_kpi"}


@app.post("/internal/yf-kpi/enrich/{company_name}")
async def trigger_yf_kpi_ondemand(company_name: str, background_tasks: BackgroundTasks):
    """
    YH-KPI-TS-01: On-Demand yfinance-KPI-Enrich für eine einzelne EU/DE Company.
    Analog /internal/edgar-kpi/enrich/{company_name} — manuelle Reserve für Testing.
    """
    async def _run():
        try:
            from src.integrations.supabase import fetch_company_by_name
            from src.services.yfinance_kpi import enrich_one_company_yf
            from src.routes.company_detail import _resolve_yf_symbol

            co = fetch_company_by_name(company_name)
            if not co:
                logger.warning("YF-KPI-OD Endpoint: Company '%s' nicht in DB", company_name)
                return
            ticker_raw = co.get("ticker_yf") or co.get("ticker") or None
            ticker     = _resolve_yf_symbol(ticker_raw) if ticker_raw else None
            result = await enrich_one_company_yf(co["id"], co.get("name", company_name), ticker)
            logger.info(
                "YF-KPI-OD Endpoint: '%s' — found=%s, %d rows written",
                company_name, result.get("found"), result.get("rows_written", 0),
            )
        except Exception as e:
            logger.exception("YF-KPI-OD Endpoint FEHLER für '%s': %s", company_name, e)

    background_tasks.add_task(_run)
    return {"status": "triggered", "company": company_name, "job": "yf_kpi_ondemand"}


@app.post("/internal/edgar-kpi/enrich/{company_name}")
async def trigger_edgar_kpi_ondemand(company_name: str, background_tasks: BackgroundTasks):
    """
    EDGAR-OD-01: On-Demand EDGAR-KPI-Enrich für eine einzelne Company (Admin/Reserve).

    Der automatische Cold-Path-Trigger sitzt in company_detail.py (Fire-and-Forget beim
    Cold-Load frischer US-Companies). Dieser Endpoint ist die manuelle Reserve für
    Testing / Nach-Enrichment einzelner Companies außerhalb des 05:15-Crons.

    Name-basiert (analog /internal/bafin/enrich/{company_name}) — löst company_id +
    Ticker selbst aus der DB auf, damit der Endpoint per Company-Name aufrufbar bleibt.
    """
    async def _run():
        try:
            from src.integrations.supabase import fetch_company_by_name
            from src.services.edgar_kpi import enrich_one_company

            co = fetch_company_by_name(company_name)
            if not co:
                logger.warning("EDGAR-OD-01 Endpoint: Company '%s' nicht in DB", company_name)
                return
            ticker = co.get("ticker_yf") or co.get("ticker") or None
            result = await enrich_one_company(co["id"], co.get("name", company_name), ticker)
            logger.info(
                "EDGAR-OD-01 Endpoint: '%s' — found=%s, %d rows written",
                company_name, result.get("found"), result.get("rows_written", 0),
            )
        except Exception as e:
            logger.exception("EDGAR-OD-01 Endpoint FEHLER für '%s': %s", company_name, e)

    background_tasks.add_task(_run)
    return {"status": "triggered", "company": company_name, "job": "edgar_kpi_ondemand"}


@app.post("/internal/funding/enrich/{company_name}")
async def trigger_funding_ondemand(company_name: str, background_tasks: BackgroundTasks):
    """
    FUNDING-OD-01: On-Demand Funding-Enrichment für eine einzelne Company (Admin/Reserve).
    Analog /internal/bafin/enrich/{company_name}. Der automatische Cold-Path-Trigger
    sitzt in company_detail.py. Dieser Endpoint ist die manuelle Reserve für Testing.
    """
    async def _run():
        try:
            from src.integrations.supabase import fetch_company_by_name
            from src.services.funding_enrichment import enrich_one_company_funding

            co = fetch_company_by_name(company_name)
            if not co:
                logger.warning("FUNDING-OD-01 Endpoint: Company '%s' nicht in DB", company_name)
                return
            stats = await enrich_one_company_funding(
                company_id=co["id"],
                company_name=co.get("name", company_name),
                ticker=co.get("ticker"),
                region=co.get("region"),
            )
            logger.info(
                "FUNDING-OD-01 Endpoint: '%s' — %d Runden geschrieben",
                company_name, stats.get("rounds_written", 0),
            )
        except Exception as e:
            logger.exception("FUNDING-OD-01 Endpoint FEHLER für '%s': %s", company_name, e)

    background_tasks.add_task(_run)
    return {"status": "triggered", "company": company_name, "job": "funding_ondemand"}


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.7.0"}
