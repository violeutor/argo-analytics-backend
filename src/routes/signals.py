"""
SE-04 · signals.py
Pfad: argo-analytics-backend/src/routes/signals.py

REST-Endpunkte für Signal History (Tab 9).

Endpoints:
    GET /api/v1/company/{name}/signals
        → Signals für eine Company (chronologisch absteigend)

    GET /api/v1/signals
        → Alle Signals (Dashboard-Übersicht, limit=100)

    POST /api/v1/signals/trigger
        → Manueller Trigger der Signal-Engine (für Tests)
"""

import logging
from fastapi import APIRouter, HTTPException, BackgroundTasks

from src.integrations.supabase import (
    fetch_company_by_name,
    fetch_signals,
    fetch_all_signals,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")


@router.get("/company/{name}/signals")
def get_company_signals(name: str, limit: int = 50):
    """
    Gibt Signal-History für eine Company zurück.

    Response:
        status:   "ready" | "empty"
        signals:  Liste von Signal-Events
        count:    Anzahl Events
    """
    company = fetch_company_by_name(name)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' nicht gefunden.")

    company_id = company["id"]
    signals = fetch_signals(company_id, limit=limit)

    return {
        "status":       "ready" if signals else "empty",
        "company_name": company["name"],
        "company_id":   company_id,
        "signals":      signals,
        "count":        len(signals),
    }


@router.get("/signals")
def get_all_signals(limit: int = 100):
    """
    Gibt alle Signals zurück — für Dashboard-Übersicht.
    Sortiert nach event_date DESC.
    """
    signals = fetch_all_signals(limit=limit)
    return {
        "status":  "ready",
        "signals": signals,
        "count":   len(signals),
    }


@router.post("/signals/trigger")
async def trigger_signal_engine(background_tasks: BackgroundTasks):
    """
    Manueller Trigger der Signal-Engine — für Tests und Debugging.
    Läuft als BackgroundTask um den Request nicht zu blockieren.
    """
    async def _run():
        try:
            from src.integrations.supabase import fetch_companies, upsert_signals
            from src.services.signal_engine import run_signal_engine

            companies = fetch_companies(limit=500)
            events = await run_signal_engine(companies, {})
            if events:
                written = upsert_signals([e.to_dict() for e in events])
                logger.info("Manual trigger: %d events, %d written", len(events), written)
        except Exception as e:
            logger.exception("Manual signal trigger FEHLER: %s", e)

    background_tasks.add_task(_run)
    return {"status": "triggered", "message": "Signal-Engine läuft im Hintergrund."}
