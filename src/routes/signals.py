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
from fastapi import APIRouter, HTTPException

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
async def trigger_signal_engine():
    """
    Manueller Trigger der Signal-Engine — für Tests und Debugging.
    Läuft synchron (blockierend) damit Render Free Tier die Task nicht killt.
    """
    logger.info("Manual trigger: gestartet")
    try:
        from src.integrations.supabase import (
            fetch_companies, upsert_signals
        )
        from src.services.signal_engine import run_signal_engine

        logger.info("Manual trigger: imports OK")

        companies = fetch_companies(limit=500)
        logger.info("Manual trigger: %d companies geladen", len(companies))

        ownership_map: dict[str, list[dict]] = {}
        try:
            from src.integrations.supabase import fetch_all_ownership_entries
            all_ownership = fetch_all_ownership_entries()
            for entry in all_ownership:
                cid = entry.get("company_id")
                if cid:
                    ownership_map.setdefault(cid, []).append(entry)
            logger.info("Manual trigger: ownership_map %d companies", len(ownership_map))
        except Exception as oe:
            logger.warning("Manual trigger: ownership fetch failed (%s) — leere Map", oe)

        events = await run_signal_engine(companies, ownership_map)
        logger.info("Manual trigger: %d events gesammelt", len(events))

        written = 0
        if events:
            written = upsert_signals([e.to_dict() for e in events])
            logger.info("Manual trigger: %d/%d events geschrieben", written, len(events))
        else:
            logger.info("Manual trigger: keine events")

        return {
            "status":        "done",
            "companies":     len(companies),
            "events_found":  len(events),
            "events_written": written,
        }

    except Exception as e:
        logger.exception("Manual signal trigger FEHLER: %s", e)
        return {"status": "error", "detail": str(e)}
