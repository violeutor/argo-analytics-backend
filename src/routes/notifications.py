"""
notifications.py — GET /api/v1/notifications
=============================================
Liefert Signals der letzten N Tage für eine Liste von Company-Namen.
Wird vom Frontend (Bell-Icon) gepollt — kein Auth nötig, kein DB-Schema-Change.

Query-Params:
  names       — Company-Namen (mehrfach; max 50)
  days        — Lookback-Fenster in Tagen (default 7, max 30)
  min_score   — Mindest-relevance_score (default 0.5)

Response: Liste von Notification-Dicts, sortiert nach event_date DESC.
"""

import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from src.integrations.supabase import get_supabase

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["notifications"])


@router.get("/notifications")
async def get_notifications(
    names:     list[str] = Query(default=[], max_length=50),
    days:      int       = Query(default=7, ge=1, le=30),
    min_score: float     = Query(default=0.5, ge=0.0, le=2.0),
):
    """
    Gibt Signals der letzten `days` Tage für die angegebenen Companies zurück.
    Gefiltert auf relevance_score >= min_score, source != 'internal_absence'.
    Sortiert nach event_date DESC, max 100 Ergebnisse.
    """
    if not names:
        return JSONResponse([])

    # Namen deduplizieren + limitieren
    unique_names = list(dict.fromkeys(n.strip() for n in names if n.strip()))[:50]

    try:
        db = get_supabase()

        # 1. Company-IDs für die angefragten Namen holen
        companies_res = (
            db.table("companies")
            .select("id, name")
            .in_("name", unique_names)
            .execute()
        )
        companies = companies_res.data or []
        if not companies:
            return JSONResponse([])

        company_map: dict[str, str] = {c["id"]: c["name"] for c in companies}
        company_ids = list(company_map.keys())

        # 2. Signals der letzten N Tage für diese Companies
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()

        signals_res = (
            db.table("signals")
            .select(
                "id, company_id, event_type, event_date, raw_title, "
                "direction, relevance_score, source, source_url"
            )
            .in_("company_id", company_ids)
            .gte("event_date", cutoff)
            .neq("source", "internal_absence")          # Absence-Sentinels raus
            .gte("relevance_score", min_score)
            .order("event_date", desc=True)
            .limit(100)
            .execute()
        )
        signals = signals_res.data or []

        # 3. Company-Name anreichern
        result = []
        for s in signals:
            cid = s.get("company_id", "")
            result.append({
                "id":             s.get("id", ""),
                "company_name":   company_map.get(cid, ""),
                "event_type":     s.get("event_type", ""),
                "raw_title":      s.get("raw_title") or s.get("summary") or "—",
                "direction":      s.get("direction", "neutral"),
                "relevance_score": s.get("relevance_score", 0.0),
                "event_date":     s.get("event_date", ""),
                "source_url":     s.get("source_url"),
            })

        return JSONResponse(result)

    except Exception as e:
        logger.warning("get_notifications failed: %s", e)
        return JSONResponse([])
