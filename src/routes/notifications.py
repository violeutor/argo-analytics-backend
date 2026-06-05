"""
notifications.py — GET /api/v1/notifications
=============================================
Liefert Signals der letzten N Tage für eine Liste von Company-Namen.
Wird vom Frontend (Bell-Icon) gepollt — kein Auth nötig, kein DB-Schema-Change.

Query-Params:
  names       — OPTIONAL. Company-Namen-Filter (mehrfach). Leer = global über
                alle Companies (Default-Verhalten für die Bell). Nicht-leer =
                nur Signals dieser Companies (z.B. künftig Watchlist-gefiltert).
  days        — Lookback-Fenster in Tagen (default 7, max 30)
  min_score   — Mindest-relevance_score (default 0.5)

Response: Liste von Notification-Dicts, sortiert nach event_date DESC.

Design (Session 55): Frontend schickt KEINE Namensliste mehr durch. Die Bell
holt die neuesten Signals global; Company-Namen werden über die company_id der
Treffer nachgeladen. Damit kein 50er-Cap (alphabetisch) und keine URL-Bombe.
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
    names:     list[str] = Query(default=[], max_length=200),
    days:      int       = Query(default=7, ge=1, le=30),
    min_score: float     = Query(default=0.5, ge=0.0, le=2.0),
):
    """
    Gibt Signals der letzten `days` Tage zurück.
    Leer `names` → global über alle Companies. Nicht-leer → nur diese Companies.
    Gefiltert auf relevance_score >= min_score, source != 'internal_absence'.
    Sortiert nach event_date DESC, max 100 Ergebnisse.
    """
    try:
        db = get_supabase()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()

        # Signal-Query — Basis identisch für global und gefiltert
        query = (
            db.table("signals")
            .select(
                "id, company_id, event_type, event_date, raw_title, "
                "direction, relevance_score, source, source_url"
            )
            .gte("event_date", cutoff)
            .neq("source", "internal_absence")          # Absence-Sentinels raus
            .gte("relevance_score", min_score)
            .order("event_date", desc=True)
            .limit(100)
        )

        # Optionaler Namensfilter. Leer → global (kein .in_ → alle Companies).
        company_map: dict[str, str] = {}
        if names:
            unique_names = list(dict.fromkeys(n.strip() for n in names if n.strip()))
            companies = (
                db.table("companies").select("id, name")
                .in_("name", unique_names).execute().data or []
            )
            if not companies:
                return JSONResponse([])
            company_map = {c["id"]: c["name"] for c in companies}
            query = query.in_("company_id", list(company_map.keys()))

        signals = query.execute().data or []
        if not signals:
            return JSONResponse([])

        # Global-Fall: Company-Namen über die company_id der Treffer nachladen.
        # (Statt alle Namen vom Frontend reinzureichen — das war der alte Umweg.)
        if not company_map:
            ids = list({s["company_id"] for s in signals if s.get("company_id")})
            companies = (
                db.table("companies").select("id, name")
                .in_("id", ids).execute().data or []
            )
            company_map = {c["id"]: c["name"] for c in companies}

        # Company-Name anreichern
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
