"""
src/routes/watchlist.py
WATCHLIST-01: Per-User Watchlist Endpoints.

Auth-Strategie (AUTH-GATE-01 aktiv):
  Bearer-Token aus Authorization-Header → Supabase auth.get_user() (serverseitige Validierung).
  Kein Token → None → starred=False / 401 je nach Endpoint.
  ARGO_DEFAULT_USER_ID-Fallback entfernt (AUTH-GATE-01, Session 57).

Endpoints:
  GET  /watchlist/status/{company_id}  → {starred: bool}
  POST /watchlist/{company_id}         → Company hinzufügen
  DELETE /watchlist/{company_id}       → Company entfernen

Tabelle: user_watchlist (WATCHLIST-01 Migration)
  user_id    UUID FK user_profiles(id)
  company_id UUID FK companies(id)
  UNIQUE(user_id, company_id)
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Header, HTTPException

from src.integrations.supabase import get_supabase

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["watchlist"])


def _resolve_user_id(authorization: str | None) -> str | None:
    """
    Löst user_id auf.
    Bearer-Token aus Authorization-Header → Supabase auth.get_user() (serverseitige Validierung).
    Kein Token oder ungültiger Token → None (Endpoints antworten leer / 401).
    """
    if authorization and authorization.startswith("Bearer "):
        token = authorization.removeprefix("Bearer ").strip()
        try:
            user = get_supabase().auth.get_user(token)
            if user and user.user:
                return str(user.user.id)
        except Exception as e:
            logger.debug("JWT-Auflösung fehlgeschlagen: %s", e)
    return None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/watchlist/status/{company_id}")
async def watchlist_status(
    company_id: str,
    authorization: Optional[str] = Header(default=None),
):
    """Prüft ob eine Company in der Watchlist des aktuellen Users ist."""
    user_id = _resolve_user_id(authorization)
    if not user_id:
        # Kein User-Kontext → starred=False (kein 401, Frontend fällt auf localStorage)
        return {"starred": False}
    try:
        rows = (
            get_supabase()
            .table("user_watchlist")
            .select("id")
            .eq("user_id", user_id)
            .eq("company_id", company_id)
            .limit(1)
            .execute()
            .data or []
        )
        return {"starred": bool(rows)}
    except Exception as e:
        logger.debug("watchlist_status error für %s: %s", company_id[:8], e)
        return {"starred": False}


@router.post("/watchlist/{company_id}")
async def watchlist_add(
    company_id: str,
    authorization: Optional[str] = Header(default=None),
):
    """Fügt eine Company zur Watchlist hinzu (idempotent via upsert)."""
    user_id = _resolve_user_id(authorization)
    if not user_id:
        raise HTTPException(status_code=401, detail="No user context — Bearer token required")
    try:
        get_supabase().table("user_watchlist").upsert(
            {"user_id": user_id, "company_id": company_id},
            on_conflict="user_id,company_id",
        ).execute()
        logger.info("Watchlist ADD: user=%.8s company=%.8s", user_id, company_id)
        return {"ok": True, "starred": True}
    except Exception as e:
        logger.warning("watchlist_add error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/watchlist/{company_id}")
async def watchlist_remove(
    company_id: str,
    authorization: Optional[str] = Header(default=None),
):
    """Entfernt eine Company aus der Watchlist."""
    user_id = _resolve_user_id(authorization)
    if not user_id:
        raise HTTPException(status_code=401, detail="No user context — Bearer token required")
    try:
        get_supabase().table("user_watchlist").delete()\
            .eq("user_id", user_id)\
            .eq("company_id", company_id)\
            .execute()
        logger.info("Watchlist REMOVE: user=%.8s company=%.8s", user_id, company_id)
        return {"ok": True, "starred": False}
    except Exception as e:
        logger.warning("watchlist_remove error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/watchlist")
async def watchlist_list(
    authorization: Optional[str] = Header(default=None),
):
    """
    Gibt alle company_ids der Watchlist zurück — für den Watchlist-Tab.
    Response: {company_ids: [uuid, ...]}
    """
    user_id = _resolve_user_id(authorization)
    if not user_id:
        return {"company_ids": []}
    try:
        rows = (
            get_supabase()
            .table("user_watchlist")
            .select("company_id, added_at, note")
            .eq("user_id", user_id)
            .order("added_at", desc=True)
            .execute()
            .data or []
        )
        return {"company_ids": [r["company_id"] for r in rows], "entries": rows}
    except Exception as e:
        logger.debug("watchlist_list error: %s", e)
        return {"company_ids": []}
