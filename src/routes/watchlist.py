"""
src/routes/watchlist.py
WATCHLIST-01: Per-User Watchlist Endpoints.

Auth-Strategie (Phasen):
  Phase 1 (jetzt): ARGO_DEFAULT_USER_ID env var — Single-User Dogfooding.
    Render → Environment → ARGO_DEFAULT_USER_ID = <deine auth.users UUID>
  Phase 2 (nach Q-D01): JWT aus Authorization-Header via supabase-py.
    Q-D01 (supabase-py Auth-Bug) muss zuerst gelöst sein.

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
import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException

from src.integrations.supabase import get_supabase

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["watchlist"])

# Phase 1: Dogfooding — UUID des geseedeten Admin-Profils (auth.users)
# Render → Settings → Environment Variables → ARGO_DEFAULT_USER_ID
_DEFAULT_USER_ID: str | None = os.getenv("ARGO_DEFAULT_USER_ID")


def _resolve_user_id(authorization: str | None) -> str | None:
    """
    Löst user_id auf.
    1. Bearer-Token aus Authorization-Header → Supabase auth.get_user() (serverseitige Validierung)
    2. Fallback: ARGO_DEFAULT_USER_ID (Dogfooding / kein Auth-Header)
    """
    if authorization and authorization.startswith("Bearer "):
        token = authorization.removeprefix("Bearer ").strip()
        try:
            user = get_supabase().auth.get_user(token)
            if user and user.user:
                return str(user.user.id)
        except Exception as e:
            logger.debug("JWT-Auflösung fehlgeschlagen: %s", e)
    return _DEFAULT_USER_ID


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
        raise HTTPException(status_code=401, detail="No user context — ARGO_DEFAULT_USER_ID not set")
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
        raise HTTPException(status_code=401, detail="No user context — ARGO_DEFAULT_USER_ID not set")
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
