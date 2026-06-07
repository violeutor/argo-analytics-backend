"""
ACTIVATE-01 — Admin Activation Flow
=====================================
GET  /api/v1/admin/access-requests     → alle Requests (neueste zuerst)
POST /api/v1/admin/activate/{id}       → Supabase-Invite + user_profiles anlegen + Status setzen

Auth:   Bearer JWT → is_admin-Check in user_profiles (service_role).
Invite: supabase.auth.admin.invite_user_by_email() — User setzt eigenes Passwort.
        Redirect-URL: APP_URL Env-Var (Render) → Vercel-Domain des Frontends.
"""

import logging
import os
from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException

from src.integrations.supabase import get_supabase

logger = logging.getLogger(__name__)
router = APIRouter()


def _require_admin(authorization: str | None) -> str:
    """
    JWT → user_id. Raises 401 wenn kein Token, 403 wenn user_profiles.is_admin != true.
    Kein separater ADMIN_SECRET — Admin-Status liegt in user_profiles.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = authorization.split(" ", 1)[1]

    db = get_supabase()
    try:
        res = db.auth.get_user(token)
    except Exception as exc:
        logger.warning("admin _require_admin get_user failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid token")

    if not res.user:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = str(res.user.id)

    # is_admin aus user_profiles — service_role hat BYPASSRLS
    try:
        profile = (
            db.table("user_profiles")
            .select("is_admin")
            .eq("id", user_id)
            .single()
            .execute()
        )
        if not profile.data or not profile.data.get("is_admin"):
            raise HTTPException(status_code=403, detail="Admin access required")
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("admin is_admin check failed for %s: %s", user_id, exc)
        raise HTTPException(status_code=403, detail="Admin check failed")

    return user_id


# ── GET /api/v1/admin/access-requests ────────────────────────────────────────

@router.get("/api/v1/admin/access-requests")
async def list_access_requests(authorization: str | None = Header(None)):
    """Alle Access Requests, neueste zuerst. Admin-only."""
    _require_admin(authorization)
    db = get_supabase()
    try:
        res = (
            db.table("access_requests")
            .select("id, full_name, company_name, email, customer_type, message, status, linked_user_id, created_at")
            .order("created_at", desc=True)
            .execute()
        )
        return {"requests": res.data or []}
    except Exception as exc:
        logger.error("list_access_requests FAILED: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to fetch requests")


# ── POST /api/v1/admin/activate/{request_id} ─────────────────────────────────

@router.post("/api/v1/admin/activate/{request_id}")
async def activate_user(request_id: str, authorization: str | None = Header(None)):
    """
    Aktiviert einen Access-Request:
      1. Supabase invite_user_by_email() → Magic-Link-Mail an User
      2. user_profiles anlegen (prefilled aus access_request)
      3. access_requests.status = 'activated' + linked_user_id setzen

    Idempotenz: bereits aktivierte Requests werden mit 409 abgelehnt.
    Rollback: kein automatischer Rollback — bei user_profiles-Fehler manuell via Supabase.
    """
    admin_id = _require_admin(authorization)
    db = get_supabase()

    # 1 · Request laden
    try:
        req_res = (
            db.table("access_requests")
            .select("*")
            .eq("id", request_id)
            .single()
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Request not found")

    req = req_res.data
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    if req.get("status") == "activated":
        raise HTTPException(status_code=409, detail="Already activated")

    email = (req.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="No email address in request")

    app_url = os.getenv("APP_URL", "https://app.argo-analytics.io")

    # 2 · Supabase Invite — User bekommt Magic-Link, setzt eigenes Passwort
    try:
        invite_res = db.auth.admin.invite_user_by_email(
            email,
            options={"redirect_to": app_url},
        )
        new_user_id = str(invite_res.user.id)
        logger.info("invite_user_by_email OK: %s → %s", email, new_user_id)
    except Exception as exc:
        logger.error("invite_user_by_email FAILED for %s: %s", email, exc)
        raise HTTPException(status_code=500, detail=f"Invite failed: {exc}")

    now_iso = datetime.now(timezone.utc).isoformat()

    # 3 · user_profiles anlegen (prefilled aus access_request)
    try:
        db.table("user_profiles").insert({
            "id":                str(new_user_id),
            "full_name":         req.get("full_name") or "",
            "email":             email,
            "company_name":      req.get("company_name") or "",
            "customer_type":     req.get("customer_type") or "other",
            "subscription_tier": "pro",
            "activated_at":      now_iso,
            "activated_by":      admin_id,
        }).execute()
        logger.info("user_profiles created for %s", new_user_id)
    except Exception as exc:
        logger.error("user_profiles insert FAILED for %s: %s", new_user_id, exc)
        # Invite ist schon raus — kein Auto-Rollback. Manuell via Supabase korrigieren.
        raise HTTPException(
            status_code=500,
            detail=f"Profile creation failed (invite already sent to {email}): {exc}",
        )

    # 4 · access_requests Status aktualisieren (non-fatal bei Fehler)
    try:
        db.table("access_requests").update({
            "status":         "activated",
            "linked_user_id": new_user_id,
        }).eq("id", request_id).execute()
    except Exception as exc:
        logger.warning("access_requests status update failed (non-fatal): %s", exc)

    return {
        "status":  "activated",
        "user_id": new_user_id,
        "email":   email,
    }
