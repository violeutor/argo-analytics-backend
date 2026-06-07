"""
ONBOARD-WIRE-01 — User Profile + Sector Preferences
======================================================
GET  /api/v1/user-profile      → Profil lesen (onboarding_completed_at-Check)
PUT  /api/v1/user-profile      → job_title, customer_type, onboarding_completed_at schreiben
POST /api/v1/user-preferences  → Sektor-Picks ersetzen (delete + re-insert)

Auth: Bearer JWT → supabase.auth.get_user() → user_id.
DB:   service_role-Client (BYPASSRLS).
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from src.integrations.supabase import get_supabase

logger = logging.getLogger(__name__)
router = APIRouter()

_VALID_SECTORS = {
    "climate_tech", "energy_transition", "mobility", "health_tech",
    "biotech_pharma", "fintech", "enterprise_software", "deep_tech",
    "consumer_tech", "industrial_tech", "food_agritech", "space_defense",
    "media_entertainment", "real_estate_proptech",
}


def _resolve_user(authorization: str | None) -> str:
    """Bearer JWT → user_id. 401 bei fehlendem/ungültigem Token."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or malformed token")
    token = authorization.split(" ", 1)[1]
    try:
        res = get_supabase().auth.get_user(token)
    except Exception as exc:
        logger.warning("get_user failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid token")
    if not res.user:
        raise HTTPException(status_code=401, detail="Invalid token")
    return str(res.user.id)


# ── GET /api/v1/user-profile ─────────────────────────────────────────────────

@router.get("/api/v1/user-profile")
async def get_user_profile(authorization: str | None = Header(None)):
    """
    Liest user_profiles für den authentifizierten User.
    LoginOnboarding nutzt onboarding_completed_at:
      None → Onboarding zeigen | not None → returning user → direkt in App.
    Fail-open: Row nicht gefunden → leeres Dict (Onboarding fängt es ab).
    """
    user_id = _resolve_user(authorization)
    db = get_supabase()
    try:
        res = (
            db.table("user_profiles")
            .select("id, full_name, company_name, job_title, customer_type, onboarding_completed_at")
            .eq("id", user_id)
            .single()
            .execute()
        )
        return res.data or {}
    except Exception as exc:
        # Kein Row = first-time user nach invite → leer zurück (kein 404)
        logger.info("get_user_profile %s: %s (fail-open)", user_id, exc)
        return {}


# ── PUT /api/v1/user-profile ─────────────────────────────────────────────────

class ProfileUpdate(BaseModel):
    job_title: Optional[str] = None           # "" → NULL, None → kein Update
    customer_type: Optional[str] = None
    mark_onboarding_complete: bool = False    # True → setzt onboarding_completed_at=NOW()


@router.put("/api/v1/user-profile")
async def update_user_profile(
    body: ProfileUpdate,
    authorization: str | None = Header(None),
):
    """Schreibt Onboarding-Felder zurück nach Step 3 Submit."""
    user_id = _resolve_user(authorization)
    db = get_supabase()

    payload: dict = {}
    if body.job_title is not None:
        payload["job_title"] = body.job_title.strip() or None
    if body.customer_type is not None:
        payload["customer_type"] = body.customer_type
    if body.mark_onboarding_complete:
        payload["onboarding_completed_at"] = datetime.now(timezone.utc).isoformat()

    if not payload:
        return {"status": "no_update"}

    try:
        db.table("user_profiles").update(payload).eq("id", user_id).execute()
        logger.info("update_user_profile %s: %s", user_id, list(payload.keys()))
        return {"status": "ok"}
    except Exception as exc:
        logger.error("update_user_profile %s FAILED: %s", user_id, exc)
        raise HTTPException(status_code=500, detail="Profile update failed")


# ── POST /api/v1/user-preferences ────────────────────────────────────────────

class PreferencesUpdate(BaseModel):
    sector_keys: list[str]


@router.post("/api/v1/user-preferences")
async def update_user_preferences(
    body: PreferencesUpdate,
    authorization: str | None = Header(None),
):
    """
    Ersetzt alle Sektor-Picks des Users (delete-then-insert).
    Unbekannte sector_keys werden still ignoriert (CHECK-Constraint-Schutz).
    """
    user_id = _resolve_user(authorization)
    sectors = [s for s in body.sector_keys if s in _VALID_SECTORS]

    db = get_supabase()
    try:
        # Bestehende löschen
        db.table("user_industry_preferences").delete().eq("user_id", user_id).execute()
        # Neue einfügen
        if sectors:
            rows = [{"user_id": user_id, "sector_key": s} for s in sectors]
            db.table("user_industry_preferences").insert(rows).execute()
        logger.info("update_user_preferences %s: %d sectors", user_id, len(sectors))
        return {"status": "ok", "sectors_saved": len(sectors)}
    except Exception as exc:
        logger.error("update_user_preferences %s FAILED: %s", user_id, exc)
        raise HTTPException(status_code=500, detail="Preferences update failed")
