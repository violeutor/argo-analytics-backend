"""
src/services/auth_context.py
AUTH-CONTEXT-01: Zentrale User-Resolution aus dem Bearer-Token.

Konsolidiert das zuvor in watchlist.py + explore.py byte-identisch duplizierte
_resolve_user_id. Die JWT-Auflösung ist der fragile, mehrfach kopierte Teil
(AUTH-PROXY-01-Historie) → genau eine Quelle.

  resolve_user_id()      → user_id | None        (reine Identität)
  resolve_user_context() → UserContext | None     (Identität + Profil-Felder)

Kein Token / ungültiges Token → None. Der Aufrufer entscheidet 401 vs. leer
(watchlist: leer bei GET, 401 bei Mutation; explore: leer; company_detail: 401).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.integrations.supabase import get_supabase

logger = logging.getLogger(__name__)


def resolve_user_id(authorization: str | None) -> str | None:
    """
    Bearer-Token aus Authorization-Header → Supabase auth.get_user()
    (serverseitige Validierung). Kein/ungültiges Token → None.
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


@dataclass
class UserContext:
    user_id: str
    customer_type: str = "other"          # vc | pe | ma_agency | corporate | family_office | other
    subscription_tier: str = "inactive"   # pro | inactive  (Sales-led Gate)
    is_admin: bool = False


def resolve_user_context(authorization: str | None) -> UserContext | None:
    """
    Bearer-Token → UserContext (user_id + customer_type + subscription_tier + is_admin).
    Ein Profil-Read. Kein/ungültiges Token → None.
    User in auth, aber kein user_profiles-Row (Edge) → Defaults (other/inactive),
    damit ein authentifizierter User nie an einem fehlenden Profil-Row scheitert.
    """
    user_id = resolve_user_id(authorization)
    if not user_id:
        return None

    customer_type = "other"
    subscription_tier = "inactive"
    is_admin = False
    try:
        rows = (
            get_supabase()
            .table("user_profiles")
            .select("customer_type, subscription_tier, is_admin")
            .eq("id", user_id)
            .limit(1)
            .execute()
            .data or []
        )
        if rows:
            customer_type     = rows[0].get("customer_type") or "other"
            subscription_tier = rows[0].get("subscription_tier") or "inactive"
            is_admin          = bool(rows[0].get("is_admin"))
    except Exception as e:
        logger.warning("resolve_user_context: user_profiles Fehler: %s", e)

    return UserContext(
        user_id=user_id,
        customer_type=customer_type,
        subscription_tier=subscription_tier,
        is_admin=is_admin,
    )
