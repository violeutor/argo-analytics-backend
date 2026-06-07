"""
ACCESS-REQ-01 — Public Request-Access Endpoint
Session 58 · 06.06.2026

Path B: Landing-Page-Formular → dieser Endpoint → service_role-INSERT
in access_requests + Mail-Kopie an Andreas.

- Öffentlich (kein Auth) — die Landing Page ist public.
- anon fasst die DB nie direkt an; der Insert läuft hier mit service_role.
- Honeypot + Basisvalidierung gegen Bot-/Spam-Submits.
- Mail-Versand ist fire-and-forget: schlägt er fehl, ist der Lead trotzdem
  gespeichert (eine Mail-Panne darf keinen Lead kosten).

Registrierung in main.py:
    from src.routes import access_request
    app.include_router(access_request.router, prefix="/api/v1")
"""

import os
import logging
from typing import Optional

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel, EmailStr, field_validator

from src.integrations.supabase import get_supabase  # service_role-Client (bestehender Helper)

logger = logging.getLogger(__name__)
router = APIRouter()

# customer_type-Werte müssen deckungsgleich mit customer_type_enum sein
_VALID_CUSTOMER_TYPES = {"vc", "pe", "ma_agency", "corporate", "family_office", "other"}


# ---------------------------------------------------------------------------
# Request-Modell
# ---------------------------------------------------------------------------

class AccessRequestIn(BaseModel):
    full_name: str
    company_name: str
    email: EmailStr
    customer_type: str
    message: Optional[str] = None
    website: Optional[str] = None  # Honeypot — muss leer sein

    @field_validator("full_name", "company_name")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("Pflichtfeld leer")
        return v[:200]  # Längen-Cap gegen Müll-Payloads

    @field_validator("customer_type")
    @classmethod
    def _valid_type(cls, v: str) -> str:
        if v not in _VALID_CUSTOMER_TYPES:
            raise ValueError("Ungültiger customer_type")
        return v

    @field_validator("message")
    @classmethod
    def _cap_message(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip()
        return v[:2000] if v else None


# ---------------------------------------------------------------------------
# Mail-Versand — AUSTAUSCHBARER BLOCK
# ---------------------------------------------------------------------------
# Default: Resend (https://resend.com) — simple HTTP-API, kein SMTP-Handshake.
# Benötigte Env Vars:
#   RESEND_API_KEY          — API-Key aus dem Resend-Dashboard
#   ACCESS_REQUEST_NOTIFY_TO    — deine Mail-Adresse (Kopie-Empfänger)
#   ACCESS_REQUEST_NOTIFY_FROM  — verifizierter Absender (z.B. "Argo <noreply@deine-domain.de>")
#
# Anderen Dienst (Postmark/SendGrid/SMTP)? Nur diese eine Funktion ersetzen,
# Signatur bleibt gleich. Der Rest des Endpoints ändert sich nicht.

async def _send_lead_notification(lead: AccessRequestIn) -> None:
    api_key = os.getenv("RESEND_API_KEY")
    notify_to = os.getenv("ACCESS_REQUEST_NOTIFY_TO")
    notify_from = os.getenv("ACCESS_REQUEST_NOTIFY_FROM")

    if not (api_key and notify_to and notify_from):
        logger.warning("ACCESS-REQ: Mail-Env unvollständig — überspringe Versand (Lead ist gespeichert).")
        return

    subject = f"[Argo Lead] {lead.company_name} · {lead.customer_type.upper()}"
    body = (
        f"Neue Access-Anfrage:\n\n"
        f"Name:          {lead.full_name}\n"
        f"Firma / Fund:  {lead.company_name}\n"
        f"E-Mail:        {lead.email}\n"
        f"Typ:           {lead.customer_type}\n"
        f"Nachricht:     {lead.message or '—'}\n"
    )

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"from": notify_from, "to": [notify_to], "subject": subject, "text": body,
                  "reply_to": lead.email},
        )
        resp.raise_for_status()


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("/access-request")
async def create_access_request(payload: AccessRequestIn, request: Request):
    # Honeypot: gefüllt = Bot. Still 200 zurückgeben (kein Signal an den Bot),
    # nichts speichern, nichts mailen.
    if payload.website:
        logger.info("ACCESS-REQ: Honeypot ausgelöst — verworfen.")
        return {"ok": True}

    # Audit-Kontext (für spätere Abuse-Analyse / RATE-LIMIT-01)
    source_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else None)
    user_agent = request.headers.get("user-agent")

    sb = get_supabase()
    try:
        sb.table("access_requests").insert({
            "full_name": payload.full_name,
            "company_name": payload.company_name,
            "email": payload.email,
            "customer_type": payload.customer_type,
            "message": payload.message,
            "source_ip": source_ip,
            "user_agent": user_agent,
        }).execute()
    except Exception:
        logger.exception("ACCESS-REQ: DB-Insert fehlgeschlagen")
        # Hier hart fehlschlagen — wenn der Lead nicht gespeichert wird,
        # ist es eine echte Fehlersituation für den User.
        return {"ok": False, "error": "persist_failed"}

    # Mail fire-and-forget: Lead ist schon sicher in der DB.
    try:
        await _send_lead_notification(payload)
    except Exception:
        logger.exception("ACCESS-REQ: Mail-Versand fehlgeschlagen (Lead ist gespeichert)")

    return {"ok": True}
