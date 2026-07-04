"""
GET /api/v1/company/{name}/value-drivers
=========================================
src/routes/value_drivers.py

Gibt Value Drivers (Enabler + Contributors + ETFs) für eine Company zurück.
Analog zu market.py und ownership.py — polling-fähig.

Status-Logik:
  ready   → value_drivers in DB vorhanden + mindestens 1 Enabler oder Contributor
  pending → noch nicht angereichert, Background-Task läuft
  empty   → keine Tags bekannt, kein Supply Chain Mapping verfügbar
"""

import logging
from fastapi import APIRouter
from pydantic import BaseModel

from src.integrations.supabase import fetch_company_by_name, search_companies_by_name, fetch_value_drivers

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["value-drivers"])


# ── Response Models ───────────────────────────────────────────────────────────

class ValueDriverEntry(BaseModel):
    type: str                          # "enabler" | "contributor"
    ticker: str
    name: str
    exchange: str | None = None
    role: str
    relevance: float
    # Enabler-spezifisch
    dependency_level: str | None = None   # critical | high | medium | commodity
    market_position: str | None = None    # dominant | contested | fragmented
    partnership_likely: bool | None = None
    # Contributor-spezifisch
    exposure_level: str | None = None     # high | medium | low
    grows_independently: str | None = None  # true | false | partial
    existing_relationship: bool | None = None
    # Gemeinsam
    context: str | None = None
    price: float | None = None
    market_cap_bn: float | None = None
    currency: str | None = None
    yahoo_symbol: str | None = None
    source: str | None = None


class ETFEntry(BaseModel):
    ticker: str
    name: str
    relevance: float


class ValueDriversResponse(BaseModel):
    status: str                        # ready | pending | empty
    company_name: str
    enablers: list[ValueDriverEntry] = []
    contributors: list[ValueDriverEntry] = []
    etfs: list[ETFEntry] = []
    enriched_at: str | None = None


# ── Route ─────────────────────────────────────────────────────────────────────

@router.get("/company/{name}/value-drivers", response_model=ValueDriversResponse)
async def get_value_drivers(name: str) -> ValueDriversResponse:
    """
    Gibt gecachte Value Drivers aus DB zurück.
    Background-Enrichment wird von company_detail.py angestoßen.
    Dieser Endpunkt ist rein lesend — kein eigener Enrichment-Trigger.
    """
    # NAME-LOOKUP-SCALE-01 (04.07.): fetch_companies(limit=500) ersetzt — bei
    # >500 Companies in der DB (alphabetisch sortiert, s. supabase.py) fehlten
    # Companies ab einem bestimmten Anfangsbuchstaben im Snapshot komplett und
    # dieser Endpunkt lieferte fälschlich status="empty", obwohl Value Drivers
    # längst in der DB standen. Gezielter Query statt Full-Table-Snapshot.
    q = name.lower().replace("-", " ").replace("_", " ")
    company = fetch_company_by_name(q)
    if not company:
        for row in search_companies_by_name(q, limit=10):
            if q in (row.get("name") or "").lower():
                company = row
                break

    if not company:
        return ValueDriversResponse(status="empty", company_name=name)

    company_name = company["name"]
    company_id   = company.get("id")

    if not company_id:
        return ValueDriversResponse(status="empty", company_name=company_name)

    # DB-Cache lesen
    vd = fetch_value_drivers(company_id)

    if not vd:
        return ValueDriversResponse(status="pending", company_name=company_name)

    # Supabase gibt JSONB-Felder manchmal als JSON-String zurück — sicher parsen
    import json

    def _parse_jsonb(val) -> list:
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
        return []

    enablers     = _parse_jsonb(vd.get("enablers", []))
    contributors = _parse_jsonb(vd.get("contributors", []))
    etfs         = _parse_jsonb(vd.get("etfs", []))

    if not enablers and not contributors:
        return ValueDriversResponse(status="empty", company_name=company_name)

    return ValueDriversResponse(
        status="ready",
        company_name=company_name,
        enablers=[ValueDriverEntry(**e) for e in enablers],
        contributors=[ValueDriverEntry(**c) for c in contributors],
        etfs=[ETFEntry(**e) for e in etfs],
        enriched_at=vd.get("enriched_at"),
    )
