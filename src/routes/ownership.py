"""
GET /api/v1/company/{name}/ownership  —  v1.0
==============================================
Eigener Endpunkt für Tab-Ownership-Daten.

Gibt ownership_entries + Cap Table Score aus DB zurück.
Triggert Background-Enrichment wenn Einträge fehlen (EN-05).

Status-Logik:
  "ready"   → Einträge vorhanden + enriched_at gesetzt
  "pending" → noch nicht angereichert, wird jetzt angestoßen
  "running" → Enrichment läuft gerade
  "empty"   → Company nicht in DB
  "manual"  → nur kuratierte Overrides, keine Pipeline-Daten
"""

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from src.integrations.supabase import (
    fetch_companies,
    fetch_funding_rounds,
    get_supabase,
    set_enrichment_status,
)
from src.services.ownership_enrichment import (
    enrich_ownership,
    compute_cap_table_score,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["ownership"])


# ── Response Models ───────────────────────────────────────────────────────────

class OwnershipEntry(BaseModel):
    name: str
    type: str | None = None         # vc | pe | corporate | individual | government | unknown
    role: str | None = None         # lead_investor | co_investor | shareholder | managing_director
    share_pct: float | None = None
    source: str | None = None       # edgar_form_d | openregister_de | manual
    as_of_date: str | None = None
    notes: str | None = None


class CapTableScore(BaseModel):
    score: float                    # 0.0–1.0
    label: str                      # Niedrig | Mittel | Hoch
    note: str


class OwnershipResponse(BaseModel):
    status: str                     # ready | pending | running | manual | empty
    region: str | None = None       # US | DE
    source_used: str | None = None  # edgar | openregister | manual | none
    entries: list[OwnershipEntry] = []
    cap_table: CapTableScore | None = None
    enriched_at: str | None = None


# ── DB Helpers ────────────────────────────────────────────────────────────────

def _fetch_ownership_entries(company_id: str) -> list[dict]:
    """Liest ownership_entries für eine Company aus DB."""
    db = get_supabase()
    try:
        result = db.table("ownership_entries").select(
            "id, name, type, role, share_pct, source, as_of_date, created_at"
        ).eq("company_id", company_id).order("share_pct", desc=True).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_ownership_entries failed for %s: %s", company_id, e)
        return []


def _upsert_ownership_entries(company_id: str, entries: list[dict]) -> None:
    """
    Schreibt neue ownership_entries in DB.
    Nur Einträge mit name vorhanden — keine Duplikate via name+company_id.
    """
    if not entries:
        return
    db = get_supabase()
    rows = []
    for e in entries:
        if not e.get("name"):
            continue
        row = {
            "company_id": company_id,
            "name":       e["name"],
            "type":       e.get("type"),
            "role":       e.get("role"),
            "share_pct":  e.get("share_pct"),
            "source":     e.get("source"),
            "as_of_date": e.get("as_of_date"),
        }
        # notes-Feld in role-Feld packen wenn vorhanden (Schema hat kein notes-Feld)
        if e.get("notes") and not row["role"]:
            row["role"] = e["notes"]
        rows.append(row)

    try:
        db.table("ownership_entries").upsert(
            rows, on_conflict="company_id,name"
        ).execute()
        logger.warning("upsert_ownership_entries OK: %s → %s entries", company_id, len(rows))
    except Exception as e:
        logger.warning("upsert_ownership_entries FAILED for %s: %s", company_id, e)


def _ownership_enriched(entries: list[dict]) -> bool:
    """Gilt als angereichert wenn mind. 1 nicht-manuelle Quelle vorhanden."""
    return any(
        e.get("source") and e["source"] != "manual"
        for e in entries
    )


# ── Route ─────────────────────────────────────────────────────────────────────

@router.get("/company/{name}/ownership", response_model=OwnershipResponse)
async def get_company_ownership(name: str, background_tasks: BackgroundTasks) -> OwnershipResponse:
    """
    Gibt Ownership-Daten für eine Company zurück.
    Triggert Background-Enrichment wenn keine Pipeline-Daten vorhanden.
    """
    # 1. Company lookup
    companies = fetch_companies(limit=500)
    q = name.lower().replace("-", " ").replace("_", " ")
    company = next(
        (c for c in companies if
         c.get("name", "").lower() == q or
         q in c.get("name", "").lower() or
         c.get("name", "").lower().replace(" ", "-") == name.lower()),
        None,
    )

    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found.")

    company_id = company.get("id")
    company_name = company["name"]
    enrichment_status = company.get("enrichment_status", "pending")

    # 2. Bestehende Einträge aus DB
    existing = _fetch_ownership_entries(company_id) if company_id else []
    funding_rounds = fetch_funding_rounds(company_id) if company_id else []

    # 3. Cap Table Score immer berechnen (auch aus vorhandenen Daten)
    cap_raw = compute_cap_table_score(existing, funding_rounds)
    cap = CapTableScore(**cap_raw)

    # 4. Bereits angereichert → direkt zurückgeben
    if _ownership_enriched(existing):
        entries = [OwnershipEntry(
            name=e["name"], type=e.get("type"), role=e.get("role"),
            share_pct=e.get("share_pct"), source=e.get("source"),
            as_of_date=str(e["as_of_date"]) if e.get("as_of_date") else None,
        ) for e in existing]
        return OwnershipResponse(
            status="ready",
            entries=entries,
            cap_table=cap,
            enriched_at=datetime.utcnow().isoformat(),
        )

    # 5. Nur manuelle Einträge vorhanden (curated overrides)
    if existing:
        entries = [OwnershipEntry(
            name=e["name"], type=e.get("type"), role=e.get("role"),
            share_pct=e.get("share_pct"), source=e.get("source"),
            as_of_date=str(e["as_of_date"]) if e.get("as_of_date") else None,
        ) for e in existing]
        # Enrichment nur anstoßen wenn noch nie gelaufen — nicht bei running/done
        if company_id and enrichment_status not in ("running", "done", "error"):
            _queue_enrichment(background_tasks, company_id, company_name, company, existing, funding_rounds)
        # Immer "manual" zurückgeben wenn Einträge vorhanden — nie "running"
        # Frontend-Polling stoppt bei "manual", wartet nicht auf Enrichment
        return OwnershipResponse(
            status="manual",
            entries=entries,
            cap_table=cap,
        )

    # 6. Keine Einträge → Background-Enrichment anstoßen
    # Aber: wenn enrichment_status="running" bereits → 202-ähnlich, kein neuer Task
    if company_id and enrichment_status not in ("running",):
        _queue_enrichment(background_tasks, company_id, company_name, company, existing, funding_rounds)
        logger.info("Ownership enrichment queued for %s", company_name)

    # "running" → Frontend pollt weiter (korrekt, Daten kommen noch)
    # "done" ohne Einträge → Pipeline hat nichts gefunden, als "empty" markieren
    terminal_status = "empty" if enrichment_status == "done" else (enrichment_status or "pending")
    return OwnershipResponse(
        status=terminal_status,
        cap_table=cap,
    )


def _queue_enrichment(
    background_tasks: BackgroundTasks,
    company_id: str,
    company_name: str,
    company: dict,
    existing: list[dict],
    funding_rounds: list[dict],
) -> None:
    """Stellt Ownership-Enrichment als BackgroundTask in die Queue."""
    from src.config import settings

    async def _bg():
        try:
            set_enrichment_status(company_id, "running")
            result = await enrich_ownership(
                company_id=company_id,
                company_name=company_name,
                company=company,
                existing_entries=existing,
                funding_rounds=funding_rounds,
                openregister_api_key=getattr(settings, "openregister_api_key", None),
            )
            if result.get("entries"):
                _upsert_ownership_entries(company_id, result["entries"])
            set_enrichment_status(company_id, "done")
            logger.info(
                "Ownership enrichment done for %s — %s new entries via %s",
                company_name, len(result.get("entries", [])), result.get("source_used"),
            )
        except Exception:
            set_enrichment_status(company_id, "error")
            logger.exception("Ownership enrichment TRACEBACK for %s", company_name)

    background_tasks.add_task(_bg)
