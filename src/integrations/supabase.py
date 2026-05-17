"""
Supabase client + DB service layer für Argo Analytics.

Alle DB-Operationen laufen hier durch — Controller bleiben frei von
Supabase-spezifischem Code.
"""

import logging
from supabase import create_client, Client
from src.config import settings

logger = logging.getLogger(__name__)

_client: Client | None = None


def get_supabase() -> Client:
    global _client
    if _client is None:
        _client = create_client(settings.supabase_url, settings.supabase_service_key)
    return _client


# ── Companies ────────────────────────────────────────────────────────────────

def fetch_companies(limit: int = 100, source: str | None = None) -> list[dict]:
    db = get_supabase()
    query = db.table("companies").select(
        "id, name, category, industry, potential, risk, ipo_potential, ipo_status, "
        "investment_path, proxy_ticker, funding_total_usd_mn, "
        "funding_last_round, last_signal, last_signal_date, source"
    ).limit(limit).order("name")

    if source:
        query = query.eq("source", source)

    result = query.execute()
    return result.data or []


def fetch_company_by_name(name: str) -> dict | None:
    db = get_supabase()
    result = db.table("companies").select("*").ilike("name", name).limit(1).execute()
    return result.data[0] if result.data else None


# ── Funding Rounds ────────────────────────────────────────────────────────────

def fetch_funding_rounds(company_id: str) -> list[dict]:
    """
    Gibt alle Funding Rounds einer Company zurück, chronologisch absteigend.
    """
    db = get_supabase()
    result = db.table("funding_rounds").select(
        "id, date, type, amount_usd_mn, lead_investor, co_investors, source, notes"
    ).eq("company_id", company_id).order("date", desc=True).execute()
    return result.data or []


# ── Buyers ───────────────────────────────────────────────────────────────────

def fetch_buyers(limit: int = 50) -> list[dict]:
    db = get_supabase()
    result = db.table("buyers").select(
        "id, name, ticker, exchange, market_cap_usd_bn, sector"
    ).limit(limit).order("name").execute()
    return result.data or []


def fetch_buyer_by_name(name: str) -> dict | None:
    db = get_supabase()
    result = db.table("buyers").select("*").ilike("name", name).limit(1).execute()
    return result.data[0] if result.data else None


# ── Deals ────────────────────────────────────────────────────────────────────

def insert_deal(request, company_id: str | None, buyer_id: str | None) -> str:
    """
    Persistiert den Analyse-Request als Deal-Eintrag.
    Gibt die UUID des neuen Deals zurück.
    """
    db = get_supabase()

    tr = request.tech_readiness_inputs
    payload = {
        "company_id": company_id,
        "buyer_id": buyer_id,
        "tam_usd_bn": request.tam_usd_bn,
        "buyer_market_cap_usd_bn": request.buyer_market_cap_usd_bn,
        "buyer_cash_usd_bn": request.buyer_cash_usd_bn,
        "buyer_debt_ebitda": request.buyer_debt_ebitda,
        "target_funding_usd_mn": request.target_funding_usd_mn,
        "target_stage": request.target_stage,
        "url": request.url,
        # TechReadiness inputs — null wenn nicht geliefert
        "tr_tech_stack_fit":             tr.tech_stack_fit if tr else None,
        "tr_gtm_fit":                    tr.gtm_fit if tr else None,
        "tr_integration_capacity":       tr.integration_capacity if tr else None,
        "tr_rd_intensity":               tr.rd_intensity if tr else None,
        "tr_capital_deployment_velocity":tr.capital_deployment_velocity if tr else None,
        "tr_regulatory_readiness":       tr.regulatory_readiness if tr else None,
        "tr_strategic_coherence":        tr.strategic_coherence if tr else None,
    }

    result = db.table("deals").insert(payload).execute()
    return result.data[0]["id"]


# ── Scores ───────────────────────────────────────────────────────────────────

def insert_score(deal_id: str, scores, summary: str, warnings: list[str]) -> str:
    """
    Persistiert das Scoring-Ergebnis verknüpft mit einem Deal.
    Gibt die UUID des Score-Eintrags zurück.
    """
    db = get_supabase()

    payload = {
        "deal_id": deal_id,
        # SRR
        "srr_value":             scores.srr.value,
        "srr_category":          scores.srr.category,
        "srr_cap_segment":       scores.srr.cap_segment,
        "srr_execution_warning": scores.srr.execution_warning,
        # MFR
        "mfr_value":             scores.mfr.value,
        "mfr_signal":            scores.mfr.signal,
        # TechReadiness
        "tr_value":              scores.tech_readiness.value,
        "tr_inputs_provided":    scores.tech_readiness.value != 0.5,
        # Deal Success
        "deal_success_score":    scores.deal_success_score,
        "rating":                scores.rating,
        "quadrant":              scores.quadrant,
        # Output
        "executive_summary":     summary,
        "warnings":              warnings,
    }

    result = db.table("scores").insert(payload).execute()
    return result.data[0]["id"]
