"""
src/routes/explore.py
EXPLORE-01: Personalisierter Explore-Feed.

Logik:
  1. user_id auflösen (ARGO_DEFAULT_USER_ID / später JWT via Q-D01)
  2. user_industry_preferences → sector_keys des Users
  3. user_profiles → customer_type (vc/pe/ma_agency/corporate/family_office)
  4. Companies nach Industry filtern + company_scores joinen
  5. Sortierung nach customer_type-relevantem Score-Profil

Scoring-Profile nach customer_type:
  vc           → Growth + FundingMomentum → composite + financial_score
  pe           → EBITDA/Cashflow-Fokus → financial_score prominent
  ma_agency    → M&A-Pfade + SRR/MFR → composite_score (bereits M&A-gewichtet)
  corporate    → Value Drivers → market_score + composite
  family_office→ ETF/Proxy + Liquidität → market_score
  other        → composite_score Default

Phase 1: Sort-Key-Berechnung in Python (einfach, deterministisch).
Phase 2: ML-basiertes Ranking wenn genug User-Interaktionsdaten vorhanden.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import APIRouter, Header, Query

from src.integrations.supabase import get_supabase

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["explore"])

_DEFAULT_USER_ID: str | None = os.getenv("ARGO_DEFAULT_USER_ID")

# ── Taxonomy-Mapping: sector_key → industry-Label-Varianten ──────────────────
# Companies.industry ist Freitext — ILIKE-Match auf bekannte Varianten.
# Taxonomy v1.0 (schema_user_profiles_v2.sql CHECK-Constraint).
_SECTOR_INDUSTRY_MAP: dict[str, list[str]] = {
    "climate_tech":        ["Climate Tech", "CleanTech", "Clean Tech", "Cleantech"],
    "energy_transition":   ["Energy Transition", "Energy", "Renewable Energy", "Clean Energy"],
    "mobility":            ["Mobility", "Transportation", "Automotive", "EV"],
    "health_tech":         ["Health Tech", "HealthTech", "Digital Health", "MedTech"],
    "biotech_pharma":      ["Biotech", "Pharma", "Life Sciences", "Pharmaceuticals", "Biopharma"],
    "fintech":             ["FinTech", "Financial Technology", "Finance", "InsurTech"],
    "enterprise_software": ["Enterprise Software", "SaaS", "B2B Software", "Software"],
    "deep_tech":           ["Deep Tech", "DeepTech", "Advanced Technology", "Hard Tech"],
    "consumer_tech":       ["Consumer Tech", "Consumer Technology", "B2C Tech"],
    "industrial_tech":     ["Industrial Tech", "IndustrialTech", "Industrials", "Manufacturing"],
    "food_agritech":       ["Food Tech", "AgriTech", "Agriculture", "Food & AgriTech", "Agritech"],
    "space_defense":       ["Space", "Defense", "Aerospace", "Space & Defense", "Space Tech"],
    "media_entertainment": ["Media", "Entertainment", "Media & Entertainment", "Content"],
    "real_estate_proptech":["PropTech", "Real Estate", "Property Tech", "Construction Tech"],
}

# ── Customer-Type Sort-Profil ─────────────────────────────────────────────────

def _sort_key(company: dict, scores: dict, customer_type: str) -> float:
    """
    Berechnet einen sortierbaren Score je customer_type.
    Alle Werte normalisiert 0–10; höher = relevanter.
    Fehlende Felder → 0 (kein Fehler, nur niedrig gerankt).
    """
    composite      = float(scores.get("composite_score") or 0)
    financial      = float(scores.get("financial_score")  or 0)
    market         = float(scores.get("market_score")     or 0)
    funding_mn     = float(company.get("funding_total_usd_mn") or 0)
    funding_bonus  = min(funding_mn / 500, 2.0)  # max 2 Punkte Bonus bei $1B+

    if customer_type == "vc":
        # Growth + Funding Momentum zählen stark
        return composite * 0.5 + financial * 0.3 + funding_bonus * 1.0
    elif customer_type == "pe":
        # Finanzielle Solidität > Wachstum
        return financial * 0.6 + composite * 0.4
    elif customer_type == "ma_agency":
        # composite_score ist bereits M&A/SRR-gewichtet
        return composite * 0.7 + market * 0.3
    elif customer_type == "corporate":
        # Value Drivers + Markt → market_score + composite
        return market * 0.5 + composite * 0.5
    elif customer_type == "family_office":
        # ETF/Proxy-Fähigkeit + Stabilität → market_score
        return market * 0.6 + composite * 0.4
    else:
        return composite


# ── User-Kontext ──────────────────────────────────────────────────────────────

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


def _get_user_context(user_id: str) -> tuple[list[str], str]:
    """
    Holt sector_keys + customer_type für user_id.
    Returns: (sector_keys, customer_type) — leer/default bei Fehler.
    """
    try:
        prefs = (
            get_supabase()
            .table("user_industry_preferences")
            .select("sector_key")
            .eq("user_id", user_id)
            .execute()
            .data or []
        )
        sector_keys = [r["sector_key"] for r in prefs]
    except Exception as e:
        logger.warning("explore: user_industry_preferences Fehler: %s", e)
        sector_keys = []

    try:
        profile = (
            get_supabase()
            .table("user_profiles")
            .select("customer_type")
            .eq("id", user_id)
            .limit(1)
            .execute()
            .data or []
        )
        customer_type = profile[0]["customer_type"] if profile else "other"
    except Exception as e:
        logger.warning("explore: user_profiles Fehler: %s", e)
        customer_type = "other"

    return sector_keys, customer_type


# ── Industry-Filter ───────────────────────────────────────────────────────────

def _industry_matches(company_industry: str | None, industry_labels: list[str]) -> bool:
    """Case-insensitive Substring-Match auf bekannte Industry-Label-Varianten."""
    if not company_industry:
        return False
    ind_lower = company_industry.lower()
    return any(label.lower() in ind_lower or ind_lower in label.lower()
               for label in industry_labels)


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.get("/explore")
async def explore(
    authorization: Optional[str] = Header(default=None),
    limit: int = Query(default=30, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """
    Personalisierter Company-Feed basierend auf user_industry_preferences
    und customer_type-gewichteter Sortierung.

    Response:
      {
        companies: [...],   # sortierte Company-Cards mit Scores
        customer_type: str, # Segment des Users (für FE-Anzeige)
        sector_keys: [...],  # aktive Präferenzen
        total: int           # Anzahl vor Pagination
      }
    """
    user_id = _resolve_user_id(authorization)

    # Kein User-Kontext → leerer Feed (kein 401, Login-Modal übernimmt)
    if not user_id:
        return {"companies": [], "customer_type": "other", "sector_keys": [], "total": 0}

    sector_keys, customer_type = _get_user_context(user_id)

    if not sector_keys:
        logger.info("explore: keine Industrie-Präferenzen für user=%.8s", user_id)
        return {"companies": [], "customer_type": customer_type, "sector_keys": [], "total": 0}

    # ── Companies laden ───────────────────────────────────────────────────────
    try:
        companies_raw = (
            get_supabase()
            .table("companies")
            .select(
                "id, name, category, industry, ipo_status, ipo_potential, "
                "investment_path, proxy_ticker, ticker, exchange, "
                "funding_total_usd_mn, funding_stage, funding_last_round, "
                "last_signal, last_signal_date, headquarters, headcount, "
                "description, region, website"
            )
            .limit(500)
            .execute()
            .data or []
        )
    except Exception as e:
        logger.error("explore: companies Fetch fehlgeschlagen: %s", e)
        return {"companies": [], "customer_type": customer_type, "sector_keys": sector_keys, "total": 0}

    # ── Industry-Filter ───────────────────────────────────────────────────────
    # Alle Label-Varianten für aktive sector_keys sammeln
    target_labels: list[str] = []
    for sk in sector_keys:
        target_labels.extend(_SECTOR_INDUSTRY_MAP.get(sk, []))

    filtered = [c for c in companies_raw if _industry_matches(c.get("industry"), target_labels)]

    if not filtered:
        logger.info(
            "explore: 0 Companies nach Industry-Filter (sector_keys=%s, user=%.8s)",
            sector_keys, user_id,
        )
        return {"companies": [], "customer_type": customer_type, "sector_keys": sector_keys, "total": 0}

    # ── Scores laden ─────────────────────────────────────────────────────────
    company_ids = [c["id"] for c in filtered if c.get("id")]
    scores_by_id: dict[str, dict] = {}
    if company_ids:
        try:
            scores_raw = (
                get_supabase()
                .table("company_scores")
                .select("company_id, composite_score, rating, financial_score, market_score")
                .in_("company_id", company_ids)
                .execute()
                .data or []
            )
            scores_by_id = {r["company_id"]: r for r in scores_raw}
        except Exception as e:
            logger.warning("explore: company_scores Fetch fehlgeschlagen: %s", e)

    # ── Sortierung nach customer_type ─────────────────────────────────────────
    scored = []
    for c in filtered:
        cid = c.get("id", "")
        s   = scores_by_id.get(cid, {})
        scored.append({
            **c,
            "composite_score": s.get("composite_score"),
            "rating":          s.get("rating"),
            "financial_score": s.get("financial_score"),
            "market_score":    s.get("market_score"),
            "_sort": _sort_key(c, s, customer_type),
        })

    scored.sort(key=lambda x: x["_sort"], reverse=True)

    # _sort ist intern — nicht im Response
    for c in scored:
        c.pop("_sort", None)

    total     = len(scored)
    paginated = scored[offset: offset + limit]

    logger.info(
        "explore: user=%.8s segment=%s sectors=%s → %d/%d Companies",
        user_id, customer_type, sector_keys, len(paginated), total,
    )

    return {
        "companies":     paginated,
        "customer_type": customer_type,
        "sector_keys":   sector_keys,
        "total":         total,
    }
