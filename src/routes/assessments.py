"""
assessments.py — R-18: Company Assessments via Claude
GET /api/v1/company/{name}/assessments

Flow:
  1. DB-Cache prüfen (company_assessments)
  2. Cache hit → sofort zurück
  3. Cache miss → Claude-Call (Wikipedia + market_data + signals + scorings)
  4. Ergebnis in company_assessments + companies.description schreiben
  5. Response zurück

Dimensionen (6):
  market · financials · strategy · political · technology · operations
"""

import json
import logging
import os
import re
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

from src.integrations.supabase import (
    fetch_company_by_name,
    fetch_directional_signals,
    fetch_market_data,
    fetch_tam_cache,
    get_supabase,
)

logger = logging.getLogger(__name__)
router = APIRouter()

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-haiku-4-5-20251001"

# ── Dimensionen-Definitionen ──────────────────────────────────────────────────

DIMENSIONS = [
    {
        "id": "market",
        "label": "Markt",
        "signal_categories_opportunity": ["market_growth", "new_partnership", "new_customer"],
        "signal_categories_risk": ["market_decline", "competitive_pressure"],
    },
    {
        "id": "financials",
        "label": "Finanzen",
        "signal_categories_opportunity": ["funding_round", "revenue_growth"],
        "signal_categories_risk": ["negative_earnings", "high_burn", "debt_increase"],
    },
    {
        "id": "strategy",
        "label": "Strategie",
        "signal_categories_opportunity": ["new_partnership", "expansion", "acquisition"],
        "signal_categories_risk": ["leadership_change", "strategy_pivot"],
    },
    {
        "id": "political",
        "label": "Political Environment",
        "signal_categories_opportunity": ["regulatory_positive", "subsidy", "policy_support"],
        "signal_categories_risk": ["regulatory_intervention", "policy_risk", "sanctions"],
    },
    {
        "id": "technology",
        "label": "Technologie",
        "signal_categories_opportunity": ["patent", "new_product", "tech_milestone"],
        "signal_categories_risk": ["ip_risk", "tech_obsolescence"],
    },
    {
        "id": "operations",
        "label": "Operative Stärke",
        "signal_categories_opportunity": ["headcount_growth", "revenue_per_fte"],
        "signal_categories_risk": ["supply_chain_issue", "key_person_risk", "customer_concentration"],
    },
]


# ── DB-Funktionen ─────────────────────────────────────────────────────────────

def fetch_assessments(company_id: str) -> dict | None:
    db = get_supabase()
    try:
        r = db.table("company_assessments").select(
            "dimensions, composite_opportunity, composite_risk, description, generated_at, model"
        ).eq("company_id", company_id).limit(1).execute()
        rows = r.data or []
        if rows and rows[0].get("dimensions"):
            return rows[0]
        return None
    except Exception as e:
        logger.warning("fetch_assessments(%s): %s", company_id, e)
        return None


def upsert_assessments(company_id: str, data: dict) -> None:
    db = get_supabase()
    try:
        db.table("company_assessments").upsert(
            {"company_id": company_id, **data},
            on_conflict="company_id",
        ).execute()
        logger.info("upsert_assessments OK: %s", company_id)
    except Exception as e:
        logger.warning("upsert_assessments FAILED(%s): %s", company_id, e)


def upsert_company_description(company_id: str, description: str) -> None:
    """Schreibt AI-generierte Beschreibung in companies.description (Tab 0)."""
    db = get_supabase()
    try:
        db.table("companies").update({"description": description}).eq("id", company_id).execute()
        logger.info("upsert_company_description OK: %s", company_id)
    except Exception as e:
        logger.warning("upsert_company_description FAILED(%s): %s", company_id, e)


def fetch_scorings(company_id: str) -> list[dict]:
    db = get_supabase()
    try:
        r = db.table("scores").select(
            "rating, srr_value, srr_category, mfr_value, mfr_signal, tr_value"
        ).eq("company_id", company_id).order("created_at", desc=True).limit(1).execute()
        return r.data or []
    except Exception as e:
        logger.warning("fetch_scorings(%s): %s", company_id, e)
        return []


# ── Context Builder ───────────────────────────────────────────────────────────

def _build_context(company: dict, market: dict | None, tam: dict | None,
                   scorings: list[dict], pos_signals: list[dict],
                   neg_signals: list[dict]) -> str:
    """Baut strukturierten Kontext-String für Claude-Prompt."""

    lines = [
        f"COMPANY: {company.get('name')}",
        f"Category: {company.get('category') or company.get('industry') or '—'}",
        f"HQ: {company.get('headquarters') or '—'}",
        f"Founded: {company.get('founding_year') or '—'}",
        f"Stage: {company.get('funding_stage') or '—'}",
        f"Funding total: {company.get('funding_total_usd_mn') or '—'} USD mn",
        f"Headcount: {company.get('headcount') or '—'}",
        f"IPO status: {company.get('ipo_status') or '—'}",
        f"Region: {company.get('region') or '—'}",
        f"Description (existing): {company.get('description') or '—'}",
    ]

    if market:
        lines += [
            "",
            "MARKET DATA:",
            f"  TAM 2035: {market.get('tam_2035_usd_bn') or '—'} USD bn",
            f"  CAGR: {market.get('cagr_pct') or '—'}%",
            f"  SAM: {market.get('sam_usd_bn') or '—'} USD bn",
            f"  Competition: {market.get('competition_score') or '—'} — {market.get('competition_note') or ''}",
            f"  Market cycle: {market.get('market_cycle') or '—'} — {market.get('market_cycle_note') or ''}",
            f"  Growth drivers: {market.get('growth_drivers') or '—'}",
        ]

    if tam:
        lines += [f"  TAM source: {tam.get('source') or '—'}"]

    if scorings:
        sc = scorings[0]
        lines += [
            "",
            "SCORING:",
            f"  Rating: {sc.get('rating') or '—'}",
            f"  SRR: {sc.get('srr_value') or '—'} ({sc.get('srr_category') or '—'})",
            f"  MFR: {sc.get('mfr_value') or '—'} → {sc.get('mfr_signal') or '—'}",
            f"  TechReadiness: {sc.get('tr_value') or '—'}",
        ]

    if pos_signals:
        lines += ["", "POSITIVE SIGNALS (recent):"]
        for s in pos_signals[:8]:
            lines.append(f"  [{s.get('signal_category') or s.get('event_type')}] {s.get('summary') or s.get('raw_title') or ''}  ({s.get('event_date') or ''})")

    if neg_signals:
        lines += ["", "NEGATIVE SIGNALS (recent):"]
        for s in neg_signals[:8]:
            lines.append(f"  [{s.get('signal_category') or s.get('event_type')}] {s.get('summary') or s.get('raw_title') or ''}  ({s.get('event_date') or ''})")

    return "\n".join(lines)


# ── Claude Call ───────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a factual M&A and investment analysis engine. 
You assess companies for professional investors (VC, PE, M&A, Corporate, Asset Manager).
No marketing language. Only facts, numbers, and evidence-based assessments.
Respond ONLY with valid JSON. No preamble, no explanation, no markdown fences."""

USER_PROMPT_TEMPLATE = """Assess the following company across 6 dimensions. 
For each dimension provide opportunity_score (0-10), risk_score (0-10), and concise factual notes (max 2 sentences each).
Also generate a company description (2-3 sentences, factual, investor-grade, no marketing).

Dimensions to assess:
1. market — market size, growth rate, market cycle stage
2. financials — funding health, burn efficiency, revenue signals, EBITDA if available
3. strategy — SRR/MFR scoring signals, buyer fit, strategic positioning
4. political — regulatory environment, subsidies, policy tailwinds/headwinds, geopolitical exposure
5. technology — tech readiness, IP position, innovation signals, obsolescence risk
6. operations — team signals, headcount trends, customer concentration, supply chain

Scoring guide:
- opportunity_score 8-10: strong positive evidence
- opportunity_score 5-7: moderate positive signals
- opportunity_score 0-4: weak or no positive signals
- risk_score 8-10: material risk with evidence
- risk_score 5-7: moderate risk signals
- risk_score 0-4: low or no risk signals

Context:
{context}

Respond with this exact JSON structure:
{{
  "description": "2-3 sentence factual company description for investors",
  "dimensions": [
    {{
      "id": "market",
      "label": "Markt",
      "opportunity_score": <0-10>,
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_score": <0-10>,
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "financials",
      "label": "Finanzen",
      "opportunity_score": <0-10>,
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_score": <0-10>,
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "strategy",
      "label": "Strategie",
      "opportunity_score": <0-10>,
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_score": <0-10>,
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "political",
      "label": "Political Environment",
      "opportunity_score": <0-10>,
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_score": <0-10>,
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "technology",
      "label": "Technologie",
      "opportunity_score": <0-10>,
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_score": <0-10>,
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "operations",
      "label": "Operative Stärke",
      "opportunity_score": <0-10>,
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_score": <0-10>,
      "risk_note": "<factual, max 2 sentences>"
    }}
  ],
  "composite_opportunity": <weighted average of opportunity_scores, 1 decimal>,
  "composite_risk": <weighted average of risk_scores, 1 decimal>
}}"""


async def _call_claude(context: str) -> dict:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    prompt = USER_PROMPT_TEMPLATE.format(context=context)

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MODEL,
                "max_tokens": 1500,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        resp.raise_for_status()
        data = resp.json()

    raw = "".join(
        block.get("text", "") for block in data.get("content", [])
        if block.get("type") == "text"
    )
    # JSON aus Antwort extrahieren — strip markdown fences falls vorhanden
    clean = re.sub(r"```(?:json)?|```", "", raw).strip()
    return json.loads(clean)


# ── Route ─────────────────────────────────────────────────────────────────────

@router.get("/api/v1/company/{name}/assessments")
async def get_assessments(name: str):
    """
    R-18: Company Assessments — 6 Dimensionen mit Opportunity/Risk Scores.
    Cache-first: DB → sofort. Cache miss → Claude-Call → DB-Write → Response.
    """
    # 1. Company laden
    company = fetch_company_by_name(name)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")

    company_id = company["id"]

    # 2. Cache prüfen
    cached = fetch_assessments(company_id)
    if cached:
        logger.info("assessments cache hit: %s", name)
        return {
            "company": name,
            "source": "cache",
            "generated_at": cached.get("generated_at"),
            "model": cached.get("model"),
            "description": cached.get("description"),
            "dimensions": cached.get("dimensions") or [],
            "composite_opportunity": cached.get("composite_opportunity"),
            "composite_risk": cached.get("composite_risk"),
        }

    # 3. Kontext zusammenbauen
    market = fetch_market_data(company_id)
    tam    = fetch_tam_cache(company_id)
    scorings = fetch_scorings(company_id)
    pos_signals = fetch_directional_signals(company_id, "positive", limit=10)
    neg_signals = fetch_directional_signals(company_id, "negative", limit=10)

    context = _build_context(company, market, tam, scorings, pos_signals, neg_signals)

    # 4. Claude-Call
    try:
        result = await _call_claude(context)
    except Exception as e:
        logger.error("Claude assessments call failed for %s: %s", name, e)
        raise HTTPException(status_code=502, detail=f"Assessment generation failed: {e}")

    # 5. Validierung
    dimensions = result.get("dimensions") or []
    description = result.get("description") or ""
    composite_opp  = result.get("composite_opportunity")
    composite_risk = result.get("composite_risk")

    if not dimensions:
        raise HTTPException(status_code=502, detail="Claude returned empty dimensions")

    # 6. In DB schreiben
    import datetime
    payload: dict[str, Any] = {
        "dimensions":             dimensions,
        "composite_opportunity":  composite_opp,
        "composite_risk":         composite_risk,
        "description":            description,
        "generated_at":           datetime.datetime.utcnow().isoformat(),
        "model":                  MODEL,
    }
    upsert_assessments(company_id, payload)

    # 7. companies.description aktualisieren (Tab 0)
    if description:
        upsert_company_description(company_id, description)

    return {
        "company": name,
        "source": "generated",
        "generated_at": payload["generated_at"],
        "model": MODEL,
        "description": description,
        "dimensions": dimensions,
        "composite_opportunity": composite_opp,
        "composite_risk": composite_risk,
    }
