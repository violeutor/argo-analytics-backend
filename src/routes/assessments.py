"""
assessments.py — R-18 v2.0: Company Assessments (algorithmisch + Claude Narrativ)
GET /api/v1/company/{name}/assessments

Flow v2.0 (BUG-31 Fix):
  1. DB-Cache prüfen (company_assessments) — frisch wenn score_source='algorithmic' + < 24h
  2. Cache hit → sofort zurück
  3. Cache miss:
     a. Algorithmische Scores via compute_dimension_risks() (score_calculator)
     b. Claude-Call NUR für Narrativ-Notes (keine Scores mehr!)
     c. Ergebnis in company_assessments schreiben
  4. Response zurück

Dimensionen (6):
  market · financials · strategy · political · technology · operations

Scoring-Prinzip (BUG-31):
  - Alle Scores kommen aus Daten (market_data, funding_stage, signals, value_drivers, peers)
  - Datenmangel → neutral (4.0–5.0), KEIN halluziniertes Risiko
  - Claude erklärt Scores, erfindet sie nicht
  - data_confidence: 'high'|'medium'|'low' je Dimension
"""

import json
import logging
import os
import re
import datetime
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

from src.integrations.supabase import (
    fetch_company_by_name,
    fetch_directional_signals,
    fetch_market_data,
    fetch_tam_cache,
    fetch_value_drivers,
    get_supabase,
)
from src.services.score_calculator import compute_dimension_risks

logger = logging.getLogger(__name__)
router = APIRouter()

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-haiku-4-5-20251001"
_CACHE_TTL_HOURS = 24


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


def _is_cache_fresh(cached: dict) -> bool:
    """Cache gültig wenn: score_source='algorithmic' + < 24h alt."""
    gen_at = cached.get("generated_at")
    if not gen_at:
        return False
    try:
        age = datetime.datetime.utcnow() - datetime.datetime.fromisoformat(gen_at.replace("Z", ""))
        if age.total_seconds() > _CACHE_TTL_HOURS * 3600:
            return False
    except Exception:
        return False
    # Prüfe ob algorithmische Scores (nicht Legacy-Claude-Scores)
    dims = cached.get("dimensions") or []
    if dims and isinstance(dims[0], dict):
        return dims[0].get("score_source") == "algorithmic"
    return False


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
    db = get_supabase()
    try:
        db.table("companies").update({"description": description}).eq("id", company_id).execute()
    except Exception as e:
        logger.warning("upsert_company_description FAILED(%s): %s", company_id, e)


# ── Context Builder für Claude-Narrativ ──────────────────────────────────────

_DIM_LABELS = {
    "market":     "Markt",
    "financials": "Finanzen",
    "strategy":   "Strategie",
    "political":  "Political Environment",
    "technology": "Technologie",
    "operations": "Operative Stärke",
}

def _build_narrative_context(
    company: dict,
    market: dict | None,
    tam: dict | None,
    pos_signals: list[dict],
    neg_signals: list[dict],
    dimension_scores: dict,
) -> str:
    """Baut strukturierten Kontext für Claude-Narrativ-Prompt."""
    lines = [
        f"COMPANY: {company.get('name')}",
        f"Category: {company.get('category') or company.get('industry') or '—'}",
        f"Stage: {company.get('funding_stage') or '—'}",
        f"Funding total: {company.get('funding_total_usd_mn') or '—'} USD mn",
        f"HQ: {company.get('headquarters') or '—'}",
        f"IPO status: {company.get('ipo_status') or '—'}",
        f"Region: {company.get('region') or '—'}",
    ]

    if market:
        lines += [
            "",
            "MARKET DATA:",
            f"  TAM 2035: {market.get('tam_2035_usd_bn') or '—'} USD bn",
            f"  CAGR: {market.get('cagr_pct') or '—'}%",
            f"  Competition: {market.get('competition_score') or '—'} — {market.get('competition_note') or ''}",
            f"  Market cycle: {market.get('market_cycle') or '—'} — {market.get('market_cycle_note') or ''}",
        ]

    # R-22: Peer Positioning Notes → strategy + operations dimension context
    peers_context: dict = company.get("peers_context") or {}
    if peers_context:
        lines += ["", "PEER POSITIONING (use for strategy + competitive risk notes):"]
        for peer_name, positioning in list(peers_context.items())[:4]:
            lines.append(f"  vs. {peer_name}: {positioning}")

    if pos_signals:
        lines += ["", "POSITIVE SIGNALS (recent):"]
        for s in pos_signals[:6]:
            lines.append(f"  [{s.get('signal_category')}] {s.get('summary') or s.get('raw_title') or ''}")

    if neg_signals:
        lines += ["", "NEGATIVE SIGNALS (recent):"]
        for s in neg_signals[:6]:
            lines.append(f"  [{s.get('signal_category')}] {s.get('summary') or s.get('raw_title') or ''}")

    lines += ["", "ALGORITHMIC SCORES (explain WHY, do not change):"]
    for dim_id, scores in dimension_scores.items():
        label = _DIM_LABELS.get(dim_id, dim_id)
        conf  = scores.get("data_confidence", "low")
        opp   = scores.get("opportunity_score", "—")
        risk  = scores.get("risk_score", "—")
        sources_opp = ", ".join(scores.get("opportunity_sources", []))
        sources_rsk = ", ".join(scores.get("risk_sources", []))
        lines.append(
            f"  {label}: opp={opp} (from: {sources_opp}) / risk={risk} (from: {sources_rsk}) — confidence={conf}"
        )
        if conf == "low":
            lines.append(f"    ↳ NOTE: {label} score is a sector baseline due to insufficient data.")

    return "\n".join(lines)


# ── Claude Narrativ-Call ──────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a factual M&A and investment analysis engine.
You explain pre-computed algorithmic scores for professional investors (VC, PE, M&A, Corporate, Asset Manager).
No marketing language. Only facts, evidence-based context, and concise notes.
Respond ONLY with valid JSON. No preamble, no explanation, no markdown fences."""

USER_PROMPT_TEMPLATE = """The following algorithmic scores have been computed for {company_name}.
Your task: write concise factual notes explaining WHY each score is what it is.
Do NOT change the scores. Do NOT invent numbers. Only explain with available evidence.

If confidence="low" for a dimension, explicitly state: "Insufficient data — score reflects sector baseline."

Dimensions to annotate:
1. market — market size, growth, competitive dynamics
2. financials — funding health, stage risk, financial signals
3. strategy — competitive positioning, peer landscape, strategic signals
4. political — regulatory environment, policy signals
5. technology — tech readiness, IP position, innovation signals
6. operations — enabler dependencies, supplier/customer concentration

Context:
{context}

Respond with this exact JSON structure:
{{
  "description": "2-3 sentence factual company description for investors",
  "dimensions": [
    {{
      "id": "market",
      "opportunity_note": "<factual, max 2 sentences, explain the score>",
      "risk_note": "<factual, max 2 sentences, explain the score>"
    }},
    {{
      "id": "financials",
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "strategy",
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "political",
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "technology",
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_note": "<factual, max 2 sentences>"
    }},
    {{
      "id": "operations",
      "opportunity_note": "<factual, max 2 sentences>",
      "risk_note": "<factual, max 2 sentences>"
    }}
  ]
}}"""


async def _call_claude_narrative(company_name: str, context: str) -> dict:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    prompt = USER_PROMPT_TEMPLATE.format(company_name=company_name, context=context)

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
                "max_tokens": 1200,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        resp.raise_for_status()
        data = resp.json()

    raw   = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
    clean = re.sub(r"```(?:json)?|```", "", raw).strip()
    return json.loads(clean)


# ── Dimensions zusammenbauen: Scores + Narrativ mergen ───────────────────────

def _merge_dimensions(
    dimension_scores: dict,
    narrative_dims: list[dict],
) -> list[dict]:
    """
    Mergt algorithmische Scores mit Claude-Narrativ-Notes.
    Scores sind autoritativ, Notes kommen von Claude.
    """
    note_map = {d["id"]: d for d in (narrative_dims or [])}

    result = []
    for dim_id, scores in dimension_scores.items():
        note = note_map.get(dim_id, {})
        result.append({
            "id":               dim_id,
            "label":            _DIM_LABELS.get(dim_id, dim_id),
            # Algorithmische Scores (autoritativ)
            "opportunity_score": scores["opportunity_score"],
            "risk_score":        scores["risk_score"],
            "data_confidence":   scores["data_confidence"],
            "opportunity_sources": scores.get("opportunity_sources", []),
            "risk_sources":        scores.get("risk_sources", []),
            # Claude Narrativ
            "opportunity_note":  note.get("opportunity_note", ""),
            "risk_note":         note.get("risk_note", ""),
            # Meta
            "score_source":      "algorithmic",
        })

    return result


# ── Composite aus Dimension-Scores ────────────────────────────────────────────

def _compute_composites(dimension_scores: dict) -> tuple[float | None, float | None]:
    """Berechnet composite_opportunity + composite_risk als Durchschnitt der Dimension-Scores."""
    opp_vals  = [v["opportunity_score"] for v in dimension_scores.values() if v.get("opportunity_score") is not None]
    risk_vals = [v["risk_score"]        for v in dimension_scores.values() if v.get("risk_score")        is not None]

    comp_opp  = round(sum(opp_vals)  / len(opp_vals),  1) if opp_vals  else None
    comp_risk = round(sum(risk_vals) / len(risk_vals), 1) if risk_vals else None
    return comp_opp, comp_risk


# ── Route ─────────────────────────────────────────────────────────────────────

@router.get("/api/v1/company/{name}/assessments")
async def get_assessments(name: str):
    """
    R-18 v2.0: Company Assessments — 6 Dimensionen, algorithmische Scores + Claude Narrativ.
    Cache-first: DB → sofort wenn frisch. Cache miss → Scores berechnen + Claude Narrativ → DB → Response.
    """
    # 1. Company laden
    company = fetch_company_by_name(name)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")

    company_id   = company["id"]
    company_name = company["name"]

    # 2. Cache prüfen (nur algorithmische Scores akzeptieren)
    cached = fetch_assessments(company_id)
    if cached and _is_cache_fresh(cached):
        logger.info("assessments cache hit (algorithmic): %s", name)
        return {
            "company":              name,
            "source":               "cache",
            "generated_at":         cached.get("generated_at"),
            "model":                cached.get("model"),
            "description":          cached.get("description"),
            "dimensions":           cached.get("dimensions") or [],
            "composite_opportunity": cached.get("composite_opportunity"),
            "composite_risk":        cached.get("composite_risk"),
        }

    # 3. Daten für algorithmische Scores laden
    market      = fetch_market_data(company_id)
    tam         = fetch_tam_cache(company_id)
    pos_signals = fetch_directional_signals(company_id, "positive", limit=15)
    neg_signals = fetch_directional_signals(company_id, "negative", limit=15)
    vd_raw      = fetch_value_drivers(company_id)

    # Value Drivers als flache Liste für compute_dimension_risks
    vd_flat: list[dict] = []
    if vd_raw:
        for key in ("enablers", "contributors"):
            vd_flat.extend(vd_raw.get(key) or [])

    all_signals = pos_signals + neg_signals

    # 4. Algorithmische Scores berechnen
    dimension_scores = compute_dimension_risks(
        company=company,
        market_data=market,
        signals=all_signals,
        value_drivers=vd_flat,
    )

    # 5. Composite berechnen
    comp_opp, comp_risk = _compute_composites(dimension_scores)

    # 6. Claude-Call — nur Narrativ, keine Scores
    narrative_dims: list[dict] = []
    description = ""
    try:
        context = _build_narrative_context(
            company=company,
            market=market,
            tam=tam,
            pos_signals=pos_signals,
            neg_signals=neg_signals,
            dimension_scores=dimension_scores,
        )
        result = await _call_claude_narrative(company_name, context)
        narrative_dims = result.get("dimensions") or []
        description    = result.get("description") or ""
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 529:
            logger.warning("Claude 529 overloaded for %s — using scores only", name)
        else:
            logger.error("Claude narrative call failed for %s: %s", name, e)
    except Exception as e:
        logger.warning("Claude narrative call failed for %s: %s — scores still returned", name, e)

    # 7. Scores + Narrativ mergen
    merged_dims = _merge_dimensions(dimension_scores, narrative_dims)

    # 8. In DB cachen
    payload: dict[str, Any] = {
        "dimensions":            merged_dims,
        "composite_opportunity": comp_opp,
        "composite_risk":        comp_risk,
        "description":           description or company.get("description") or "",
        "generated_at":          datetime.datetime.utcnow().isoformat(),
        "model":                 MODEL,
    }
    upsert_assessments(company_id, payload)

    if description:
        upsert_company_description(company_id, description)

    return {
        "company":               company_name,
        "source":                "generated",
        "generated_at":          payload["generated_at"],
        "model":                 MODEL,
        "description":           description,
        "dimensions":            merged_dims,
        "composite_opportunity": comp_opp,
        "composite_risk":        comp_risk,
    }
