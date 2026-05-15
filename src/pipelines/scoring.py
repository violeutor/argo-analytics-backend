"""
Argo Analytics – Scoring Engine
================================
Implements:
  - SRR  (Strategic Relevance Ratio)   = TAM / Buyer MarketCap
  - MFR  (M&A Feasibility Ratio)       = f(cash, debt/EBITDA, target valuation)
  - TechReadiness                       = weighted mean of 7 Amazon/Google-style factors
  - DealSuccessScore                    = SRR_norm × MFR_norm × TechReadiness
"""

import math
from src.models.schemas import (
    AnalyzeRequest,
    SRRResult,
    MFRResult,
    TechReadinessResult,
    ScoreResult,
    TechReadinessInputs,
)

# ── Constants ─────────────────────────────────────────────────────────────────

# Stage-based valuation multiplier applied to total funding to estimate EV
_STAGE_MULTIPLIERS = {
    "seed": 8.0,
    "series_a": 6.0,
    "series_b": 5.0,
    "series_c": 4.0,
    "series_d_plus": 3.5,
    "public": 1.0,  # use funding as proxy only; real EV should be supplied
}

# TechReadiness factor weights (sum = 1.0) — Amazon/Google framework
_TR_WEIGHTS = {
    "tech_stack_fit": 0.20,
    "gtm_fit": 0.15,
    "integration_capacity": 0.20,
    "rd_intensity": 0.10,
    "capital_deployment_velocity": 0.15,
    "regulatory_readiness": 0.10,
    "strategic_coherence": 0.10,
}

# MFR normalization bounds
_MFR_FEASIBLE_THRESHOLD = 0.15
_MFR_WATCH_THRESHOLD = 0.50

# SRR thresholds
_SRR_HIGH_STRATEGIC = 0.15
_SRR_TRANSFORMATIONAL = 0.50
_SRR_TRANSFORMATIONAL_PP = 1.0

# Execution warning: low cap + high SRR is a false positive risk
_LOW_CAP_THRESHOLD_BN = 10.0
_HIGH_SRR_WARNING = 5.0


# ── SRR ──────────────────────────────────────────────────────────────────────

def compute_srr(tam_usd_bn: float, buyer_market_cap_usd_bn: float) -> SRRResult:
    if buyer_market_cap_usd_bn <= 0:
        raise ValueError("Buyer market cap must be > 0")

    srr = tam_usd_bn / buyer_market_cap_usd_bn

    if srr >= _SRR_TRANSFORMATIONAL_PP:
        category = "Transformational++"
    elif srr >= _SRR_TRANSFORMATIONAL:
        category = "Transformational"
    elif srr >= _SRR_HIGH_STRATEGIC:
        category = "High Strategic"
    else:
        category = "Low Strategic"

    if buyer_market_cap_usd_bn < _LOW_CAP_THRESHOLD_BN:
        cap_segment = "low"
    elif buyer_market_cap_usd_bn < 100:
        cap_segment = "mid"
    else:
        cap_segment = "high"

    execution_warning = (
        cap_segment == "low" and srr > _HIGH_SRR_WARNING
    )

    return SRRResult(
        value=round(srr, 4),
        category=category,
        cap_segment=cap_segment,
        execution_warning=execution_warning,
    )


# ── MFR ──────────────────────────────────────────────────────────────────────

def compute_mfr(
    buyer_market_cap_usd_bn: float,
    buyer_cash_usd_bn: float,
    buyer_debt_ebitda: float,
    target_funding_usd_mn: float,
    target_stage: str,
) -> MFRResult:
    """
    MFR = estimated target EV / buyer market cap

    Thresholds (from validated M&A dataset):
      Feasible   < 0.15
      Watch      0.15 – 0.50
      Overstretch > 0.50
    """
    multiplier = _STAGE_MULTIPLIERS.get(target_stage, 5.0)
    estimated_ev_usd_mn = target_funding_usd_mn * multiplier
    estimated_ev_usd_bn = estimated_ev_usd_mn / 1000

    mfr = estimated_ev_usd_bn / buyer_market_cap_usd_bn if buyer_market_cap_usd_bn > 0 else 999

    # Adjust for leverage headroom: high debt/EBITDA tightens feasibility
    if buyer_debt_ebitda > 3.0:
        mfr = mfr * (1 + (buyer_debt_ebitda - 3.0) * 0.1)

    if mfr < _MFR_FEASIBLE_THRESHOLD:
        signal = "Feasible"
    elif mfr < _MFR_WATCH_THRESHOLD:
        signal = "Watch"
    else:
        signal = "Overstretch"

    return MFRResult(value=round(mfr, 4), signal=signal)


# ── TechReadiness ─────────────────────────────────────────────────────────────

def compute_tech_readiness(inputs: TechReadinessInputs | None) -> TechReadinessResult:
    """
    Weighted mean across 7 factors (Amazon/Google M&A readiness framework).
    If inputs are not provided, returns a neutral midpoint (0.5) with a flag.
    """
    if inputs is None:
        # Neutral fallback — caller should warn user
        factor_scores = {k: 0.5 for k in _TR_WEIGHTS}
        return TechReadinessResult(value=0.5, factor_scores=factor_scores)

    factor_scores = {
        "tech_stack_fit": inputs.tech_stack_fit,
        "gtm_fit": inputs.gtm_fit,
        "integration_capacity": inputs.integration_capacity,
        "rd_intensity": inputs.rd_intensity,
        "capital_deployment_velocity": inputs.capital_deployment_velocity,
        "regulatory_readiness": inputs.regulatory_readiness,
        "strategic_coherence": inputs.strategic_coherence,
    }

    score = sum(v * _TR_WEIGHTS[k] for k, v in factor_scores.items())

    return TechReadinessResult(
        value=round(score, 4),
        factor_scores={k: round(v, 4) for k, v in factor_scores.items()},
    )


# ── Normalization helpers ─────────────────────────────────────────────────────

def _normalize_srr(srr_value: float) -> float:
    """Map SRR to 0–1 using log-scaling to dampen extreme values (e.g. 28x)."""
    if srr_value <= 0:
        return 0.0
    # log(1 + srr) / log(1 + ceiling) — ceiling at 10x
    ceiling = 10.0
    return min(math.log1p(srr_value) / math.log1p(ceiling), 1.0)


def _normalize_mfr(mfr_value: float) -> float:
    """Invert MFR: lower ratio = more feasible = higher score.
    
    Feasible (<0.15) → 1.0
    Watch (0.15–0.50) → 0.5–0.7
    Overstretch (>0.50) → approaches 0
    Uses a floor of 0.4 for Feasible cases so DealSuccessScore stays meaningful.
    """
    if mfr_value <= 0:
        return 1.0
    if mfr_value < _MFR_FEASIBLE_THRESHOLD:
        return 1.0  # Comfortably feasible — full score
    return max(0.0, 1.0 - min(mfr_value / _MFR_WATCH_THRESHOLD, 1.0))


# ── Rating & Quadrant ─────────────────────────────────────────────────────────

def _derive_rating(srr: SRRResult, mfr: MFRResult, tr: TechReadinessResult) -> str:
    """
    Rating matrix aligned with PROJEKT-MASTER framework:
      A · No-Brainer  — Transformational++ + Feasible + TR ≥ 0.6
      B · Solide       — Transformational   + Feasible/Watch + TR ≥ 0.5
      C · Abwägen     — High Strategic or Watch + moderate TR
      D · Uninteressant — Low Strategic or Overstretch
    """
    if srr.category == "Transformational++" and mfr.signal == "Feasible" and tr.value >= 0.6:
        return "A · No-Brainer"
    if srr.category in ("Transformational++", "Transformational") and mfr.signal in ("Feasible", "Watch") and tr.value >= 0.5:
        return "B · Solide"
    if srr.category in ("High Strategic", "Transformational") and mfr.signal != "Overstretch":
        return "C · Abwägen"
    return "D · Uninteressant"


def _derive_quadrant(deal_success_score: float, mfr: MFRResult) -> str:
    high_potential = deal_success_score >= 0.5
    low_risk = mfr.signal == "Feasible"

    if high_potential and low_risk:
        return "HighPotential_LowRisk"
    if high_potential and not low_risk:
        return "HighPotential_HighRisk"
    if not high_potential and low_risk:
        return "LowPotential_LowRisk"
    return "LowPotential_HighRisk"


# ── Master scoring function ───────────────────────────────────────────────────

def compute_scores(request: AnalyzeRequest) -> ScoreResult:
    srr = compute_srr(request.tam_usd_bn, request.buyer_market_cap_usd_bn)
    mfr = compute_mfr(
        buyer_market_cap_usd_bn=request.buyer_market_cap_usd_bn,
        buyer_cash_usd_bn=request.buyer_cash_usd_bn,
        buyer_debt_ebitda=request.buyer_debt_ebitda,
        target_funding_usd_mn=request.target_funding_usd_mn,
        target_stage=request.target_stage,
    )
    tr = compute_tech_readiness(request.tech_readiness_inputs)

    srr_norm = _normalize_srr(srr.value)
    mfr_norm = _normalize_mfr(mfr.value)

    deal_success_score = round(srr_norm * mfr_norm * tr.value, 4)

    rating = _derive_rating(srr, mfr, tr)
    quadrant = _derive_quadrant(deal_success_score, mfr)

    return ScoreResult(
        srr=srr,
        mfr=mfr,
        tech_readiness=tr,
        deal_success_score=deal_success_score,
        rating=rating,
        quadrant=quadrant,
    )
