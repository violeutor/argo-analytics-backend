"""
Tests for Argo Analytics Scoring Engine
Run with: pytest tests/test_scoring.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.pipelines.scoring import compute_srr, compute_mfr, compute_tech_readiness, compute_scores
from src.models.schemas import AnalyzeRequest, TechReadinessInputs


# ── SRR Tests ─────────────────────────────────────────────────────────────────

def test_srr_transformational_pp():
    result = compute_srr(tam_usd_bn=300, buyer_market_cap_usd_bn=60)
    assert result.value == 5.0
    assert result.category == "Transformational++"
    assert result.cap_segment == "mid"
    assert not result.execution_warning


def test_srr_low_strategic():
    result = compute_srr(tam_usd_bn=10, buyer_market_cap_usd_bn=430)
    assert result.category == "Low Strategic"
    assert result.cap_segment == "high"


def test_srr_execution_warning_triggered():
    # Low cap buyer, very high SRR — should flag
    result = compute_srr(tam_usd_bn=200, buyer_market_cap_usd_bn=7)
    assert result.cap_segment == "low"
    assert result.execution_warning is True


def test_srr_execution_warning_not_triggered_high_cap():
    result = compute_srr(tam_usd_bn=200, buyer_market_cap_usd_bn=200)
    assert not result.execution_warning


# ── MFR Tests ─────────────────────────────────────────────────────────────────

def test_mfr_feasible():
    # CRH ($76B cap) acquiring Brimstone (~$300M EV est.) → trivially feasible
    result = compute_mfr(
        buyer_market_cap_usd_bn=76,
        buyer_cash_usd_bn=5,
        buyer_debt_ebitda=1.5,
        target_funding_usd_mn=60,
        target_stage="series_b",
    )
    assert result.signal == "Feasible"
    assert result.value < 0.15


def test_mfr_watch():
    # $2B EV target (series_c $500M × 4x) / $10B buyer cap = 0.20 → Watch
    result = compute_mfr(
        buyer_market_cap_usd_bn=10,
        buyer_cash_usd_bn=0.5,
        buyer_debt_ebitda=2.0,
        target_funding_usd_mn=500,
        target_stage="series_c",
    )
    assert result.signal in ("Watch", "Overstretch")


def test_mfr_high_leverage_increases_ratio():
    base = compute_mfr(76, 5, 1.0, 500, "series_b")
    leveraged = compute_mfr(76, 5, 5.0, 500, "series_b")
    assert leveraged.value > base.value


# ── TechReadiness Tests ───────────────────────────────────────────────────────

def test_tech_readiness_perfect_score():
    inputs = TechReadinessInputs(
        tech_stack_fit=1.0,
        gtm_fit=1.0,
        integration_capacity=1.0,
        rd_intensity=1.0,
        capital_deployment_velocity=1.0,
        regulatory_readiness=1.0,
        strategic_coherence=1.0,
    )
    result = compute_tech_readiness(inputs)
    assert result.value == 1.0


def test_tech_readiness_neutral_fallback():
    result = compute_tech_readiness(None)
    assert result.value == 0.5


def test_tech_readiness_weighted():
    # tech_stack_fit (weight 0.20) = 1.0, all others 0.0
    inputs = TechReadinessInputs(
        tech_stack_fit=1.0,
        gtm_fit=0.0,
        integration_capacity=0.0,
        rd_intensity=0.0,
        capital_deployment_velocity=0.0,
        regulatory_readiness=0.0,
        strategic_coherence=0.0,
    )
    result = compute_tech_readiness(inputs)
    assert abs(result.value - 0.20) < 0.001


# ── Integration: Full Scoring ─────────────────────────────────────────────────

def test_full_score_crh_carboncure():
    """
    Validate against known A · No-Brainer case from PROJEKT-MASTER:
    CRH ($76B) / CarbonCure ($169M total funding, Series F)
    Green cement TAM 2035: ~$100B
    """
    request = AnalyzeRequest(
        company_name="CarbonCure",
        buyer_name="CRH",
        tam_usd_bn=100,
        buyer_market_cap_usd_bn=76,
        buyer_cash_usd_bn=5,
        buyer_debt_ebitda=1.5,
        target_funding_usd_mn=169,
        target_stage="series_d_plus",
        tech_readiness_inputs=TechReadinessInputs(
            tech_stack_fit=0.9,
            gtm_fit=0.85,
            integration_capacity=0.8,
            rd_intensity=0.7,
            capital_deployment_velocity=0.75,
            regulatory_readiness=0.85,
            strategic_coherence=0.95,
        ),
    )
    result = compute_scores(request)
    assert result.srr.category == "Transformational++"
    assert result.mfr.signal == "Feasible"
    assert result.tech_readiness.value >= 0.6
    assert result.rating == "A · No-Brainer"
    assert result.deal_success_score > 0.25


def test_full_score_low_case():
    """D · Uninteressant: tiny TAM, stretched buyer."""
    request = AnalyzeRequest(
        company_name="SmallTarget",
        buyer_name="OverstretchedBuyer",
        tam_usd_bn=5,
        buyer_market_cap_usd_bn=50,
        buyer_cash_usd_bn=0.5,
        buyer_debt_ebitda=6.0,
        target_funding_usd_mn=800,
        target_stage="series_d_plus",
    )
    result = compute_scores(request)
    assert result.rating == "D · Uninteressant"
