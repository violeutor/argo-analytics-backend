from pydantic import BaseModel, Field
from typing import Literal


# ── Input ────────────────────────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    company_name: str
    buyer_name: str
    tam_usd_bn: float = Field(..., description="Total Addressable Market in USD billions (2035 estimate)")
    buyer_market_cap_usd_bn: float = Field(..., description="Buyer market cap in USD billions")
    buyer_cash_usd_bn: float = Field(0.0, description="Buyer cash & equivalents in USD billions")
    buyer_debt_ebitda: float = Field(0.0, description="Buyer net debt / EBITDA ratio")
    target_funding_usd_mn: float = Field(0.0, description="Target total funding raised in USD millions")
    target_stage: Literal["seed", "series_a", "series_b", "series_c", "series_d_plus", "public"] = "series_b"
    tech_readiness_inputs: "TechReadinessInputs | None" = None
    url: str | None = None


class TechReadinessInputs(BaseModel):
    tech_stack_fit: float = Field(..., ge=0, le=1, description="0–1: how well target tech fits buyer stack")
    gtm_fit: float = Field(..., ge=0, le=1, description="0–1: go-to-market overlap")
    integration_capacity: float = Field(..., ge=0, le=1, description="0–1: buyer's M&A integration track record")
    rd_intensity: float = Field(..., ge=0, le=1, description="0–1: buyer R&D spend relative to revenue")
    capital_deployment_velocity: float = Field(..., ge=0, le=1, description="0–1: speed of capital deployment post-deal")
    regulatory_readiness: float = Field(..., ge=0, le=1, description="0–1: regulatory alignment")
    strategic_coherence: float = Field(..., ge=0, le=1, description="0–1: deal fits stated strategic priorities")


# ── Scores ───────────────────────────────────────────────────────────────────

class SRRResult(BaseModel):
    value: float
    category: Literal["Low Strategic", "High Strategic", "Transformational", "Transformational++"]
    cap_segment: Literal["low", "mid", "high"]  # <10B / 10–100B / >100B
    execution_warning: bool  # True if low cap + high SRR


class MFRResult(BaseModel):
    value: float
    signal: Literal["Feasible", "Watch", "Overstretch"]


class TechReadinessResult(BaseModel):
    value: float  # 0–1
    factor_scores: dict[str, float]


class ScoreResult(BaseModel):
    srr: SRRResult
    mfr: MFRResult
    tech_readiness: TechReadinessResult
    deal_success_score: float = Field(..., description="SRR_norm × MFR_norm × TechReadiness — 0 to 1")
    rating: Literal["A · No-Brainer", "B · Solide", "C · Abwägen", "D · Uninteressant"]
    quadrant: Literal[
        "HighPotential_LowRisk",
        "HighPotential_HighRisk",
        "LowPotential_LowRisk",
        "LowPotential_HighRisk",
    ]


# ── Full Report ───────────────────────────────────────────────────────────────

class AnalyzeResponse(BaseModel):
    company_name: str
    buyer_name: str
    scores: ScoreResult
    executive_summary: str
    warnings: list[str] = []
