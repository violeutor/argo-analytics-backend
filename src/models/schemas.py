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
    tech_stack_fit: float = Field(..., ge=0, le=1)
    gtm_fit: float = Field(..., ge=0, le=1)
    integration_capacity: float = Field(..., ge=0, le=1)
    rd_intensity: float = Field(..., ge=0, le=1)
    capital_deployment_velocity: float = Field(..., ge=0, le=1)
    regulatory_readiness: float = Field(..., ge=0, le=1)
    strategic_coherence: float = Field(..., ge=0, le=1)


# ── Scores ───────────────────────────────────────────────────────────────────

class SRRResult(BaseModel):
    value: float
    category: Literal["Low Strategic", "High Strategic", "Transformational", "Transformational++"]
    cap_segment: Literal["low", "mid", "high"]
    execution_warning: bool


class MFRResult(BaseModel):
    value: float
    signal: Literal["Feasible", "Watch", "Overstretch"]


class TechReadinessResult(BaseModel):
    value: float
    factor_scores: dict[str, float]


class ScoreResult(BaseModel):
    srr: SRRResult
    mfr: MFRResult
    tech_readiness: TechReadinessResult
    deal_success_score: float = Field(..., description="SRR_norm x MFR_norm x TechReadiness")
    rating: Literal["A · No-Brainer", "B · Solide", "C · Abwägen", "D · Uninteressant"]
    quadrant: Literal[
        "HighPotential_LowRisk",
        "HighPotential_HighRisk",
        "LowPotential_LowRisk",
        "LowPotential_HighRisk",
    ]


# ── Full Report ───────────────────────────────────────────────────────────────

class AnalyzeResponse(BaseModel):
    deal_id: str | None = None
    company_name: str
    buyer_name: str
    scores: ScoreResult
    executive_summary: str
    warnings: list[str] = []


# ── Company & Buyer List ──────────────────────────────────────────────────────

class CompanyListItem(BaseModel):
    id: str
    name: str
    category: str | None = None
    industry: str | None = None
    potential: str | None = None
    risk: str | None = None
    ipo_potential: str | None = None
    investment_path: str | None = None
    proxy_ticker: str | None = None
    funding_total_usd_mn: float | None = None
    funding_last_round: str | None = None
    last_signal: str | None = None
    last_signal_date: str | None = None
    source: str | None = None


class BuyerListItem(BaseModel):
    id: str
    name: str
    ticker: str | None = None
    exchange: str | None = None
    market_cap_usd_bn: float | None = None
    sector: str | None = None
