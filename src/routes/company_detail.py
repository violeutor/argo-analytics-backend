"""
GET /api/v1/company/{name}

Full company detail profile:
  - Claude-generated intro (Morning Briefing style)
  - Industry & product description
  - Ownership structure (from enrichment)
  - Fundamentals (Yahoo Finance if listed, Bundesanzeiger later for Mittelstand)
  - Full scoring breakdown incl. TechReadiness 7 factors
"""

import logging
import httpx
import json
import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from src.integrations.supabase import fetch_companies, fetch_buyers
from src.services.supply_chain import get_supply_chain, COMPANY_TAGS
from src.services.tam import get_tam, COMPANY_PRIMARY_TAG
from src.pipelines.scoring import compute_scores
from src.models.schemas import AnalyzeRequest

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["company"])


# ── Response models ───────────────────────────────────────────────────────────

class OwnershipItem(BaseModel):
    name: str
    type: str           # "VC" | "Corporate" | "Fund" | "Government" | "Unknown"
    notes: str | None = None


class FundamentalsData(BaseModel):
    is_listed: bool
    ticker: str | None = None
    exchange: str | None = None
    price: float | None = None
    market_cap_bn: float | None = None
    pe_ratio: float | None = None
    revenue_bn: float | None = None
    ebitda_bn: float | None = None
    debt_ebitda: float | None = None
    week_52_high: float | None = None
    week_52_low: float | None = None
    currency: str | None = None


class TechReadinessDetail(BaseModel):
    overall: float
    inputs_provided: bool
    factors: dict[str, float]
    factor_weights: dict[str, float]


class ScoringDetail(BaseModel):
    buyer_name: str
    ticker: str | None
    srr_value: float
    srr_category: str
    mfr_value: float
    mfr_signal: str
    tech_readiness: TechReadinessDetail
    deal_success_score: float
    rating: str
    execution_warning: bool


class CompanyDetailResponse(BaseModel):
    # Identity
    name: str
    category: str | None
    core_technology: str | None
    website: str | None
    founded: str | None

    # AI-generated intro
    intro: str

    # Industry & Product
    industry: str | None
    product_description: str | None
    technology_tags: list[str]

    # Market
    tam_usd_bn: float
    tam_source: str
    tam_confidence: str
    ipo_potential: str | None
    ipo_probability_pct: int | None
    investment_path: str | None
    proxy_ticker: str | None

    # Funding
    funding_total_usd_mn: float | None
    funding_last_round: str | None
    funding_stage: str | None

    # Ownership
    ownership: list[OwnershipItem]

    # Fundamentals
    fundamentals: FundamentalsData

    # Scoring (all buyers)
    scorings: list[ScoringDetail]

    # Supply chain
    supply_chain_upstream: list[dict]
    supply_chain_downstream: list[dict]
    supply_chain_etfs: list[dict]

    # Signal
    last_signal: str | None
    last_signal_date: str | None

    # Meta
    is_known: bool
    warnings: list[str]


# ── Claude intro generator ────────────────────────────────────────────────────

async def _generate_intro(company: dict, tam: dict) -> str:
    """Generate a Morning Briefing-style intro via Claude API."""
    name        = company.get("name", "")
    category    = company.get("category", "")
    technology  = company.get("core_technology", "") or company.get("summary", "")
    funding     = company.get("funding_last_round", "")
    signal      = company.get("last_signal", "")
    path        = company.get("investment_path", "")
    ipo         = company.get("ipo_potential", "")
    tam_bn      = tam.get("tam_usd_bn", 100)
    tam_src     = tam.get("source", "")

    prompt = f"""You are writing a Morning Briefing entry for a Climate Tech & Investment intelligence platform.
Write a concise, sharp intro (3–4 sentences, ~80 words) about this company for a sophisticated VC/PE audience.

Company: {name}
Sector: {category}
Technology: {technology}
Funding: {funding}
Latest signal: {signal}
Investment path: {path}
IPO potential: {ipo}
TAM 2035: ${tam_bn}B ({tam_src})

Style rules:
- Direct, no fluff, no hype
- Lead with what makes them differentiated
- Include market context (TAM) naturally
- End with the investment angle (IPO, M&A, proxy)
- Write in English
- No bullet points, flowing prose only"""

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"Content-Type": "application/json"},
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 200,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        if resp.status_code == 200:
            data = resp.json()
            return data["content"][0]["text"].strip()
    except Exception as e:
        logger.warning("Claude intro generation failed: %s", e)

    # Fallback: structured text
    parts = []
    if name and category:
        parts.append(f"{name} operates in {category}.")
    if technology:
        parts.append(technology)
    if funding:
        parts.append(f"Latest funding: {funding}.")
    if path and path != "Beobachten":
        parts.append(f"Investment path: {path}.")
    return " ".join(parts) if parts else f"{name} — no description available."


# ── Yahoo Finance fundamentals ─────────────────────────────────────────────────

async def _fetch_fundamentals(ticker: str | None) -> FundamentalsData:
    if not ticker:
        return FundamentalsData(is_listed=False)

    symbol = ticker.split("·")[0].split("→")[-1].strip()

    try:
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as client:
            chart_resp = await client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
            )
            summary_resp = await client.get(
                f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
                "?modules=summaryDetail,financialData,defaultKeyStatistics"
            )

        meta = {}
        if chart_resp.status_code == 200:
            meta = chart_resp.json().get("chart", {}).get("result", [{}])[0].get("meta", {})

        fd = FundamentalsData(
            is_listed=True,
            ticker=symbol,
            exchange=meta.get("exchangeName"),
            price=meta.get("regularMarketPrice"),
            market_cap_bn=meta.get("marketCap", 0) / 1e9 if meta.get("marketCap") else None,
            currency=meta.get("currency"),
        )

        if summary_resp.status_code == 200:
            sd = summary_resp.json().get("quoteSummary", {}).get("result", [{}])[0]
            detail = sd.get("summaryDetail", {})
            fin    = sd.get("financialData", {})

            fd.pe_ratio      = detail.get("trailingPE", {}).get("raw")
            fd.week_52_high  = detail.get("fiftyTwoWeekHigh", {}).get("raw")
            fd.week_52_low   = detail.get("fiftyTwoWeekLow", {}).get("raw")
            fd.revenue_bn    = fin.get("totalRevenue", {}).get("raw", 0) / 1e9 or None
            fd.ebitda_bn     = fin.get("ebitda", {}).get("raw", 0) / 1e9 or None
            if fd.ebitda_bn and fin.get("totalDebt", {}).get("raw"):
                fd.debt_ebitda = (fin["totalDebt"]["raw"] / 1e9) / fd.ebitda_bn

        return fd

    except Exception as e:
        logger.warning("Yahoo Finance fetch failed for %s: %s", symbol, e)
        return FundamentalsData(is_listed=True, ticker=symbol)


# ── Ownership inference ───────────────────────────────────────────────────────

_KNOWN_INVESTORS: dict[str, list[OwnershipItem]] = {
    "Climeworks": [
        OwnershipItem(name="Swiss Federal Railways (SBB)", type="Corporate"),
        OwnershipItem(name="Shopify", type="Corporate"),
        OwnershipItem(name="Baillie Gifford", type="Fund"),
        OwnershipItem(name="M&G Investments", type="Fund"),
    ],
    "CarbonCure": [
        OwnershipItem(name="Amazon Climate Pledge Fund", type="Corporate"),
        OwnershipItem(name="Breakthrough Energy Ventures", type="Fund"),
        OwnershipItem(name="Microsoft Climate Innovation Fund", type="Corporate"),
    ],
    "Brimstone": [
        OwnershipItem(name="CRH Ventures", type="Corporate", notes="Strategic investor"),
        OwnershipItem(name="Breakthrough Energy Ventures", type="Fund"),
        OwnershipItem(name="DCVC", type="VC"),
    ],
    "Sublime Systems": [
        OwnershipItem(name="CRH", type="Corporate", notes="Direct investment"),
        OwnershipItem(name="Breakthrough Energy Ventures", type="Fund"),
        OwnershipItem(name="Prelude Ventures", type="VC"),
    ],
    "VoltaGrid": [
        OwnershipItem(name="Blackstone", type="Fund", notes="$1B equity round May 2026"),
        OwnershipItem(name="Halliburton", type="Corporate", notes="Strategic co-investor"),
    ],
    "Fervo Energy": [
        OwnershipItem(name="DCVC", type="VC"),
        OwnershipItem(name="Breakthrough Energy Ventures", type="Fund"),
        OwnershipItem(name="Liberty Energy", type="Corporate"),
    ],
    "Factorial Energy": [
        OwnershipItem(name="Stellantis", type="Corporate", notes="Strategic OEM investor"),
        OwnershipItem(name="Samsung SDI", type="Corporate"),
        OwnershipItem(name="Cartesian Growth Corporation III (SPAC)", type="Fund"),
    ],
    "Syzygy Plasmonics": [
        OwnershipItem(name="Saudi Aramco Energy Ventures", type="Corporate"),
        OwnershipItem(name="Honeywell", type="Corporate", notes="Technology partner"),
        OwnershipItem(name="Cottonwood Technology Fund", type="VC"),
    ],
    "Indigo Ag": [
        OwnershipItem(name="Flagship Pioneering", type="VC"),
        OwnershipItem(name="Investment Corporation of Dubai", type="Government"),
        OwnershipItem(name="Alaska Permanent Fund", type="Fund"),
    ],
    "Pairwise": [
        OwnershipItem(name="Corteva Agriscience", type="Corporate", notes="Commercial partnership"),
        OwnershipItem(name="Deerfield Management", type="Fund"),
        OwnershipItem(name="Fall Line Capital", type="VC"),
    ],
}

_TECH_READINESS_WEIGHTS = {
    "tech_stack_fit": 0.20,
    "integration_capacity": 0.20,
    "gtm_fit": 0.15,
    "capital_deployment_velocity": 0.15,
    "rd_intensity": 0.10,
    "regulatory_readiness": 0.10,
    "strategic_coherence": 0.10,
}


# ── Main route ────────────────────────────────────────────────────────────────

@router.get("/company/{name}", response_model=CompanyDetailResponse)
async def get_company_detail(name: str) -> CompanyDetailResponse:
    warnings: list[str] = []

    # ── 1. Find company ───────────────────────────────────────────────────────
    companies = fetch_companies(limit=500)
    q = name.lower().replace("-", " ").replace("_", " ")
    company = next(
        (c for c in companies if
         c.get("name", "").lower() == q or
         q in c.get("name", "").lower() or
         c.get("name", "").lower().replace(" ", "-") == name.lower()),
        None
    )

    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")

    company_name = company["name"]
    is_known = True

    # ── 2. TAM ────────────────────────────────────────────────────────────────
    tam = await get_tam(company_name, company.get("category"))

    # ── 3. Claude intro ───────────────────────────────────────────────────────
    intro = await _generate_intro(company, tam)

    # ── 4. Fundamentals ───────────────────────────────────────────────────────
    proxy = company.get("proxy_ticker")
    is_listed = company.get("investment_path") in ("IPO-direkt",) or \
                company.get("ipo_potential") == "IPO erfolgt"

    fundamentals = await _fetch_fundamentals(proxy if is_listed else None)
    if not is_listed:
        fundamentals = FundamentalsData(is_listed=False)

    # ── 5. Ownership ──────────────────────────────────────────────────────────
    ownership = _KNOWN_INVESTORS.get(company_name, [
        OwnershipItem(name="Not publicly disclosed", type="Unknown",
                      notes="Ownership structure not available in public sources")
    ])

    # ── 6. Scoring for all buyers ─────────────────────────────────────────────
    buyers = fetch_buyers(limit=50)
    scorings: list[ScoringDetail] = []

    for buyer in buyers:
        if not buyer.get("market_cap_usd_bn"):
            continue
        try:
            req = AnalyzeRequest(
                company_name=company_name,
                buyer_name=buyer["name"],
                tam_usd_bn=tam["tam_usd_bn"],
                buyer_market_cap_usd_bn=buyer["market_cap_usd_bn"],
                buyer_cash_usd_bn=buyer.get("cash_usd_bn") or buyer["market_cap_usd_bn"] * 0.05,
                buyer_debt_ebitda=buyer.get("debt_ebitda") or 1.5,
                target_funding_usd_mn=company.get("funding_total_usd_mn") or 50,
                target_stage=company.get("funding_stage") or "series_b",
            )
            scores = compute_scores(req)

            scorings.append(ScoringDetail(
                buyer_name=buyer["name"],
                ticker=buyer.get("ticker"),
                srr_value=scores.srr.value,
                srr_category=scores.srr.category,
                mfr_value=scores.mfr.value,
                mfr_signal=scores.mfr.signal,
                tech_readiness=TechReadinessDetail(
                    overall=scores.tech_readiness.value,
                    inputs_provided=scores.tech_readiness.value != 0.5,
                    factors=scores.tech_readiness.factor_scores,
                    factor_weights=_TECH_READINESS_WEIGHTS,
                ),
                deal_success_score=scores.deal_success_score,
                rating=scores.rating,
                execution_warning=scores.srr.execution_warning,
            ))
        except Exception as e:
            logger.debug("Scoring failed for %s / %s: %s", company_name, buyer["name"], e)

    scorings.sort(key=lambda x: -x.deal_success_score)

    # ── 7. Supply chain ───────────────────────────────────────────────────────
    sc_tags = COMPANY_TAGS.get(company_name, [])
    sc = get_supply_chain(sc_tags)

    # ── 8. Tags ───────────────────────────────────────────────────────────────
    tags = sc_tags or []

    # ── 9. IPO probability ────────────────────────────────────────────────────
    ipo_map = {"Hoch": 75, "Mittel-hoch": 55, "Mittel": 35, "Niedrig": 10, "IPO erfolgt": 100}
    ipo_pct = ipo_map.get(company.get("ipo_potential", ""), None)

    if tam.get("method") == "fallback":
        warnings.append("TAM uses sector median fallback — verify with primary source.")
    if not _KNOWN_INVESTORS.get(company_name):
        warnings.append("Ownership data not publicly available — shown as placeholder.")

    return CompanyDetailResponse(
        name=company_name,
        category=company.get("category"),
        core_technology=company.get("core_technology"),
        website=company.get("website"),
        founded=None,
        intro=intro,
        industry=company.get("category"),
        product_description=company.get("core_technology") or company.get("summary"),
        technology_tags=tags,
        tam_usd_bn=tam["tam_usd_bn"],
        tam_source=tam.get("source", ""),
        tam_confidence=tam.get("confidence", "medium"),
        ipo_potential=company.get("ipo_potential"),
        ipo_probability_pct=ipo_pct,
        investment_path=company.get("investment_path"),
        proxy_ticker=proxy,
        funding_total_usd_mn=company.get("funding_total_usd_mn"),
        funding_last_round=company.get("funding_last_round"),
        funding_stage=company.get("funding_stage"),
        ownership=ownership,
        fundamentals=fundamentals,
        scorings=scorings,
        supply_chain_upstream=sc.get("upstream", []),
        supply_chain_downstream=sc.get("downstream", []),
        supply_chain_etfs=sc.get("etfs", []),
        last_signal=company.get("last_signal"),
        last_signal_date=str(company.get("last_signal_date")) if company.get("last_signal_date") else None,
        is_known=is_known,
        warnings=warnings,
    )
