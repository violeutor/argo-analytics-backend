"""
GET /api/v1/company/{name}  —  v2.1

Changes vs v2.0:
  - ipo_status (listed/pre_ipo_*) aus DB gelesen
  - is_listed basiert auf ipo_status == 'listed' ODER investment_path == 'IPO'
    mit bekanntem proxy_ticker — robust, kein hartkodierter String-Vergleich
  - ipo_status in Response durchgereicht
  - ipo_potential bleibt für Kompatibilität erhalten
"""

import logging
import asyncio
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.integrations.supabase import fetch_companies, fetch_buyers
from src.services.supply_chain import get_supply_chain, COMPANY_TAGS
from src.services.tam import get_tam
from src.pipelines.scoring import compute_scores
from src.models.schemas import AnalyzeRequest
from src.services.enrichment import (
    enrich_company,
    BundesanzeigerData,
    EnrichmentResult,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["company"])


# ── Response models ───────────────────────────────────────────────────────────

class OwnershipItem(BaseModel):
    name: str
    type: str
    role: str | None = None
    notes: str | None = None


class FundingRoundItem(BaseModel):
    round_name: str
    amount_mn: float | None = None
    date: str | None = None
    investors: list[str] = []


class FundamentalsData(BaseModel):
    is_listed: bool
    # Yahoo Finance (listed)
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
    # Bundesanzeiger (private DE)
    ba_found: bool = False
    ba_legal_form: str | None = None
    ba_registered_at: str | None = None
    ba_last_report_year: str | None = None
    ba_revenue_mn: float | None = None
    ba_equity_mn: float | None = None
    ba_total_assets_mn: float | None = None
    ba_employees: int | None = None
    ba_source_url: str | None = None


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
    name: str
    category: str | None
    industry: str | None
    core_technology: str | None
    website: str | None
    founded: str | None
    intro: str
    description: str | None
    wikipedia_url: str | None
    crunchbase_url: str | None
    headquarters: str | None
    employee_count: str | None
    # IPO
    ipo_status: str | None          # listed | pre_ipo_high | pre_ipo_medium | pre_ipo_low
    ipo_potential: str | None       # legacy label für Frontend-Anzeige
    ipo_probability_pct: int | None
    # Market
    tam_usd_bn: float
    tam_source: str
    tam_confidence: str
    investment_path: str | None
    proxy_ticker: str | None
    # Funding
    funding_total_usd_mn: float | None
    funding_last_round: str | None
    funding_stage: str | None
    funding_rounds: list[FundingRoundItem]
    # Ownership
    ownership: list[OwnershipItem]
    # Fundamentals
    fundamentals: FundamentalsData
    # Scoring
    scorings: list[ScoringDetail]
    # Supply chain
    supply_chain_upstream: list[dict]
    supply_chain_downstream: list[dict]
    supply_chain_etfs: list[dict]
    # Signal
    last_signal: str | None
    last_signal_date: str | None
    # Meta
    technology_tags: list[str]
    is_known: bool
    warnings: list[str]


# ── is_listed logic (B-05) ────────────────────────────────────────────────────

def _resolve_is_listed(company: dict) -> bool:
    """
    Robust listing detection — B-05.
    A company is considered listed if ANY of:
      1. ipo_status == 'listed'  (new canonical field)
      2. ipo_potential == 'IPO erfolgt'  (legacy fallback)
      3. investment_path == 'IPO' AND proxy_ticker is set  (has a real ticker)
    """
    if company.get("ipo_status") == "listed":
        return True
    if company.get("ipo_potential") == "IPO erfolgt":
        return True
    if company.get("investment_path") == "IPO" and company.get("proxy_ticker"):
        return True
    return False


def _ipo_probability(ipo_status: str | None, ipo_potential: str | None) -> int | None:
    """Map ipo_status → probability pct. Falls back to ipo_potential for legacy data."""
    status_map = {
        "listed":          100,
        "pre_ipo_high":     70,
        "pre_ipo_medium":   40,
        "pre_ipo_low":      10,
    }
    if ipo_status and ipo_status in status_map:
        return status_map[ipo_status]
    # legacy fallback
    legacy_map = {
        "IPO erfolgt":  100,
        "Hoch":          75,
        "Mittel-hoch":   55,
        "Mittel":        35,
        "Niedrig":       10,
    }
    return legacy_map.get(ipo_potential or "")


# ── Curated ownership overrides ───────────────────────────────────────────────

_OWNERSHIP_OVERRIDES: dict[str, list[OwnershipItem]] = {
    "CarbonCure": [
        OwnershipItem(name="Amazon Climate Pledge Fund", type="Corporate", role="Strategic"),
        OwnershipItem(name="Breakthrough Energy Ventures", type="Impact VC", role="Lead"),
        OwnershipItem(name="Microsoft Climate Innovation Fund", type="Corporate", role="Strategic"),
        OwnershipItem(name="Clean Growth Fund", type="Impact VC", role="Co-Investor"),
        OwnershipItem(name="New Enterprise Associates", type="VC", role="Co-Investor"),
    ],
    "Brimstone": [
        OwnershipItem(name="CRH Ventures", type="Corporate", role="Strategic", notes="Strategic investor"),
        OwnershipItem(name="Breakthrough Energy Ventures", type="Impact VC", role="Lead"),
        OwnershipItem(name="DCVC", type="VC", role="Co-Investor"),
    ],
    "Sublime Systems": [
        OwnershipItem(name="CRH", type="Corporate", role="Strategic", notes="Direct investment"),
        OwnershipItem(name="Breakthrough Energy Ventures", type="Impact VC", role="Lead"),
        OwnershipItem(name="Prelude Ventures", type="VC", role="Co-Investor"),
    ],
    "VoltaGrid": [
        OwnershipItem(name="Blackstone", type="Fund", role="Lead", notes="$1B equity round May 2026"),
        OwnershipItem(name="Halliburton", type="Corporate", role="Strategic"),
    ],
    "Fervo Energy": [
        OwnershipItem(name="DCVC", type="VC", role="Lead"),
        OwnershipItem(name="Breakthrough Energy Ventures", type="Impact VC", role="Co-Investor"),
        OwnershipItem(name="Liberty Energy", type="Corporate", role="Strategic"),
    ],
    "Factorial Energy": [
        OwnershipItem(name="Stellantis", type="Corporate", role="Strategic", notes="OEM investor"),
        OwnershipItem(name="Samsung SDI", type="Corporate", role="Strategic"),
        OwnershipItem(name="Cartesian Growth Corporation III", type="Fund", role="SPAC"),
    ],
    "Syzygy Plasmonics": [
        OwnershipItem(name="Saudi Aramco Energy Ventures", type="Corporate", role="Strategic"),
        OwnershipItem(name="Honeywell", type="Corporate", role="Strategic", notes="Technology partner"),
        OwnershipItem(name="Cottonwood Technology Fund", type="VC", role="Co-Investor"),
    ],
    "Indigo Ag": [
        OwnershipItem(name="Flagship Pioneering", type="VC", role="Lead"),
        OwnershipItem(name="Investment Corporation of Dubai", type="Government", role="Co-Investor"),
        OwnershipItem(name="Alaska Permanent Fund", type="Fund", role="Co-Investor"),
    ],
    "Pairwise": [
        OwnershipItem(name="Corteva Agriscience", type="Corporate", role="Strategic"),
        OwnershipItem(name="Deerfield Management", type="Fund", role="Co-Investor"),
        OwnershipItem(name="Fall Line Capital", type="VC", role="Early"),
    ],
    "Climeworks": [
        OwnershipItem(name="Swiss Federal Railways (SBB)", type="Corporate", role="Strategic"),
        OwnershipItem(name="Shopify", type="Corporate", role="Strategic"),
        OwnershipItem(name="Baillie Gifford", type="Fund", role="Co-Investor"),
        OwnershipItem(name="M&G Investments", type="Fund", role="Co-Investor"),
    ],
}

_TR_WEIGHTS = {
    "tech_stack_fit": 0.20, "integration_capacity": 0.20,
    "gtm_fit": 0.15, "capital_deployment_velocity": 0.15,
    "rd_intensity": 0.10, "regulatory_readiness": 0.10,
    "strategic_coherence": 0.10,
}


# ── Claude intro ──────────────────────────────────────────────────────────────

async def _generate_intro(company: dict, tam: dict) -> str:
    is_listed = _resolve_is_listed(company)
    listing_context = "Already publicly listed." if is_listed else f"IPO potential: {company.get('ipo_potential','')}"

    prompt = f"""Morning Briefing entry for a cross-industry investment intelligence platform.
Write 3-4 sentences (~80 words) for a VC/PE audience. Direct, no hype.
Lead with differentiation. Include TAM context. End with investment angle.
Write in English, flowing prose only.

Company: {company.get('name','')}
Sector: {company.get('category','')} / {company.get('industry','')}
Technology: {company.get('core_technology','') or company.get('summary','')}
Funding: {company.get('funding_last_round','')}
Signal: {company.get('last_signal','')}
Path: {company.get('investment_path','')} | {listing_context}
TAM 2035: ${tam.get('tam_usd_bn',100)}B"""

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
            return resp.json()["content"][0]["text"].strip()
    except Exception as e:
        logger.warning("Claude intro failed: %s", e)

    parts = []
    if company.get("category"):
        parts.append(f"{company.get('name','')} operates in {company['category']}.")
    if company.get("funding_last_round"):
        parts.append(f"Latest funding: {company['funding_last_round']}.")
    return " ".join(parts) or f"{company.get('name','')} — no description available."


# ── Yahoo Finance ─────────────────────────────────────────────────────────────

async def _fetch_yahoo(ticker: str | None) -> dict:
    if not ticker:
        return {}
    symbol = ticker.split("·")[0].split("→")[-1].strip()
    try:
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as client:
            cr = await client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
            )
            sr = await client.get(
                f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
                "?modules=summaryDetail,financialData"
            )
        meta = {}
        if cr.status_code == 200:
            meta = cr.json().get("chart",{}).get("result",[{}])[0].get("meta",{})
        out = {
            "ticker": symbol,
            "exchange": meta.get("exchangeName"),
            "price": meta.get("regularMarketPrice"),
            "market_cap_bn": (meta.get("marketCap") or 0) / 1e9 or None,
            "currency": meta.get("currency"),
        }
        if sr.status_code == 200:
            res = sr.json().get("quoteSummary",{}).get("result",[{}])[0]
            det = res.get("summaryDetail",{})
            fin = res.get("financialData",{})
            out["pe_ratio"]     = det.get("trailingPE",{}).get("raw")
            out["week_52_high"] = det.get("fiftyTwoWeekHigh",{}).get("raw")
            out["week_52_low"]  = det.get("fiftyTwoWeekLow",{}).get("raw")
            out["revenue_bn"]   = (fin.get("totalRevenue",{}).get("raw") or 0) / 1e9 or None
            out["ebitda_bn"]    = (fin.get("ebitda",{}).get("raw") or 0) / 1e9 or None
            if out.get("ebitda_bn") and fin.get("totalDebt",{}).get("raw"):
                out["debt_ebitda"] = (fin["totalDebt"]["raw"]/1e9) / out["ebitda_bn"]
        return out
    except Exception as e:
        logger.warning("Yahoo Finance failed for %s: %s", symbol, e)
        return {"ticker": symbol}


def _build_fundamentals(
    is_listed: bool,
    yahoo: dict,
    ba: BundesanzeigerData | None,
    proxy: str | None,
) -> FundamentalsData:
    if is_listed:
        return FundamentalsData(
            is_listed=True,
            ticker=yahoo.get("ticker") or (proxy.split("·")[0].strip() if proxy else None),
            exchange=yahoo.get("exchange"), price=yahoo.get("price"),
            market_cap_bn=yahoo.get("market_cap_bn"), pe_ratio=yahoo.get("pe_ratio"),
            revenue_bn=yahoo.get("revenue_bn"), ebitda_bn=yahoo.get("ebitda_bn"),
            debt_ebitda=yahoo.get("debt_ebitda"), week_52_high=yahoo.get("week_52_high"),
            week_52_low=yahoo.get("week_52_low"), currency=yahoo.get("currency"),
        )
    fd = FundamentalsData(is_listed=False)
    if ba and ba.found:
        fd.ba_found=True; fd.ba_legal_form=ba.legal_form
        fd.ba_registered_at=ba.registered_at; fd.ba_last_report_year=ba.last_annual_report_year
        fd.ba_revenue_mn=ba.revenue_mn; fd.ba_equity_mn=ba.equity_mn
        fd.ba_total_assets_mn=ba.total_assets_mn; fd.ba_employees=ba.employees
        fd.ba_source_url=ba.source_url
    return fd


# ── Supabase query ────────────────────────────────────────────────────────────
# supabase.py fetch_companies muss ipo_status selektieren — siehe unten

# ── Main route ────────────────────────────────────────────────────────────────

@router.get("/company/{name}", response_model=CompanyDetailResponse)
async def get_company_detail(name: str) -> CompanyDetailResponse:
    warnings: list[str] = []

    # 1. Lookup
    companies = fetch_companies(limit=500)
    q = name.lower().replace("-"," ").replace("_"," ")
    company = next(
        (c for c in companies if
         c.get("name","").lower()==q or
         q in c.get("name","").lower() or
         c.get("name","").lower().replace(" ","-")==name.lower()),
        None,
    )
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")

    company_name = company["name"]

    # 2. is_listed — robust (B-05)
    is_listed = _resolve_is_listed(company)
    proxy = company.get("proxy_ticker")

    # 3. TAM first (needed for intro) — then run enrichment + yahoo + intro in parallel
    tam = await get_tam(company_name, company.get("category"))

    # 4. Parallel: enrichment (with timeout) + yahoo + intro
    async def _safe_enrichment():
        try:
            return await asyncio.wait_for(
                enrich_company(company_name=company_name, company_record=company),
                timeout=8.0,
            )
        except asyncio.TimeoutError:
            logger.warning("Enrichment timeout for %s — using empty result", company_name)
            from src.services.enrichment import EnrichmentResult
            return EnrichmentResult(name=company_name)
        except Exception as e:
            logger.warning("Enrichment failed for %s: %s", company_name, e)
            from src.services.enrichment import EnrichmentResult
            return EnrichmentResult(name=company_name)

    enrichment, yahoo, intro = await asyncio.gather(
        _safe_enrichment(),
        _fetch_yahoo(proxy if is_listed else None),
        _generate_intro(company, tam),
    )

    # 5. Fundamentals
    fundamentals = _build_fundamentals(is_listed, yahoo, enrichment.bundesanzeiger, proxy)

    # 7. Ownership
    if company_name in _OWNERSHIP_OVERRIDES:
        ownership = _OWNERSHIP_OVERRIDES[company_name]
    elif enrichment.investors:
        ownership = [
            OwnershipItem(name=inv.name, type=inv.type, role=inv.role, notes=inv.notes)
            for inv in enrichment.investors
        ]
    else:
        ownership = [OwnershipItem(
            name="Not publicly disclosed", type="Unknown",
            notes="Not available in Crunchbase or Bundesanzeiger",
        )]
        warnings.append("Ownership data not available in public sources.")

    # 8. Funding rounds
    funding_rounds = [
        FundingRoundItem(round_name=r.round_name, amount_mn=r.amount_mn,
                         date=r.date, investors=r.investors)
        for r in enrichment.funding_rounds
    ]

    # 9. Scoring
    buyers = fetch_buyers(limit=50)
    scorings: list[ScoringDetail] = []
    for buyer in buyers:
        if not buyer.get("market_cap_usd_bn"):
            continue
        try:
            req = AnalyzeRequest(
                company_name=company_name, buyer_name=buyer["name"],
                tam_usd_bn=tam["tam_usd_bn"],
                buyer_market_cap_usd_bn=buyer["market_cap_usd_bn"],
                buyer_cash_usd_bn=buyer.get("cash_usd_bn") or buyer["market_cap_usd_bn"]*0.05,
                buyer_debt_ebitda=buyer.get("debt_ebitda") or 1.5,
                target_funding_usd_mn=company.get("funding_total_usd_mn") or 50,
                target_stage=company.get("funding_stage") or "series_b",
            )
            scores = compute_scores(req)
            scorings.append(ScoringDetail(
                buyer_name=buyer["name"], ticker=buyer.get("ticker"),
                srr_value=scores.srr.value, srr_category=scores.srr.category,
                mfr_value=scores.mfr.value, mfr_signal=scores.mfr.signal,
                tech_readiness=TechReadinessDetail(
                    overall=scores.tech_readiness.value,
                    inputs_provided=scores.tech_readiness.value != 0.5,
                    factors=scores.tech_readiness.factor_scores,
                    factor_weights=_TR_WEIGHTS,
                ),
                deal_success_score=scores.deal_success_score,
                rating=scores.rating, execution_warning=scores.srr.execution_warning,
            ))
        except Exception as e:
            logger.debug("Scoring failed %s/%s: %s", company_name, buyer["name"], e)

    scorings.sort(key=lambda x: -x.deal_success_score)

    # 10. Supply chain
    sc_tags = COMPANY_TAGS.get(company_name, enrichment.tags)
    sc = get_supply_chain(sc_tags)

    if tam.get("method") == "fallback":
        warnings.append("TAM uses sector median fallback — verify with primary source.")

    ipo_status = company.get("ipo_status")

    return CompanyDetailResponse(
        name=company_name,
        category=company.get("category"),
        industry=company.get("industry"),
        core_technology=company.get("core_technology"),
        website=company.get("website"),
        founded=enrichment.founded_year,
        intro=intro,
        description=enrichment.description,
        wikipedia_url=enrichment.wikipedia_url,
        crunchbase_url=enrichment.crunchbase.url if enrichment.crunchbase else None,
        headquarters=enrichment.headquarters,
        employee_count=enrichment.employee_count,
        ipo_status=ipo_status,
        ipo_potential=company.get("ipo_potential"),
        ipo_probability_pct=_ipo_probability(ipo_status, company.get("ipo_potential")),
        tam_usd_bn=tam["tam_usd_bn"],
        tam_source=tam.get("source",""),
        tam_confidence=tam.get("confidence","medium"),
        investment_path=company.get("investment_path"),
        proxy_ticker=proxy,
        funding_total_usd_mn=company.get("funding_total_usd_mn"),
        funding_last_round=company.get("funding_last_round"),
        funding_stage=company.get("funding_stage"),
        funding_rounds=funding_rounds,
        ownership=ownership,
        fundamentals=fundamentals,
        scorings=scorings,
        supply_chain_upstream=sc.get("upstream",[]),
        supply_chain_downstream=sc.get("downstream",[]),
        supply_chain_etfs=sc.get("etfs",[]),
        last_signal=company.get("last_signal"),
        last_signal_date=str(company.get("last_signal_date")) if company.get("last_signal_date") else None,
        technology_tags=enrichment.tags,
        is_known=True,
        warnings=warnings,
    )
