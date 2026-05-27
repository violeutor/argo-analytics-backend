"""
GET /api/v1/company/{name}  —  v2.7

Changes vs v2.6:
  - Auto TechReadiness (v1.1): compute_auto_tech_readiness() ersetzt pauschalen 0.5-Fallback
  - is_listed Gate: TR wird nur für private Companies berechnet + im Frontend angezeigt
  - TechReadinessDetail: confidence-Feld (auto_low | auto_medium | auto_high | user | listed)
  - Scoring-Loop: tech_readiness_override aus Auto-TR in AnalyzeRequest
"""

import logging
import asyncio
import httpx
from src.config import settings
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from src.integrations.supabase import (
    get_supabase,
    fetch_companies,
    fetch_buyers,
    fetch_potential_buyers,
    fetch_funding_rounds,
    fetch_all_funding_rounds,
    upsert_company_enrichment,
    upsert_tam_cache,
    fetch_tam_cache,
    fetch_market_data,
    upsert_market_data,
    set_enrichment_status,
    fetch_value_drivers,
    upsert_value_drivers,
    fetch_company_scores,
    upsert_company_scores,
    fetch_signals,
    fetch_ownership_entries,
    upsert_ownership_entries,
)
from src.services.supply_chain import get_supply_chain, COMPANY_TAGS
from src.services.score_calculator import compute_all_scores
from src.services.tam import get_tam
from src.services.market_data_enrichment import (
    enrich_market_data,
    enrich_market_data_sync_wrapper,
)
from src.services.value_drivers_enrichment import enrich_value_drivers
from src.pipelines.scoring import compute_scores, compute_auto_tech_readiness
from src.models.schemas import AnalyzeRequest
from src.services.enrichment import (
    enrich_company,
    infer_category_industry,
    _claude_infer_category,
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
    date: str | None = None
    type: str | None = None
    amount_usd_mn: float | None = None
    lead_investor: str | None = None
    co_investors: list[str] = []
    source: str | None = None
    notes: str | None = None


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
    # Margen (%)
    gross_margin_pct: float | None = None
    operating_margin_pct: float | None = None
    profit_margin_pct: float | None = None
    # Growth
    revenue_growth_pct: float | None = None
    earnings_growth_pct: float | None = None
    # Cashflow
    free_cashflow_bn: float | None = None
    operating_cashflow_bn: float | None = None
    # Multiples (berechnet)
    ev_revenue: float | None = None
    ev_ebitda: float | None = None
    enterprise_value_bn: float | None = None
    # Bundesanzeiger (private DE)
    ba_found: bool = False
    ba_legal_form: str | None = None
    ba_registered_at: str | None = None
    ba_last_report_year: str | None = None
    ba_revenue_mn: float | None = None
    ba_ebitda_mn: float | None = None
    ba_ebit_mn: float | None = None
    ba_net_income_mn: float | None = None
    ba_equity_mn: float | None = None
    ba_total_assets_mn: float | None = None
    ba_employees: int | None = None
    ba_source_url: str | None = None
    # Beta (YH-06)
    # listed  → beta_cache (BA-Bridge, yfinance)
    # private → damodaran_beta (Branchen-Beta, NYU Damodaran)
    beta_1y: float | None = None
    beta_3y: float | None = None
    volatility_30d: float | None = None
    beta_source: str | None = None          # 'market' | 'damodaran'
    beta_benchmark: str | None = None       # z.B. '^GDAXI', '^GSPC'
    beta_benchmark_is_fallback: bool = False
    beta_calculated_at: str | None = None   # ISO 8601 — für Frontend-Tooltip "Stand 19.05."
    beta_data_quality: str | None = None    # 'full' | 'partial'
    # FD-01 Routing — Herkunfts-Badge je Feld im Frontend
    fundamentals_source: str | None = None          # 'yahoo' | 'ba_bridge' | 'edgar' | 'none'
    fundamentals_source_secondary: str | None = None # z.B. 'ba_bridge' als Ergänzung zu Yahoo (listed DE)
    fundamentals_quality_flag: str | None = None    # None | 'partial' | 'no_data'
    # partial  → Yahoo lückenhaft (kleinere EU-Börse) — Frontend zeigt Hinweis
    # no_data  → keine Finanzdaten öffentlich verfügbar — Frontend zeigt Badge

class TechReadinessDetail(BaseModel):
    overall: float
    inputs_provided: bool
    factors: dict[str, float]
    factor_weights: dict[str, float]
    confidence: str = "auto_medium"  # listed | auto_low | auto_medium | auto_high | user


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
    # DQ-04: Proxy Beta (Market Beta des Investment-Instruments)
    proxy_beta_1y: float | None = None
    proxy_beta_benchmark: str | None = None
    proxy_beta_source: str | None = None
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
    # Market Data (gecacht, optional)
    market_data: dict | None = None
    # Meta
    technology_tags: list[str]
    is_known: bool
    warnings: list[str]
    # SC-01–SC-13: Scoring Engine
    scores: dict | None = None      # ScoreResult.to_dict() — hero_path, rating, alle Sub-/Path-Scores


def _parse_year(value: str | None) -> int | None:
    """Konvertiert Enrichment-Jahresstring ('2009', 'Founded 2009') → int."""
    if not value:
        return None
    try:
        import re
        match = re.search(r"\b(19|20)\d{2}\b", str(value))
        return int(match.group()) if match else None
    except Exception:
        return None


def _parse_headcount(value: str | None) -> int | None:
    """Konvertiert Enrichment-Mitarbeiterstring ('~200', '100-500') → int (Mittelwert).
    Gibt None zurück wenn Wert 0 oder außerhalb [1, 100_000] — verhindert headcount=0 in DB.
    """
    if not value:
        return None
    try:
        import re
        # Range: "100-500" → 300
        match = re.search(r"(\d[\d,]*)\s*[-–]\s*(\d[\d,]*)", str(value))
        if match:
            lo = int(match.group(1).replace(",", ""))
            hi = int(match.group(2).replace(",", ""))
            n = (lo + hi) // 2
        else:
            # Einzelzahl: "~200" → 200
            match = re.search(r"\d[\d,]*", str(value))
            if not match:
                return None
            n = int(match.group().replace(",", ""))
        # Plausibilitätscheck — 0 und Werte > 100.000 ausschließen
        return n if 1 <= n <= 100_000 else None
    except Exception:
        return None



# ── is_listed logic (B-05) ────────────────────────────────────────────────────

def _resolve_is_listed(company: dict) -> bool:
    """
    Robust listing detection — v2.2.
    A company is considered listed if:
      1. ipo_status == 'listed'  (canonical field, migration_003)
      2. ipo_potential == 'IPO erfolgt'  (legacy fallback)

    NOTE: investment_path == 'IPO' deliberately NOT used here —
    that value means pre-IPO candidate, not yet listed.
    """
    if company.get("ipo_status") == "listed":
        return True
    if company.get("ipo_potential") == "IPO erfolgt":
        return True
    # Fallback: eigener Ticker in DB gesetzt → Company ist selbst börsennotiert
    if company.get("ticker"):
        return True
    return False


def _resolve_investment_path(company: dict) -> str | None:
    """
    Investitionspfad-Logik basierend auf ipo_status:

    - pre_ipo_*  → immer 'IPO' (direkter Einstieg vor Börsengang ist der primäre Pfad)
    - listed     → DB-Wert; 'listed' ist ein Faktum der Company, nicht der Pfad.
                   Pfad beschreibt wie man indirekt investiert (Käufer-Proxy, ETF-Proxy etc.)
                   Fallback auf 'Käufer-Proxy' falls DB noch 'IPO' stehen hat.
    - kein Status → DB-Wert
    """
    ipo_status = company.get("ipo_status")
    db_path = company.get("investment_path")

    if ipo_status in ("pre_ipo_high", "pre_ipo_medium", "pre_ipo_low"):
        return "IPO"

    # listed oder kein Status — DB-Wert; 'IPO' als Pfad wäre hier falsch
    if db_path == "IPO":
        return "Käufer-Proxy"  # Fallback bis DB-Wert korrigiert (D-07)

    return db_path



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


# ── BUG-13: Buyer-Relevanz-Filter ────────────────────────────────────────────

_SECTOR_FIT: dict[str, list[str]] = {
    "energy":          ["energy", "solar", "wind", "hydrogen", "battery", "grid", "geothermal", "nuclear", "cleantech"],
    "industrials":     ["manufacturing", "construction", "materials", "cement", "steel", "chemical", "industrial"],
    "technology":      ["software", "ai", "saas", "semiconductor", "cloud", "iot", "robotics", "tech"],
    "agriculture":     ["agritech", "food", "agriculture", "biotech"],
    "transportation":  ["mobility", "evs", "logistics", "aviation", "maritime", "transport"],
    "materials":       ["mining", "materials", "recycling", "carbon capture", "carbon", "climate"],
    "healthcare":      ["medtech", "biotech", "pharma", "health"],
    "finance":         ["fintech", "insurance", "payments"],
}


def _filter_relevant_buyers_detail(buyers: list[dict], company: dict) -> list[dict]:
    """
    BUG-13: Filtert Buyers nach Sektor-Fit zur Company.
    Fallback auf alle Buyers wenn < 2 Treffer (kleines Universe).
    """
    industry = (company.get("industry") or "").lower()
    category = (company.get("category") or "").lower()
    inv_path = (company.get("investment_path") or "").lower()
    region   = (company.get("region") or "").lower()

    def _fits(buyer: dict) -> bool:
        buyer_sector = (buyer.get("sector") or "").lower()
        fit_kws = _SECTOR_FIT.get(buyer_sector, [])
        if any(kw in industry or kw in category for kw in fit_kws):
            return True
        buyer_region = (buyer.get("region") or "").lower()
        if buyer_region and region and buyer_region == region:
            return True
        if inv_path in ("käufer-proxy", "kaufer-proxy") and buyer.get("market_cap_usd_bn", 0) > 5:
            return True
        return False

    relevant = [b for b in buyers if _fits(b)]
    return relevant if len(relevant) >= 2 else buyers


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
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "claude-sonnet-4-6",
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

# Exchange → Yahoo-Suffix Mapping für internationale Börsen
_EXCHANGE_SUFFIX: dict[str, str] = {
    "xetra": ".DE", "frankfurt": ".DE", "fse": ".F",
    "euronext": ".PA", "euronext paris": ".PA", "euronext amsterdam": ".AS",
    "london": ".L", "lse": ".L",
    "swiss": ".SW", "six": ".SW",
    "bmv": ".MX", "milan": ".MI",
    "tsx": ".TO", "asx": ".AX",
    "hkex": ".HK", "tokyo": ".T",
}

# Yahoo exchangeName → lesbarer Display-Name
_EXCHANGE_DISPLAY: dict[str, str] = {
    "NMS": "Nasdaq", "NGM": "Nasdaq", "NCM": "Nasdaq", "NasdaqGM": "Nasdaq",
    "NasdaqGS": "Nasdaq", "NasdaqCM": "Nasdaq",
    "NYQ": "NYSE", "NYSE": "NYSE", "NYSEArca": "NYSE Arca",
    "PCX": "NYSE Arca",
    "GER": "Frankfurt", "FRA": "Frankfurt", "FSX": "Frankfurt",
    "XETRA": "Xetra",
    "LSE": "London", "IOB": "London",
    "PAR": "Euronext Paris", "AMS": "Euronext Amsterdam",
    "SWX": "SIX Swiss Exchange",
    "TYO": "Tokyo", "HKG": "Hong Kong", "TSX": "Toronto",
    "ASX": "ASX",
}

def _pct(v: float | None) -> float | None:
    """Konvertiert Yahoo-Dezimalwert (0.23) → Prozent (23.0), None wenn fehlt."""
    return round(v * 100, 1) if v is not None else None


# Twelve Data API Key — aus settings (konsistent mit restlicher Config-Nutzung)
_TWELVE_DATA_API_KEY: str | None = None  # wird lazy aus settings geladen

_TD_BASE = "https://api.twelvedata.com"

_TD_EXCHANGE: dict[str, str] = {
    "nasdaq":    "NASDAQ",
    "nyse":      "NYSE",
    "frankfurt": "XETR",
    "xetra":     "XETR",
    "london":    "LSE",
    "euronext":  "XPAR",
    "milan":     "MIL",
    "swiss":     "SIX",
    "amsterdam": "XAMS",
    "stockholm": "STO",
}


def _safe_float(val) -> float | None:
    """Konvertiert Twelve Data Felder sicher zu float."""
    if val is None or val == "" or val == "None":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


async def _fetch_twelve_data(symbol: str, exchange_key: str | None) -> dict:
    """
    YH-08 · Twelve Data — nur Quote (Preis + MarktCap + 52W).
    /statistics ist Pro-only — Fundamentals kommen via Bridge /yahoo/fundamentals/.
    Free Tier: 800 calls/day.

    Symbol-Format: Twelve Data erwartet Bare-Symbol + exchange als MIC-Code,
    NICHT Yahoo-Format (SIE.DE). Wir übergeben symbol=SIE&exchange=XETR.
    """
    api_key = settings.twelve_data_api_key or _TWELVE_DATA_API_KEY
    if not api_key:
        logger.warning("TWELVE_DATA_API_KEY nicht gesetzt")
        return {}

    # Symbol bereinigen: Yahoo-Suffix (.DE, .L etc.) entfernen — Twelve Data will Bare-Symbol
    bare_symbol = symbol.split(".")[0] if "." in symbol else symbol
    mic = _TD_EXCHANGE.get((exchange_key or "").lower(), "")
    # Twelve Data: symbol + exchange als separate Parameter, kein Yahoo-Format
    params = f"symbol={bare_symbol}&apikey={api_key}" + (f"&exchange={mic}" if mic else "")

    try:
        timeout = httpx.Timeout(8.0, connect=3.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            quote_resp = await client.get(f"{_TD_BASE}/quote?{params}")

        out: dict = {"ticker": symbol}  # Original-Symbol (mit Suffix) für Downstream

        if not isinstance(quote_resp, Exception) and quote_resp.status_code == 200:
            q = quote_resp.json()
            if q.get("status") != "error":
                out["price"]         = _safe_float(q.get("close"))
                out["currency"]      = q.get("currency")
                out["exchange"]      = _EXCHANGE_DISPLAY.get(q.get("exchange", ""), q.get("exchange"))
                out["week_52_high"]  = _safe_float(q.get("fifty_two_week", {}).get("high"))
                out["week_52_low"]   = _safe_float(q.get("fifty_two_week", {}).get("low"))
                mc = _safe_float(q.get("market_cap"))
                if mc:
                    out["market_cap_bn"] = mc / 1e9
                logger.info("TWELVE_DATA quote OK: %s price=%s mcap=%.1fBn",
                            bare_symbol, out.get("price"), out.get("market_cap_bn") or 0)
            else:
                logger.warning("TWELVE_DATA quote error: %s → %s", bare_symbol, q.get("message"))
        else:
            sc = quote_resp.status_code if not isinstance(quote_resp, Exception) else repr(quote_resp)
            logger.warning("TWELVE_DATA quote HTTP %s for %s", sc, bare_symbol)

        return out

    except Exception as e:
        logger.warning("TWELVE_DATA failed for %s: %s", bare_symbol, e)
        return {}


async def _fetch_yahoo_price_fallback(symbol: str) -> dict:
    """Yahoo Chart-API — nur Preis + MarktCap, kein Auth nötig."""
    try:
        timeout = httpx.Timeout(6.0, connect=3.0)
        async with httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as client:
            cr = await client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
            )
        if cr.status_code == 200:
            meta = cr.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
            return {
                "price":         meta.get("regularMarketPrice"),
                "market_cap_bn": (meta.get("marketCap") or 0) / 1e9 or None,
                "currency":      meta.get("currency"),
                "exchange":      _EXCHANGE_DISPLAY.get(meta.get("exchangeName", ""), meta.get("exchangeName")),
            }
    except Exception as e:
        logger.debug("Yahoo chart fallback failed for %s: %s", symbol, e)
    return {}


async def _fetch_yf_fundamentals(symbol: str) -> dict:
    """
    YH-08 · Direkter yfinance-Aufruf im Argo Backend — kein Bridge-Hop.

    Architektur-Entscheidung: Bridge nur für beta_cache (Kursverlauf-Buffering).
    Fundamentals brauchen kein Caching — on-demand, 1 Call pro Lookup.

    Strategie:
      fast_info → Preis, Marktcap, 52W (zuverlässig, ~0.3s)
      .info     → Revenue, EBITDA, Margen, Multiples (~2–6s, kann {} sein)
    Suffix-Fallback: .DE → .F für Frankfurt-Listings (yfinance Abdeckung variiert).
    Timeout 8s — asyncio.to_thread (yfinance ist synchron).
    """
    import yfinance as yf

    candidates = [symbol]
    if symbol.endswith(".DE"):
        candidates.append(symbol[:-3] + ".F")
    elif symbol.endswith(".F"):
        candidates.append(symbol[:-2] + ".DE")

    def _pct(v) -> float | None:
        return round(v * 100, 2) if v is not None else None

    def _bn(v) -> float | None:
        return round(v / 1e9, 3) if v else None

    def _sync_fetch(sym: str) -> dict:
        try:
            t    = yf.Ticker(sym)
            fi   = t.fast_info          # zuverlässig, schnell
            info = t.info or {}         # langsamer, kann {} sein

            # fast_info: Preis-Check — wenn kein Preis → Ticker unbekannt
            price      = getattr(fi, "last_price",   None)
            market_cap = getattr(fi, "market_cap",   None)
            currency   = getattr(fi, "currency",     None)
            w52_high   = getattr(fi, "year_high",    None)
            w52_low    = getattr(fi, "year_low",     None)

            if not price and not info.get("regularMarketPrice"):
                return {}

            # market_cap: fast_info primär → info.marketCap → price × sharesOutstanding
            # Kleine Nasdaq-Caps (z.B. LNZA) haben oft None in fast_info.market_cap
            _price_val  = price or info.get("regularMarketPrice")
            _shares     = info.get("sharesOutstanding")
            _mktcap_raw = (
                market_cap
                or info.get("marketCap")
                or (_price_val * _shares if _price_val and _shares else None)
            )

            out: dict = {
                "ticker":               sym,
                "price":                _price_val,
                "market_cap_bn":        _bn(_mktcap_raw),
                "currency":             currency or info.get("currency"),
                "week_52_high":         w52_high or info.get("fiftyTwoWeekHigh"),
                "week_52_low":          w52_low  or info.get("fiftyTwoWeekLow"),
                "pe_ratio":             info.get("trailingPE"),
                "revenue_bn":           _bn(info.get("totalRevenue")),
                "ebitda_bn":            _bn(info.get("ebitda")),
                "gross_margin_pct":     _pct(info.get("grossMargins")),
                "operating_margin_pct": _pct(info.get("operatingMargins")),
                "profit_margin_pct":    _pct(info.get("profitMargins")),
                "revenue_growth_pct":   _pct(info.get("revenueGrowth")),
                "earnings_growth_pct":  _pct(info.get("earningsGrowth")),
                "free_cashflow_bn":     _bn(info.get("freeCashflow")),
                "operating_cashflow_bn":_bn(info.get("operatingCashflow")),
            }

            # ── Derived metrics: berechne aus Komponenten wenn Direktwert fehlt ──────
            # Prinzip: wenn Yahoo das Feld nicht direkt liefert, haben wir meist
            # alle Komponenten um es selbst zu berechnen.

            rev        = info.get("totalRevenue")
            ebitda     = info.get("ebitda")
            debt       = info.get("totalDebt")
            cash       = info.get("totalCash") or info.get("cashAndCashEquivalents") or 0
            net_income = info.get("netIncome")
            gross_prof = info.get("grossProfits")
            op_income  = info.get("operatingIncome")
            dep_amort  = info.get("depreciationAmortization") or info.get("depreciation")

            # EBITDA: direkt → operatingIncome + D&A
            if not ebitda and op_income and dep_amort:
                ebitda = op_income + dep_amort
                out["ebitda_bn"] = _bn(ebitda)
                logger.debug("EBITDA berechnet (op_income+D&A) für %s: %.1fM", sym, ebitda / 1e6)

            # Enterprise Value: direkt → Mktcap + Debt - Cash
            ev = info.get("enterpriseValue")
            if not ev and _mktcap_raw and debt is not None:
                ev = _mktcap_raw + (debt or 0) - cash
                logger.debug("EV berechnet (mktcap+debt-cash) für %s: %.1fBn", sym, ev / 1e9)

            out["enterprise_value_bn"] = _bn(ev)
            if ev and rev:    out["ev_revenue"]  = round(ev / rev, 1)
            if ev and ebitda: out["ev_ebitda"]   = round(ev / ebitda, 1)
            if debt and ebitda:
                out["debt_ebitda"] = round((debt / 1e9) / (ebitda / 1e9), 2)

            # Margen: direkt → berechnet aus Komponenten / Revenue
            if not out.get("gross_margin_pct") and gross_prof and rev:
                out["gross_margin_pct"] = round(gross_prof / rev * 100, 1)
            if not out.get("operating_margin_pct") and op_income and rev:
                out["operating_margin_pct"] = round(op_income / rev * 100, 1)
            if not out.get("profit_margin_pct") and net_income and rev:
                out["profit_margin_pct"] = round(net_income / rev * 100, 1)
            if ebitda and rev and not info.get("ebitdaMargins"):
                out["ebitda_margin_pct"] = round(ebitda / rev * 100, 1)

            # Beta direkt aus Yahoo Finance .info — Fallback wenn Bridge-Cache fehlt.
            # Yahoos pre-calculated Beta (trailing 12M vs. S&P 500).
            # Präzisere Berechnung (252-Tage-Returns) läuft über BA-Bridge beta_cache.
            _yf_beta = info.get("beta")
            if _yf_beta is not None:
                try:
                    out["yf_beta"] = round(float(_yf_beta), 3)
                except (TypeError, ValueError):
                    pass

            return out
        except Exception as e:
            logger.debug("_fetch_yf_fundamentals sync failed for %s: %s", sym, e)
            return {}

    for sym in candidates:
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(_sync_fetch, sym),
                timeout=8.0,
            )
            if result.get("price") or result.get("revenue_bn"):
                logger.info(
                    "YF_FUNDAMENTALS OK: %s rev=%.1fBn price=%s",
                    sym, result.get("revenue_bn") or 0, result.get("price"),
                )
                return result
        except asyncio.TimeoutError:
            logger.warning("_fetch_yf_fundamentals timeout for %s", sym)
        except Exception as e:
            logger.warning("_fetch_yf_fundamentals failed for %s: %s", sym, e)

    return {}


async def _fetch_yahoo(ticker: str | None) -> dict:
    """
    YH-08 Hybrid-Architektur:
      1. Twelve Data /quote   → Preis + MarktCap + 52W  (Free Tier, Prio)
      2. yfinance direkt      → Revenue, EBITDA, Margen, EV + Preis via fast_info
      3. Yahoo Chart /v8/     → Preis-Fallback wenn weder TD noch yfinance Preis liefert
    Bridge nur noch für beta_cache — kein Bridge-Hop für Fundamentals.
    """
    if not ticker:
        return {}
    parts = ticker.split("·")
    symbol = parts[0].split("→")[-1].strip()
    exchange_key: str | None = None
    if len(parts) > 1 and "." not in symbol:
        exchange_key = parts[1].strip().lower()
        suffix = _EXCHANGE_SUFFIX.get(exchange_key, "")
        if suffix:
            symbol = symbol + suffix

    # Parallel: Twelve Data Quote + direkte yfinance Fundamentals
    td_out, yf_out = await asyncio.gather(
        _fetch_twelve_data(symbol, exchange_key),
        _fetch_yf_fundamentals(symbol),
        return_exceptions=True,
    )
    if isinstance(td_out, Exception):
        td_out = {}
    if isinstance(yf_out, Exception):
        yf_out = {}

    # Merge: Twelve Data Preis (Prio) + yfinance Fundamentals
    # td_out überschreibt bei Preis/Exchange/52W — yf_out liefert Revenue/EBITDA/Margen
    out: dict = {**yf_out, **td_out}

    # Fallback: Yahoo Chart wenn Preis oder Market Cap fehlt.
    # market_cap_bn kann None sein obwohl Preis da — yfinance fast_info.market_cap
    # ist für kleine Nasdaq-Caps (z.B. LNZA) oft None obwohl Preis zurückkommt.
    if not out.get("price") or not out.get("market_cap_bn"):
        logger.info(
            "Preis oder Market Cap fehlt für %s (price=%s mktcap=%s) — Yahoo Chart Fallback",
            symbol, out.get("price"), out.get("market_cap_bn"),
        )
        fallback = await _fetch_yahoo_price_fallback(symbol)
        for k in ("price", "market_cap_bn", "currency", "exchange"):
            if fallback.get(k) and not out.get(k):
                out[k] = fallback[k]

    out.setdefault("ticker", symbol)
    return out


async def _fetch_from_shadow(name: str) -> dict | None:
    """
    ARCH-01: Lookup in BA-Bridge Shadow-DB.
    Gibt Shadow-Daten zurück wenn Company bekannt und angereichert.
    Timeout 5s — blockiert One-Click nicht bei Bridge-Ausfall.
    """
    import httpx
    from src.config import settings
    bridge_url = getattr(settings, "ba_bridge_url", None)
    if not bridge_url:
        return None
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{bridge_url}/shadow/company/{name}")
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") not in ("not_found", None):
                return data
    except Exception as e:
        logger.debug("_fetch_from_shadow failed für '%s': %s", name, e)
    return None



async def _fetch_beta_from_bridge(
    ticker: str | None,
    category: str | None,
    is_listed: bool,
) -> dict:
    """
    YH-06 — Beta-Routing:
        listed  → GET /yahoo/ticker/{ticker}           → beta_source='market'
        private → GET /yahoo/ticker/_/damodaran        → beta_source='damodaran'

    Gibt leeres dict zurück bei Fehler / nicht gefunden — kein Hard-Fail.
    """
    if not settings.ba_bridge_url:
        return {}

    headers = {"X-API-Key": settings.ba_bridge_api_key}
    timeout = httpx.Timeout(5.0, connect=2.0)

    try:
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            if is_listed and ticker:
                # BUG-37: Exchange-Suffix analog zu _fetch_yahoo auflösen
                # ticker kann "SIE · Frankfurt", "SIE.DE" oder "SIE" sein
                _parts = ticker.split("·")
                symbol = _parts[0].split("→")[-1].strip().upper()
                if "." not in symbol and len(_parts) > 1:
                    _exch = _parts[1].strip().lower()
                    _sfx  = _EXCHANGE_SUFFIX.get(_exch, "")
                    if _sfx:
                        symbol = symbol + _sfx
                resp = await client.get(f"{settings.ba_bridge_url.rstrip('/')}/yahoo/ticker/{symbol}")
                if resp.status_code == 200:
                    data = resp.json()
                    return {
                        "beta_1y":                    data.get("beta_1y"),
                        "beta_3y":                    data.get("beta_3y"),
                        "volatility_30d":             data.get("volatility_30d"),
                        "beta_source":                "market",
                        "beta_benchmark":             data.get("benchmark_ticker"),
                        "beta_benchmark_is_fallback": data.get("benchmark_is_fallback", False),
                        "beta_calculated_at":         data.get("calculated_at"),
                        "beta_data_quality":          data.get("data_quality"),
                    }
                logger.debug("Beta bridge miss for ticker=%s: HTTP %s", symbol, resp.status_code)

            elif not is_listed and category:
                resp = await client.get(
                    f"{settings.ba_bridge_url.rstrip('/')}/yahoo/ticker/_/damodaran",
                    params={"category": category},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return {
                        "beta_1y":                    data.get("unlevered_beta"),
                        "beta_3y":                    None,
                        "volatility_30d":             None,
                        "beta_source":                "damodaran",
                        "beta_benchmark":             f"Damodaran · {data.get('sector', '')}",
                        "beta_benchmark_is_fallback": False,
                        "beta_calculated_at":         None,
                        "beta_data_quality":          "full",
                    }
                logger.debug("Damodaran miss for category=%s: HTTP %s", category, resp.status_code)

    except Exception as e:
        logger.debug("_fetch_beta_from_bridge failed: %s", e)

    return {}


def _build_fundamentals(
    is_listed: bool,
    yahoo: dict,
    ba: BundesanzeigerData | None,
    proxy: str | None,
    beta: dict | None = None,
    fd_source: dict | None = None,
) -> FundamentalsData:
    beta      = beta or {}
    fd_source = fd_source or {}

    def _apply_beta(fd: FundamentalsData) -> FundamentalsData:
        fd.beta_1y                    = beta.get("beta_1y")
        fd.beta_3y                    = beta.get("beta_3y")
        fd.volatility_30d             = beta.get("volatility_30d")
        fd.beta_source                = beta.get("beta_source")
        fd.beta_benchmark             = beta.get("beta_benchmark")
        fd.beta_benchmark_is_fallback = beta.get("beta_benchmark_is_fallback", False)
        fd.beta_calculated_at         = beta.get("beta_calculated_at")
        fd.beta_data_quality          = beta.get("beta_data_quality")
        # FD-01 Routing — Herkunfts-Badge
        fd.fundamentals_source           = fd_source.get("primary")
        fd.fundamentals_source_secondary = fd_source.get("secondary")
        fd.fundamentals_quality_flag     = fd_source.get("quality_flag")
        return fd

    if is_listed:
        fd = FundamentalsData(
            is_listed=True,
            ticker=yahoo.get("ticker") or (proxy.split("·")[0].strip() if proxy else None),
            exchange=yahoo.get("exchange"), price=yahoo.get("price"),
            market_cap_bn=yahoo.get("market_cap_bn"), pe_ratio=yahoo.get("pe_ratio"),
            revenue_bn=yahoo.get("revenue_bn"), ebitda_bn=yahoo.get("ebitda_bn"),
            debt_ebitda=yahoo.get("debt_ebitda"), week_52_high=yahoo.get("week_52_high"),
            week_52_low=yahoo.get("week_52_low"), currency=yahoo.get("currency"),
            gross_margin_pct=yahoo.get("gross_margin_pct"),
            operating_margin_pct=yahoo.get("operating_margin_pct"),
            profit_margin_pct=yahoo.get("profit_margin_pct"),
            revenue_growth_pct=yahoo.get("revenue_growth_pct"),
            earnings_growth_pct=yahoo.get("earnings_growth_pct"),
            free_cashflow_bn=yahoo.get("free_cashflow_bn"),
            operating_cashflow_bn=yahoo.get("operating_cashflow_bn"),
            ev_revenue=yahoo.get("ev_revenue"),
            ev_ebitda=yahoo.get("ev_ebitda"),
            enterprise_value_bn=yahoo.get("enterprise_value_bn"),
        )
        return _apply_beta(fd)

    fd = FundamentalsData(is_listed=False)
    if ba and ba.found:
        fd.ba_found=True; fd.ba_legal_form=ba.legal_form
        fd.ba_registered_at=ba.registered_at; fd.ba_last_report_year=ba.last_annual_report_year
        fd.ba_revenue_mn=ba.revenue_mn; fd.ba_ebitda_mn=ba.ebitda_mn
        fd.ba_ebit_mn=getattr(ba, "ebit_mn", None)
        fd.ba_net_income_mn=getattr(ba, "net_income_mn", None)
        fd.ba_equity_mn=ba.equity_mn
        fd.ba_total_assets_mn=ba.total_assets_mn; fd.ba_employees=ba.employees
        fd.ba_source_url=ba.source_url
    return _apply_beta(fd)


# ── FD-01 · Fundamentals-Routing ─────────────────────────────────────────────

# Mögliche Quellen-Typen — für Herkunfts-Badge im Frontend
FUNDAMENTALS_SOURCE_YAHOO    = "yahoo"        # Yahoo Finance (listed)
FUNDAMENTALS_SOURCE_BA       = "ba_bridge"    # Bundesanzeiger via BA-Bridge (private DE)
FUNDAMENTALS_SOURCE_EDGAR    = "edgar"        # SEC EDGAR (private/listed US) — Phase 2
FUNDAMENTALS_SOURCE_NONE     = "none"         # Keine Fundamentals öffentlich verfügbar


def _get_fundamentals_source(
    is_listed: bool,
    exchange: str | None,
    region: str | None,
) -> dict:
    """
    FD-01 — Routing-Logik: welche Quellen für Fundamentals nutzen.

    Returns:
        {
            "primary":   str,   # Haupt-Quelle
            "secondary": str | None,  # Ergänzung (z.B. BA-Bridge für listed DE)
            "beta":      str,   # 'market' (listed) | 'damodaran' (private)
            "quality_flag": str | None,  # None | 'partial' — für Frontend-Hinweis
        }

    Routing-Tabelle (aus REQUESTS v1.8 FD-Block):
        Private DE          → BA-Bridge primär
        Private US          → EDGAR primär (Phase 2), Wikipedia Fallback
        Private EU non-DE   → none (Revenue oft nicht öffentlich)
        Private non-EU/US   → none
        Listed US           → Yahoo Finance
        Listed EU DE        → Yahoo Finance + BA-Bridge (Ergänzung)
        Listed EU non-DE    → Yahoo Finance (quality_flag='partial' — lückenhaft)
        Listed non-EU/US    → Yahoo Finance (quality_flag='partial')
    """
    exchange_norm = (exchange or "").lower()
    region_norm   = (region or "").lower()

    # ── Listed ──────────────────────────────────────────────────────────────
    if is_listed:
        # US-Börsen — Yahoo zuverlässig
        if exchange_norm in ("nyse", "nasdaq", "nyse arca"):
            return {
                "primary":      FUNDAMENTALS_SOURCE_YAHOO,
                "secondary":    None,
                "beta":         "market",
                "quality_flag": None,
            }
        # Frankfurt / Xetra — Yahoo + BA-Bridge Ergänzung
        if exchange_norm in ("frankfurt", "xetra"):
            return {
                "primary":      FUNDAMENTALS_SOURCE_YAHOO,
                "secondary":    FUNDAMENTALS_SOURCE_BA,
                "beta":         "market",
                "quality_flag": None,
            }
        # Sonstige EU + internationale Börsen — Yahoo, aber lückenhaft
        return {
            "primary":      FUNDAMENTALS_SOURCE_YAHOO,
            "secondary":    None,
            "beta":         "market",
            "quality_flag": "partial",  # Frontend zeigt Hinweis
        }

    # ── Private ─────────────────────────────────────────────────────────────
    # DE (region oder exchange gibt Hinweis)
    if region_norm in ("de", "deu", "germany") or exchange_norm in ("frankfurt", "xetra"):
        return {
            "primary":      FUNDAMENTALS_SOURCE_BA,
            "secondary":    None,
            "beta":         "damodaran",
            "quality_flag": None,
        }
    # US
    if region_norm in ("us", "usa", "united states"):
        return {
            "primary":      FUNDAMENTALS_SOURCE_EDGAR,   # Phase 2 — aktuell Wikipedia-Fallback
            "secondary":    None,
            "beta":         "damodaran",
            "quality_flag": None,
        }
    # EU non-DE + Rest
    return {
        "primary":      FUNDAMENTALS_SOURCE_NONE,
        "secondary":    None,
        "beta":         "damodaran",
        "quality_flag": "no_data",  # Frontend zeigt Badge "Keine Finanzdaten öffentlich verfügbar"
    }


# ── Supabase query ────────────────────────────────────────────────────────────
# supabase.py fetch_companies muss ipo_status selektieren — siehe unten

# ── Main route ────────────────────────────────────────────────────────────────

@router.get("/company/{name}", response_model=CompanyDetailResponse)
async def get_company_detail(name: str, background_tasks: BackgroundTasks) -> CompanyDetailResponse:
    warnings: list[str] = []

    # 1. Lookup
    companies = fetch_companies(limit=500)
    q = name.lower().replace("-"," ").replace("_"," ")
    def _proxy_match(c: dict, ticker: str) -> bool:
        proxy = c.get("proxy_ticker") or c.get("proxy") or ""
        return proxy.upper().startswith(ticker.upper())
    company = next(
        (c for c in companies if
         c.get("name","").lower()==q or
         q in c.get("name","").lower() or
         c.get("name","").lower().replace(" ","-")==name.lower() or
         _proxy_match(c, name)),
        None,
    )

    # 1b. One-Click: ARCH-01 Shadow-DB zuerst prüfen, dann Blank-Entry
    if not company:
        # Shadow-DB Lookup via BA-Bridge (max 5s Timeout — non-blocking)
        shadow_data = await _fetch_from_shadow(name)

        if shadow_data and shadow_data.get("status") == "done":
            logger.info("Shadow-DB hit für '%s' — promote to Supabase", name)
            from src.integrations.supabase import promote_shadow_to_supabase
            company = promote_shadow_to_supabase(shadow_data)
            if company:
                warnings.append(f"'{name}' aus Shadow-DB geladen (Bundesanzeiger-Daten verfügbar).")

        if not company:
            # Normaler Flow: Blank-Entry + async Enrichment
            logger.info("Unknown company '%s' — creating DB entry + enriching", name)
            try:
                db = get_supabase()
                result = db.table("companies").insert({
                    "name": name,
                    "investment_path": "Beobachten",
                    "enrichment_status": "pending",
                }).execute()
                company = result.data[0] if result.data else {"name": name}
                warnings.append(f"'{name}' war nicht in der Datenbank — wird gerade angereichert.")
            except Exception as e:
                logger.warning("Could not create company '%s': %s — %s", name, type(e).__name__, e)
                raise HTTPException(status_code=404, detail=f"Company '{name}' not found and could not be created: {e}")

    company_name = company["name"]

    # 2. is_listed — robust (B-05)
    is_listed = _resolve_is_listed(company)
    proxy = company.get("proxy_ticker")
    logger.warning("TICKER_DEBUG %s — ticker=%s exchange=%s proxy_ticker=%s is_listed=%s", company_name, company.get("ticker"), company.get("exchange"), proxy, is_listed)

    # Für selbst-börsennotierte Companies (is_listed, kein proxy_ticker):
    # eigenen Ticker aus companies.ticker verwenden → Yahoo Finance Lookup
    if is_listed and not proxy and company.get("ticker"):
        ticker_raw = company["ticker"]
        exchange_raw = company.get("exchange", "")
        proxy = f"{ticker_raw} · {exchange_raw}".strip(" ·") if exchange_raw else ticker_raw

    # 3. TAM — erst DB-Cache prüfen, dann scrapen, Ergebnis persistieren
    company_id = company.get("id")
    tam_cached = fetch_tam_cache(company_id) if company_id else None
    if tam_cached and tam_cached.get("tam_2035_usd_bn"):
        tam = {
            "tam_usd_bn":  float(tam_cached["tam_2035_usd_bn"]),
            "source":      tam_cached.get("source", "cache"),
            "confidence":  "high",
            "method":      "cache",
        }
        logger.debug("TAM from cache for %s: %.1f Bn", company_name, tam["tam_usd_bn"])
    else:
        tam = await get_tam(company_name, company.get("category"))
        # Ergebnis persistieren für nächsten Aufruf
        if company_id and tam.get("tam_usd_bn"):
            upsert_tam_cache(
                company_id=company_id,
                tam_usd_bn=tam["tam_usd_bn"],
                cagr_pct=tam.get("cagr_pct"),
                source=tam.get("source", "scrape"),
            )

    # 4. Market Data — Cache prüfen, bei Bedarf Background-Enrichment anstoßen (MD-B07)
    # _tr_ref: Closure-Container für tech_readiness — wird nach Scoring-Block gefüllt
    # (Schritt 9). Background-Task läuft nach Response-Aufbau, liest dann den echten Wert.
    _tr_ref: list[float | None] = [None]

    market_data_cached = fetch_market_data(company_id) if company_id else None
    # Leere Row (nur company_id, alle Felder NULL) gilt nicht als befüllt.
    # Staleness-Guard: enriched_at vor 2026 = Sentinel/Pre-Session-27-Datum → re-enrich.
    # Auch Daten älter als 30 Tage werden neu angereichert (MD-B01/05/06-Fixes greifen dann).
    def _market_data_fresh(cached: dict | None) -> bool:
        if not cached:
            return False
        if not cached.get("enriched_at") or not cached.get("sam_usd_bn"):
            return False
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(cached["enriched_at"].replace("Z", "+00:00"))
            if dt.year < 2026:          # Sentinel-Datum (z.B. 2020-01-01) → stale
                return False
            if (datetime.now(timezone.utc) - dt).days > 30:  # Älter als 30 Tage → re-enrich
                return False
            return True
        except Exception:
            return False

    _market_data_valid = _market_data_fresh(market_data_cached)
    if company_id and not _market_data_valid:
        async def _market_enrichment_bg():
            try:
                set_enrichment_status(company_id, "running")
                async_result = await enrich_market_data(
                    company_id=company_id,
                    company_name=company_name,
                    category=company.get("category"),
                    sector_tag=None,
                    tam_usd_bn=tam.get("tam_usd_bn"),
                    tech_readiness=_tr_ref[0],   # gesetzt nach Scoring-Block
                )
                all_companies = fetch_companies(limit=500)
                all_rounds = fetch_all_funding_rounds()
                sync_result = enrich_market_data_sync_wrapper(
                    company_id=company_id,
                    company_name=company_name,
                    category=company.get("category"),
                    sector_tag=None,
                    tam_usd_bn=tam.get("tam_usd_bn"),
                    all_companies=all_companies,
                    all_funding_rounds=all_rounds,
                    async_result=async_result,
                    peers_context=company.get("peers_context"),  # R-22
                    is_listed=is_listed,                         # BUG-51: mature statt early für listed
                )
                # _competition_signals ist internes Übergabe-Feld — nicht in DB schreiben
                upsert_payload = {
                    **{k: v for k, v in async_result.items() if k != "_competition_signals"},
                    **sync_result,
                }
                upsert_market_data(company_id, upsert_payload)
                set_enrichment_status(company_id, "done")
                logger.info("Market enrichment done for %s", company_name)
            except Exception as e:
                set_enrichment_status(company_id, "error")
                logger.exception("Market enrichment TRACEBACK for %s", company_name)

        background_tasks.add_task(_market_enrichment_bg)
        logger.info("Market enrichment queued (BackgroundTasks) for %s", company_name)

    elif company_id and company.get("peers_context"):
        # R-22 follow-up: competition_note mit aktuellem peers_context refreshen.
        # Läuft wenn market_data bereits gecacht ist — kein teurer async-Teil (kein DDG/Claude).
        # Nur competition_score + competition_note + market_cycle werden upgesertet.
        async def _competition_refresh_bg():
            try:
                all_companies = fetch_companies(limit=500)
                all_rounds    = fetch_all_funding_rounds()
                comp_result   = enrich_market_data_sync_wrapper(
                    company_id=company_id,
                    company_name=company_name,
                    category=company.get("category"),
                    sector_tag=None,
                    tam_usd_bn=tam.get("tam_usd_bn"),
                    all_companies=all_companies,
                    all_funding_rounds=all_rounds,
                    async_result=None,   # keine neuen DDG-Signale — Peers-Kontext genügt
                    peers_context=company.get("peers_context"),
                    is_listed=is_listed,  # BUG-51: mature statt early für listed
                )
                upsert_market_data(company_id, {
                    k: v for k, v in comp_result.items()
                    if k in ("competition_note",)  # Score bleibt aus originalem DDG-Enrichment
                })
                logger.debug("R-22 competition_note refreshed for %s", company_name)
            except Exception as e:
                logger.warning("R-22 competition refresh failed for %s: %s", company_name, e)

        background_tasks.add_task(_competition_refresh_bg)

    # 5. Parallel: enrichment (with timeout) + yahoo + intro
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

    async def _safe_intro():
        try:
            return await asyncio.wait_for(
                _generate_intro(company, tam),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            logger.warning("Intro timeout for %s — using fallback", company_name)
            return f"{company_name} — no description available."
        except Exception as e:
            logger.warning("Intro failed for %s: %s", company_name, e)
            return f"{company_name} — no description available."

    # BUG-36: Yahoo auch aufrufen wenn proxy gesetzt und Ticker erkennbar,
    # auch wenn is_listed noch False (z.B. De-SPAC in progress, ticker bereits handelbar).
    _yahoo_ticker = proxy if is_listed else (proxy if proxy and proxy != "—" else None)
    enrichment, yahoo, intro = await asyncio.gather(
        _safe_enrichment(),
        _fetch_yahoo(_yahoo_ticker),
        _safe_intro(),
    )

    # 4b. Enrichment-Ergebnisse in DB persistieren (nur wenn Werte vorhanden)
    #     DB-Werte als Fallback wenn Enrichment leer (z.B. bei Timeout)
    if company_id:
        _ba_emp_for_upsert = (
            getattr(enrichment.bundesanzeiger, "employees", None)
            if enrichment.bundesanzeiger else None
        )
        upsert_payload = {
            "founding_year": _parse_year(enrichment.founded_year),
            "headquarters":  enrichment.headquarters or None,
            "headcount":     (
                _parse_headcount(enrichment.employee_count)
                or (int(_ba_emp_for_upsert) if _ba_emp_for_upsert else None)
            ),
            "description":   enrichment.description or None,
            "website":       enrichment.website or None,
            "tags":          enrichment.tags if enrichment.tags else None,   # Technologie-Chips Tab 0 — persistieren, [] nicht schreiben
        }
        # BUG-47: ipo_status aus EnrichmentResult in DB schreiben
        # Nur wenn Enrichment einen Wert liefert UND DB noch keinen hat
        if enrichment.ipo_status and not company.get("ipo_status"):
            upsert_payload["ipo_status"] = enrichment.ipo_status
        # BUG-34 complete: Wikipedia-Titel als kanonischen Namen übernehmen
        # ("Linde" → "Linde plc", "SpaceX" → korrekte Schreibweise)
        # Nur wenn enrichment.name vom DB-Namen abweicht und plausibel ist
        if (enrichment.name
                and enrichment.name.lower() != company_name.lower()
                and 2 <= len(enrichment.name) <= 120):
            upsert_payload["name"] = enrichment.name
            logger.info("Canonical name update: '%s' → '%s'", company_name, enrichment.name)
        # EN-06: Ticker + Exchange nur für börsennotierte Companies schreiben
        # Verhindert dass private Companies versehentlich einen Ticker bekommen
        if is_listed:
            if enrichment.ticker and not company.get("ticker"):
                upsert_payload["ticker"] = enrichment.ticker
            if enrichment.exchange and not company.get("exchange"):
                upsert_payload["exchange"] = enrichment.exchange
        # category / industry — erst Tag-Inferenz (aus enrichment), dann Claude-Fallback
        inferred_cat  = enrichment.category
        inferred_ind  = enrichment.industry

        # description: enrichment-Wert oder DB-Fallback (falls Timeout)
        _desc_for_claude = enrichment.description or company.get("description")

        logger.warning(
            "CATEGORY_DEBUG %s — inferred_cat=%s desc_len=%s company_cat=%s",
            company_name,
            inferred_cat,
            len(_desc_for_claude) if _desc_for_claude else 0,
            company.get("category"),
        )

        # Claude-Fallback: nur wenn Tags keinen Treffer hatten + description vorhanden
        if not inferred_cat and _desc_for_claude and not company.get("category"):
            try:
                inferred_cat, inferred_ind = await asyncio.wait_for(
                    _claude_infer_category(company_name, _desc_for_claude),
                    timeout=6.0,
                )
                logger.warning("Claude category result for %s: cat=%s ind=%s", company_name, inferred_cat, inferred_ind)
            except asyncio.TimeoutError:
                logger.warning("Claude category inference TIMEOUT for %s", company_name)
            except Exception as e:
                logger.warning("Claude category inference FAILED for %s: %s — %s", company_name, type(e).__name__, e)

        if inferred_cat and not company.get("category"):
            upsert_payload["category"] = inferred_cat
            company["category"] = inferred_cat
        if inferred_ind and not company.get("industry"):
            upsert_payload["industry"] = inferred_ind
            company["industry"] = inferred_ind

        upsert_company_enrichment(company_id, upsert_payload)

        # 4c. Fundamentals → companies table (Background Task)
        # Schließt Architektur-Lücke: revenue_usd_mn / is_profitable / growth_rate_pct
        # Strategie: Yahoo primär (wenn yfinance .info nicht timeout) →
        #            kpi_timeseries Fallback (eigene DB, bereits gecacht, zuverlässig).
        # Für kleine Nasdaq-Caps (LNZA etc.) läuft yfinance .info oft in den 8s-Timeout
        # → yahoo hat keine Fundamentals → kpi_timeseries ist der robustere Pfad.
        if is_listed and company_id:
            # Yahoo-Werte wenn vorhanden (yfinance hat nicht ge-timeout)
            _yf_direct: dict = {}
            if yahoo.get("revenue_bn") is not None:
                _yf_direct["revenue_usd_mn"] = round(yahoo["revenue_bn"] * 1000, 1)
            if yahoo.get("profit_margin_pct") is not None:
                _yf_direct["is_profitable"] = yahoo["profit_margin_pct"] > 0
            if yahoo.get("revenue_growth_pct") is not None:
                _yf_direct["growth_rate_pct"] = yahoo["revenue_growth_pct"]

            async def _persist_fundamentals_bg(_direct: dict = _yf_direct) -> None:
                try:
                    persist: dict = dict(_direct)  # Yahoo-Werte als Ausgangspunkt

                    # Fallback: kpi_timeseries aus DB — zuverlässig auch wenn yfinance timeout
                    # Füllt Lücken die Yahoo nicht geliefert hat
                    _missing = {"revenue_usd_mn", "is_profitable", "growth_rate_pct"} - set(persist)
                    if _missing:
                        db = get_supabase()
                        rows = (
                            db.table("kpi_timeseries")
                            .select("metric,fiscal_year,value")
                            .eq("company_id", company_id)
                            .execute()
                            .data or []
                        )
                        # Neuesten Wert je Metrik
                        by_metric: dict = {}
                        for r in rows:
                            m = r["metric"]
                            if m not in by_metric or r["fiscal_year"] > by_metric[m]["fiscal_year"]:
                                by_metric[m] = r

                        if "revenue_usd_mn" in _missing and "revenue_mn" in by_metric:
                            persist["revenue_usd_mn"] = round(by_metric["revenue_mn"]["value"], 1)

                        if "is_profitable" in _missing and "net_income_mn" in by_metric:
                            persist["is_profitable"] = by_metric["net_income_mn"]["value"] > 0

                        if "growth_rate_pct" in _missing and "revenue_mn" in by_metric:
                            # CAGR aus kpi_timeseries berechnen wenn ≥2 Datenpunkte
                            rev_rows = sorted(
                                [r for r in rows if r["metric"] == "revenue_mn"],
                                key=lambda r: r["fiscal_year"],
                            )
                            if len(rev_rows) >= 2:
                                first, last = rev_rows[0], rev_rows[-1]
                                n = last["fiscal_year"] - first["fiscal_year"]
                                if n > 0 and first["value"] and first["value"] > 0:
                                    cagr = ((last["value"] / first["value"]) ** (1 / n) - 1) * 100
                                    persist["growth_rate_pct"] = round(cagr, 1)

                    if persist:
                        upsert_company_enrichment(company_id, persist)
                        logger.info(
                            "Fundamentals persisted für %s (yahoo=%d kpi=%d): %s",
                            company_name,
                            len(_direct),
                            len(persist) - len(_direct),
                            list(persist.keys()),
                        )
                    else:
                        logger.debug("Keine Fundamentals zu persistieren für %s", company_name)
                except Exception as _e:
                    logger.warning("Fundamentals persist failed für %s: %s", company_name, _e)

            background_tasks.add_task(_persist_fundamentals_bg)

    # TAM-Re-Lookup: wenn erster TAM-Call Fallback war und jetzt category bekannt
    if tam.get("method") == "fallback" and company.get("category"):
        logger.info("TAM re-lookup with inferred category '%s' for %s", company["category"], company_name)
        tam_retry = await get_tam(company_name, company["category"])
        if tam_retry.get("method") != "fallback":
            tam = tam_retry
            if company_id and tam.get("tam_usd_bn"):
                upsert_tam_cache(
                    company_id=company_id,
                    tam_usd_bn=tam["tam_usd_bn"],
                    cagr_pct=tam.get("cagr_pct"),
                    source=tam.get("source", "scrape"),
                )

    # DB-Werte als Fallback wenn Enrichment-Felder leer
    # BA-Bridge: ba_employees als weiterer Fallback für Headcount (private DE)
    _ba_employees = (
        getattr(enrichment.bundesanzeiger, "employees", None)
        if enrichment.bundesanzeiger else None
    )
    founded_display   = enrichment.founded_year   or (str(company.get("founding_year")) if company.get("founding_year") else None)
    headquarters_disp = enrichment.headquarters   or company.get("headquarters")
    headcount_disp    = (
        enrichment.employee_count
        or (str(company.get("headcount")) if company.get("headcount") else None)
        or (str(_ba_employees) if _ba_employees else None)
    )
    description_disp  = enrichment.description    or company.get("description")

    # 5. Fundamentals + Beta (YH-06) + Routing (FD-01)
    fd_source = _get_fundamentals_source(
        is_listed=is_listed,
        exchange=company.get("exchange") or yahoo.get("exchange"),
        region=company.get("region"),
    )
    # BUG-37: ticker mit Exchange zusammenbauen damit Suffix-Auflösung in _fetch_beta_from_bridge greift
    # "SIE" (kein Exchange) → "SIE · Frankfurt" → _fetch_beta_from_bridge löst zu "SIE.DE" auf
    _beta_ticker_raw = company.get("ticker") or (proxy.split("·")[0].strip() if proxy else None)
    _beta_exchange   = company.get("exchange") or (yahoo.get("exchange") if yahoo else None)
    if _beta_ticker_raw and _beta_exchange and "·" not in _beta_ticker_raw and "." not in _beta_ticker_raw:
        _beta_ticker_raw = f"{_beta_ticker_raw} · {_beta_exchange}"
    beta = await _fetch_beta_from_bridge(
        ticker=_beta_ticker_raw,
        category=company.get("category") or getattr(enrichment, "category", None),
        is_listed=is_listed,
    )

    # yfinance Beta-Fallback: wenn Bridge keinen Cache hat (404) und Company listed
    # yahoo["yf_beta"] = Yahoos pre-calculated trailing-12M Beta vs. S&P 500
    # Mittelfristig: Bridge seeded proaktiv Kurse für alle listed Argo-Companies
    if not beta.get("beta_1y") and is_listed and yahoo and yahoo.get("yf_beta"):
        beta = {
            "beta_1y":                    yahoo["yf_beta"],
            "beta_3y":                    None,
            "volatility_30d":             None,
            "beta_source":                "yahoo",
            "beta_benchmark":             "Yahoo Finance · S&P 500 (trailing 12M)",
            "beta_benchmark_is_fallback": False,
            "beta_calculated_at":         None,
            "beta_data_quality":          "partial",
        }
        logger.info("Beta Fallback yfinance für %s: β=%.2f", company_name, yahoo["yf_beta"])

    # DQ-04: Proxy Beta — Beta des Investment-Instruments (z.B. NEE, CRH) für Tab 1
    # Nur wenn die Company selbst NICHT listed ist (sonst ist Company = Instrument)
    proxy_beta: dict = {}
    if not is_listed and proxy:
        proxy_symbol = proxy.split("·")[0].split("→")[-1].strip()
        if proxy_symbol and proxy_symbol != "—":
            try:
                proxy_beta = await _fetch_beta_from_bridge(
                    ticker=proxy_symbol,
                    category=None,
                    is_listed=True,   # Proxy ist immer listed
                )
            except Exception as _pb_err:
                logger.debug("Proxy beta fetch failed for %s: %s", proxy_symbol, _pb_err)

    fundamentals = _build_fundamentals(
        is_listed=is_listed or bool(yahoo.get("price") or yahoo.get("market_cap_bn")),
        yahoo=yahoo,
        ba=enrichment.bundesanzeiger,
        proxy=proxy,
        beta=beta,
        fd_source=fd_source,
    )
    # FD-02 — Lücken-Badge: keine öffentlichen Finanzdaten
    if fd_source["primary"] == "none":
        warnings.append("Keine Finanzdaten öffentlich verfügbar für diese Company.")
    # FD-03 — Qualitäts-Flag: Yahoo-Daten lückenhaft (kleinere EU-Börsen)
    if fd_source.get("quality_flag") == "partial" and is_listed:
        warnings.append("Marktdaten eingeschränkt verfügbar — kleinere Börse, Yahoo Finance lückenhaft.")

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

    # 8. Funding rounds — aus DB (migration_004)
    db_rounds = fetch_funding_rounds(company["id"]) if company.get("id") else []
    funding_rounds = [
        FundingRoundItem(
            date=str(r["date"]) if r.get("date") else None,
            type=r.get("type"),
            amount_usd_mn=r.get("amount_usd_mn"),
            lead_investor=r.get("lead_investor"),
            co_investors=r.get("co_investors") or [],
            source=r.get("source"),
            notes=r.get("notes"),
        )
        for r in db_rounds
    ]

    # BUG-30: Funding History → Investoren immer in Ownership aufnehmen
    # Investoren aus funding_rounds werden immer gemerged (Lead + Co-Investoren),
    # deduped gegen bereits vorhandene Einträge (curated overrides / enrichment).
    if db_rounds:
        _existing_names = {o.name.lower() for o in ownership}
        for r in db_rounds:
            lead = r.get("lead_investor")
            if lead and lead.lower() not in _existing_names:
                _existing_names.add(lead.lower())
                _round_label = f"{r.get('type', 'Funding')} {str(r.get('date', ''))[:4]}".strip()
                ownership.append(OwnershipItem(
                    name=lead,
                    type="VC/Investor",
                    role="Lead Investor",
                    notes=_round_label or None,
                ))
            for co in (r.get("co_investors") or []):
                if co and co.lower() not in _existing_names:
                    _existing_names.add(co.lower())
                    ownership.append(OwnershipItem(
                        name=co,
                        type="VC/Investor",
                        role="Co-Investor",
                        notes="Funding History",
                    ))
        # "Not publicly disclosed" entfernen wenn wir jetzt echte Einträge haben
        ownership = [o for o in ownership if o.name != "Not publicly disclosed"] or ownership
        logger.info("BUG-30: Ownership nach Funding-Merge: %d Einträge für %s", len(ownership), company_name)

    # EN-08: DB-Ownership-Einträge (ownership_entries-Tabelle) laden + in Response mergen.
    # Für listed Companies: Yahoo institutional_holders (yfinance, via BA-Bridge).
    # Für private Companies: EDGAR Form D / BA-Bridge (enrich_ownership).
    # Einträge werden deduped gegen bereits vorhandene (curated overrides + enrichment + funding).
    _db_ownership_raw = fetch_ownership_entries(company_id) if company_id else []
    # _enrichment_attempted: Loop-Guard — verhindert Re-Trigger wenn Enrichment 0 Ergebnisse hatte
    _enrichment_attempted = any(e.get("source") == "enrichment_attempted" for e in _db_ownership_raw)
    # Sentinel-Rows herausfiltern — werden nur als Loop-Guard geschrieben, nie angezeigt
    db_ownership_entries = [e for e in _db_ownership_raw if e.get("source") != "enrichment_attempted"]
    if db_ownership_entries:
        _existing_names_db = {o.name.lower() for o in ownership}
        _added_from_db = 0
        for e in db_ownership_entries:
            entry_name = (e.get("name") or "").strip()
            if not entry_name or entry_name.lower() in _existing_names_db:
                continue
            _existing_names_db.add(entry_name.lower())
            ownership.append(OwnershipItem(
                name=entry_name,
                type=e.get("type") or "Unknown",
                role=e.get("role"),
                notes=None,  # notes-Feld nicht in ownership_entries Schema
            ))
            _added_from_db += 1
        ownership = [o for o in ownership if o.name != "Not publicly disclosed"] or ownership
        if _added_from_db:
            logger.info(
                "EN-08: %d DB-Ownership-Einträge für %s gemerged (source=%s)",
                _added_from_db, company_name,
                db_ownership_entries[0].get("source") if db_ownership_entries else "?",
            )

    # BaFin On-Demand — listed DE Companies ohne bafin_stimmrechte-Einträge.
    # Unabhängig von db_ownership_entries: BaFin-Daten können fehlen auch wenn
    # andere Sources (EDGAR, Yahoo) bereits Einträge geliefert haben.
    _bafin_in_db = any(
        e.get("source") == "bafin_stimmrechte"
        for e in db_ownership_entries
    )
    _is_de_listed = is_listed and (
        any(
            x in (company.get("exchange") or "").lower()
            for x in ("xetra", "frankfurt", "fse", "xfra", "m:access")
        ) or any(
            x in (company.get("headquarters") or "").lower()
            for x in ("germany", "deutschland", "berlin", "munich", "münchen",
                      "hamburg", "frankfurt", "cologne", "köln", "düsseldorf",
                      "stuttgart", "hannover", "dortmund", "essen", "leipzig",
                      "bremen", "dresden", "nuremberg", "nürnberg")
        )
    )
    if company_id and _is_de_listed and not _bafin_in_db:
        async def _bafin_enrich_bg():
            try:
                import os
                from src.services.bafin_ownership import run_bafin_on_demand
                stats = await asyncio.to_thread(
                    run_bafin_on_demand,
                    company_name,
                    os.getenv("SUPABASE_URL", ""),
                    os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY", ""),
                )
                logger.info(
                    "BaFin on-demand '%s' fertig — %d Einträge geschrieben",
                    company_name, stats.get("entries_written", 0),
                )
            except Exception:
                logger.exception("BaFin on-demand FAILED für %s", company_name)

        background_tasks.add_task(_bafin_enrich_bg)
        logger.info("BaFin on-demand queued für %s", company_name)

    # Ownership Enrichment Background Task — feuert nur wenn DB leer (kein Re-Trigger).
    # Refresh via Rolling Refresh Cron (ARCH-02) in einer späteren Session.
    if company_id and not db_ownership_entries and not _enrichment_attempted:
        _existing_for_bg = [
            {"name": o.name, "type": o.type, "role": o.role}
            for o in ownership
            if o.name != "Not publicly disclosed"
        ]
        _db_rounds_for_bg = db_rounds  # aus Schritt 8

        async def _ownership_enrichment_bg():
            try:
                from src.services.ownership_enrichment import enrich_ownership
                result = await enrich_ownership(
                    company_id=company_id,
                    company_name=company_name,
                    company=company,
                    existing_entries=_existing_for_bg,
                    funding_rounds=_db_rounds_for_bg,
                )
                new_entries = result.get("entries", [])
                if new_entries:
                    written = upsert_ownership_entries(company_id, new_entries)
                    logger.info(
                        "EN-08: %d Ownership-Einträge geschrieben für %s "
                        "(source=%s region=%s written=%d)",
                        len(new_entries), company_name,
                        result.get("source_used"), result.get("region"), written,
                    )
                else:
                    # Loop-Guard: Sentinel-Row schreiben damit nächster Request
                    # nicht erneut triggert (db_ownership_entries wäre sonst wieder leer).
                    # source='enrichment_attempted' — filterbar im Frontend.
                    upsert_ownership_entries(company_id, [{
                        "name":   "_no_data",
                        "type":   "sentinel",
                        "source": "enrichment_attempted",
                        "role":   None,
                    }])
                    logger.info(
                        "EN-08: 0 Ownership-Einträge für %s (source=%s) — Sentinel geschrieben",
                        company_name, result.get("source_used"),
                    )
            except Exception:
                logger.exception("Ownership enrichment FAILED für %s", company_name)

        background_tasks.add_task(_ownership_enrichment_bg)
        logger.info(
            "Ownership enrichment queued für %s (is_listed=%s)", company_name, is_listed
        )

    # 9. Scoring — R-23: company-spezifische Käufer (kein Fallback auf globale Seed-Buyers)
    potential_buyers_raw = fetch_potential_buyers(company_id) if company_id else []
    from src.services.buyer_enrichment import is_cache_valid, enrich_buyers_for_company

    if not is_cache_valid(potential_buyers_raw):
        # Noch nicht generiert oder abgelaufen → BackgroundTask, scorings=[]
        if company_id:
            background_tasks.add_task(enrich_buyers_for_company, company, company_id)
            logger.info("Buyer-Enrichment scheduled als BackgroundTask für %s", company_name)
        scorings: list[ScoringDetail] = []
        buyers = []
    else:
        buyers = potential_buyers_raw
        logger.info("Scoring: %d company-spezifische Buyer für %s", len(buyers), company_name)
        scorings = []

        if is_listed:
            auto_tr = 0.5
            tr_confidence = "listed"
        else:
            auto_tr, tr_confidence = compute_auto_tech_readiness(
                stage=company.get("funding_stage"),
                category=company.get("category"),
                funding_total_usd_mn=company.get("funding_total_usd_mn"),
                funding_last_round=company.get("funding_last_round"),
            )
            logger.info("Auto-TR for %s: %.3f (confidence=%s)", company_name, auto_tr, tr_confidence)

        for buyer in buyers:
            mcap = buyer.get("market_cap_usd_bn")
            if not mcap:
                continue
            try:
                req = AnalyzeRequest(
                    company_name=company_name,
                    buyer_name=buyer["name"],
                    tam_usd_bn=tam["tam_usd_bn"],
                    buyer_market_cap_usd_bn=float(mcap),
                    buyer_cash_usd_bn=float(mcap) * 0.05,
                    buyer_debt_ebitda=1.5,
                    target_funding_usd_mn=company.get("funding_total_usd_mn") or 50,
                    target_stage=company.get("funding_stage") or "series_b",
                    tech_readiness_override=auto_tr,
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
                        overall=auto_tr,
                        inputs_provided=tr_confidence == "user",
                        factors=scores.tech_readiness.factor_scores,
                        factor_weights=_TR_WEIGHTS,
                        confidence=tr_confidence,
                    ),
                    deal_success_score=scores.deal_success_score,
                    rating=scores.rating,
                    execution_warning=scores.srr.execution_warning,
                ))
            except Exception as e:
                logger.debug("Scoring failed %s/%s: %s", company_name, buyer["name"], e)

        scorings.sort(key=lambda x: -x.deal_success_score)

        if scorings:
            _tr_ref[0] = scorings[0].tech_readiness.overall

    # 10. Supply chain
    sc_tags = COMPANY_TAGS.get(company_name, enrichment.tags)
    sc = get_supply_chain(sc_tags)

    # 10b. Value Drivers — Background-Enrichment wenn nicht in DB oder >30d alt
    # TTL 30d: verhindert Stale Data (z.B. Fervo 1442 Enablers aus alter Pipeline).
    # Hard Cap im enrich_value_drivers selbst: max 15 Enabler + 15 Contributors.
    if company_id and sc_tags:
        from datetime import timezone as _tz
        vd_cached = fetch_value_drivers(company_id)
        _vd_stale = True
        if vd_cached:
            try:
                _enriched_at = vd_cached.get("enriched_at") or vd_cached.get("created_at")
                if _enriched_at:
                    _vd_age = datetime.now(_tz.utc) - datetime.fromisoformat(
                        _enriched_at.replace("Z", "+00:00")
                    )
                    _vd_stale = _vd_age.days > 30
                    if _vd_stale:
                        logger.info("Value drivers stale (%dd) für %s — re-enriching", _vd_age.days, company_name)
            except Exception:
                _vd_stale = not vd_cached  # Fallback: stale wenn kein Cache

        if _vd_stale:
            async def _value_drivers_bg():
                try:
                    vd_result = await enrich_value_drivers(
                        company_id=company_id,
                        company_name=company_name,
                        category=company.get("category"),
                        tags=sc_tags,
                    )
                    upsert_value_drivers(company_id, vd_result)
                    logger.info("Value drivers enrichment done for %s", company_name)
                except Exception:
                    logger.exception("Value drivers enrichment FAILED for %s", company_name)
            background_tasks.add_task(_value_drivers_bg)
            logger.info("Value drivers enrichment queued for %s", company_name)

    if tam.get("method") == "fallback":
        warnings.append("TAM uses sector median fallback — verify with primary source.")

    ipo_status = company.get("ipo_status")

    # 11. SC-01–SC-13 · Scores on-demand
    # Erst DB-Cache prüfen (computed_at < 24h → verwenden).
    # Sonst: compute_all_scores() + in Background cachen.
    scores_result: dict | None = None
    if company_id:
        try:
            from datetime import datetime, timezone, timedelta
            cached_scores = fetch_company_scores(company_id)
            _cache_fresh = False
            if cached_scores and cached_scores.get("computed_at"):
                _age = datetime.now(timezone.utc) - datetime.fromisoformat(
                    cached_scores["computed_at"].replace("Z", "+00:00")
                )
                _cache_fresh = _age < timedelta(hours=24)

            if _cache_fresh:
                scores_result = cached_scores
                logger.debug("Scores from cache for %s (age %.0fh)", company_name, _age.total_seconds() / 3600)
            else:
                # On-demand berechnen
                signals_raw   = fetch_signals(company_id, limit=50)
                ownership_raw = [
                    {"name": inv.name, "investor_type": inv.type,
                     "share_pct": None, "source": "enrichment"}
                    for inv in enrichment.investors
                ]
                vd_cached_now = fetch_value_drivers(company_id)
                vd_list: list[dict] = []
                if vd_cached_now:
                    for key in ("enablers", "contributors", "buyers"):
                        vd_list.extend(vd_cached_now.get(key) or [])

                buyers_raw = [
                    {"name": b.get("name"), "mfr": b.get("mfr_confidence"),
                     "sector": b.get("sector")}
                    for b in buyers
                ]

                sc_result = compute_all_scores(
                    company={
                        **company,
                        # DQ-04: Beta aus FundamentalsData in company-dict übergeben
                        # SC-04 liest beta_1y + beta_source für Risk-Score
                        "beta_1y":    beta.get("beta_1y"),
                        "beta_source": beta.get("beta_source"),
                    },
                    market_data=market_data_cached or {},
                    signals=signals_raw,
                    ownership_entries=ownership_raw,
                    buyers=buyers_raw,
                    value_drivers=vd_list,
                )
                scores_result = sc_result.to_dict()

                # Async-cachen ohne Response zu blockieren
                _sc_dict_snapshot = scores_result
                async def _cache_scores_bg():
                    try:
                        upsert_company_scores(company_id, _sc_dict_snapshot)
                    except Exception as _e:
                        logger.warning("Score cache write failed for %s: %s", company_name, _e)
                background_tasks.add_task(_cache_scores_bg)
                logger.info(
                    "Scores computed for %s — hero=%s(%.1f) rating=%s conf=%s",
                    company_name,
                    scores_result.get("hero_path"),
                    scores_result.get("hero_score") or 0,
                    scores_result.get("rating"),
                    scores_result.get("confidence"),
                )
        except Exception as _sc_err:
            logger.warning("SC scoring failed for %s: %s", company_name, _sc_err)

    return CompanyDetailResponse(
        name=company_name,
        category=company.get("category"),
        industry=company.get("industry"),
        core_technology=company.get("core_technology"),
        website=company.get("website"),
        founded=founded_display,
        intro=intro,
        description=description_disp,
        wikipedia_url=enrichment.wikipedia_url,
        crunchbase_url=enrichment.crunchbase.url if enrichment.crunchbase else None,
        headquarters=headquarters_disp,
        employee_count=headcount_disp,
        ipo_status=ipo_status,
        ipo_potential=company.get("ipo_potential"),
        ipo_probability_pct=_ipo_probability(ipo_status, company.get("ipo_potential")),
        tam_usd_bn=tam["tam_usd_bn"],
        tam_source=tam.get("source",""),
        tam_confidence=tam.get("confidence","medium"),
        investment_path=_resolve_investment_path(company),
        proxy_ticker=proxy,
        proxy_beta_1y=proxy_beta.get("beta_1y"),
        proxy_beta_benchmark=proxy_beta.get("beta_benchmark"),
        proxy_beta_source=proxy_beta.get("beta_source"),
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
        market_data=market_data_cached,
        technology_tags=enrichment.tags,
        is_known=True,
        warnings=warnings,
        scores=scores_result,
    )


# ── UX-01: Enrichment Status Endpoint ────────────────────────────────────────

@router.get("/company/{name}/status")
async def get_enrichment_status(name: str):
    """
    UX-01: Gibt pro Tab zurück ob Daten vorhanden sind (ready) oder noch ausstehen (pending).
    Wird vom Frontend alle 3s gepolllt bis alle relevanten Tabs ready sind.
    Kein Heavy-Fetch — nur DB-Checks auf Existenz, keine Berechnungen.
    """
    sb = get_supabase()

    # Company laden
    rows = sb.table("companies").select(
        "id, name, enrichment_status, description, industry, category, "
        "founding_year, headquarters, headcount, tags"
    ).ilike("name", name).limit(1).execute()

    if not rows.data:
        raise HTTPException(status_code=404, detail=f"Company '{name}' not found")

    company = rows.data[0]
    company_id = company["id"]

    # Parallel-Checks — alle nur Existenz/Count, kein Heavy-Fetch
    def _has(table: str, col: str = "id", **filters) -> bool:
        try:
            q = sb.table(table).select(col, count="exact").eq("company_id", company_id)
            for k, v in filters.items():
                q = q.eq(k, v)
            r = q.limit(1).execute()
            return (r.count or 0) > 0
        except Exception:
            return False

    def _count(table: str, **filters) -> int:
        try:
            q = sb.table(table).select("id", count="exact").eq("company_id", company_id)
            for k, v in filters.items():
                q = q.eq(k, v)
            r = q.limit(1).execute()
            return r.count or 0
        except Exception:
            return 0

    # Tab-Status berechnen
    has_description  = bool(company.get("description"))
    has_tam          = _has("tam_cache", "tam_2035_usd_bn")
    has_market       = _has("market_data", "sam_usd_bn")
    has_ownership    = _count("ownership_entries") > 0
    has_funding      = _count("funding_rounds") > 0
    has_scores       = _has("company_scores", "composite_score")
    has_assessments  = _has("company_assessments", "dimensions")
    has_peers        = bool(company.get("tags"))  # peers kommen mit company
    has_value_drivers = _has("value_drivers", "enablers")
    has_signals      = _count("signals") > 0
    has_kpi          = _count("kpi_timeseries") > 0

    def _status(ready: bool) -> str:
        return "ready" if ready else "pending"

    tabs = {
        "overview":     _status(has_description and has_tam),
        "market":       _status(has_market),
        "ownership":    _status(has_ownership or has_funding),
        "fundamentals": _status(has_kpi or has_funding),
        "assessments":  _status(has_assessments and has_scores),
        "peers":        _status(has_peers),
        "value_drivers":_status(has_value_drivers),
        "scoring":      _status(has_scores),
        "paths":        _status(has_scores),
        "signals":      _status(has_signals),
    }

    all_ready = all(v == "ready" for v in tabs.values())

    return {
        "company":     name,
        "company_id":  company_id,
        "enrichment_status": company.get("enrichment_status") or "pending",
        "all_ready":   all_ready,
        "tabs":        tabs,
    }
