"""
GET /api/v1/company/{name}  —  v2.6

Changes vs v2.5:
  - Market Data Enrichment Trigger (MD-B07): nach TAM-Block
  - fetch_market_data() Cache-Check — Background-Task wenn leer
  - set_enrichment_status() — pending → running → done/error
  - enrich_market_data() non-blocking via FastAPI BackgroundTasks
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
    fetch_funding_rounds,
    fetch_all_funding_rounds,
    upsert_company_enrichment,
    upsert_tam_cache,
    fetch_tam_cache,
    fetch_market_data,
    upsert_market_data,
    set_enrichment_status,
)
from src.services.supply_chain import get_supply_chain, COMPANY_TAGS
from src.services.tam import get_tam
from src.services.market_data_enrichment import (
    enrich_market_data,
    enrich_market_data_sync_wrapper,
)
from src.pipelines.scoring import compute_scores
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
    # Market Data (gecacht, optional)
    market_data: dict | None = None
    # Meta
    technology_tags: list[str]
    is_known: bool
    warnings: list[str]


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

    # 1b. One-Click: unbekannte Company anlegen + anreichern
    if not company:
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
    # Leere Row (nur company_id, alle Felder NULL) gilt nicht als befüllt
    _market_data_valid = bool(
        market_data_cached
        and market_data_cached.get("enriched_at")
        and market_data_cached.get("sam_usd_bn")  # SAM wird immer berechnet — zuverlässigstes Vollständigkeits-Signal
    )
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
                )
                upsert_market_data(company_id, {**async_result, **sync_result})
                set_enrichment_status(company_id, "done")
                logger.info("Market enrichment done for %s", company_name)
            except Exception as e:
                set_enrichment_status(company_id, "error")
                logger.exception("Market enrichment TRACEBACK for %s", company_name)

        background_tasks.add_task(_market_enrichment_bg)
        logger.info("Market enrichment queued (BackgroundTasks) for %s", company_name)

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

    enrichment, yahoo, intro = await asyncio.gather(
        _safe_enrichment(),
        _fetch_yahoo(proxy if is_listed else None),
        _safe_intro(),
    )

    # 4b. Enrichment-Ergebnisse in DB persistieren (nur wenn Werte vorhanden)
    #     DB-Werte als Fallback wenn Enrichment leer (z.B. bei Timeout)
    if company_id:
        upsert_payload = {
            "founding_year": _parse_year(enrichment.founded_year),
            "headquarters":  enrichment.headquarters or None,
            "headcount":     _parse_headcount(enrichment.employee_count),
            "description":   enrichment.description or None,
            "website":       enrichment.website or None,
        }
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
    founded_display   = enrichment.founded_year   or (str(company.get("founding_year")) if company.get("founding_year") else None)
    headquarters_disp = enrichment.headquarters   or company.get("headquarters")
    headcount_disp    = enrichment.employee_count or (str(company.get("headcount")) if company.get("headcount") else None)
    description_disp  = enrichment.description    or company.get("description")

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

    # tech_readiness für Market-Enrichment nachreichen (bester Scoring-Wert)
    # Background-Task läuft nach Response-Aufbau → _tr_ref[0] ist dann gesetzt
    if scorings:
        _tr_ref[0] = scorings[0].tech_readiness.overall
        logger.debug("_tr_ref set to %.2f for %s", _tr_ref[0], company_name)

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
    )
