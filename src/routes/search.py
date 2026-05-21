"""
POST /api/v1/search

Full company search with enrichment:
  1. Check Supabase for known company
  2. If unknown → enrich (scrape + TAM + supply chain)
  3. Run scoring for all known buyers
  4. Return complete SearchResult
"""

import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from src.integrations.supabase import (
    get_supabase,
    fetch_companies,
    fetch_buyers,
    fetch_company_by_name,
)
from src.services.enrichment import enrich_company
from src.services.supply_chain import get_supply_chain, COMPANY_TAGS
from src.services.tam import get_tam
from src.pipelines.scoring import compute_scores
from src.models.schemas import AnalyzeRequest

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["search"])


class SearchRequest(BaseModel):
    query: str                  # company name or ticker


class SupplyChainItem(BaseModel):
    ticker: str
    name: str
    exchange: str | None = None
    role: str
    relevance: float


class SupplyChainResult(BaseModel):
    upstream: list[SupplyChainItem]
    downstream: list[SupplyChainItem]
    etfs: list[dict]


class TAMResult(BaseModel):
    tam_usd_bn: float
    source: str
    confidence: str
    method: str


class BuyerScore(BaseModel):
    buyer_name: str
    ticker: str | None
    exchange: str | None
    market_cap_usd_bn: float | None
    srr_value: float
    srr_category: str
    mfr_value: float
    mfr_signal: str
    tech_readiness: float
    deal_success_score: float
    rating: str
    execution_warning: bool


class SearchResponse(BaseModel):
    # Company data
    company_name: str
    category: str | None
    potential: str | None
    risk: str | None
    ipo_potential: str | None
    investment_path: str | None
    proxy_ticker: str | None
    funding_total_usd_mn: float | None
    funding_last_round: str | None
    last_signal: str | None
    last_signal_date: str | None
    source: str | None
    description: str | None

    # Enrichment
    tam: TAMResult
    supply_chain: SupplyChainResult
    buyer_scores: list[BuyerScore]

    # Flags
    is_known: bool          # True = found in Supabase seed data
    enriched: bool          # True = freshly enriched via scraping
    warnings: list[str]


@router.post("/search", response_model=SearchResponse)
async def search_company(request: SearchRequest) -> SearchResponse:
    """
    Full company intelligence lookup.
    Known companies return immediately from DB + TAM curated data.
    Unknown companies are enriched on first search and cached.
    """
    query = request.query.strip()
    warnings: list[str] = []
    enriched = False

    # ── 1. Look up in Supabase ─────────────────────────────────────────────
    company = None
    try:
        company = await _fetch_company_fuzzy(query)
    except Exception as e:
        warnings.append(f"DB lookup failed: {e}")

    is_known = company is not None

    # ── 2. Enrich if unknown ───────────────────────────────────────────────
    if not is_known:
        logger.info("Unknown company '%s' — running enrichment pipeline", query)
        try:
            enrichment = await enrich_company(query)
            # Persist to Supabase as manual entry
            company = await _persist_enriched_company(query, enrichment)
            enriched = True
        except Exception as e:
            logger.warning("Enrichment failed for '%s': %s", query, e)
            warnings.append(f"Enrichment partially failed: {e}. Showing available data.")
            # Minimal fallback company dict
            company = {
                "name": query, "category": None, "potential": None,
                "risk": None, "ipo_potential": None, "investment_path": "Beobachten",
                "proxy_ticker": None, "funding_total_usd_mn": None,
                "funding_last_round": None, "last_signal": None,
                "last_signal_date": None, "source": "manual",
                "tags": [],
            }

    company_name = company.get("name", query)

    # ── 3. TAM ────────────────────────────────────────────────────────────
    tam_data = {"tam_usd_bn": 100, "source": "fallback", "confidence": "low", "method": "fallback"}
    try:
        tam_data = await get_tam(company_name, company.get("category"))
    except Exception as e:
        warnings.append(f"TAM lookup failed: {e}")

    # ── 4. Supply Chain ───────────────────────────────────────────────────
    sc_tags = COMPANY_TAGS.get(company_name, company.get("tags") or [])
    supply_chain_raw = get_supply_chain(sc_tags)

    if not sc_tags:
        warnings.append("Supply chain mapping: no tags found — showing empty. Will auto-populate on next enrichment.")

    # ── 5. Buyer scoring ──────────────────────────────────────────────────
    buyer_scores: list[BuyerScore] = []
    try:
        buyers = fetch_buyers(limit=50)

        # Only score buyers that are likely relevant
        relevant_buyers = _filter_relevant_buyers(buyers, company)

        for buyer in relevant_buyers:
            if not buyer.get("market_cap_usd_bn"):
                continue
            try:
                req = AnalyzeRequest(
                    company_name=company_name,
                    buyer_name=buyer["name"],
                    tam_usd_bn=tam_data["tam_usd_bn"],
                    buyer_market_cap_usd_bn=buyer["market_cap_usd_bn"],
                    buyer_cash_usd_bn=buyer.get("cash_usd_bn") or buyer["market_cap_usd_bn"] * 0.05,
                    buyer_debt_ebitda=buyer.get("debt_ebitda") or 1.5,
                    target_funding_usd_mn=company.get("funding_total_usd_mn") or 50,
                    target_stage=_map_stage(company.get("funding_stage") or "series_b"),
                )
                scores = compute_scores(req)
                buyer_scores.append(BuyerScore(
                    buyer_name=buyer["name"],
                    ticker=buyer.get("ticker"),
                    exchange=buyer.get("exchange"),
                    market_cap_usd_bn=buyer.get("market_cap_usd_bn"),
                    srr_value=scores.srr.value,
                    srr_category=scores.srr.category,
                    mfr_value=scores.mfr.value,
                    mfr_signal=scores.mfr.signal,
                    tech_readiness=scores.tech_readiness.value,
                    deal_success_score=scores.deal_success_score,
                    rating=scores.rating,
                    execution_warning=scores.srr.execution_warning,
                ))
            except Exception as e:
                logger.debug("Scoring failed for buyer '%s': %s", buyer["name"], e)

        # Sort by deal_success_score desc
        buyer_scores.sort(key=lambda x: -x.deal_success_score)

        if not buyer_scores:
            warnings.append("No buyer scores computed — buyers table may be empty.")

    except Exception as e:
        warnings.append(f"Buyer scoring failed: {e}")

    if tam_data.get("method") == "fallback":
        warnings.append(
            "TAM verwendet Standardwert (100B). "
            "Für präzisere Scores TAM-Quelle manuell ergänzen."
        )

    return SearchResponse(
        company_name=company_name,
        category=company.get("category"),
        potential=company.get("potential"),
        risk=company.get("risk"),
        ipo_potential=company.get("ipo_potential"),
        investment_path=company.get("investment_path"),
        proxy_ticker=company.get("proxy_ticker"),
        funding_total_usd_mn=company.get("funding_total_usd_mn"),
        funding_last_round=company.get("funding_last_round"),
        last_signal=company.get("last_signal"),
        last_signal_date=str(company.get("last_signal_date")) if company.get("last_signal_date") else None,
        source=company.get("source"),
        description=company.get("description") or company.get("summary"),
        tam=TAMResult(**tam_data),
        supply_chain=SupplyChainResult(
            upstream=[SupplyChainItem(**i) for i in supply_chain_raw.get("upstream", [])],
            downstream=[SupplyChainItem(**i) for i in supply_chain_raw.get("downstream", [])],
            etfs=supply_chain_raw.get("etfs", []),
        ),
        buyer_scores=buyer_scores,
        is_known=is_known,
        enriched=enriched,
        warnings=warnings,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _fetch_company_fuzzy(query: str) -> dict | None:
    """
    BUG-12: Case-insensitive fuzzy match — gibt immer DB-canonical-Namen zurück.
    'spacex' findet 'SpaceX' und gibt {'name': 'SpaceX', ...} zurück.
    Matching-Reihenfolge: exact → prefix → substring → ticker.
    """
    companies = fetch_companies(limit=500)
    q = query.lower().strip()

    # 1. Exact match (case-insensitive)
    for c in companies:
        if c.get("name", "").lower() == q:
            return c

    # 2. Ticker match
    for c in companies:
        if c.get("ticker") and q == c.get("ticker", "").lower().split("·")[0].strip():
            return c
        if c.get("proxy_ticker") and q in c.get("proxy_ticker", "").lower():
            return c

    # 3. Substring match (query als Teil des DB-Namens)
    for c in companies:
        if q in c.get("name", "").lower():
            return c

    return None


async def _persist_enriched_company(name: str, enrichment: dict) -> dict:
    """
    BUG-12: Speichert enriched Company mit canonical name aus Wikipedia/Enrichment.
    Bevorzugt enrichment['canonical_name'] > enrichment['name'] > User-Input.
    """
    try:
        db = get_supabase()
        # Canonical name: Wikipedia-Name hat Priorität über User-Input (verhindert lowercase-Einträge)
        canonical_name = (
            enrichment.get("canonical_name")
            or enrichment.get("name")
            or name
        )
        payload = {
            "name":            canonical_name,
            "summary":         enrichment.get("description"),
            "tags":            enrichment.get("tags", []),
            "source":          "manual",
            "investment_path": "Beobachten",
        }
        if enrichment.get("funding_hint_mn"):
            payload["funding_total_usd_mn"] = enrichment["funding_hint_mn"]

        result = db.table("companies").insert(payload).execute()
        persisted = result.data[0] if result.data else {}
        return {**payload, **persisted, "description": enrichment.get("description")}
    except Exception as e:
        logger.warning("Could not persist enriched company: %s", e)
        return {"name": name, "description": enrichment.get("description"), "tags": enrichment.get("tags", [])}

def _filter_relevant_buyers(buyers: list[dict], company: dict) -> list[dict]:
    """
    BUG-13: Gibt nur Buyers zurück die zur Company passen.
    Matching nach industry + investment_path + region.
    Fallback: alle Buyers wenn zu wenige Treffer (< 2).
    """
    industry   = (company.get("industry") or "").lower()
    category   = (company.get("category") or "").lower()
    inv_path   = (company.get("investment_path") or "").lower()
    region     = (company.get("region") or "").lower()

    # Sektor-Mapping: Buyer-Sektor → Company-Industrien/Kategorien die passen
    _SECTOR_FIT: dict[str, list[str]] = {
        "energy":          ["energy", "solar", "wind", "hydrogen", "battery", "grid", "geothermal", "nuclear"],
        "industrials":     ["manufacturing", "construction", "materials", "cement", "steel", "chemical"],
        "technology":      ["software", "ai", "saas", "semiconductor", "cloud", "iot", "robotics"],
        "agriculture":     ["agritech", "food", "agriculture", "biotech"],
        "transportation":  ["mobility", "evs", "logistics", "aviation", "maritime"],
        "materials":       ["mining", "materials", "recycling", "carbon capture"],
        "healthcare":      ["medtech", "biotech", "pharma", "health"],
        "finance":         ["fintech", "insurance", "payments"],
    }

    def _buyer_fits(buyer: dict) -> bool:
        buyer_sector = (buyer.get("sector") or "").lower()
        fit_industries = _SECTOR_FIT.get(buyer_sector, [])

        # Direkte Sektor-Übereinstimmung
        if any(ind in industry or ind in category for ind in fit_industries):
            return True

        # Regional-Fit: EU-Buyer bevorzugt EU-Companies, US-Buyer bevorzugt US
        buyer_region = (buyer.get("region") or "").lower()
        if buyer_region and region and buyer_region == region:
            return True

        # Investment-Path-Fit: Käufer-Proxy Companies passen zu strategischen Buyern
        if inv_path in ("käufer-proxy", "kaufer-proxy") and buyer.get("market_cap_usd_bn", 0) > 5:
            return True

        return False

    relevant = [b for b in buyers if _buyer_fits(b)]

    # Fallback: wenn < 2 Treffer → alle Buyers (kleines Universe noch)
    if len(relevant) < 2:
        logger.info(
            "_filter_relevant_buyers: nur %d Treffer für %s → alle %d Buyers",
            len(relevant), company.get("name"), len(buyers),
        )
        return buyers

    logger.info(
        "_filter_relevant_buyers: %d/%d Buyers relevant für %s (industry=%s)",
        len(relevant), len(buyers), company.get("name"), industry,
    )
    return relevant


async def _ensure_buyer_in_db(buyer_name: str, ticker: str | None = None) -> dict | None:
    """
    BUG-13: Legt Buyer an wenn noch nicht in DB — analog One-Click für Companies.
    Minimaler Eintrag mit name + ticker; Enrichment folgt via Background-Pipeline.
    """
    db = get_supabase()
    try:
        # Erst prüfen ob schon vorhanden (ilike)
        existing = db.table("buyers").select("*").ilike("name", buyer_name).limit(1).execute()
        if existing.data:
            return existing.data[0]

        # Neu anlegen
        payload: dict = {"name": buyer_name, "sector": "unknown"}
        if ticker:
            payload["ticker"] = ticker

        result = db.table("buyers").insert(payload).execute()
        logger.info("BUG-13: Neuer Buyer angelegt: %s", buyer_name)
        return result.data[0] if result.data else None
    except Exception as e:
        logger.warning("_ensure_buyer_in_db failed for %s: %s", buyer_name, e)
        return None


def _map_stage(stage: str) -> str:
    mapping = {
        "seed": "seed",
        "series_a": "series_a",
        "series_b": "series_b",
        "series_c": "series_c",
        "series_d_plus": "series_d_plus",
        "public": "public",
        # Handle raw strings from DB
        "Series A": "series_a",
        "Series B": "series_b",
        "Series C": "series_c",
        "Series D": "series_d_plus",
        "Series D+": "series_d_plus",
    }
    return mapping.get(stage, "series_b")
