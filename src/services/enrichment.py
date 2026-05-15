"""
Company Enrichment Pipeline
============================
Search-time enrichment with progressive caching.

Flow:
  1. Check Supabase — is company known? Is data fresh (< 7 days)?
  2. YES fresh  → return cached data immediately
  3. YES stale  → return cached, trigger background refresh
  4. NO         → scrape now → persist → return enriched data

Sources:
  - Crunchbase public (funding, stage, investors)
  - Company website (description, tech tags)
  - Wikipedia API (founding year, description)
  - Yahoo Finance (if ticker known)
  - TAM service (curated + Google + Claude)
  - Supply Chain mapping (tag-based)
"""

import httpx
import re
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

CACHE_TTL_DAYS = 7


# ── Wikipedia ─────────────────────────────────────────────────────────────────

async def _fetch_wikipedia(company: str) -> dict:
    """Get company summary from Wikipedia API."""
    try:
        async with httpx.AsyncClient(timeout=8, headers=HEADERS) as client:
            resp = await client.get(
                "https://en.wikipedia.org/api/rest_v1/page/summary/" +
                company.replace(" ", "_"),
            )
        if resp.status_code == 200:
            data = resp.json()
            return {
                "description": data.get("extract", "")[:500],
                "wikipedia_url": data.get("content_urls", {}).get("desktop", {}).get("page"),
            }
    except Exception as e:
        logger.debug("Wikipedia lookup failed for '%s': %s", company, e)
    return {}


# ── Crunchbase (public HTML) ──────────────────────────────────────────────────

async def _fetch_crunchbase(company: str) -> dict:
    """
    Scrape basic public Crunchbase data.
    Returns funding stage + last round + investor hints from meta tags.
    """
    slug = company.lower().replace(" ", "-").replace(".", "")
    url = f"https://www.crunchbase.com/organization/{slug}"
    try:
        async with httpx.AsyncClient(timeout=10, headers=HEADERS, follow_redirects=True) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            return {}
        html = resp.text

        # Extract from meta description (Crunchbase always has this)
        meta = re.search(r'<meta name="description" content="([^"]{20,400})"', html)
        description = meta.group(1) if meta else ""

        # Try to extract funding figures from description
        funding_match = re.search(r"\$([0-9,.]+)\s*(M|B|million|billion)", description, re.I)
        funding_mn = None
        if funding_match:
            val = float(funding_match.group(1).replace(",", ""))
            unit = funding_match.group(2).upper()
            funding_mn = val * 1000 if unit in ("B", "BILLION") else val

        # Stage hints
        stage = None
        for s in ["Series D", "Series C", "Series B", "Series A", "Seed", "IPO", "Public"]:
            if s.lower() in description.lower():
                stage = s
                break

        return {
            "crunchbase_url": url,
            "crunchbase_description": description[:400],
            "funding_hint_mn": funding_mn,
            "stage_hint": stage,
        }
    except Exception as e:
        logger.debug("Crunchbase scrape failed for '%s': %s", company, e)
    return {}


# ── Company website ───────────────────────────────────────────────────────────

async def _fetch_website_description(website: str | None) -> dict:
    """Extract meta description and title from company website."""
    if not website:
        return {}
    url = website if website.startswith("http") else f"https://{website}"
    try:
        async with httpx.AsyncClient(timeout=8, headers=HEADERS, follow_redirects=True) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            return {}
        html = resp.text[:5000]  # only need head section

        title = re.search(r"<title[^>]*>([^<]{5,120})</title>", html, re.I)
        desc  = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{20,400})["\']', html, re.I)

        return {
            "website_title": title.group(1).strip() if title else None,
            "website_description": desc.group(1).strip() if desc else None,
        }
    except Exception as e:
        logger.debug("Website fetch failed for '%s': %s", website, e)
    return {}


# ── Tag inference from description ────────────────────────────────────────────

TAG_KEYWORDS: dict[str, list[str]] = {
    "carbon-capture":       ["co2 capture", "carbon capture", "ccs", "dac", "direct air"],
    "low-carbon-cement":    ["cement", "concrete", "clinker", "calcite"],
    "battery":              ["battery", "lithium", "cell", "bess", "energy storage"],
    "long-duration-storage":["iron air", "long duration", "ldes", "multi-day storage"],
    "solid-state-battery":  ["solid state", "solid-state", "solid electrolyte"],
    "grid":                 ["grid", "microgrid", "transmission", "distribution", "utility"],
    "solar":                ["solar", "photovoltaic", "pv panel"],
    "hydrogen":             ["hydrogen", "electrolyzer", "fuel cell", "h2"],
    "geothermal":           ["geothermal", "enhanced geothermal", "egs", "hot rock"],
    "agritech":             ["agriculture", "crop", "farm", "irrigation", "precision ag"],
    "bioengineering":       ["crispr", "gene edit", "genomic", "synthetic biology"],
    "co2-to-fuels":         ["saf", "sustainable aviation", "e-fuel", "syngas", "co2 to fuel"],
    "datacenter-cooling":   ["cooling", "hvac", "data center", "datacenter", "thermal"],
    "waste-to-energy":      ["waste", "biogas", "landfill", "anaerobic"],
    "climate-risk-saas":    ["climate risk", "climate analytics", "esg platform", "adaptation"],
    "carbon-credits":       ["carbon credit", "carbon market", "offset", "voluntary carbon"],
    "soil-carbon":          ["soil carbon", "microbial", "soil sequestration"],
    "irrigation":           ["irrigation", "drip", "water management"],
    "sustainable-materials":["packaging", "bioplastic", "algae", "biodegradable"],
}


def _infer_tags(text: str) -> list[str]:
    text_lower = text.lower()
    return [
        tag for tag, keywords in TAG_KEYWORDS.items()
        if any(kw in text_lower for kw in keywords)
    ]


# ── Master enrichment function ────────────────────────────────────────────────

async def enrich_company(
    company_name: str,
    website: str | None = None,
    existing_tags: list[str] | None = None,
    existing_category: str | None = None,
) -> dict:
    """
    Full enrichment for a company. Returns enriched data dict.
    Caller is responsible for persisting to Supabase.

    Returns:
      {
        name, description, wikipedia_url, crunchbase_url,
        funding_hint_mn, stage_hint,
        tags, tam, supply_chain,
        enriched_at
      }
    """
    logger.info("Enriching company: %s", company_name)

    # Run non-TAM scrapes (can run concurrently in Phase 2 with asyncio.gather)
    wiki    = await _fetch_wikipedia(company_name)
    cb      = await _fetch_crunchbase(company_name)
    web     = await _fetch_website_description(website)

    # Build description from best available source
    description = (
        wiki.get("description")
        or cb.get("crunchbase_description")
        or web.get("website_description")
        or ""
    )

    # Infer tags from description + existing
    inferred_tags = _infer_tags(description)
    all_tags = list(set((existing_tags or []) + inferred_tags))

    # TAM lookup (uses curated dataset first, then web extraction)
    from src.services.tam import get_tam, COMPANY_PRIMARY_TAG
    tam_data = await get_tam(company_name, existing_category)

    # Supply chain
    from src.services.supply_chain import get_supply_chain, COMPANY_TAGS
    tags_for_sc = COMPANY_TAGS.get(company_name, all_tags)
    supply_chain = get_supply_chain(tags_for_sc)

    return {
        "name": company_name,
        "description": description[:500] or None,
        "wikipedia_url": wiki.get("wikipedia_url"),
        "crunchbase_url": cb.get("crunchbase_url"),
        "funding_hint_mn": cb.get("funding_hint_mn"),
        "stage_hint": cb.get("stage_hint"),
        "tags": all_tags,
        "tam": tam_data,
        "supply_chain": supply_chain,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }
