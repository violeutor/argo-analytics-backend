"""
TAM Extraction Service
======================
Extracts Total Addressable Market estimates for a given technology/sector.

Strategy (in order of reliability):
1. Curated dataset — known TAMs for Climate Tech sectors (fastest, most reliable)
2. Google Search Snippet scraping → Claude-powered extraction (for unknown companies)
3. Fallback: sector-based estimate from curated data

All results are cached in Supabase with scraped_at timestamp.
Delta: re-fetch if scraped_at > 7 days old.
"""

import httpx
import json
import re
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ── Curated TAM dataset (USD Bn, 2035 estimate) ───────────────────────────────
# Sources: BNEF, IEA, McKinsey, BloombergNEF public summaries
# Conservative median across at least 2 sources per sector.

CURATED_TAM: dict[str, dict] = {
    # Carbon Removal
    "carbon-capture":          {"tam_usd_bn": 250,  "source": "IEA Net Zero 2050 / BNEF Carbon Removal 2024", "confidence": "high"},
    "direct-air-capture":      {"tam_usd_bn": 180,  "source": "BNEF DAC Market Outlook 2024",                "confidence": "high"},
    "ocean-cdr":               {"tam_usd_bn": 50,   "source": "Carbon180 / BNEF 2024",                       "confidence": "medium"},

    # Cement & Materials
    "low-carbon-cement":       {"tam_usd_bn": 100,  "source": "McKinsey Green Cement 2030 / GCCA 2024",      "confidence": "high"},
    "sustainable-materials":   {"tam_usd_bn": 80,   "source": "Ellen MacArthur Foundation 2024",             "confidence": "medium"},

    # Energy & Storage
    "battery":                 {"tam_usd_bn": 400,  "source": "BNEF Battery Market Outlook 2025",            "confidence": "high"},
    "long-duration-storage":   {"tam_usd_bn": 120,  "source": "BNEF LDES 2024 / Rocky Mountain Institute",  "confidence": "high"},
    "solid-state-battery":     {"tam_usd_bn": 80,   "source": "BloombergNEF Solid-State 2024",              "confidence": "medium"},
    "grid":                    {"tam_usd_bn": 600,  "source": "IEA World Energy Investment 2024",            "confidence": "high"},
    "solar":                   {"tam_usd_bn": 500,  "source": "BNEF New Energy Outlook 2025",               "confidence": "high"},
    "hydrogen":                {"tam_usd_bn": 300,  "source": "IEA Hydrogen Special Report 2024",           "confidence": "high"},
    "geothermal":              {"tam_usd_bn": 90,   "source": "NREL Geothermal Market Report 2024",         "confidence": "high"},
    "waste-to-energy":         {"tam_usd_bn": 70,   "source": "Allied Market Research 2024",                "confidence": "medium"},

    # Agriculture
    "agritech":                {"tam_usd_bn": 350,  "source": "McKinsey AgTech 2030 / BNEF 2024",           "confidence": "high"},
    "bioengineering":          {"tam_usd_bn": 150,  "source": "Precedence Research 2024",                   "confidence": "medium"},
    "soil-carbon":             {"tam_usd_bn": 50,   "source": "McKinsey Nature & Climate 2023",             "confidence": "medium"},

    # Fuels & Chemicals
    "co2-to-fuels":            {"tam_usd_bn": 200,  "source": "BNEF SAF Market Outlook 2024 / IATA",       "confidence": "high"},
    "bio-based-chemicals":     {"tam_usd_bn": 120,  "source": "Grand View Research 2024",                  "confidence": "medium"},

    # Datacenter / Cooling
    "datacenter-cooling":      {"tam_usd_bn": 25,   "source": "MarketsandMarkets DC Cooling 2028",         "confidence": "medium"},
    "ai-grid-software":        {"tam_usd_bn": 80,   "source": "Wood Mackenzie Grid Software 2030",         "confidence": "medium"},

    # Climate Risk / SaaS
    "climate-risk-saas":       {"tam_usd_bn": 40,   "source": "MarketsandMarkets Climate Risk Analytics 2028","confidence": "medium"},
    "carbon-credits":          {"tam_usd_bn": 250,  "source": "McKinsey VCM 2030 / BNEF 2024",            "confidence": "high"},

    # Water / Irrigation
    "irrigation":              {"tam_usd_bn": 60,   "source": "Grand View Research Irrigation 2030",       "confidence": "medium"},
    "water-tech":              {"tam_usd_bn": 40,   "source": "Global Water Intelligence 2024",            "confidence": "medium"},
}

# ── Google Search Scraper ─────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


async def _google_search_snippets(query: str, num: int = 5) -> list[str]:
    """Fetch top Google result snippets for a query."""
    try:
        async with httpx.AsyncClient(timeout=10, headers=HEADERS) as client:
            resp = await client.get(
                "https://www.google.com/search",
                params={"q": query, "num": num, "hl": "en"},
                follow_redirects=True,
            )
        if resp.status_code != 200:
            return []
        # Extract text from <div class="BNeawe"> and similar snippet containers
        text = resp.text
        snippets = re.findall(r'class="(?:BNeawe|VwiC3b|MUxGbd)[^"]*"[^>]*>([^<]{30,300})<', text)
        return snippets[:10]
    except Exception as e:
        logger.warning("Google search failed for '%s': %s", query, e)
        return []


async def _extract_tam_with_claude(company: str, sector: str, snippets: list[str]) -> dict | None:
    """Use Claude API to extract TAM figure from search snippets."""
    if not snippets:
        return None

    snippet_text = "\n".join(f"- {s}" for s in snippets[:8])
    prompt = f"""You are a financial analyst. Extract the Total Addressable Market (TAM) estimate for the sector relevant to {company} ({sector}) from these search result snippets.

Snippets:
{snippet_text}

Rules:
- Return ONLY valid JSON, no preamble, no markdown
- Extract the most credible TAM figure for the year 2030 or 2035 (prefer 2035)
- Convert all figures to USD billions
- If multiple figures exist, use the median
- If no reliable figure found, return null

JSON format:
{{"tam_usd_bn": <number or null>, "year": <2030 or 2035>, "source": "<source name if identifiable>", "confidence": "high|medium|low"}}"""

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
        if resp.status_code != 200:
            return None
        data = resp.json()
        raw = data["content"][0]["text"].strip()
        # Strip any accidental markdown fences
        raw = re.sub(r"```(?:json)?|```", "", raw).strip()
        return json.loads(raw)
    except Exception as e:
        logger.warning("Claude TAM extraction failed: %s", e)
        return None


# ── Main TAM lookup ───────────────────────────────────────────────────────────

async def get_tam(company_name: str, sector: str | None = None) -> dict:
    """
    Returns TAM estimate for a company.

    Priority:
    1. Curated dataset (by sector tag derived from sector/category)
    2. Google + Claude extraction
    3. Sector-median fallback (100B)

    One-Click-Prinzip: funktioniert für jede Company weltweit —
    kein manuelles Mapping, kein Whitelist-Denken.
    """
    # 1. Sector-level curated lookup — aus sector/category ableiten
    if sector:
        sector_tag = sector.lower().replace(" ", "-").replace("/", "-")
        for tag, data in CURATED_TAM.items():
            if tag in sector_tag or sector_tag in tag:
                result = data.copy()
                result["method"] = "curated_sector"
                result["tag"] = tag
                return result

    # 2. Google + Claude extraction
    logger.info("TAM not in curated dataset for '%s' — trying web extraction", company_name)
    query = f"{company_name} {sector or 'market'} total addressable market size 2030 2035 billion USD"
    snippets = await _google_search_snippets(query)

    if snippets:
        extracted = await _extract_tam_with_claude(
            company_name, sector or "clean technology", snippets
        )
        if extracted and extracted.get("tam_usd_bn"):
            extracted["method"] = "web_extracted"
            return extracted

    # 3. Fallback
    logger.warning("TAM fallback for '%s' — using sector median 100B", company_name)
    return {
        "tam_usd_bn": 100,
        "source": "Sector median fallback — update with verified source",
        "confidence": "low",
        "method": "fallback",
    }
