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
from src.config import settings

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

# ── Curated CAGR dataset (%, 2024–2035) ──────────────────────────────────────
# Konservative Mediane aus BNEF, IEA, McKinsey — gleiche Quellen wie TAM.
# Ergänzt CURATED_TAM: wenn TAM aus Curated-Dataset kommt, CAGR direkt ablesen.

CURATED_CAGR: dict[str, dict] = {
    "carbon-capture":          {"cagr_pct": 28.0, "confidence": "high",   "source": "IEA Net Zero 2050 · BNEF Carbon Removal 2024"},
    "direct-air-capture":      {"cagr_pct": 35.0, "confidence": "high",   "source": "BNEF DAC Market Outlook 2024"},
    "ocean-cdr":               {"cagr_pct": 20.0, "confidence": "medium", "source": "Carbon180 · BNEF 2024"},
    "low-carbon-cement":       {"cagr_pct": 12.0, "confidence": "high",   "source": "McKinsey Green Cement 2030 · GCCA 2024"},
    "sustainable-materials":   {"cagr_pct": 10.0, "confidence": "medium", "source": "Ellen MacArthur Foundation 2024"},
    "battery":                 {"cagr_pct": 18.0, "confidence": "high",   "source": "BNEF Battery Market Outlook 2025"},
    "long-duration-storage":   {"cagr_pct": 32.0, "confidence": "high",   "source": "BNEF LDES 2024 · Rocky Mountain Institute"},
    "solid-state-battery":     {"cagr_pct": 38.0, "confidence": "medium", "source": "BloombergNEF Solid-State 2024"},
    "grid":                    {"cagr_pct": 9.0,  "confidence": "high",   "source": "IEA World Energy Investment 2024"},
    "solar":                   {"cagr_pct": 14.0, "confidence": "high",   "source": "BNEF New Energy Outlook 2025"},
    "hydrogen":                {"cagr_pct": 22.0, "confidence": "high",   "source": "IEA Hydrogen Special Report 2024"},
    "geothermal":              {"cagr_pct": 11.0, "confidence": "high",   "source": "NREL Geothermal Market Report 2024"},
    "waste-to-energy":         {"cagr_pct": 7.0,  "confidence": "medium", "source": "Allied Market Research 2024"},
    "agritech":                {"cagr_pct": 13.0, "confidence": "high",   "source": "McKinsey AgTech 2030 · BNEF 2024"},
    "bioengineering":          {"cagr_pct": 16.0, "confidence": "medium", "source": "Precedence Research 2024"},
    "soil-carbon":             {"cagr_pct": 24.0, "confidence": "medium", "source": "McKinsey Nature & Climate 2023"},
    "co2-to-fuels":            {"cagr_pct": 26.0, "confidence": "high",   "source": "BNEF SAF Market Outlook 2024 · IATA"},
    "bio-based-chemicals":     {"cagr_pct": 11.0, "confidence": "medium", "source": "Grand View Research 2024"},
    "datacenter-cooling":      {"cagr_pct": 15.0, "confidence": "medium", "source": "MarketsandMarkets DC Cooling 2028"},
    "ai-grid-software":        {"cagr_pct": 19.0, "confidence": "medium", "source": "Wood Mackenzie Grid Software 2030"},
    "climate-risk-saas":       {"cagr_pct": 21.0, "confidence": "medium", "source": "MarketsandMarkets Climate Risk Analytics 2028"},
    "carbon-credits":          {"cagr_pct": 29.0, "confidence": "high",   "source": "McKinsey VCM 2030 · BNEF 2024"},
    "irrigation":              {"cagr_pct": 8.0,  "confidence": "medium", "source": "Grand View Research Irrigation 2030"},
    "water-tech":              {"cagr_pct": 7.5,  "confidence": "medium", "source": "Global Water Intelligence 2024"},
}

# Marktgröße heute (2024, USD Bn) — für mathematische CAGR-Hochrechnung als Fallback
# CAGR = (TAM_2035 / BASE_2024) ^ (1/11) - 1
MARKET_BASE_2024: dict[str, float] = {
    "battery":           120.0,
    "solar":             200.0,
    "grid":              250.0,
    "hydrogen":           50.0,
    "carbon-capture":      5.0,
    "direct-air-capture":  0.5,
    "agritech":          100.0,
    "co2-to-fuels":       15.0,
    "geothermal":         35.0,
    "carbon-credits":     20.0,
}


# ── Branchen-Fallback-CAGR (Cross-Industry, außerhalb Climate Tech) ───────────
# CURATED_CAGR oben deckt nur Climate-Tech-Sektoren ab. Für jede andere Branche
# (Pharma, Software, Industrials etc.) fehlte bisher jeder Match → Code fiel auf
# den 12%-Climate-Median. Das ist für einen Cross-Industry-Screener falsch und
# unglaubwürdig (Pharma wächst ~6%, nicht 12% wie Climate Tech).
#
# Diese Tabelle liefert grobe, konservative Branchen-Mediane für die häufigsten
# Nicht-Climate-Sektoren. Match via lowercase-Substring (sektor- ODER category-Name).
# Bewusst grob — ein ehrlicher Branchen-Median schlägt einen falschen Climate-Wert.
# Confidence immer "low": es ist eine Branchen-Heuristik, kein recherchierter Wert.
# Quellen: aggregierte Branchen-Wachstumsraten (öffentliche Marktberichte, grobe Mediane).
_SECTOR_FALLBACK_CAGR: dict[str, dict] = {
    # Healthcare / Life Sciences
    "pharma":         {"cagr_pct": 6.0,  "source": "Branchen-Median Pharma"},
    "biotech":        {"cagr_pct": 11.0, "source": "Branchen-Median Biotech"},
    "medtech":        {"cagr_pct": 7.0,  "source": "Branchen-Median Medizintechnik"},
    "medical":        {"cagr_pct": 7.0,  "source": "Branchen-Median Medizintechnik"},
    "healthcare":     {"cagr_pct": 7.0,  "source": "Branchen-Median Healthcare"},
    "health":         {"cagr_pct": 7.0,  "source": "Branchen-Median Healthcare"},
    "diagnostics":    {"cagr_pct": 8.0,  "source": "Branchen-Median Diagnostik"},
    # Tech / Software
    "software":       {"cagr_pct": 14.0, "source": "Branchen-Median Software"},
    "saas":           {"cagr_pct": 16.0, "source": "Branchen-Median SaaS"},
    "ai":             {"cagr_pct": 25.0, "source": "Branchen-Median KI/ML"},
    "artificial-intelligence": {"cagr_pct": 25.0, "source": "Branchen-Median KI/ML"},
    "cybersecurity":  {"cagr_pct": 13.0, "source": "Branchen-Median Cybersecurity"},
    "security":       {"cagr_pct": 13.0, "source": "Branchen-Median Security"},
    "cloud":          {"cagr_pct": 17.0, "source": "Branchen-Median Cloud"},
    "fintech":        {"cagr_pct": 15.0, "source": "Branchen-Median Fintech"},
    "semiconductor":  {"cagr_pct": 9.0,  "source": "Branchen-Median Halbleiter"},
    "hardware":       {"cagr_pct": 6.0,  "source": "Branchen-Median Hardware"},
    # Industrials / Materials
    "industrial":     {"cagr_pct": 4.0,  "source": "Branchen-Median Industrie"},
    "manufacturing":  {"cagr_pct": 4.0,  "source": "Branchen-Median Fertigung"},
    "automotive":     {"cagr_pct": 4.0,  "source": "Branchen-Median Automotive"},
    "aerospace":      {"cagr_pct": 6.0,  "source": "Branchen-Median Luft-/Raumfahrt"},
    "defense":        {"cagr_pct": 6.0,  "source": "Branchen-Median Verteidigung"},
    "chemical":       {"cagr_pct": 4.0,  "source": "Branchen-Median Chemie"},
    "materials":      {"cagr_pct": 4.0,  "source": "Branchen-Median Materials"},
    "construction":   {"cagr_pct": 4.0,  "source": "Branchen-Median Bau"},
    "logistics":      {"cagr_pct": 6.0,  "source": "Branchen-Median Logistik"},
    # Consumer / Services
    "consumer":       {"cagr_pct": 5.0,  "source": "Branchen-Median Consumer"},
    "retail":         {"cagr_pct": 5.0,  "source": "Branchen-Median Retail"},
    "ecommerce":      {"cagr_pct": 10.0, "source": "Branchen-Median E-Commerce"},
    "food":           {"cagr_pct": 5.0,  "source": "Branchen-Median Food/Beverage"},
    "media":          {"cagr_pct": 6.0,  "source": "Branchen-Median Media"},
    "telecom":        {"cagr_pct": 4.0,  "source": "Branchen-Median Telekom"},
    "financial":      {"cagr_pct": 6.0,  "source": "Branchen-Median Finanzdienstleistungen"},
    "insurance":      {"cagr_pct": 6.0,  "source": "Branchen-Median Versicherung"},
    "real-estate":    {"cagr_pct": 4.0,  "source": "Branchen-Median Immobilien"},
    "energy":         {"cagr_pct": 6.0,  "source": "Branchen-Median Energie"},
}


def _match_sector_fallback(tag: str) -> dict | None:
    """
    Findet einen Branchen-Fallback-CAGR via lowercase-Substring.
    Längster matchender Key gewinnt (spezifischster Sektor).
    Match in beide Richtungen: key-in-tag ODER tag-in-key.
    """
    best: dict | None = None
    best_len = -1
    for key, data in _SECTOR_FALLBACK_CAGR.items():
        if (key in tag or tag in key) and len(key) > best_len:
            best = data
            best_len = len(key)
    return best


def compute_cagr(
    sector_tag: str | None,
    tam_usd_bn: float | None,
    base_year: int = 2024,
    target_year: int = 2035,
) -> dict:
    """
    Berechnet CAGR für einen Sektor.

    Priorität:
    1. CURATED_CAGR — direkt ablesen wenn Sektor bekannt
    2. Mathematische Hochrechnung: CAGR = (TAM_2035 / BASE_2024)^(1/n) - 1
       wenn Basiswert in MARKET_BASE_2024 vorhanden
    3. Konservativer Fallback: 12% (Median Climate Tech)

    Returns:
      {"cagr_pct": float, "cagr_source": str, "cagr_confidence": str}
    """
    n = target_year - base_year  # Anzahl Jahre

    if sector_tag:
        tag = sector_tag.lower().strip()
        # Unicode-Normalisierung: Subscript-Ziffern → ASCII (₂→2, ₃→3 etc.)
        _UNICODE_SUB = str.maketrans("₀₁₂₃₄₅₆₇₈₉", "0123456789")
        tag = tag.translate(_UNICODE_SUB)
        # Sonderzeichen → Bindestriche
        tag = re.sub(r"[/\\|&+]", "-", tag)
        tag = re.sub(r"\s+", "-", tag)
        tag = re.sub(r"-+", "-", tag)
        tag = tag.strip("-")
        # Alias-Map: Kurzschreibweisen + Category-Namen → kanonischer Tag-Key
        _ALIASES = {
            # Kurzformen
            "co2-fuels":            "co2-to-fuels",
            "co2-chemicals":        "co2-to-chemicals",
            "dac":                  "direct-air-capture",
            "ccus":                 "carbon-capture",
            "bess":                 "long-duration-storage",
            "ev-battery":           "battery",
            "ldes":                 "long-duration-storage",
            # Category-Namen → Tag (für den Fall dass category statt tag übergeben wird)
            "co2-utilization":      "co2-to-fuels",
            "co2-to-chemicals":     "co2-to-fuels",
            "carbon-removal":       "carbon-capture",
            "mineralization":       "carbon-capture",
            "biomass-cdr":          "carbon-capture",
            "electrochemical-capture": "carbon-capture",
            "modular-capture":      "carbon-capture",
            "mobile-capture":       "carbon-capture",
            "industrial-capture":   "carbon-capture",
            "ocean-cdr":            "ocean-cdr",
            "electrified-cement":   "low-carbon-cement",
            "distributed-battery":  "battery",
            "circular-battery":     "battery",
            "second-life-bess":     "battery",
            "solid-state-battery":  "solid-state-battery",
            "distributed-power":    "grid",
            "ai-grid":              "ai-grid-software",
            "geothermal-egs":       "geothermal",
            "egs":                  "geothermal",
            "agritech-saas":        "agritech",
            "precision-agriculture":"agritech",
            "vertical-farming":     "agritech",
            "solar-irrigation":     "agritech",
            "climate-risk":         "climate-risk-saas",
            "climate-adaptation":   "climate-risk-saas",
            "datacenter-cooling-hvac": "datacenter-cooling",
            "bio-based-chemicals":  "bio-based-chemicals",
            "sustainable-materials":"sustainable-materials",
        }
        tag = _ALIASES.get(tag, tag)

        # 0. Branchen-Gate: Ist der Tag ein generischer Branchenname (pharma, software,
        # energy etc.)? Dann nimm den Branchen-Median und überspring die Climate-Substring-
        # Suche ganz. Verhindert dass ein generischer Sektor ("software") einen spezifischen
        # Climate-Nischen-Key ("ai-grid-software") per Substring kapert. Nur EXAKTER
        # Branchen-Match gated — Climate-Spezialtags (z.B. "co2-to-fuels") treffen die
        # Branchentabelle nicht und laufen unverändert in die Climate-Suche unten.
        if tag in _SECTOR_FALLBACK_CAGR:
            _fb = _SECTOR_FALLBACK_CAGR[tag]
            return {
                "cagr_pct":        _fb["cagr_pct"],
                "cagr_source":     _fb["source"],
                "cagr_confidence": "low",
            }

        # 1. Curated CAGR — exakter Match zuerst, dann längster Substring-Match
        # Reihenfolge: exact → tag-in-key (spezifischster key gewinnt) → key-in-tag
        # Bugfix: "battery" darf nicht auf "solid-state-battery" matchen wenn
        # "solid-state-battery" der exaktere Key ist.
        best_key: str | None = None
        best_len: int = -1

        # Exact match hat immer Prio
        if tag in CURATED_CAGR:
            best_key = tag
        else:
            # Längster Key der im tag vorkommt ODER tag der im key vorkommt
            for key in CURATED_CAGR:
                if key == tag:
                    best_key = key
                    break
                # tag enthält key ("solid-state-battery" enthält "battery" → key_len=7)
                # oder key enthält tag ("carbon-capture" enthält "carbon" → key_len=14)
                # Wir bevorzugen immer den längeren/spezifischeren Match
                if key in tag and len(key) > best_len:
                    best_key = key
                    best_len = len(key)
                elif tag in key and len(key) > best_len:
                    best_key = key
                    best_len = len(key)

        if best_key:
            data = CURATED_CAGR[best_key]
            return {
                "cagr_pct":        data["cagr_pct"],
                "cagr_source":     data.get("source", "BNEF · IEA · McKinsey"),
                "cagr_confidence": data["confidence"],
            }

        # 2. Mathematische Hochrechnung aus TAM + Basiswert
        if tam_usd_bn:
            best_math_key: str | None = None
            best_math_len: int = -1
            for key in MARKET_BASE_2024:
                if key in tag and len(key) > best_math_len:
                    best_math_key = key
                    best_math_len = len(key)
                elif tag in key and len(key) > best_math_len:
                    best_math_key = key
                    best_math_len = len(key)

            if best_math_key:
                base = MARKET_BASE_2024[best_math_key]
                try:
                    cagr = ((tam_usd_bn / base) ** (1 / n) - 1) * 100
                    cagr = round(min(max(cagr, 0.0), 80.0), 1)  # Cap: 0–80%
                    return {
                        "cagr_pct":        cagr,
                        "cagr_source":     f"Berechnet: ({tam_usd_bn}B / {base}B)^(1/{n}) − 1",
                        "cagr_confidence": "medium",
                    }
                except Exception:
                    pass

    # 3. Branchen-Fallback — sektor-spezifischer Median (Cross-Industry).
    # Match auf der normalisierten tag-Variante (oben gebaut), sonst auf der rohen
    # category/sector-Eingabe. Climate-Median (12%) nur noch als allerletzter Notnagel
    # wenn die Branche unbekannt ist — dann ehrlich als "Markt-Median" gelabelt.
    if sector_tag:
        _fb_tag = sector_tag.lower().strip()
        _fb = _match_sector_fallback(tag) or _match_sector_fallback(_fb_tag)
        if _fb:
            return {
                "cagr_pct":        _fb["cagr_pct"],
                "cagr_source":     _fb["source"],
                "cagr_confidence": "low",
            }

    # 4. Letzter Notnagel — Branche unbekannt. Konservativer Markt-Median, ehrlich gelabelt.
    return {
        "cagr_pct":        8.0,
        "cagr_source":     "Markt-Median (Branche nicht klassifiziert)",
        "cagr_confidence": "low",
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
    """
    Snippets für TAM-Extraktion via DuckDuckGo HTML-Suche.
    Kein API-Key, kein CAPTCHA-Problem auf Render.
    Google-Scraping entfernt — CSS-Klassen instabil, CAPTCHA auf Cloud-IPs.
    """
    try:
        async with httpx.AsyncClient(
            timeout=10,
            headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"},
            follow_redirects=True,
        ) as client:
            resp = await client.get(
                "https://html.duckduckgo.com/html/",
                params={"q": query},
            )
        if resp.status_code != 200:
            return []
        # DuckDuckGo HTML Snippets
        snippets = re.findall(
            r'class="result__snippet"[^>]*>([^<]{30,400})<',
            resp.text,
        )
        return snippets[:num]
    except Exception as e:
        logger.warning("DuckDuckGo search failed for '%s': %s", query[:60], e)
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
    # 1. Sector-level curated lookup — längster/spezifischster Match gewinnt
    if sector:
        sector_tag = sector.lower().strip()
        sector_tag = re.sub(r"[/\\|&+]", "-", sector_tag)
        sector_tag = re.sub(r"\s+", "-", sector_tag)
        sector_tag = re.sub(r"-+", "-", sector_tag).strip("-")
        best_key: str | None = None
        best_len: int = -1

        if sector_tag in CURATED_TAM:
            best_key = sector_tag
        else:
            for tag in CURATED_TAM:
                if tag in sector_tag and len(tag) > best_len:
                    best_key = tag
                    best_len = len(tag)
                elif sector_tag in tag and len(tag) > best_len:
                    best_key = tag
                    best_len = len(tag)

        if best_key:
            result = CURATED_TAM[best_key].copy()
            result["method"] = "curated_sector"
            result["tag"] = best_key
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
