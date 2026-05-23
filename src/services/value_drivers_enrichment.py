"""
Value Drivers Enrichment Pipeline
===================================
src/services/value_drivers_enrichment.py

One-Click Flow:
  1. supply_chain.py → Enabler (upstream) + Contributors (downstream) + ETFs identifizieren
  2. Claude-Enrichment je Eintrag → Abhängigkeitsgrad, Marktposition, Kontext-Satz
  3. Yahoo Finance → Kurs-Signal + Marktcap für gelistete Einträge
  4. upsert_value_drivers() → persistiert in value_drivers-Tabelle

Wird async via BackgroundTasks angestoßen nach Company-Lookup.
Polling-Endpunkt: GET /api/v1/company/{name}/value-drivers

Abhängigkeitsgrad (dependency_level) — nur für Enabler:
  critical   → Single Source of Failure, kein Ersatz verfügbar
  high       → Schwer substituierbar, lange Switching Costs
  medium     → Substituierbar, aber mit Aufwand
  commodity  → Austauschbar, Preis ist einziger Differenziator

Marktposition (market_position) — nur für Enabler:
  dominant   → Marktführer, >40% Share oder de-facto Standard
  contested  → 2–3 gleichwertige Player, kein klarer Gewinner
  fragmented → Viele kleine Player, kein Preissetter

Exposure-Grad (exposure_level) — nur für Contributors:
  high       → Material: >10% Revenue-Exposure auf diesen Sektor
  medium     → Relevant: 5–10% Revenue-Exposure
  low        → Marginal: <5%, aber strategisch interessant

Growth-Unabhängigkeit (grows_independently) — nur für Contributors:
  true       → Wächst auch ohne das Target (breitere Sektor-Exposure)
  false      → Stark abhängig vom Erfolg des Targets / Sektors
  partial    → Gemischt: Kernsegment wächst, aber Target-Sektor ist Boost
"""

import asyncio
import logging
import httpx
from src.config import settings
from src.services.supply_chain import get_supply_chain, COMPANY_TAGS

logger = logging.getLogger(__name__)

# ── Konfidenz-Schwelle für Yahoo-Kurs-Signal ──────────────────────────────────
_YAHOO_TIMEOUT = 5.0

# ── Claude-Prompt Templates ───────────────────────────────────────────────────

_ENABLER_PROMPT = """You are a senior M&A analyst. Assess this supplier/enabler in the context of the target company.

Target company: {company_name}
Target sector: {category}

Enabler: {enabler_name} ({ticker})
Role: {role}
Current relevance score: {relevance}

Respond ONLY with valid JSON, no markdown, no preamble:
{{
  "dependency_level": "critical|high|medium|commodity",
  "market_position": "dominant|contested|fragmented",
  "context": "<one crisp sentence explaining WHY this enabler matters for the target — include a specific risk or opportunity>",
  "partnership_likely": true|false
}}

Rules:
- dependency_level: critical = no substitute exists; commodity = fully replaceable
- market_position: dominant = >40% share or de-facto standard
- context: max 20 words, no generic statements, specific to this pair
- partnership_likely: true if this type of supplier typically has formal agreements with targets like this"""

_CONTRIBUTOR_PROMPT = """You are a senior M&A analyst. Assess this downstream company's exposure to the target's growth.

Target company: {company_name}
Target sector: {category}

Contributor: {contributor_name} ({ticker})
Role: {role}
Current relevance score: {relevance}

Respond ONLY with valid JSON, no markdown, no preamble:
{{
  "exposure_level": "high|medium|low",
  "grows_independently": "true|false|partial",
  "context": "<one crisp sentence on HOW this contributor profits from the target's success — quantify if possible>",
  "existing_relationship": true|false
}}

Rules:
- exposure_level: high = >10% revenue touch; low = <5% but strategically notable
- grows_independently: true = broad sector exposure; false = highly dependent on this niche
- context: max 20 words, specific, no generic statements
- existing_relationship: true if there's a known commercial or strategic tie"""


async def _claude_enrich_entry(
    client: httpx.AsyncClient,
    entry_type: str,  # "enabler" | "contributor"
    company_name: str,
    category: str | None,
    entry: dict,
) -> dict:
    """Ruft Claude für einen einzelnen Enabler oder Contributor auf."""
    try:
        if entry_type == "enabler":
            prompt = _ENABLER_PROMPT.format(
                company_name=company_name,
                category=category or "Climate Tech",
                enabler_name=entry["name"],
                ticker=entry["ticker"],
                role=entry["role"],
                relevance=entry["relevance"],
            )
        else:
            prompt = _CONTRIBUTOR_PROMPT.format(
                company_name=company_name,
                category=category or "Climate Tech",
                contributor_name=entry["name"],
                ticker=entry["ticker"],
                role=entry["role"],
                relevance=entry["relevance"],
            )

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
            timeout=10.0,
        )

        if resp.status_code == 200:
            text = resp.json()["content"][0]["text"].strip()
            # JSON sauber parsen
            import json, re
            text = re.sub(r"```json|```", "", text).strip()
            return json.loads(text)

    except Exception as e:
        logger.debug("Claude enrichment failed for %s/%s: %s", company_name, entry.get("ticker"), e)

    return {}


async def _fetch_yahoo_signal(client: httpx.AsyncClient, ticker: str, exchange: str | None) -> dict:
    """Holt Kurs + Marktcap für einen Ticker — Light-Version ohne Crumb-Auth."""
    try:
        # Exchange-Suffix für internationale Ticker
        _SUFFIX = {
            "xetra": ".DE", "frankfurt": ".F",
            "euronext": ".PA", "euronext paris": ".PA",
            "london": ".L", "lse": ".L",
            "swiss": ".SW", "bmv": ".MX",
            "tsx": ".TO", "asx": ".AX",
        }
        symbol = ticker
        if exchange and "." not in ticker:
            suffix = _SUFFIX.get(exchange.lower(), "")
            symbol = ticker + suffix

        resp = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=_YAHOO_TIMEOUT,
        )
        if resp.status_code == 200:
            meta = resp.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
            price = meta.get("regularMarketPrice")
            mcap  = meta.get("marketCap")
            return {
                "price":         price,
                "market_cap_bn": round(mcap / 1e9, 1) if mcap else None,
                "currency":      meta.get("currency"),
                "symbol":        symbol,
            }
    except Exception as e:
        logger.debug("Yahoo signal failed for %s: %s", ticker, e)
    return {}


# ── BUG-27 · Kategorie-basierter Fallback ─────────────────────────────────────

# Minimales Mapping: Kategorie → generische Enabler (börsennotiert, überprüfbar)
# Nur die häufigsten Argo-Kategorien — lieber wenig als falsch.
_CATEGORY_FALLBACK_ENABLERS: dict[str, list[dict]] = {
    "battery":            [{"ticker": "ALB",  "name": "Albemarle",          "exchange": "NYSE",   "role": "Lithium-Rohstoff",        "relevance": 0.8}],
    "solid-state battery":[{"ticker": "ALB",  "name": "Albemarle",          "exchange": "NYSE",   "role": "Lithium-Rohstoff",        "relevance": 0.8}],
    "solar":              [{"ticker": "ENPH", "name": "Enphase Energy",      "exchange": "Nasdaq", "role": "Inverter-Technologie",    "relevance": 0.75}],
    "grid":               [{"ticker": "AME",  "name": "AMETEK",              "exchange": "NYSE",   "role": "Grid-Elektronik",         "relevance": 0.7}],
    "hydrogen":           [{"ticker": "APD",  "name": "Air Products",        "exchange": "NYSE",   "role": "Industriegase / H₂-Infra","relevance": 0.8}],
    "geothermal":         [{"ticker": "SLB",  "name": "SLB (Schlumberger)",  "exchange": "NYSE",   "role": "Bohrtechnologie",         "relevance": 0.75}],
    "carbon-capture":     [{"ticker": "HON",  "name": "Honeywell",           "exchange": "Nasdaq", "role": "Prozessautomation",       "relevance": 0.7}],
    "direct-air-capture": [{"ticker": "HON",  "name": "Honeywell",           "exchange": "Nasdaq", "role": "Prozessautomation",       "relevance": 0.7}],
    "agritech":           [{"ticker": "DE",   "name": "Deere & Company",     "exchange": "NYSE",   "role": "Landmaschinentechnologie","relevance": 0.7}],
    "co2-to-fuels":       [{"ticker": "HON",  "name": "Honeywell",           "exchange": "Nasdaq", "role": "Katalysator-Technologie", "relevance": 0.7}],
    "cement":             [{"ticker": "CRH",  "name": "CRH",                 "exchange": "NYSE",   "role": "Zement-Infrastruktur",   "relevance": 0.8}],
    "software":           [{"ticker": "MSFT", "name": "Microsoft",           "exchange": "Nasdaq", "role": "Cloud-Infrastruktur",     "relevance": 0.65}],
    "saas":               [{"ticker": "MSFT", "name": "Microsoft",           "exchange": "Nasdaq", "role": "Cloud-Infrastruktur",     "relevance": 0.65}],
}


def _build_category_fallback(category: str | None) -> list[dict]:
    """
    BUG-27: Gibt generische Enabler zurück wenn kein supply_chain Mapping vorhanden.
    Matcht Kategorie case-insensitive gegen _CATEGORY_FALLBACK_ENABLERS.
    Gibt leere Liste zurück wenn kein Match (besser als falscher Fallback).
    """
    if not category:
        return []
    cat_lower = category.lower().replace(" ", "-").replace("_", "-")
    # Direkter Match
    if cat_lower in _CATEGORY_FALLBACK_ENABLERS:
        entries = _CATEGORY_FALLBACK_ENABLERS[cat_lower]
    else:
        # Teilstring-Match (z.B. "Solid-State Battery" → "battery")
        entries = next(
            (v for k, v in _CATEGORY_FALLBACK_ENABLERS.items() if k in cat_lower or cat_lower in k),
            [],
        )
    # Fallback-Entries mit Quelle markieren
    return [
        {**e, "source": "category_fallback", "dependency_level": "medium",
         "market_position": "contested", "context": None,
         "partnership_likely": False, "price": None, "market_cap_bn": None,
         "currency": None, "yahoo_symbol": None, "type": "enabler"}
        for e in entries
    ]


async def enrich_value_drivers(
    company_id: str,
    company_name: str,
    category: str | None,
    tags: list[str],
) -> dict:
    """
    Hauptfunktion — wird via BackgroundTasks aufgerufen.
    Gibt Dict zurück das direkt an upsert_value_drivers() übergeben wird.

    Struktur:
    {
      "enablers":     [enriched enabler dicts],
      "contributors": [enriched contributor dicts],
      "etfs":         [etf dicts],
    }
    """
    # 1. Supply Chain aus bestehendem Mapping holen
    sc = get_supply_chain(tags)
    upstream   = sc.get("upstream", [])
    downstream = sc.get("downstream", [])
    etfs       = sc.get("etfs", [])

    if not upstream and not downstream:
        logger.info("No supply chain data for %s (tags=%s) — using category fallback", company_name, tags)
        # BUG-27: Kategorie-basierter Fallback statt leerem Tab.
        fallback = _build_category_fallback(category)
        if fallback:
            # Yahoo-Preise für Fallback-Ticker fetchen
            async with httpx.AsyncClient() as client:
                yahoo_tasks = [
                    _fetch_yahoo_signal(client, e["ticker"], e.get("exchange"))
                    for e in fallback
                ]
                yahoo_results = await asyncio.gather(*yahoo_tasks, return_exceptions=True)
            for i, entry in enumerate(fallback):
                yr = yahoo_results[i]
                if isinstance(yr, dict) and yr:
                    entry.update({
                        "price":         yr.get("price"),
                        "market_cap_bn": yr.get("market_cap_bn"),
                        "currency":      yr.get("currency"),
                        "yahoo_symbol":  yr.get("symbol"),
                    })
        return {"enablers": fallback, "contributors": [], "etfs": etfs}

    # 2. Claude-Enrichment + Yahoo parallel
    async with httpx.AsyncClient() as client:

        # Claude: alle Enabler + Contributors gleichzeitig
        enabler_tasks = [
            _claude_enrich_entry(client, "enabler", company_name, category, e)
            for e in upstream
        ]
        contributor_tasks = [
            _claude_enrich_entry(client, "contributor", company_name, category, c)
            for c in downstream
        ]

        # Yahoo: alle gelisteten Ticker (Enabler + Contributors)
        all_entries = upstream + downstream
        yahoo_tasks = [
            _fetch_yahoo_signal(client, e["ticker"], e.get("exchange"))
            for e in all_entries
        ]

        # Alles parallel ausführen
        results = await asyncio.gather(
            *enabler_tasks, *contributor_tasks, *yahoo_tasks,
            return_exceptions=True,
        )

    n_enablers     = len(upstream)
    n_contributors = len(downstream)

    enabler_enrichments     = results[:n_enablers]
    contributor_enrichments = results[n_enablers:n_enablers + n_contributors]
    yahoo_results           = results[n_enablers + n_contributors:]

    # Yahoo-Map: ticker → signal
    yahoo_map: dict[str, dict] = {}
    for i, entry in enumerate(all_entries):
        yr = yahoo_results[i]
        if isinstance(yr, dict) and yr:
            yahoo_map[entry["ticker"]] = yr

    # 3. Enabler zusammenbauen
    enriched_enablers = []
    for i, entry in enumerate(upstream):
        er = enabler_enrichments[i]
        if isinstance(er, Exception):
            er = {}
        yahoo = yahoo_map.get(entry["ticker"], {})
        enriched_enablers.append({
            "type":             "enabler",
            "ticker":           entry["ticker"],
            "name":             entry["name"],
            "exchange":         entry.get("exchange"),
            "role":             entry["role"],
            "relevance":        entry["relevance"],
            # Claude-Felder
            "dependency_level":  er.get("dependency_level", "medium"),
            "market_position":   er.get("market_position", "contested"),
            "context":           er.get("context"),
            "partnership_likely": er.get("partnership_likely", False),
            # Yahoo-Felder
            "price":            yahoo.get("price"),
            "market_cap_bn":    yahoo.get("market_cap_bn"),
            "currency":         yahoo.get("currency"),
            "yahoo_symbol":     yahoo.get("symbol"),
            # Meta
            "source":           "supply_chain_py+claude",
        })

    # 4. Contributors zusammenbauen
    enriched_contributors = []
    for i, entry in enumerate(downstream):
        er = contributor_enrichments[i]
        if isinstance(er, Exception):
            er = {}
        yahoo = yahoo_map.get(entry["ticker"], {})
        enriched_contributors.append({
            "type":               "contributor",
            "ticker":             entry["ticker"],
            "name":               entry["name"],
            "exchange":           entry.get("exchange"),
            "role":               entry["role"],
            "relevance":          entry["relevance"],
            # Claude-Felder
            "exposure_level":     er.get("exposure_level", "medium"),
            "grows_independently": er.get("grows_independently", "partial"),
            "context":            er.get("context"),
            "existing_relationship": er.get("existing_relationship", False),
            # Yahoo-Felder
            "price":              yahoo.get("price"),
            "market_cap_bn":      yahoo.get("market_cap_bn"),
            "currency":           yahoo.get("currency"),
            "yahoo_symbol":       yahoo.get("symbol"),
            # Meta
            "source":             "supply_chain_py+claude",
        })

    logger.info(
        "Value drivers enriched for %s: %d enablers, %d contributors, %d etfs",
        company_name, len(enriched_enablers), len(enriched_contributors), len(etfs),
    )

    return {
        "enablers":     enriched_enablers,
        "contributors": enriched_contributors,
        "etfs":         etfs,
    }


# ── Concentration Risk ─────────────────────────────────────────────────────────

def compute_concentration_risk(enablers: list[dict], contributors: list[dict]) -> dict:
    """
    Berechnet Konzentrations- und Abhängigkeitsrisiko aus Value Drivers.
    Wird von assessments.py für die operations-Dimension verwendet.

    Returns:
        {risk_score (0–10 | None), concentration_note, key_dependencies: [str]}
    """
    if not enablers and not contributors:
        return {"risk_score": None, "concentration_note": "Keine Daten", "key_dependencies": []}

    critical = [e for e in enablers if e.get("dependency_level") == "critical"]
    high_dep  = [e for e in enablers if e.get("dependency_level") == "high"]
    high_exp  = [c for c in contributors if c.get("exposure_level") == "high"]

    risk = 2.0
    risk += len(critical) * 2.5
    risk += len(high_dep) * 1.0
    # Kundenseite: einzelner dominanter Abnehmer
    if len(high_exp) == 1:
        risk += 1.5
    elif len(high_exp) > 3:
        risk -= 0.5   # Diversifikation auf Kundenseite senkt Risiko leicht
    risk = min(10.0, risk)

    key_deps = [e["name"] for e in critical] + [e["name"] for e in high_dep[:2]]

    note_parts = []
    if critical:
        note_parts.append(f"{len(critical)} kritische{'r' if len(critical)==1 else ''} Enabler")
    if high_dep:
        note_parts.append(f"{len(high_dep)} High-Dependency-Lieferant{'en' if len(high_dep)>1 else ''}")
    if high_exp:
        note_parts.append(f"{len(high_exp)} Abnehmer mit hoher Exposure")

    return {
        "risk_score":          round(risk, 1),
        "concentration_note":  " · ".join(note_parts) if note_parts else "Abhängigkeiten diversifiziert",
        "key_dependencies":    key_deps[:5],
    }

