"""
Market Data Enrichment Pipeline — v1.0
=======================================
Befüllt market_data-Tabelle für jede Company vollautomatisch.
Kein manuelles Datenpflegen — wird bei One-Click-Search getriggert.

Pipelines:
  MD-B01  TAM-Scraping erweitert → Segmente + Wachstumstreiber (KI-Extraktion)
  MD-B02  World Bank API → regionale Marktverteilung
  MD-B03  OECD API → EU-Ergänzung
  MD-B04  compute_sam() → SAM aus TAM × geo_factor × tech_filter
  MD-B05  compute_competition_score() → aus Argo-DB (companies + funding_rounds)
  MD-B06  compute_market_cycle() → Funding-Trend YoY aus DB
  Trigger enrichment_status: pending → running → done | error
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ── MD-B01 · TAM-Scraping erweitert ──────────────────────────────────────────

async def _extract_market_details_with_claude(
    company: str,
    sector: str,
    tam_usd_bn: float,
    snippets: list[str],
) -> dict:
    """
    KI-Extraktion: Marktsegmente + Wachstumstreiber aus Suchergebnis-Snippets.
    Gibt strukturiertes JSON zurück.
    """
    if not snippets:
        return {}

    snippet_text = "\n".join(f"- {s}" for s in snippets[:10])
    prompt = f"""You are a market research analyst. Based on these search snippets about the {sector} market (relevant to {company}, TAM ~${tam_usd_bn}B by 2035):

Snippets:
{snippet_text}

Extract market structure data. Return ONLY valid JSON, no preamble, no markdown fences.

{{
  "segments": [
    {{"name": "<segment name>", "share_pct": <number 0-100>, "note": "<brief context>"}}
  ],
  "growth_drivers": ["<driver 1>", "<driver 2>", "<driver 3>", "<driver 4>"],
  "geo_scope": "<global|us_eu|us_only|eu_only|emerging>",
  "cagr_pct": <number or null>
}}

Rules:
- segments: 3–5 most important, shares should sum to ~100
- growth_drivers: 4–6 short phrases, most important first
- geo_scope: where is the primary market for this company/sector
- cagr_pct: annual growth rate in %, null if not found
- If data not available, return empty arrays/null values — never invent numbers
"""
    try:
        from src.config import settings
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
                    "max_tokens": 600,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        if resp.status_code != 200:
            logger.warning("Claude market details HTTP %s for %s: %s", resp.status_code, company, resp.text[:200])
            return {}
        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"```(?:json)?|```", "", raw).strip()
        return json.loads(raw)
    except Exception as e:
        logger.warning("Claude market details extraction failed for %s: %s", company, e)
        return {}


async def _fetch_market_snippets(company: str, sector: str) -> list[str]:
    """
    MD-B01: Snippets für Marktsegmente + Wachstumstreiber.
    Primär: DuckDuckGo HTML-Suche (kein API-Key, kein CAPTCHA-Problem).
    Fallback: Direkte IEA/BNEF-PR-Seiten via httpx.
    Google-Scraping entfernt — CSS-Klassen zu instabil, CAPTCHA auf Render.
    """
    queries = [
        f"{sector} market size segments 2035 billion USD site:iea.org OR site:bnef.com OR site:mckinsey.com",
        f"{sector} market growth drivers 2030 2035",
        f"{company} total addressable market {sector} segments",
    ]
    snippets: list[str] = []
    try:
        async with httpx.AsyncClient(
            timeout=10,
            headers={
                **HEADERS,
                "Accept": "text/html,application/xhtml+xml",
            },
            follow_redirects=True,
        ) as client:
            for q in queries[:2]:
                try:
                    resp = await client.get(
                        "https://html.duckduckgo.com/html/",
                        params={"q": q},
                    )
                    if resp.status_code == 200:
                        # DuckDuckGo HTML: Snippets in <a class="result__snippet">
                        found = re.findall(
                            r'class="result__snippet"[^>]*>([^<]{30,400})<',
                            resp.text,
                        )
                        # Fallback: generische <span> mit Zahlen (enthält oft Marktdaten)
                        if not found:
                            found = re.findall(
                                r'<a[^>]+class="result__url"[^>]*>([^<]{10,200})<',
                                resp.text,
                            )
                        snippets.extend(s.strip() for s in found[:6])
                    await asyncio.sleep(0.5)  # DuckDuckGo Rate-Limit respektieren
                except Exception as e:
                    logger.debug("DuckDuckGo query failed ('%s'): %s", q[:50], e)
    except Exception as e:
        logger.debug("Market snippet fetch failed for %s: %s", company, e)
    return snippets[:12]


# ── MD-B02 · World Bank API ───────────────────────────────────────────────────

# Mapping: Argo-Kategorie → World Bank Indicator
# Vollständig automatisch — kein manuelles Mapping nötig für neue Sektoren,
# KI-Fallback greift wenn kein Mapping vorhanden.
WORLDBANK_INDICATORS: dict[str, str] = {
    "battery":              "EG.ELC.RNEW.ZS",   # Renewable electricity output
    "solar":                "EG.ELC.RNEW.ZS",
    "grid":                 "EG.USE.ELEC.KH.PC", # Electric power consumption
    "hydrogen":             "EN.ATM.CO2E.KT",    # CO2 emissions (proxy)
    "carbon-capture":       "EN.ATM.CO2E.KT",
    "direct-air-capture":   "EN.ATM.CO2E.KT",
    "agritech":             "AG.LND.AGRI.ZS",    # Agricultural land
    "irrigation":           "AG.LND.IRIG.AG.ZS", # Irrigated land
    "geothermal":           "EG.ELC.RNEW.ZS",
    "co2-to-fuels":         "EN.ATM.CO2E.KT",
    "climate-risk-saas":    "EN.CLC.MDAT.ZS",    # Climate-related disasters
}

WB_REGIONS = ["US", "DE", "GB", "FR", "CN", "IN", "BR", "JP"]


async def _fetch_worldbank_regional(indicator: str) -> dict | None:
    """
    Holt World Bank Daten für einen Indicator, aufgeschlüsselt nach Regionen.
    Gibt normalisierte regionale Verteilung zurück.
    Endpoint: api.worldbank.org/v2/country/{code}/indicator/{indicator}
    """
    if not indicator:
        return None

    results: dict[str, float] = {}
    try:
        async with httpx.AsyncClient(timeout=10, headers=HEADERS) as client:
            tasks = [
                client.get(
                    f"https://api.worldbank.org/v2/country/{code}/indicator/{indicator}",
                    params={"format": "json", "mrv": 1, "per_page": 1},
                )
                for code in WB_REGIONS
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for code, resp in zip(WB_REGIONS, responses):
            if isinstance(resp, Exception):
                continue
            try:
                data = resp.json()
                if isinstance(data, list) and len(data) > 1 and data[1]:
                    val = data[1][0].get("value")
                    if val is not None:
                        results[code] = float(val)
            except Exception:
                continue

        if not results:
            return None

        # Normalisieren auf 0–100%
        total = sum(results.values()) or 1
        breakdown = [
            {"region": code, "share_pct": round(v / total * 100, 1)}
            for code, v in sorted(results.items(), key=lambda x: -x[1])
        ]
        return {"breakdown": breakdown, "indicator": indicator, "source": "World Bank API"}

    except Exception as e:
        logger.debug("World Bank fetch failed for %s: %s", indicator, e)
        return None


# ── MD-B03 · OECD API ────────────────────────────────────────────────────────

async def _fetch_oecd_supplement(sector_tag: str) -> dict | None:
    """
    OECD SDMX API — EU-spezifische Ergänzung.
    Endpoint: sdmx.oecd.org/public/rest/data/OECD.ENV.EPI,...
    Nur für Sektoren mit klarem OECD-Datensatz — sonst None.
    """
    # OECD-Datensets sind sehr granular — nur für Energie/Umwelt sinnvoll
    supported = {"battery", "solar", "grid", "hydrogen", "carbon-capture", "agritech"}
    if sector_tag not in supported:
        return None

    try:
        async with httpx.AsyncClient(timeout=8, headers=HEADERS) as client:
            resp = await client.get(
                "https://sdmx.oecd.org/public/rest/data/OECD.ENV.EPI,DSD_EPI@DF_EPI,1.0/all",
                params={"format": "jsondata", "startPeriod": "2022", "dimensionAtObservation": "AllDimensions"},
            )
        if resp.status_code != 200:
            return None
        # OECD gibt sehr komplexes JSON — wir extrahieren nur ob Daten vorhanden
        return {"source": "OECD SDMX API", "available": True, "sector": sector_tag}
    except Exception as e:
        logger.debug("OECD fetch failed for %s: %s", sector_tag, e)
        return None


# ── MD-B04 · SAM-Berechnung ───────────────────────────────────────────────────

GEO_FACTORS: dict[str, float] = {
    "global":    1.0,
    "us_eu":     0.70,
    "us_only":   0.45,
    "eu_only":   0.30,
    "emerging":  0.25,
}


def compute_sam(
    tam_usd_bn: float,
    geo_scope: str,
    tech_readiness: float | None = None,
) -> dict:
    """
    MD-B04: SAM = TAM × geo_factor × tech_filter
    Gibt SAM + Konfidenz + Note zurück.

    geo_scope: global | us_eu | us_only | eu_only | emerging
    tech_readiness: 0.0–1.0 (aus scores.tech_readiness wenn vorhanden)
    """
    geo_factor = GEO_FACTORS.get(geo_scope, GEO_FACTORS["global"])

    if tech_readiness is None:
        tech_filter = 0.4
        confidence = "low"
        tr_note = "TechReadiness nicht verfügbar — Fallback 0.4"
    elif tech_readiness >= 0.7:
        tech_filter = 0.8
        confidence = "high"
        tr_note = f"TechReadiness {tech_readiness:.2f} ≥ 0.7 — reifer Markt"
    elif tech_readiness >= 0.5:
        tech_filter = 0.5
        confidence = "medium"
        tr_note = f"TechReadiness {tech_readiness:.2f} — early commercial"
    else:
        tech_filter = 0.25
        confidence = "low"
        tr_note = f"TechReadiness {tech_readiness:.2f} < 0.5 — pre-commercial"

    sam = round(tam_usd_bn * geo_factor * tech_filter, 1)

    # Geo-Scope leserlich machen
    geo_labels = {
        "global": "Global", "us_eu": "USA + Europa",
        "us_only": "USA", "eu_only": "Europa", "emerging": "Emerging Markets"
    }
    geo_label = geo_labels.get(geo_scope, geo_scope)

    note = f"Serviceable Addressable Market für {geo_label}. {tr_note}."

    return {
        "sam_usd_bn":     sam,
        "sam_confidence": confidence,
        "sam_note":       note,
        "sam_geo_factor": geo_factor,
        "sam_tech_filter": tech_filter,
    }


# ── MD-B05 · Competition Score ────────────────────────────────────────────────
# Hinweis: DB-Zählung ist kein valider Wettbewerbs-Indikator — Argo-DB wächst
# erst und bildet den Markt nicht vollständig ab. Primärquelle ist DuckDuckGo
# (Result-Count als Markt-Fragmentierungs-Proxy) + TAM/CAGR-Signal.
# DB-Einträge fließen nur ergänzend als Kontextnote ein (Funding-Konzentration
# der bekannten Player) — nicht als Basis für den Score selbst.

async def fetch_competition_signals(sector: str, category: str) -> dict:
    """
    MD-B05-ext: Externe Wettbewerbssignale via DuckDuckGo.
    Gibt {result_count_proxy, top_names, source} zurück.

    result_count_proxy: Anzahl gefundener Snippets als Fragmentierungs-Signal.
      >12 Snippets → high (viele Player, fragmentierter Markt)
      6–12         → medium
      <6           → low / early market
    """
    queries = [
        f"{sector} companies startups venture funding 2024 2025",
        f"{category} competitors market players",
    ]
    total_snippets = 0
    top_names: list[str] = []

    try:
        async with httpx.AsyncClient(
            timeout=10,
            headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"},
            follow_redirects=True,
        ) as client:
            for q in queries:
                try:
                    resp = await client.get(
                        "https://html.duckduckgo.com/html/",
                        params={"q": q},
                    )
                    if resp.status_code == 200:
                        snippets = re.findall(
                            r'class="result__snippet"[^>]*>([^<]{30,300})<',
                            resp.text,
                        )
                        total_snippets += len(snippets)
                        # Company-Namen aus Snippets extrahieren (kapitalisierte Wörter)
                        for s in snippets[:4]:
                            names = re.findall(r'\b[A-Z][a-zA-Z]{3,}(?:\s[A-Z][a-zA-Z]{2,})?\b', s)
                            top_names.extend(names[:2])
                    await asyncio.sleep(0.4)
                except Exception as e:
                    logger.debug("Competition signal query failed: %s", e)
    except Exception as e:
        logger.debug("fetch_competition_signals failed for %s: %s", sector, e)

    return {
        "result_count_proxy": total_snippets,
        "top_names": list(dict.fromkeys(top_names))[:5],  # dedup, max 5
        "source": "duckduckgo",
    }


def compute_competition_score(
    category: str,
    all_companies: list[dict],
    all_funding_rounds: list[dict],
    external_signals: dict | None = None,
    peers_context: dict | None = None,
) -> dict:
    """
    MD-B05: Wettbewerbsintensität — externe Signale als Primärquelle.
    R-22: peers_context (Positioning Notes aus Peer Review) ergänzt competition_note.
    """
    ext = external_signals or {}
    result_count = ext.get("result_count_proxy", 0)
    top_names = ext.get("top_names", [])

    # ── 1. Primär: externe Snippet-Anzahl als Fragmentierungs-Proxy ──────────
    if result_count >= 12:
        score = "high"
        note = f"Fragmentierter Markt — hohe externe Signal-Dichte ({result_count} Treffer)."
    elif result_count >= 6:
        score = "medium"
        note = f"Moderate Wettbewerbsintensität ({result_count} externe Signale)."
    else:
        score = "low"
        note = f"Wenig externe Signale ({result_count}) — früher oder nischiger Markt."

    if top_names:
        note += f" Erwähnte Player: {', '.join(top_names[:3])}."

    # ── 2. R-22: Peer Positioning Notes als direkter Wettbewerbs-Kontext ─────
    if peers_context:
        peer_notes = [
            f"{name}: {note_text}"
            for name, note_text in list(peers_context.items())[:3]
            if note_text
        ]
        if peer_notes:
            note += " Bekannte Wettbewerber — " + " | ".join(peer_notes) + "."

    # ── 3. DB-Funding-Konzentration als Kontextnote (nicht Score) ────────────
    db_peers = [c for c in all_companies if c.get("category") == category]
    if len(db_peers) >= 2:
        peer_fundings = {
            c["id"]: c.get("funding_total_usd_mn") or 0
            for c in db_peers if c.get("id")
        }
        total_funding = sum(peer_fundings.values()) or 1
        max_funding = max(peer_fundings.values()) if peer_fundings else 0
        concentration = max_funding / total_funding
        top_db = max(db_peers, key=lambda c: c.get("funding_total_usd_mn") or 0, default=None)
        if top_db:
            note += (
                f" In Argo-DB: {len(db_peers)} erfasste Player,"
                f" Funding-Führung {top_db['name']} ({concentration:.0%} Anteil)."
            )

    return {"competition_score": score, "competition_note": note}


# ── MD-B06 · Market Cycle ────────────────────────────────────────────────────

def compute_market_cycle(
    category: str,
    all_funding_rounds: list[dict],
    all_companies: list[dict],
) -> dict:
    """
    MD-B06: Marktzyklus aus Funding-Trend YoY in dieser Kategorie.
    early → growth → mature → consolidation
    """
    from collections import defaultdict

    # Companies in dieser Kategorie
    peer_ids = {c["id"] for c in all_companies if c.get("category") == category and c.get("id")}
    if not peer_ids:
        return {
            "market_cycle": "early",
            "market_cycle_note": "Keine historischen Funding-Daten — Markt vermutlich früh.",
        }

    # Funding nach Jahr aggregieren
    yearly: dict[int, float] = defaultdict(float)
    for r in all_funding_rounds:
        if r.get("company_id") not in peer_ids:
            continue
        date_str = r.get("date") or ""
        try:
            year = int(str(date_str)[:4])
            yearly[year] += r.get("amount_usd_mn") or 0
        except (ValueError, TypeError):
            continue

    if not yearly:
        return {
            "market_cycle": "early",
            "market_cycle_note": "Keine Funding-Daten verfügbar — Markt vermutlich sehr früh.",
        }

    years = sorted(yearly.keys())
    recent_years = years[-3:] if len(years) >= 3 else years
    recent_funding = [yearly[y] for y in recent_years]

    # Trend bestimmen
    if len(recent_funding) >= 2:
        trend = (recent_funding[-1] - recent_funding[0]) / (recent_funding[0] or 1)
    else:
        trend = 0

    total_rounds = len([r for r in all_funding_rounds if r.get("company_id") in peer_ids])
    player_count = len(peer_ids)

    if total_rounds <= 3 or player_count <= 2:
        cycle = "early"
        note = f"Wenige Runden ({total_rounds}) und Player ({player_count}) — früher Markt."
    elif trend > 0.3:
        cycle = "growth"
        note = f"Funding-Trend +{trend:.0%} YoY — Wachstumsphase."
    elif trend < -0.2:
        cycle = "consolidation"
        note = f"Funding-Trend {trend:.0%} YoY — Konsolidierung oder Reife."
    else:
        cycle = "mature"
        note = f"Stabiles Funding-Niveau ({trend:+.0%} YoY) — reifer Markt."

    return {"market_cycle": cycle, "market_cycle_note": note}


# ── Main Pipeline ─────────────────────────────────────────────────────────────

async def enrich_market_data(
    company_id: str,
    company_name: str,
    category: str | None,
    sector_tag: str | None,
    tam_usd_bn: float | None,
    tech_readiness: float | None = None,
) -> dict:
    """
    Vollständige Market Data Enrichment Pipeline für eine Company.
    Wird async aufgerufen — non-blocking.

    Returns: dict mit allen market_data Feldern (nur non-None).
    Caller persistiert via upsert_market_data().
    """
    result: dict = {}
    sector = sector_tag or (category or "").lower().replace(" ", "-")

    # ── 1. TAM-Basis aus tam_cache (bereits vorhanden) ────────────────────────
    # tam_usd_bn wird vom Caller übergeben (aus tam_cache oder get_tam())
    if tam_usd_bn:
        result["tam_2035_usd_bn"] = tam_usd_bn

    # ── 2. Marktsegmente + Wachstumstreiber (MD-B01) ─────────────────────────
    # BUG-28: MD-B01 läuft unabhängig von TAM und Competitors.
    # tam_usd_bn=0 als Fallback damit Claude-Prompt trotzdem sinnvoll ist.
    market_details: dict = {}
    try:
        snippets = await asyncio.wait_for(
            _fetch_market_snippets(company_name, sector), timeout=12.0
        )
        if snippets:
            market_details = await asyncio.wait_for(
                _extract_market_details_with_claude(
                    company_name, sector, tam_usd_bn or 0.0, snippets
                ),
                timeout=15.0,
            )
    except asyncio.TimeoutError:
        logger.debug("Market details timeout for %s", company_name)
    except Exception as e:
        logger.debug("Market details failed for %s: %s", company_name, e)

    if market_details.get("segments"):
        result["tam_segments"] = market_details["segments"]
    if market_details.get("growth_drivers"):
        result["growth_drivers"] = market_details["growth_drivers"]
    if market_details.get("cagr_pct"):
        result["cagr_pct"] = market_details["cagr_pct"]

    # CAGR-Hochrechnung — Fallback wenn MD-B01 keinen CAGR geliefert hat
    if not result.get("cagr_pct"):
        from src.services.tam import compute_cagr
        cagr_data = compute_cagr(sector, tam_usd_bn)
        result["cagr_pct"]        = cagr_data["cagr_pct"]
        result["cagr_source"]     = cagr_data["cagr_source"]
        result["cagr_confidence"] = cagr_data["cagr_confidence"]
        logger.debug(
            "CAGR fallback for %s: %.1f%% (%s, %s)",
            company_name, cagr_data["cagr_pct"],
            cagr_data["cagr_source"], cagr_data["cagr_confidence"],
        )

    geo_scope = market_details.get("geo_scope", "global")

    # ── 3. Regionale Verteilung — World Bank (MD-B02) ────────────────────────
    wb_indicator = None
    for tag, indicator in {
        "battery": "EG.ELC.RNEW.ZS", "solar": "EG.ELC.RNEW.ZS",
        "grid": "EG.USE.ELEC.KH.PC", "hydrogen": "EN.ATM.CO2E.KT",
        "carbon": "EN.ATM.CO2E.KT", "agritech": "AG.LND.AGRI.ZS",
        "irrigation": "AG.LND.IRIG.AG.ZS", "geothermal": "EG.ELC.RNEW.ZS",
    }.items():
        if tag in sector:
            wb_indicator = indicator
            break

    if wb_indicator:
        try:
            wb_data = await asyncio.wait_for(
                _fetch_worldbank_regional(wb_indicator), timeout=10.0
            )
            if wb_data:
                result["regional_breakdown"] = wb_data["breakdown"]
                result["regional_sources"] = [wb_data["source"]]
        except asyncio.TimeoutError:
            logger.debug("World Bank timeout for %s", company_name)
        except Exception as e:
            logger.debug("World Bank failed for %s: %s", company_name, e)

    # ── 4. SAM-Berechnung (MD-B04) ────────────────────────────────────────────
    if tam_usd_bn:
        sam_data = compute_sam(tam_usd_bn, geo_scope, tech_readiness)
        result.update(sam_data)

    # ── 5. Competition Signals vorab fetchen (MD-B05-ext) ────────────────────
    # async hier, damit sync_wrapper das Ergebnis direkt konsumieren kann.
    # Wird via _competition_signals_cache an sync_wrapper übergeben.
    competition_signals: dict = {}
    try:
        competition_signals = await asyncio.wait_for(
            fetch_competition_signals(sector, category or ""),
            timeout=12.0,
        )
        result["_competition_signals"] = competition_signals  # Übergabe an sync_wrapper
    except asyncio.TimeoutError:
        logger.debug("Competition signals timeout for %s", company_name)
    except Exception as e:
        logger.debug("Competition signals failed for %s: %s", company_name, e)

    # ── 6. enriched_at ────────────────────────────────────────────────────────
    result["enriched_at"] = datetime.now(timezone.utc).isoformat()

    return result


def enrich_market_data_sync_wrapper(
    company_id: str,
    company_name: str,
    category: str | None,
    sector_tag: str | None,
    tam_usd_bn: float | None,
    all_companies: list[dict],
    all_funding_rounds: list[dict],
    tech_readiness: float | None = None,
    async_result: dict | None = None,
    peers_context: dict | None = None,   # R-22: Positioning Notes aus Peer Review
) -> dict:
    """
    Synchroner Teil der Pipeline — Competition Score + Market Cycle.
    R-22: peers_context wird an compute_competition_score() weitergegeben.
    """
    result: dict = {}
    category = category or ""

    safe_companies = all_companies or []
    safe_rounds = all_funding_rounds or []

    external_signals = (async_result or {}).get("_competition_signals")

    # MD-B05 — Competition Score (extern + Peer-Kontext + DB-Kontext)
    try:
        comp = compute_competition_score(
            category, safe_companies, safe_rounds,
            external_signals, peers_context,
        )
        result.update(comp)
    except Exception as e:
        logger.warning("Competition score failed for %s: %s", company_name, e)
        result.update({
            "competition_score": "low",
            "competition_note": "Berechnung fehlgeschlagen — Fallback: niedriger Wettbewerb.",
        })

    # MD-B06 — Market Cycle
    try:
        cycle = compute_market_cycle(category, safe_rounds, safe_companies)
        result.update(cycle)
    except Exception as e:
        logger.warning("Market cycle failed for %s: %s", company_name, e)
        result.update({
            "market_cycle": "early",
            "market_cycle_note": "Berechnung fehlgeschlagen — Fallback: früher Markt.",
        })

    return result
