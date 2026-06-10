"""
BUYER-AS-COMPANY-01 · buyer_enrichment.py
Pfad: argo-analytics-backend/src/services/buyer_enrichment.py

Generiert ein company-spezifisches Käufer-Universum für M&A-Scoring.

Architektur (Rewrite ggü. R-23):
  Ein Buyer IST eine Company. Identität + Financials kommen aus dem
  deterministischen Resolver + der companies-SSOT, NICHT mehr aus einem
  selbstberichteten Haiku-Ticker + parallelem yfinance-Pfad.

Pipeline:
  0. Acquirability-Gate (target-seitig): Mega-Cap-Target → kein Käufer-Universum
     (BUYER-IDENT-01/NVIDIA — Investitionspfad dort = Direktkauf der Aktie).
  1. Claude Haiku → 5–8 strategische Käufer-NAMEN (+ Ticker nur als Hinweis).
     Haiku ist Kandidaten-Generator, nicht Identitäts-Quelle.
  2. resolve_entity (Wikidata) → Auto-Pick des obersten listed-Kandidaten,
     kein Modal. Nicht-listed/unauflösbar → drop (kein null-financial-Junk).
  3. find_or_create_buyer_company → companies-Row (dedup über Ticker/Name).
  4. Financials-Fill: hat die Buyer-Company noch keine market_cap, einmal via
     yfinance holen und in die companies-SSOT-Spalte persistieren (derselbe
     eine Call wie früher, nur in die richtige Tabelle).
  5. Kante (target_company_id, buyer_company_id, rationale, confidence) upserten.

Was strukturell verschwindet:
  - market_cap=NULL-Dauerzustand (kommt jetzt aus companies, self-healing).
  - Cross-Target-Junk (derselbe Buyer = dieselbe buyer_company_id, keine 1000 Rows).
  - Vertrauen in den Haiku-Ticker (Resolver-Anker statt Format-Regex).

Selektionslogik (welche Kandidaten Haiku findet) bleibt bewusst unverändert →
BUYER-IDENT-02 (fundierte Selektion aus Verflechtungen) ist ein eigenes Ticket.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
_CACHE_TTL_DAYS = 30

# BUYER-IDENT-01 (NVIDIA) · target-seitiges Acquirability-Gate.
# Ein Target oberhalb dieser market_cap ist faktisch nicht übernehmbar —
# der Investitionspfad ist dort Direktkauf der Aktie, kein Käufer-Universum.
# Tunable: bei Bedarf justieren, wenn das Target-Spektrum sich verschiebt.
_MAX_TARGET_MKTCAP_FOR_BUYERS_USD_BN = 100.0

# ── BUYER-IDENT-02 · Pool-Aufbau + Size-Gate ────────────────────────────────────
# Ein potential buyer ist eine börsennotierte Firma, die (a) strategisch an das
# Target angrenzt (Quelle = source_type) UND (b) materiell größer ist (Size-Gate).
# Korrektheit trägt allein das Size-Gate. source_type/relevance sind Provenance +
# Ranking + rationale-Anker, NIE korrektheitstragend.

# Size-Gate (Korrektheit). Getrennt von Ranking — das Gate wirft nur das
# strukturell Unmögliche raus; den 3–5×-Stretch differenziert die Scoring-Engine
# (MFR/_DEPLOYABLE_CASH_FRACTION/execution_warning), nicht dieses Gate.
_MIN_BUYER_SIZE_RATIO          = 3.0   # listed Target: hartes Feasibility-Minimum
_COMFORTABLE_BUYER_SIZE_RATIO  = 5.0   # nur Ranking-Signal (≥5× = komfortabel)
_MIN_BUYER_MKTCAP_FLOOR_USD_BN = 2.0   # privates Target: Micro-Cap-Backstop

# Pool-Kaskade. Layer 1 (supply_chain + peers) läuft immer (kuratiert/DB, kein
# externer Call außer 1 DB-Read). Sektor (Wikidata) + Adjazenz (Haiku) ziehen
# nur, wenn der Pool darunter bleibt — kostenbewusst, Haiku als Gap-Filler.
_MIN_POOL        = 4
_MAX_ADJACENT_LLM = 3

# Provenance-Prioritätsreihenfolge für Pool-Dedup (höchste zuerst). Same identity
# aus mehreren Quellen → die kuratierte/stärkere gewinnt. Reine Sortier-Hilfe,
# entscheidet NICHT über Aufnahme (das tut das Size-Gate).
_SOURCE_PRIORITY: dict[str, int] = {
    "supply_downstream": 0,
    "supply_upstream":   1,
    "peer_larger":       2,
    "same_sector":       3,
    "adjacent_llm":      4,
}


# ── Kandidaten-Datenklasse (Haiku-Output, vor Resolve) ──────────────────────────

class BuyerCandidate:
    """Roher Haiku-Vorschlag — Name + Hinweise, noch nicht aufgelöst."""
    __slots__ = ("name", "ticker_hint", "exchange_hint", "strategic_rationale", "confidence")

    def __init__(
        self,
        name: str,
        ticker_hint: str | None = None,
        exchange_hint: str | None = None,
        strategic_rationale: str | None = None,
        confidence: str = "medium",
    ):
        self.name                = name
        self.ticker_hint         = ticker_hint
        self.exchange_hint       = exchange_hint
        self.strategic_rationale = strategic_rationale
        self.confidence          = confidence


# ── Cache-Check (TTL auf der Kante) ─────────────────────────────────────────────

def is_cache_valid(buyers: list[dict]) -> bool:
    """
    Prüft ob die gecachten Buyer-Kanten noch gültig sind (< 30d).
    `buyers` ist der JOIN-Output von fetch_potential_buyers (enthält generated_at
    aus der Kante). Alters-basiert — die Datenqualität (market_cap) heilt sich
    jetzt selbst über die companies-SSOT, daher kein Daten-Gate mehr nötig.
    """
    if not buyers:
        return False
    try:
        gen_at = buyers[0].get("generated_at")
        if not gen_at:
            return False
        gen_dt = datetime.fromisoformat(gen_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - gen_dt).days < _CACHE_TTL_DAYS
    except Exception:
        return False


# ── Schritt 1: Claude generiert Käufer-Namen ────────────────────────────────────

async def _claude_generate_adjacent(
    company: dict,
    client: httpx.AsyncClient,
    limit: int = _MAX_ADJACENT_LLM,
) -> list[dict]:
    """
    BUYER-IDENT-02 · Layer 3 (Gap-Filler). Haiku generiert NUR Kandidaten aus
    ANGRENZENDEN Sektoren — Capability-Buys über die Sektorgrenze (z.B. ein
    Autobauer kauft eine Lidar-Firma). Same-Sector deckt der deterministische
    Wikidata-Layer ab; Supply-Chain die kuratierte Schiene. Haikus Stärke ist
    genau die Sektor-Adjazenz, die kein Graph sauber kennt.

    Gibt normalisierte Pool-Dicts zurück (source_type='adjacent_llm',
    needs_resolve=True — der Name ist ein Vorschlag, die Identität klärt der
    Wikidata-Resolver, das Size-Gate entscheidet über Aufnahme).
    """
    api_key = settings.anthropic_api_key
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY fehlt — Adjazenz-Generierung nicht möglich")
        return []

    name     = company.get("name", "")
    category = company.get("category") or "—"
    industry = company.get("industry") or "—"
    region   = company.get("region") or "—"
    desc     = (company.get("description") or company.get("summary") or "—")[:300]

    prompt = f"""Du bist ein erfahrener M&A-Analyst. Nenne {limit} börsennotierte Unternehmen aus ANGRENZENDEN Sektoren (NICHT demselben Sektor), die dieses Target übernehmen würden, um dessen Technologie für den EIGENEN Bedarf zu nutzen (Capability-Buy über die Sektorgrenze).

Target Company: {name}
Kategorie: {category}
Industrie: {industry}
Region: {region}
Beschreibung: {desc}

Regeln:
- NICHT derselbe Sektor wie das Target (das wird separat abgedeckt) — echte Sektor-Adjazenz
- Börsennotiert, real existierend, deutlich größer als das Target
- rationale: 1 Satz — welchen eigenen Bedarf deckt der Käufer mit dieser Technologie?
- Ticker/Exchange nur als Hinweis; bei Unsicherheit leer ("")

Antworte NUR mit JSON-Array, kein Markdown:
[
  {{"name": "Volkswagen", "ticker": "VOW3", "exchange": "Frankfurt", "rationale": "..."}}
]"""

    try:
        resp = await client.post(
            _CLAUDE_API_URL,
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 600,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=25.0,
        )
        if resp.status_code != 200:
            logger.warning("Adjazenz-Gen Claude API %s für %s", resp.status_code, name)
            return []

        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)

        out: list[dict] = []
        seen: set[str] = set()
        for item in parsed:
            if not isinstance(item, dict):
                continue
            bname = (item.get("name") or "").strip()
            if not bname or bname.lower() in seen:
                continue
            seen.add(bname.lower())
            out.append(_mk_pool_item(
                name=bname,
                source_type="adjacent_llm",
                ticker_hint=(item.get("ticker") or "").strip().upper() or None,
                exchange_hint=(item.get("exchange") or "").strip() or None,
                rationale_seed=(item.get("rationale") or "").strip() or None,
                confidence="low",
                needs_resolve=True,
            ))
            if len(out) >= limit:
                break

        logger.info("Adjazenz-Gen für %s: %d Kandidaten", name, len(out))
        return out

    except Exception as e:
        logger.warning("_claude_generate_adjacent failed für %s: %s", name, e)
        return []


# ── Schritt 4-Helfer: yfinance-Financials (für companies-SSOT-Fill) ─────────────

# Exchange → Yahoo-Suffix (Hinweis-Form von Haiku, deckungsgleich mit company_detail)
_EXCHANGE_SUFFIX: dict[str, str] = {
    "frankfurt": ".DE", "xetra": ".DE", "paris": ".PA", "amsterdam": ".AS",
    "madrid": ".MC", "milan": ".MI", "milano": ".MI", "london": ".L",
    "stockholm": ".ST", "zurich": ".SW", "zürich": ".SW", "euronext": ".PA",
    "nasdaq": "", "nyse": "", "nyse arca": "",
}


def _yahoo_ticker(ticker: str, exchange: str | None) -> str:
    """Baut Yahoo-Ticker aus Ticker + Exchange (z.B. 'ENR' + 'Frankfurt' → 'ENR.DE')."""
    if not ticker:
        return ""
    if not exchange:
        return ticker
    suffix = _EXCHANGE_SUFFIX.get(exchange.lower().strip(), "")
    if suffix and not ticker.endswith(suffix):
        return ticker + suffix
    return ticker


def _fetch_company_financials_sync(ticker: str, exchange: str | None) -> dict:
    """
    Synchroner yfinance-Abruf: market_cap_usd_bn + cash_usd_bn + debt_ebitda.
    Schreibt in die companies-SSOT (nicht in eine Buyer-Spalte). Werte None
    wenn nicht beschaffbar. Währung via valuation.to_usd normalisiert.
    """
    import yfinance as yf
    import pandas as pd
    from src.services.valuation import to_usd

    out = {"market_cap_usd_bn": None, "cash_usd_bn": None, "debt_ebitda": None}

    candidates = [ticker]
    if ticker.upper().endswith(".DE"):
        candidates.append(ticker[:-3] + ".F")

    for sym in candidates:
        try:
            t  = yf.Ticker(sym)
            fi = t.fast_info
            currency    = getattr(fi, "currency", None) or "USD"
            mcap_native = getattr(fi, "market_cap", None)
            if not mcap_native or float(mcap_native) <= 0:
                continue

            mcap_usd = to_usd(float(mcap_native), currency)
            if mcap_usd is None:
                logger.debug("Company-Fin: unbekannte Währung '%s' für %s", currency, sym)
                continue
            out["market_cap_usd_bn"] = round(mcap_usd / 1e9, 2)

            try:
                bs = t.balance_sheet
                if bs is not None and not bs.empty and len(bs.columns) > 0:
                    col = bs.columns[0]
                    for row_name in ("Cash And Cash Equivalents",
                                     "Cash Cash Equivalents And Short Term Investments"):
                        if row_name in bs.index:
                            cv = bs.loc[row_name, col]
                            if not pd.isna(cv):
                                cash_usd = to_usd(float(cv), currency)
                                if cash_usd is not None:
                                    out["cash_usd_bn"] = round(cash_usd / 1e9, 2)
                                break

                    total_debt = None
                    if "Total Debt" in bs.index and not pd.isna(bs.loc["Total Debt", col]):
                        total_debt = float(bs.loc["Total Debt", col])

                inc = t.income_stmt
                ebitda = None
                if inc is not None and not inc.empty and len(inc.columns) > 0:
                    ic = inc.columns[0]
                    if "EBITDA" in inc.index and not pd.isna(inc.loc["EBITDA", ic]):
                        ebitda = float(inc.loc["EBITDA", ic])

                if total_debt is not None and ebitda and ebitda > 0:
                    out["debt_ebitda"] = round(total_debt / ebitda, 2)
            except Exception as _e:
                logger.debug("Company-Fin: Statement-Parse für %s teilweise fehlgeschlagen: %s", sym, _e)

            return out
        except Exception as e:
            logger.debug("Company-Fin sync failed für %s: %s", sym, e)
            continue

    return out


# ── Resolver-Auto-Pick (kein Modal) ─────────────────────────────────────────────

def _autopick_listed(result) -> "object | None":
    """
    Nicht-interaktiver Pick aus einem WikidataResolutionResult.
    Ein Buyer MUSS börsennotiert sein → wir nehmen den obersten listed-Kandidaten
    (candidates sind listed-first sortiert). resolved nur, wenn selbst listed.
    None → kein listed-Treffer → Buyer wird verworfen (privat/unauflösbar).
    """
    resolved = getattr(result, "resolved", None)
    if resolved is not None and getattr(resolved, "is_listed", False):
        return resolved
    for c in getattr(result, "candidates", []) or []:
        if getattr(c, "is_listed", False):
            return c
    return None


# ── BUYER-IDENT-02 · Pool-Builder + Size-Gate ───────────────────────────────────

def _mk_pool_item(
    *,
    name: str,
    source_type: str,
    ticker_hint: str | None = None,
    exchange_hint: str | None = None,
    source_relevance: float | None = None,
    rationale_seed: str | None = None,
    confidence: str = "medium",
    needs_resolve: bool = False,
    buyer_company_id: str | None = None,
    prefetched_mcap: float | None = None,
) -> dict:
    """Normalisierter Pool-Kandidat — eine Form über alle Quellen."""
    return {
        "name":             name,
        "source_type":      source_type,
        "ticker_hint":      ticker_hint,
        "exchange_hint":    exchange_hint,
        "source_relevance": source_relevance,
        "rationale_seed":   rationale_seed,
        "confidence":       confidence,
        "needs_resolve":    needs_resolve,
        "buyer_company_id": buyer_company_id,   # gesetzt für peers (schon companies-Row)
        "prefetched_mcap":  prefetched_mcap,
    }


def _target_size_usd_bn(company: dict) -> float | None:
    """Target-Größe für das Size-Gate. None = privates Target (kein market_cap)."""
    mc = company.get("market_cap_usd_bn")
    try:
        return float(mc) if mc else None
    except (TypeError, ValueError):
        return None


def _passes_size_gate(buyer_mcap: float | None, target_mcap: float | None) -> bool:
    """
    Korrektheits-Filter (das EINZIGE korrektheitstragende Kriterium).
      - buyer_mcap fehlt/0 → nicht verifizierbar → raus.
      - Listed Target (target_mcap gesetzt): Ratio (hartes Feasibility-Minimum).
        Den 3–5×-Stretch differenziert die Scoring-Engine, nicht dieses Gate.
      - Privates Target (kein market_cap): absoluter Floor gegen Micro-Caps.
    """
    if not buyer_mcap or buyer_mcap <= 0:
        return False
    if target_mcap and target_mcap > 0:
        return buyer_mcap >= target_mcap * _MIN_BUYER_SIZE_RATIO
    return buyer_mcap >= _MIN_BUYER_MKTCAP_FLOOR_USD_BN


# ── Harvest-Layer ───────────────────────────────────────────────────────────────

def _harvest_supply_chain(company: dict) -> list[dict]:
    """
    Layer 1a (kuratiert, deterministisch, 0 externe Calls). supply_chain.py liefert
    große listed Firmen, die an das Technologie-Thema des Targets angrenzen — mit
    kuratierter `relevance` (→ source_relevance) und `role` (→ rationale_seed).
    Identität ist kuratiert → kein Wikidata-Resolve nötig (needs_resolve=False).
    """
    try:
        from src.services.supply_chain import COMPANY_TAGS, get_supply_chain
    except ImportError as e:
        logger.warning("supply_chain-Layer übersprungen (Import fehlgeschlagen): %s", e)
        return []

    name = company.get("name", "")
    tags = COMPANY_TAGS.get(name) or company.get("tags") or []
    if not tags:
        return []

    sc = get_supply_chain(tags)
    out: list[dict] = []
    for direction, st in (("upstream", "supply_upstream"),
                          ("downstream", "supply_downstream")):
        for item in sc.get(direction, []):
            tk = (item.get("ticker") or "").strip().upper() or None
            out.append(_mk_pool_item(
                name=item.get("name") or tk or "",
                source_type=st,
                ticker_hint=tk,
                exchange_hint=item.get("exchange") or None,
                source_relevance=item.get("relevance"),
                rationale_seed=item.get("role"),
                confidence="high",
                needs_resolve=False,
            ))
    return out


def _harvest_peers(company: dict, company_id: str | None) -> list[dict]:
    """
    Layer 1b (DB). peers_resolved sind schon companies-Rows → market_cap gratis
    (prefetched_mcap), kein Resolve. Größenfilter macht später das Gate.
    peers_resolved wird AUTORITATIV aus der DB gelesen (fetch_peers_resolved) —
    die Cold-Path-Konsolidierung schreibt sie evtl. nach dem company-Snapshot;
    Fallback auf den Snapshot, falls company_id fehlt.
    """
    from src.integrations.supabase import fetch_companies_by_ids, fetch_peers_resolved

    peer_ids = fetch_peers_resolved(company_id) if company_id else []
    if not peer_ids:
        peer_ids = company.get("peers_resolved") or []
    if not peer_ids:
        return []

    out: list[dict] = []
    for p in fetch_companies_by_ids(peer_ids):
        if not p.get("id"):
            continue
        out.append(_mk_pool_item(
            name=p.get("name") or "",
            source_type="peer_larger",
            ticker_hint=p.get("ticker"),
            exchange_hint=p.get("exchange"),
            rationale_seed="Etablierter Wettbewerber im selben Raum",
            confidence="medium",
            needs_resolve=False,
            buyer_company_id=p["id"],
            prefetched_mcap=p.get("market_cap_usd_bn"),
        ))
    return out


async def _harvest_sector(company: dict, client: httpx.AsyncClient) -> list[dict]:
    """
    Layer 2 (Wikidata, conditional). Listed Sektor-Incumbents über P452 (Industrie).
    Bereits aufgelöst (Ticker/Exchange aus Wikidata) → needs_resolve=False.
    """
    from src.services.wikidata_resolver import find_sector_incumbents

    industry = company.get("industry") or company.get("category")
    if not industry:
        return []
    try:
        cands = await find_sector_incumbents(industry, client=client)
    except Exception as e:
        logger.debug("Sector-Harvest failed für '%s': %s", industry, e)
        return []

    out: list[dict] = []
    for c in cands:
        if not getattr(c, "is_listed", False):
            continue
        out.append(_mk_pool_item(
            name=getattr(c, "display_name", None) or getattr(c, "name", ""),
            source_type="same_sector",
            ticker_hint=getattr(c, "ticker", None),
            exchange_hint=getattr(c, "display_exchange", None),
            rationale_seed=f"Sektor-Incumbent ({industry})",
            confidence="medium",
            needs_resolve=False,
        ))
    return out


def _dedup_pool(pool: list[dict]) -> list[dict]:
    """
    Dieselbe Identität aus mehreren Quellen → kuratierte/stärkere Quelle gewinnt
    (Sortierung nach _SOURCE_PRIORITY vor dem Dedup). Key: Ticker, sonst Name.
    """
    ranked = sorted(pool, key=lambda x: _SOURCE_PRIORITY.get(x["source_type"], 9))
    seen: set[str] = set()
    out: list[dict] = []
    for item in ranked:
        key = (item.get("ticker_hint") or item.get("name") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


async def _build_buyer_pool(company: dict, company_id: str | None, client: httpx.AsyncClient) -> list[dict]:
    """
    Kaskade: Layer 1 (supply_chain + peers) immer; Sektor (Wikidata) + Adjazenz
    (Haiku) nur, wenn der Pool darunter unter _MIN_POOL bleibt — kostenbewusst.
    Gibt deduplizierte, nach Provenance-Priorität geordnete Kandidaten zurück.
    """
    pool: list[dict] = []
    pool += _harvest_supply_chain(company)
    pool += _harvest_peers(company, company_id)

    if len(pool) < _MIN_POOL:
        pool += await _harvest_sector(company, client)
    if len(pool) < _MIN_POOL:
        pool += await _claude_generate_adjacent(company, client)

    deduped = _dedup_pool(pool)
    by_src = ", ".join(sorted({c["source_type"] for c in deduped})) or "—"
    logger.info("Buyer-Pool '%s': %d Kandidaten (%s)",
                company.get("name", ""), len(deduped), by_src)
    return deduped


# ── Haupt-Pipeline ──────────────────────────────────────────────────────────────

async def enrich_buyers_for_company(
    company: dict,
    company_id: str,
) -> int:
    """
    Haupt-Pipeline (BUYER-IDENT-02): Gate → Pool aufbauen (supply_chain/peers/
    sector/adjacency) → je Kandidat resolve (nur adjacent_llm) → find-or-create →
    Financials-Fill → SIZE-GATE → Kante mit source_type/source_relevance upserten.
    Gibt die Anzahl geschriebener Kanten zurück.

    Aufgerufen:
      - On-demand als BackgroundTask in company_detail.py (erster Request)
      - Täglich via Cron in main.py (nur Screening-Targets, nicht Buyer-Origins)
    """
    from src.integrations.supabase import (
        find_or_create_buyer_company,
        persist_company_financials,
        upsert_potential_buyers,
    )
    from src.services.wikidata_resolver import resolve_entity

    name        = company.get("name", "")
    target_size = _target_size_usd_bn(company)

    # ── 0. Acquirability-Gate (target-seitig, BUYER-IDENT-01/NVIDIA) ─────────────
    if target_size and target_size > _MAX_TARGET_MKTCAP_FOR_BUYERS_USD_BN:
        logger.info(
            "Buyer-Enrichment: '%s' ist Mega-Cap (%.0f Mrd USD > %.0f) → kein Käufer-Universum "
            "(Investitionspfad = Direktkauf der Aktie)",
            name, target_size, _MAX_TARGET_MKTCAP_FOR_BUYERS_USD_BN,
        )
        return 0

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=4.0),
        follow_redirects=True,
    ) as client:
        # ── 1. Pool aufbauen (deterministisch first, extern nur zum Auffüllen) ────
        pool = await _build_buyer_pool(company, company_id, client)
        if not pool:
            logger.info("Buyer-Enrichment: leerer Pool für %s", name)
            return 0

        edges: list[dict] = []
        seen_buyer_ids: set[str] = set()
        now_iso = datetime.now(timezone.utc).isoformat()

        for cand in pool:
            buyer_id  = cand.get("buyer_company_id")   # gesetzt nur für peers
            buyer_co  = None
            buyer_mcap = cand.get("prefetched_mcap")

            if not buyer_id:
                # ── 2. Resolve (Wikidata) — nur für adjacent_llm (Name → Identität) ──
                r_name   = cand["name"]
                r_ticker = cand.get("ticker_hint")
                r_exch   = cand.get("exchange_hint")
                r_hq     = None
                r_founded: int | None = None

                if cand.get("needs_resolve"):
                    try:
                        res = await resolve_entity(cand["name"], client=client)
                    except Exception as e:
                        logger.debug("Buyer-Resolve failed für '%s': %s", cand["name"], e)
                        continue
                    picked = _autopick_listed(res)
                    if picked is None:
                        logger.info(
                            "Buyer-Resolve: '%s' nicht als listed auflösbar (reason=%s) → verworfen",
                            cand["name"], getattr(res, "reason", "?"),
                        )
                        continue
                    r_name   = getattr(picked, "display_name", None) or cand["name"]
                    r_ticker = getattr(picked, "ticker", None) or cand.get("ticker_hint")
                    r_exch   = cand.get("exchange_hint") or getattr(picked, "display_exchange", None)
                    r_hq     = getattr(picked, "headquarters", None)
                    founded  = getattr(picked, "founded_year", None)
                    try:
                        r_founded = int(founded) if founded else None
                    except (ValueError, TypeError):
                        r_founded = None

                # ── 3. find-or-create Buyer-Company ───────────────────────────────
                buyer_co = find_or_create_buyer_company(
                    name=r_name, ticker=r_ticker, exchange=r_exch,
                    headquarters=r_hq, founding_year=r_founded,
                )
                if not buyer_co or not buyer_co.get("id"):
                    continue
                buyer_id   = buyer_co["id"]
                buyer_mcap = buyer_co.get("market_cap_usd_bn")

            # Self-Edge- + Dedup-Schutz (CHECK fängt Self-Edge zusätzlich ab)
            if buyer_id == company_id or buyer_id in seen_buyer_ids:
                continue
            seen_buyer_ids.add(buyer_id)

            # ── 4. Financials-Fill: nur wenn noch keine market_cap bekannt ────────
            if not buyer_mcap:
                tk = (buyer_co.get("ticker")   if buyer_co else None) or cand.get("ticker_hint")
                ex = (buyer_co.get("exchange") if buyer_co else None) or cand.get("exchange_hint")
                yt = _yahoo_ticker(tk or "", ex)
                if yt:
                    try:
                        fin = await asyncio.wait_for(
                            asyncio.to_thread(_fetch_company_financials_sync, yt, ex),
                            timeout=12.0,
                        )
                        persist_company_financials(
                            buyer_id,
                            fin.get("market_cap_usd_bn"),
                            fin.get("cash_usd_bn"),
                            fin.get("debt_ebitda"),
                        )
                        buyer_mcap = fin.get("market_cap_usd_bn")
                    except asyncio.TimeoutError:
                        logger.warning("Company-Fin Timeout für %s (%s)", cand["name"], yt)
                    except Exception as e:
                        logger.debug("Company-Fin failed für %s (%s): %s", cand["name"], yt, e)

            # ── 5. SIZE-GATE (Korrektheit) — materiell größer als das Target ──────
            if not _passes_size_gate(buyer_mcap, target_size):
                logger.debug(
                    "Size-Gate: '%s' (mcap=%s) unter Schwelle für target_size=%s [%s] → raus",
                    cand["name"], buyer_mcap, target_size, cand["source_type"],
                )
                continue

            # ── 6. Kante mit Provenance ───────────────────────────────────────────
            edges.append({
                "target_company_id":   company_id,
                "buyer_company_id":    buyer_id,
                "strategic_rationale": cand.get("rationale_seed"),
                "confidence":          cand.get("confidence", "medium"),
                "source_type":         cand["source_type"],
                "source_relevance":    cand.get("source_relevance"),
                "generated_at":        now_iso,
            })

        if not edges:
            logger.info("Buyer-Enrichment %s: 0 Kanten nach Pool+Size-Gate", name)
            return 0

        written = upsert_potential_buyers(edges)
        logger.info(
            "Buyer-Enrichment %s: Pool=%d → %d Kanten (%s)",
            name, len(pool), written,
            ", ".join(sorted({e["source_type"] for e in edges})),
        )
        return written
