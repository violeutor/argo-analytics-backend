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

async def _claude_generate_buyers(
    company: dict,
    client: httpx.AsyncClient,
) -> list[BuyerCandidate]:
    """
    Claude Haiku generiert 5–8 strategische Käufer-Kandidaten für eine Company.
    Liefert NAMEN (+ Ticker/Exchange als Hinweis für den Resolver). Der Ticker
    ist KEINE Wahrheit mehr — er hilft dem Resolver nur beim Disambiguieren.
    """
    api_key = settings.anthropic_api_key
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY fehlt — Buyer-Generierung nicht möglich")
        return []

    name     = company.get("name", "")
    category = company.get("category") or "—"
    industry = company.get("industry") or "—"
    region   = company.get("region") or "—"
    stage    = company.get("funding_stage") or "—"
    desc     = (company.get("description") or company.get("summary") or "—")[:300]
    funding  = company.get("funding_total_usd_mn")
    funding_str = f"${funding:.0f}M" if funding else "—"

    prompt = f"""Du bist ein erfahrener M&A-Analyst. Identifiziere 5–8 strategische Käufer für dieses Unternehmen.

Target Company: {name}
Kategorie: {category}
Industrie: {industry}
Region: {region}
Funding Stage: {stage} ({funding_str} gesamt)
Beschreibung: {desc}

Regeln für Käufer:
- Muss börsennotiert sein (sonst keine direkte Partizipation am Käufer möglich)
- Gleiche oder angrenzende Industrie (strategischer Fit, nicht nur finanziell)
- Deutlich größer als das Target (realistischer Akquisiteur)
- Reale, existierende Unternehmen — keine fiktiven Namen
- Mische US + Europa wenn relevant
- strategic_rationale: 1 präziser Satz — welche strategische Logik treibt diese Akquisition?

Ticker/Exchange sind nur HINWEISE für die Disambiguierung — wenn du unsicher bist,
lasse sie leer (""). Der exakte Name ist wichtiger als der Ticker.

Antworte NUR mit einem JSON-Array, kein Markdown:
[
  {{
    "name": "Siemens Energy",
    "ticker": "ENR",
    "exchange": "Frankfurt",
    "confidence": "high",
    "strategic_rationale": "..."
  }}
]"""

    try:
        resp = await client.post(
            _CLAUDE_API_URL,
            headers={
                "x-api-key":         api_key,
                "anthropic-version":  "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 800,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=25.0,
        )

        if resp.status_code != 200:
            logger.warning("Buyer-Gen Claude API %s für %s", resp.status_code, name)
            return []

        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)

        candidates: list[BuyerCandidate] = []
        seen: set[str] = set()
        for item in parsed:
            if not isinstance(item, dict):
                continue
            bname  = (item.get("name") or "").strip()
            ticker = (item.get("ticker") or "").strip().upper() or None
            exch   = (item.get("exchange") or "").strip() or None
            conf   = item.get("confidence", "medium")
            rat    = (item.get("strategic_rationale") or "").strip() or None

            if not bname or bname.lower() in seen:
                continue
            seen.add(bname.lower())
            if conf not in ("high", "medium", "low"):
                conf = "medium"

            candidates.append(BuyerCandidate(
                name=bname,
                ticker_hint=ticker,
                exchange_hint=exch,
                strategic_rationale=rat,
                confidence=conf,
            ))
            if len(candidates) >= 8:
                break

        logger.info("Claude Buyer-Gen für %s: %d Kandidaten", name, len(candidates))
        return candidates

    except Exception as e:
        logger.warning("_claude_generate_buyers failed für %s: %s", name, e)
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


# ── Haupt-Pipeline ──────────────────────────────────────────────────────────────

async def enrich_buyers_for_company(
    company: dict,
    company_id: str,
) -> int:
    """
    Haupt-Pipeline: Gate → Haiku-Namen → Resolve → find-or-create Company →
    Financials-Fill → Kante upserten. Gibt die Anzahl geschriebener Kanten zurück.

    Aufgerufen:
      - On-demand als BackgroundTask in company_detail.py (erster Request)
      - Täglich via Cron in main.py (nur für Screening-Targets, nicht Buyer-Origins)
    """
    from src.integrations.supabase import (
        find_or_create_buyer_company,
        persist_company_financials,
        upsert_potential_buyers,
    )
    from src.services.wikidata_resolver import resolve_entity

    name = company.get("name", "")

    # ── 0. Acquirability-Gate (target-seitig, BUYER-IDENT-01/NVIDIA) ─────────────
    target_mktcap = company.get("market_cap_usd_bn")
    if target_mktcap and float(target_mktcap) > _MAX_TARGET_MKTCAP_FOR_BUYERS_USD_BN:
        logger.info(
            "Buyer-Enrichment: '%s' ist Mega-Cap (%.0f Mrd USD > %.0f) → kein Käufer-Universum "
            "(Investitionspfad = Direktkauf der Aktie)",
            name, float(target_mktcap), _MAX_TARGET_MKTCAP_FOR_BUYERS_USD_BN,
        )
        return 0

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=4.0),
        follow_redirects=True,
    ) as client:
        # ── 1. Haiku generiert Kandidaten-Namen ──────────────────────────────────
        candidates = await _claude_generate_buyers(company, client)
        if not candidates:
            logger.info("Buyer-Enrichment: keine Kandidaten für %s", name)
            return 0

        edges: list[dict] = []
        seen_buyer_ids: set[str] = set()
        now_iso = datetime.now(timezone.utc).isoformat()

        for cand in candidates:
            # ── 2. Resolve (Wikidata) — Auto-Pick listed, kein Modal ──────────────
            try:
                res = await resolve_entity(cand.name, client=client)
            except Exception as e:
                logger.debug("Buyer-Resolve failed für '%s': %s", cand.name, e)
                continue

            picked = _autopick_listed(res)
            if picked is None:
                logger.info(
                    "Buyer-Resolve: '%s' nicht als listed auflösbar (reason=%s) → verworfen",
                    cand.name, getattr(res, "reason", "?"),
                )
                continue

            resolved_name   = getattr(picked, "display_name", None) or cand.name
            resolved_ticker = getattr(picked, "ticker", None) or cand.ticker_hint
            # Exchange in Hinweis-Form (matcht _EXCHANGE_SUFFIX + companies.exchange).
            resolved_exch   = cand.exchange_hint or getattr(picked, "display_exchange", None)
            founded         = getattr(picked, "founded_year", None)
            try:
                founded_int = int(founded) if founded else None
            except (ValueError, TypeError):
                founded_int = None

            # ── 3. find-or-create Buyer-Company ───────────────────────────────────
            buyer_co = find_or_create_buyer_company(
                name=resolved_name,
                ticker=resolved_ticker,
                exchange=resolved_exch,
                headquarters=getattr(picked, "headquarters", None),
                founding_year=founded_int,
            )
            if not buyer_co or not buyer_co.get("id"):
                continue
            buyer_id = buyer_co["id"]
            if buyer_id == company_id:
                continue  # Self-Edge-Schutz (CHECK fängt es zusätzlich ab)
            if buyer_id in seen_buyer_ids:
                continue  # Dedup innerhalb dieses Targets
            seen_buyer_ids.add(buyer_id)

            # ── 4. Financials-Fill: nur wenn die Buyer-Company noch keine hat ─────
            if not buyer_co.get("market_cap_usd_bn"):
                yt = _yahoo_ticker(resolved_ticker or "", resolved_exch)
                if yt:
                    try:
                        fin = await asyncio.wait_for(
                            asyncio.to_thread(_fetch_company_financials_sync, yt, resolved_exch),
                            timeout=12.0,
                        )
                        persist_company_financials(
                            buyer_id,
                            fin.get("market_cap_usd_bn"),
                            fin.get("cash_usd_bn"),
                            fin.get("debt_ebitda"),
                        )
                    except asyncio.TimeoutError:
                        logger.warning("Company-Fin Timeout für %s (%s)", resolved_name, yt)
                    except Exception as e:
                        logger.debug("Company-Fin failed für %s (%s): %s", resolved_name, yt, e)

            # ── 5. Kante sammeln ──────────────────────────────────────────────────
            edges.append({
                "target_company_id":   company_id,
                "buyer_company_id":    buyer_id,
                "strategic_rationale": cand.strategic_rationale,
                "confidence":          cand.confidence,
                "generated_at":        now_iso,
            })

        if not edges:
            logger.info("Buyer-Enrichment %s: 0 Kanten nach Resolve", name)
            return 0

        written = upsert_potential_buyers(edges)
        logger.info(
            "Buyer-Enrichment %s: %d Kandidaten → %d Kanten geschrieben",
            name, len(candidates), written,
        )
        return written
