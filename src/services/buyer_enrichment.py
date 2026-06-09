"""
R-23 · buyer_enrichment.py
Pfad: argo-analytics-backend/src/services/buyer_enrichment.py

Generiert ein company-spezifisches Käufer-Universum für M&A-Scoring.

Pipeline:
  1. Claude Haiku → 5–8 strategische Käufer (public, industry-adjacent, groß genug)
     + strategic_rationale pro Käufer (warum Käufer, welche strategische Logik)
  2. Yahoo Finance → market_cap_usd_bn per Ticker
  3. Upsert in potential_buyers (UNIQUE company_id + name → kein Duplikat)

Architektur-Entscheidungen:
  - Kein Fallback auf globale 8 Seed-Buyers (Datentransparenz-Prinzip)
  - Keine Daten → scorings=[] + BackgroundTask → UI zeigt "—"
  - TTL 30d via generated_at (analog zu peers.py)
  - Yahoo-Fehler → market_cap_usd_bn=None → Buyer wird im Scoring-Loop übersprungen
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone, timedelta

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
_CACHE_TTL_DAYS = 30


# ── Datenklasse ───────────────────────────────────────────────────────────────

class PotentialBuyer:
    __slots__ = (
        "name", "ticker", "exchange",
        "market_cap_usd_bn", "strategic_rationale", "confidence",
        "cash_usd_bn", "debt_ebitda",
    )

    def __init__(
        self,
        name: str,
        ticker: str | None = None,
        exchange: str | None = None,
        market_cap_usd_bn: float | None = None,
        strategic_rationale: str | None = None,
        confidence: str = "medium",
        cash_usd_bn: float | None = None,
        debt_ebitda: float | None = None,
    ):
        self.name                = name
        self.ticker              = ticker
        self.exchange            = exchange
        self.market_cap_usd_bn   = market_cap_usd_bn
        self.strategic_rationale = strategic_rationale
        self.confidence          = confidence
        self.cash_usd_bn         = cash_usd_bn
        self.debt_ebitda         = debt_ebitda

    def to_db_dict(self, company_id: str) -> dict:
        return {
            "company_id":          company_id,
            "name":                self.name,
            "ticker":              self.ticker,
            "exchange":            self.exchange,
            "market_cap_usd_bn":   self.market_cap_usd_bn,
            "strategic_rationale": self.strategic_rationale,
            "confidence":          self.confidence,
            "cash_usd_bn":         self.cash_usd_bn,
            "debt_ebitda":         self.debt_ebitda,
            "generated_at":        datetime.now(timezone.utc).isoformat(),
        }


# ── Cache-Check ───────────────────────────────────────────────────────────────

def is_cache_valid(buyers: list[dict]) -> bool:
    """Prüft ob gecachte Buyers noch gültig sind (< 30d)."""
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


# ── Schritt 1: Claude generiert Käufer ───────────────────────────────────────

async def _claude_generate_buyers(
    company: dict,
    client: httpx.AsyncClient,
) -> list[PotentialBuyer]:
    """
    Claude Haiku generiert 5–8 strategische Käufer für eine Company.
    Gibt Liste von PotentialBuyer zurück (ohne market_cap — kommt von Yahoo).
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
- Muss börsennotiert sein (Ticker + Börse nötig für MFR-Berechnung)
- Gleiche oder angrenzende Industrie (strategischer Fit, nicht nur finanziell)
- Marktkapitalisierung mindestens 5× die geschätzte Target-Bewertung
- Reale, existierende Unternehmen — keine fiktiven Namen
- Mische US + Europa wenn relevant
- strategic_rationale: 1 präziser Satz — welche strategische Logik treibt diese Akquisition?

KRITISCH für Ticker:
- Nur verifizierte, echte Börsenticker verwenden — keine erfundenen Kürzel
- US-Ticker: 1–5 Buchstaben (z.B. MSFT, GE, HON, CRM)
- DE-Ticker: meist 3 Buchstaben ohne Suffix (z.B. ENR, SIE, BAS)
- Wenn du den Ticker eines Unternehmens nicht sicher kennst, lasse ticker leer ("")

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

        buyers: list[PotentialBuyer] = []
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

            # Ticker-Validierung: Format-Check vor Yahoo-Fetch
            if ticker:
                if not re.match(r"^[A-Z0-9]{1,6}$", ticker):
                    logger.warning(
                        "Buyer-Gen: Ticker '%s' für '%s' hat ungültiges Format — wird geleert",
                        ticker, bname,
                    )
                    ticker = None

            buyers.append(PotentialBuyer(
                name=bname,
                ticker=ticker,
                exchange=exch,
                strategic_rationale=rat,
                confidence=conf,
            ))
            if len(buyers) >= 8:
                break

        logger.info("Claude Buyer-Gen für %s: %d Käufer", name, len(buyers))
        return buyers

    except Exception as e:
        logger.warning("_claude_generate_buyers failed für %s: %s", name, e)
        return []


# ── Schritt 2: Yahoo Finance — market_cap_usd_bn ─────────────────────────────

_YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"

# Exchange → Yahoo-Suffix
_EXCHANGE_SUFFIX: dict[str, str] = {
    "frankfurt": ".DE",
    "xetra":     ".DE",
    "paris":     ".PA",
    "amsterdam": ".AS",
    "madrid":    ".MC",
    "milan":     ".MI",
    "london":    ".L",
    "stockholm": ".ST",
    "zurich":    ".SW",
    "euronext":  ".PA",
    # US-Börsen: kein Suffix
    "nasdaq":    "",
    "nyse":      "",
    "nyse arca": "",
}


def _yahoo_ticker(ticker: str, exchange: str | None) -> str:
    """Baut Yahoo-Ticker aus Ticker + Exchange (z.B. 'ENR' + 'Frankfurt' → 'ENR.DE')."""
    if not exchange:
        return ticker
    suffix = _EXCHANGE_SUFFIX.get(exchange.lower().strip(), "")
    if suffix and not ticker.endswith(suffix):
        return ticker + suffix
    return ticker


def _fetch_buyer_financials_sync(ticker: str, exchange: str | None) -> dict:
    """
    Synchroner yfinance-Abruf: market_cap_usd_bn + cash_usd_bn + debt_ebitda.

    BUYER-MKTCAP-01 + BUYER-FIN-01: ersetzt den kaputten chart-meta-Pfad
    (marketCap existierte dort nie) und beschafft zusätzlich die echten
    Buyer-Financials, die vorher im Scoring hartcodiert waren
    (cash = mcap×0.05, debt_ebitda = 1.5 — beide erfunden).

    cash_usd_bn:  t.balance_sheet 'Cash And Cash Equivalents' → USD-Mrd
    debt_ebitda:  Total Debt / EBITDA (beide aus yfinance). None wenn EBITDA<=0
                  oder Daten fehlen → Scoring lässt debt-Korrektur dann weg (1c).

    Returns {"market_cap_usd_bn", "cash_usd_bn", "debt_ebitda"} — Werte None
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
                logger.debug("Buyer-Fin: unbekannte Währung '%s' für %s", currency, sym)
                continue
            out["market_cap_usd_bn"] = round(mcap_usd / 1e9, 2)

            # Cash + Debt + EBITDA aus Statements (best-effort, Fehler tolerieren)
            try:
                bs = t.balance_sheet
                if bs is not None and not bs.empty and len(bs.columns) > 0:
                    col = bs.columns[0]   # jüngste Periode
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
                logger.debug("Buyer-Fin: Statement-Parse für %s teilweise fehlgeschlagen: %s", sym, _e)

            return out
        except Exception as e:
            logger.debug("Buyer-Fin sync failed für %s: %s", sym, e)
            continue

    return out


async def _fetch_market_cap(
    buyer: PotentialBuyer,
    client: httpx.AsyncClient,   # beibehalten für Signatur-Kompatibilität (ungenutzt)
) -> float | None:
    """
    Async Wrapper: holt market_cap + cash + debt_ebitda via yfinance (to_thread)
    und schreibt cash/debt direkt auf den Buyer. Gibt market_cap_usd_bn zurück
    (Rückwärtskompatibilität mit dem bestehenden gather-Loop).
    """
    if not buyer.ticker:
        return None

    yahoo_ticker = _yahoo_ticker(buyer.ticker, buyer.exchange)
    try:
        fin = await asyncio.wait_for(
            asyncio.to_thread(_fetch_buyer_financials_sync, yahoo_ticker, buyer.exchange),
            timeout=12.0,
        )
        buyer.cash_usd_bn = fin.get("cash_usd_bn")
        buyer.debt_ebitda = fin.get("debt_ebitda")
        return fin.get("market_cap_usd_bn")
    except asyncio.TimeoutError:
        logger.warning("Buyer-Fin: Timeout für %s (%s)", buyer.name, yahoo_ticker)
        return None
    except Exception as e:
        logger.debug("Buyer-Fin failed für %s (%s): %s", buyer.name, yahoo_ticker, e)
        return None



# ── Haupt-Pipeline ────────────────────────────────────────────────────────────

async def enrich_buyers_for_company(
    company: dict,
    company_id: str,
) -> list[PotentialBuyer]:
    """
    Haupt-Pipeline: Claude → Yahoo → DB-Upsert.
    Gibt angereicherte PotentialBuyer zurück (auch wenn market_cap fehlt).
    Wird aufgerufen:
      - On-demand als BackgroundTask in company_detail.py (erster Request)
      - Täglich via Cron in main.py (07:30 UTC)
    """
    from src.integrations.supabase import upsert_potential_buyers

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=4.0),
        follow_redirects=True,
    ) as client:
        # 1. Claude generiert Käufer
        buyers = await _claude_generate_buyers(company, client)
        if not buyers:
            logger.info("Buyer-Enrichment: keine Käufer für %s", company.get("name"))
            return []

        # 2. Yahoo Finance — market_cap parallel fetchen (max 4 concurrent)
        semaphore = asyncio.Semaphore(4)

        async def _fetch_with_sem(buyer: PotentialBuyer) -> None:
            async with semaphore:
                buyer.market_cap_usd_bn = await _fetch_market_cap(buyer, client)
                await asyncio.sleep(0.15)   # Yahoo Rate-Limit

        await asyncio.gather(*[_fetch_with_sem(b) for b in buyers])

        mc_count = sum(1 for b in buyers if b.market_cap_usd_bn is not None)
        logger.info(
            "Buyer-Enrichment %s: %d Käufer, %d mit MarketCap",
            company.get("name"), len(buyers), mc_count,
        )

        # 3. DB-Upsert
        rows = [b.to_db_dict(company_id) for b in buyers]
        upsert_potential_buyers(rows)

        return buyers
