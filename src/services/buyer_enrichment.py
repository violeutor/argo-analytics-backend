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
    )

    def __init__(
        self,
        name: str,
        ticker: str | None = None,
        exchange: str | None = None,
        market_cap_usd_bn: float | None = None,
        strategic_rationale: str | None = None,
        confidence: str = "medium",
    ):
        self.name                = name
        self.ticker              = ticker
        self.exchange            = exchange
        self.market_cap_usd_bn   = market_cap_usd_bn
        self.strategic_rationale = strategic_rationale
        self.confidence          = confidence

    def to_db_dict(self, company_id: str) -> dict:
        return {
            "company_id":          company_id,
            "name":                self.name,
            "ticker":              self.ticker,
            "exchange":            self.exchange,
            "market_cap_usd_bn":   self.market_cap_usd_bn,
            "strategic_rationale": self.strategic_rationale,
            "confidence":          self.confidence,
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


async def _fetch_market_cap(
    buyer: PotentialBuyer,
    client: httpx.AsyncClient,
) -> float | None:
    """
    Fetcht market_cap_usd_bn via Yahoo Finance für einen Ticker.
    Gibt None zurück wenn Ticker unbekannt oder API-Fehler.
    """
    if not buyer.ticker:
        return None

    yahoo_ticker = _yahoo_ticker(buyer.ticker, buyer.exchange)
    try:
        resp = await client.get(
            _YAHOO_QUOTE_URL.format(ticker=yahoo_ticker),
            params={"interval": "1d", "range": "1d"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8.0,
        )
        if resp.status_code != 200:
            # Fallback: .F statt .DE für Frankfurt
            if yahoo_ticker.endswith(".DE"):
                yahoo_ticker_f = yahoo_ticker[:-3] + ".F"
                resp2 = await client.get(
                    _YAHOO_QUOTE_URL.format(ticker=yahoo_ticker_f),
                    params={"interval": "1d", "range": "1d"},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=6.0,
                )
                if resp2.status_code == 200:
                    resp = resp2
                else:
                    return None
            else:
                return None

        data = resp.json()
        meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
        mcap = meta.get("marketCap")
        if mcap and float(mcap) > 0:
            return round(float(mcap) / 1e9, 2)

        return None

    except Exception as e:
        logger.debug("Yahoo market_cap failed für %s (%s): %s", buyer.name, yahoo_ticker, e)
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
