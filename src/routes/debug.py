"""
GET /api/v1/company/{name}/debug
Datenstatus-Endpunkt — zeigt für eine Company welche Felder befüllt sind,
woher die Daten kommen und wann sie zuletzt angereichert wurden.
Nur für interne Nutzung (API-Key Guard via Middleware).
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from src.integrations.supabase import (
    get_supabase,
    fetch_companies,
    fetch_tam_cache,
    fetch_market_data,
    fetch_company_scores,
    fetch_signals,
    fetch_funding_rounds,
    fetch_value_drivers,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["debug"])


def _val(v) -> dict:
    """Gibt {value, status} zurück — 'ok' | 'empty'."""
    filled = v is not None and v != "" and v != [] and v != {}
    return {"value": v, "status": "ok" if filled else "empty"}


def _ts(iso: str | None) -> str | None:
    """ISO-Timestamp → lesbares Format."""
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M UTC")
    except Exception:
        return iso


def _fetch_beta_debug(company: dict) -> dict:
    """
    Holt Beta-Daten für den Debug-Block.
    Listed Companies → BA-Bridge beta_cache (market beta).
    Private Companies → Damodaran-Lookup via Bridge.
    Fehler → leerer Block mit Fehlerinfo.
    """
    import os, httpx
    bridge_url = os.getenv("BA_BRIDGE_URL", "")
    bridge_key = os.getenv("BA_BRIDGE_API_KEY", os.getenv("API_KEY", ""))
    base = {
        "_source": "BA-Bridge · yfinance beta_cache (listed) / Damodaran (private)",
        "_calculated_at": None,
        "yf_ticker":     company.get("ticker_yf") or None,
        "beta_source":   None,
    }

    if not bridge_url:
        base["_error"] = "BA_BRIDGE_URL nicht gesetzt"
        return base

    ticker_yf = company.get("ticker_yf") or ""
    ticker    = company.get("ticker") or ""
    exchange  = company.get("exchange") or ""
    category  = company.get("category") or company.get("industry") or ""
    is_listed = company.get("ipo_status") == "listed"

    try:
        if is_listed and ticker:
            # Market Beta aus beta_cache
            resp = httpx.get(
                f"{bridge_url}/beta/{ticker}",
                params={"exchange": exchange, "ticker_yf": ticker_yf},
                headers={"X-API-Key": bridge_key},
                timeout=8,
            )
            if resp.status_code == 200:
                d = resp.json()
                base.update({
                    "beta_source":          "market",
                    "beta_1y":              d.get("beta_1y"),
                    "beta_3y":              d.get("beta_3y"),
                    "volatility_30d":       d.get("volatility_30d"),
                    "benchmark_ticker":     d.get("benchmark_ticker"),
                    "benchmark_is_fallback":d.get("benchmark_is_fallback"),
                    "data_quality":         d.get("data_quality"),
                    "_calculated_at":       _ts(d.get("calculated_at")),
                })
            else:
                base["_error"] = f"Bridge HTTP {resp.status_code} — ticker_yf evtl. nicht gesetzt"
        else:
            # Damodaran Beta
            resp = httpx.get(
                f"{bridge_url}/beta/damodaran",
                params={"category": category},
                headers={"X-API-Key": bridge_key},
                timeout=8,
            )
            if resp.status_code == 200:
                d = resp.json()
                base.update({
                    "beta_source":      "damodaran",
                    "damodaran_beta":   d.get("beta"),
                    "damodaran_sector": d.get("sector"),
                })
            else:
                base["_error"] = f"Damodaran Bridge HTTP {resp.status_code}"
    except httpx.TimeoutException:
        base["_error"] = "Bridge Timeout (>8s) — Dyno schläft?"
    except Exception as e:
        base["_error"] = f"Bridge Fehler: {e}"

    return base


@router.get("/company/{name}/debug")
def get_company_debug(name: str) -> dict:
    """
    Datenstatus für eine Company — alle Pipeline-Blöcke mit Wert + Status + Timestamp.
    """
    # 1. Company laden
    companies = fetch_companies(limit=500)
    q = name.lower().replace("-", " ").replace("_", " ")
    company = next(
        (c for c in companies if
         c.get("name", "").lower() == q or
         q in c.get("name", "").lower() or
         c.get("name", "").lower() in q),
        None,
    )
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' nicht gefunden.")

    company_id   = company.get("id")
    company_name = company.get("name")

    # 2. Alle Datenquellen laden
    tam          = fetch_tam_cache(company_id) if company_id else None
    market_data  = fetch_market_data(company_id) if company_id else None
    scores       = fetch_company_scores(company_id) if company_id else None
    signals      = fetch_signals(company_id, limit=10) if company_id else []
    funding      = fetch_funding_rounds(company_id) if company_id else []
    value_drivers = fetch_value_drivers(company_id) if company_id else None

    # Ownership aus companies-Tabelle (ownership_entries)
    ownership_entries = []
    if company_id:
        try:
            db = get_supabase()
            res = db.table("ownership_entries").select("*").eq("company_id", company_id).execute()
            ownership_entries = res.data or []
        except Exception:
            pass

    # Peers
    peers_resolved = company.get("peers_resolved") or []
    peers_generated_at = company.get("peers_generated_at")

    # 3. Debug-Response aufbauen
    return {
        "company": {
            "name":             company_name,
            "id":               company_id,
            "enrichment_status": _val(company.get("enrichment_status")),
        },

        # ── Block 1: Basis-Enrichment (Wikipedia / Website) ──────────────────
        "enrichment": {
            "_source": "Wikipedia + Website",
            "founding_year":  _val(company.get("founding_year")),
            "headquarters":   _val(company.get("headquarters")),
            "headcount":      _val(company.get("headcount")),
            "description":    _val(company.get("description")),
            "website":        _val(company.get("website")),
            "category":       _val(company.get("category")),
            "industry":       _val(company.get("industry")),
            "ticker":         _val(company.get("ticker")),
            "ticker_yf":      _val(company.get("ticker_yf")),
            "exchange":       _val(company.get("exchange")),
            "ipo_status":     _val(company.get("ipo_status")),
            "region":         _val(company.get("region")),
        },

        # ── Block 2: TAM-Cache ────────────────────────────────────────────────
        "tam": {
            "_source":    "TAM-Scraper (DuckDuckGo + AI)",
            "_cached_at": _ts(tam.get("scraped_at")) if tam else None,
            "tam_2035_usd_bn": _val(tam.get("tam_2035_usd_bn") if tam else None),
            "cagr_pct":        _val(tam.get("cagr_pct") if tam else None),
            "source":          _val(tam.get("source") if tam else None),
        },

        # ── Block 3: Market Data ──────────────────────────────────────────────
        "market_data": {
            "_source":    "DuckDuckGo Page-Fetch + AI + Peer-Sync",
            "_enriched_at": _ts(market_data.get("enriched_at")) if market_data else None,
            "sam_usd_bn":       _val(market_data.get("sam_usd_bn") if market_data else None),
            "cagr_pct":         _val(market_data.get("cagr_pct") if market_data else None),
            "competition_score":_val(market_data.get("competition_score") if market_data else None),
            "competition_note": _val(market_data.get("competition_note") if market_data else None),
            "market_cycle":     _val(market_data.get("market_cycle") if market_data else None),
            "growth_drivers":   _val(market_data.get("growth_drivers") if market_data else None),
            "tam_segments":     _val(market_data.get("tam_segments") if market_data else None),
        },

        # ── Block 4: Fundamentals ─────────────────────────────────────────────
        "fundamentals": {
            "_source": "Yahoo Finance (listed) / BA-Bridge (private DE) / EDGAR (private US)",
            "is_listed":     _val(company.get("ipo_status") == "listed"),
            "proxy_ticker":  _val(company.get("proxy_ticker")),
            # Yahoo / BA-Bridge Daten kommen on-demand — hier nur DB-Felder
            "revenue_usd_mn":   _val(company.get("revenue_usd_mn")),
            "is_profitable":    _val(company.get("is_profitable")),
            "growth_rate_pct":  _val(company.get("growth_rate_pct")),
        },

        # ── Block 5: Funding ──────────────────────────────────────────────────
        "funding": {
            "_source": "EDGAR Form D + TechCrunch NER",
            "total_usd_mn":     _val(company.get("funding_total_usd_mn")),
            "stage":            _val(company.get("funding_stage")),
            "last_round":       _val(company.get("funding_last_round")),
            "rounds_in_db":     len(funding),
            "rounds":           [
                {
                    "date":   r.get("date"),
                    "type":   r.get("type"),
                    "amount": r.get("amount_usd_mn"),
                    "lead":   r.get("lead_investor"),
                }
                for r in funding[:5]
            ],
        },

        # ── Block 6: Ownership ────────────────────────────────────────────────
        "ownership": {
            "_source": "SEC EDGAR Form D + Wikipedia",
            "entries_in_db": len(ownership_entries),
            "entries": [
                {"name": e.get("name"), "type": e.get("type"), "source": e.get("source")}
                for e in ownership_entries[:5]
            ],
        },

        # ── Block 7: Peers ────────────────────────────────────────────────────
        "peers": {
            "_source": "Claude (generiert) + Enrichment Background",
            "_generated_at": _ts(peers_generated_at),
            "peers_count":   len(peers_resolved),
            "peers_context_filled": bool(company.get("peers_context")),
        },

        # ── Block 8: Value Drivers ────────────────────────────────────────────
        "value_drivers": {
            "_source": "supply_chain.py (13 Sektoren)",
            "enablers_count":     len((value_drivers or {}).get("enablers") or []),
            "contributors_count": len((value_drivers or {}).get("contributors") or []),
            "buyers_count":       len((value_drivers or {}).get("buyers") or []),
        },

        # ── Block 9: Scoring ──────────────────────────────────────────────────
        "scoring": {
            "_source":      "score_calculator.py SC-01–SC-13",
            "_computed_at": _ts(scores.get("computed_at") if scores else None),
            "_score_source": scores.get("score_source") if scores else None,
            "composite_score":    _val(scores.get("composite_score") if scores else None),
            "financial_score":    _val(scores.get("financial_score") if scores else None),
            "market_score":       _val(scores.get("market_score") if scores else None),
            "strategic_score":    _val(scores.get("strategic_score") if scores else None),
            "risk_score":         _val(scores.get("risk_score") if scores else None),
            "compound_risk_score":_val(scores.get("compound_risk_score") if scores else None),
            "rating":             _val(scores.get("rating") if scores else None),
        },

        # ── Block 10: Signals ─────────────────────────────────────────────────
        "signals": {
            "_source": "Signal-Engine SE-01–SE-15 (Cron 04:00 UTC)",
            "signals_count": len(signals),
            # BUG-49: internal_absence nie als "latest" zeigen — kein echter Event
            "latest": next(
                (
                    {
                        "event_type": s.get("event_type"),
                        "event_date": s.get("event_date"),
                        "source":     s.get("source"),
                    }
                    for s in signals
                    if s.get("source") != "internal_absence"
                ),
                None,
            ),
            "absence_count": sum(1 for s in signals if s.get("source") == "internal_absence"),
        },

        # ── Block 11: Patents ─────────────────────────────────────────────────
        "patents": {
            "_source": "EPO OPS API (SE-14, Cron 04:00 UTC)",
            "_fetched_at": _ts(company.get("patents_fetched_at")),
            "patent_count":         _val(company.get("patent_count")),
            "patent_granted_ratio": _val(company.get("patent_granted_ratio")),
            "patent_ipc_codes":     _val(company.get("patent_ipc_codes")),
        },

        # ── Block 12: Beta (BA-Bridge) ────────────────────────────────────────
        "beta": _fetch_beta_debug(company),
    }
