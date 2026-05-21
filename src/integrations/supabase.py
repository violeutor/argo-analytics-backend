"""
Supabase client + DB service layer für Argo Analytics.

Alle DB-Operationen laufen hier durch — Controller bleiben frei von
Supabase-spezifischem Code.
"""

import logging
from supabase import create_client, Client
from src.config import settings

logger = logging.getLogger(__name__)

_client: Client | None = None


def get_supabase() -> Client:
    global _client
    if _client is None:
        _client = create_client(settings.supabase_url, settings.supabase_service_key)
    return _client


# ── Companies ────────────────────────────────────────────────────────────────

def fetch_companies(limit: int = 100, source: str | None = None) -> list[dict]:
    db = get_supabase()
    query = db.table("companies").select(
        "id, name, category, industry, potential, risk, ipo_potential, ipo_status, "
        "investment_path, proxy_ticker, ticker, exchange, funding_total_usd_mn, funding_stage, "
        "funding_last_round, last_signal, last_signal_date, source, "
        "founding_year, headquarters, headcount, description, peers, region"
    ).limit(limit).order("name")

    if source:
        query = query.eq("source", source)

    result = query.execute()
    return result.data or []


def fetch_company_by_name(name: str) -> dict | None:
    db = get_supabase()
    result = db.table("companies").select("*").ilike("name", name).limit(1).execute()
    return result.data[0] if result.data else None


# ── Funding Rounds ────────────────────────────────────────────────────────────

def fetch_funding_rounds(company_id: str) -> list[dict]:
    """
    Gibt alle Funding Rounds einer Company zurück, chronologisch absteigend.
    """
    db = get_supabase()
    result = db.table("funding_rounds").select(
        "id, date, type, amount_usd_mn, lead_investor, co_investors, source, notes"
    ).eq("company_id", company_id).order("date", desc=True).execute()
    return result.data or []


def fetch_all_funding_rounds() -> list[dict]:
    """
    Gibt alle Funding Rounds aller Companies zurück.
    Wird von market_data_enrichment für Competition Score + Market Cycle genutzt.
    Felder: company_id, date, amount_usd_mn — nur was die Pipeline braucht.
    """
    db = get_supabase()
    try:
        result = db.table("funding_rounds").select(
            "company_id, date, amount_usd_mn, type"
        ).order("date", desc=True).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_all_funding_rounds failed: %s", e)
        return []


# ── Buyers ───────────────────────────────────────────────────────────────────

def fetch_buyers(limit: int = 50) -> list[dict]:
    db = get_supabase()
    result = db.table("buyers").select(
        "id, name, ticker, exchange, market_cap_usd_bn, sector"
    ).limit(limit).order("name").execute()
    return result.data or []


def fetch_buyer_by_name(name: str) -> dict | None:
    db = get_supabase()
    result = db.table("buyers").select("*").ilike("name", name).limit(1).execute()
    return result.data[0] if result.data else None


# ── Company Enrichment Upsert ────────────────────────────────────────────────

def upsert_company_enrichment(company_id: str, data: dict) -> None:
    """
    Schreibt Enrichment-Ergebnisse zurück in die companies-Tabelle.
    Nur Felder die tatsächlich einen Wert haben werden geschrieben
    (keine None-Overwrites auf bereits vorhandene DB-Werte).

    Felder: founding_year, headquarters, headcount, description, website,
            category, industry  ← neu: aus Tag-Inferenz für One-Click-Companies
    Aufgerufen von: company_detail.py nach _safe_enrichment()
    """
    db = get_supabase()

    payload = {k: v for k, v in data.items() if v is not None}
    if not payload:
        return

    try:
        result = db.table("companies").update(payload).eq("id", company_id).execute()
        logger.warning("upsert_company_enrichment OK: %s → %s rows=%s", company_id, list(payload.keys()), len(result.data or []))
    except Exception as e:
        logger.warning("upsert_company_enrichment FAILED: %s — %s", company_id, e)


# ── TAM Cache ────────────────────────────────────────────────────────────────

def fetch_tam_cache(company_id: str) -> dict | None:
    """Gibt gecachten TAM-Wert zurück wenn vorhanden."""
    db = get_supabase()
    try:
        result = db.table("tam_cache").select(
            "tam_2035_usd_bn, cagr_pct, source, scraped_at"
        ).eq("company_id", company_id).limit(1).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logger.warning("fetch_tam_cache failed: %s", e)
        return None


def upsert_tam_cache(company_id: str, tam_usd_bn: float, cagr_pct: float | None, source: str) -> None:
    """Persistiert TAM-Scraping-Ergebnis in tam_cache (INSERT OR UPDATE)."""
    db = get_supabase()
    try:
        db.table("tam_cache").upsert({
            "company_id":       company_id,
            "tam_2035_usd_bn":  tam_usd_bn,
            "cagr_pct":         cagr_pct,
            "source":           source,
        }, on_conflict="company_id").execute()
        logger.debug("upsert_tam_cache: %s tam=%.1f", company_id, tam_usd_bn)
    except Exception as e:
        logger.warning("upsert_tam_cache failed for %s: %s", company_id, e)


# ── Market Data ──────────────────────────────────────────────────────────────

def fetch_market_data(company_id: str) -> dict | None:
    """Gibt market_data-Row zurück wenn vorhanden."""
    db = get_supabase()
    try:
        result = db.table("market_data").select("*").eq("company_id", company_id).limit(1).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logger.warning("fetch_market_data failed for %s: %s", company_id, e)
        return None


def upsert_market_data(company_id: str, data: dict) -> None:
    """
    Schreibt Market-Data-Ergebnisse in market_data (INSERT OR UPDATE).
    Nur non-None Werte werden geschrieben — kein Overwrite vorhandener DB-Werte.
    Felder: tam_2035_usd_bn, cagr_pct, tam_source, tam_segments, regional_breakdown,
            growth_drivers, sam_usd_bn, sam_confidence, sam_note, sam_geo_factor,
            sam_tech_filter, competition_score, competition_note,
            market_cycle, market_cycle_note, regional_sources, enriched_at
    """
    db = get_supabase()
    payload = {k: v for k, v in data.items() if v is not None}
    if not payload:
        return
    payload["company_id"] = company_id

    try:
        db.table("market_data").upsert(payload, on_conflict="company_id").execute()
        logger.warning("upsert_market_data OK: %s → %s", company_id, list(payload.keys()))
    except Exception as e:
        logger.warning("upsert_market_data FAILED for %s: %s", company_id, e)


def set_enrichment_status(company_id: str, status: str) -> None:
    """Setzt companies.enrichment_status — pending | running | done | error."""
    db = get_supabase()
    try:
        db.table("companies").update({"enrichment_status": status}).eq("id", company_id).execute()
        logger.debug("enrichment_status %s → %s", company_id, status)
    except Exception as e:
        logger.warning("set_enrichment_status FAILED for %s: %s", company_id, e)


# ── Value Drivers ────────────────────────────────────────────────────────────

def fetch_value_drivers(company_id: str) -> dict | None:
    """
    Gibt Value Drivers aus value_drivers-Tabelle zurück.
    Erwartet eine Row mit JSONB-Feldern: enablers, contributors, etfs, enriched_at.
    """
    db = get_supabase()
    try:
        result = db.table("value_drivers").select(
            "enablers, contributors, etfs, enriched_at"
        ).eq("company_id", company_id).limit(1).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logger.warning("fetch_value_drivers failed for %s: %s", company_id, e)
        return None


def upsert_value_drivers(company_id: str, data: dict) -> None:
    """
    Persistiert Value Drivers in value_drivers-Tabelle (INSERT OR UPDATE).

    Erwartet:
      data = {
        "enablers":     [...],   # Liste von Enabler-Dicts
        "contributors": [...],   # Liste von Contributor-Dicts
        "etfs":         [...],   # Liste von ETF-Dicts
      }
    """
    import json
    from datetime import datetime, timezone

    db = get_supabase()
    payload = {
        "company_id":   company_id,
        "enablers":     json.dumps(data.get("enablers", [])),
        "contributors": json.dumps(data.get("contributors", [])),
        "etfs":         json.dumps(data.get("etfs", [])),
        "enriched_at":  datetime.now(timezone.utc).isoformat(),
    }
    try:
        db.table("value_drivers").upsert(payload, on_conflict="company_id").execute()
        logger.info(
            "upsert_value_drivers OK: %s → %d enablers, %d contributors, %d etfs",
            company_id,
            len(data.get("enablers", [])),
            len(data.get("contributors", [])),
            len(data.get("etfs", [])),
        )
    except Exception as e:
        logger.warning("upsert_value_drivers FAILED for %s: %s", company_id, e)


# ── Deals ────────────────────────────────────────────────────────────────────

def insert_deal(request, company_id: str | None, buyer_id: str | None) -> str:
    """
    Persistiert den Analyse-Request als Deal-Eintrag.
    Gibt die UUID des neuen Deals zurück.
    """
    db = get_supabase()

    tr = request.tech_readiness_inputs
    payload = {
        "company_id": company_id,
        "buyer_id": buyer_id,
        "tam_usd_bn": request.tam_usd_bn,
        "buyer_market_cap_usd_bn": request.buyer_market_cap_usd_bn,
        "buyer_cash_usd_bn": request.buyer_cash_usd_bn,
        "buyer_debt_ebitda": request.buyer_debt_ebitda,
        "target_funding_usd_mn": request.target_funding_usd_mn,
        "target_stage": request.target_stage,
        "url": request.url,
        # TechReadiness inputs — null wenn nicht geliefert
        "tr_tech_stack_fit":             tr.tech_stack_fit if tr else None,
        "tr_gtm_fit":                    tr.gtm_fit if tr else None,
        "tr_integration_capacity":       tr.integration_capacity if tr else None,
        "tr_rd_intensity":               tr.rd_intensity if tr else None,
        "tr_capital_deployment_velocity":tr.capital_deployment_velocity if tr else None,
        "tr_regulatory_readiness":       tr.regulatory_readiness if tr else None,
        "tr_strategic_coherence":        tr.strategic_coherence if tr else None,
    }

    result = db.table("deals").insert(payload).execute()
    return result.data[0]["id"]


# ── Scores ───────────────────────────────────────────────────────────────────

def insert_score(deal_id: str, scores, summary: str, warnings: list[str]) -> str:
    """
    Persistiert das Scoring-Ergebnis verknüpft mit einem Deal.
    Gibt die UUID des Score-Eintrags zurück.
    """
    db = get_supabase()

    payload = {
        "deal_id": deal_id,
        # SRR
        "srr_value":             scores.srr.value,
        "srr_category":          scores.srr.category,
        "srr_cap_segment":       scores.srr.cap_segment,
        "srr_execution_warning": scores.srr.execution_warning,
        # MFR
        "mfr_value":             scores.mfr.value,
        "mfr_signal":            scores.mfr.signal,
        # TechReadiness
        "tr_value":              scores.tech_readiness.value,
        "tr_inputs_provided":    scores.tech_readiness.value != 0.5,
        # Deal Success
        "deal_success_score":    scores.deal_success_score,
        "rating":                scores.rating,
        "quadrant":              scores.quadrant,
        # Output
        "executive_summary":     summary,
        "warnings":              warnings,
    }

    result = db.table("scores").insert(payload).execute()
    return result.data[0]["id"]


# ── Signals (SE-01–SE-13) ─────────────────────────────────────────────────────

def fetch_signals(company_id: str, limit: int = 50) -> list[dict]:
    """Gibt Signals für eine Company zurück, chronologisch absteigend."""
    db = get_supabase()
    try:
        result = db.table("signals").select(
            "id, event_type, event_date, summary, source, source_url, "
            "severity, is_read, raw_title, direction, signal_category, created_at"
        ).eq("company_id", company_id).order("event_date", desc=True).limit(limit).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_signals failed for %s: %s", company_id, e)
        return []


def fetch_all_signals(limit: int = 500) -> list[dict]:
    """Gibt alle Signals zurück — für Dashboard-Übersicht."""
    db = get_supabase()
    try:
        result = db.table("signals").select(
            "id, company_id, event_type, event_date, summary, source, severity, created_at"
        ).order("event_date", desc=True).limit(limit).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_all_signals failed: %s", e)
        return []


def upsert_signals(events: list[dict]) -> int:
    """
    Schreibt Signal-Events in die signals-Tabelle.
    Duplikat-sicher via UNIQUE CONSTRAINT (company_id, event_type, event_date, source).
    Gibt Anzahl geschriebener Rows zurück.

    Erwartet dicts mit:
        company_id, event_type, event_date, summary,
        source, source_url, severity, raw_title,
        direction, signal_category           ← SE-09/SE-11/SE-12/SE-13
    """
    if not events:
        return 0
    db = get_supabase()
    written = 0
    for event in events:
        try:
            # direction + signal_category mit sicheren Defaults falls fehlen
            payload = {
                **event,
                "direction":       event.get("direction", "neutral"),
                "signal_category": event.get("signal_category", "general_news"),
            }
            db.table("signals").upsert(
                payload,
                on_conflict="company_id,event_type,event_date,source",
                ignore_duplicates=True,
            ).execute()
            written += 1
        except Exception as e:
            logger.warning("upsert_signals FAILED for event %s: %s", event.get("event_type"), e)
    logger.info("upsert_signals: %d/%d events written", written, len(events))
    return written


def fetch_last_signal_date(company_id: str) -> str | None:
    """Gibt das Datum des jüngsten Signals für eine Company zurück (ISO 8601)."""
    db = get_supabase()
    try:
        result = db.table("signals").select("event_date").eq(
            "company_id", company_id
        ).order("event_date", desc=True).limit(1).execute()
        return result.data[0]["event_date"] if result.data else None
    except Exception as e:
        logger.warning("fetch_last_signal_date failed for %s: %s", company_id, e)
        return None


def fetch_directional_signals(
    company_id: str,
    direction: str,
    limit: int = 10,
) -> list[dict]:
    """
    SE-09: Gibt Signals nach direction gefiltert zurück.
    direction: 'positive' | 'negative' | 'neutral'
    Wird von Tab 5 (Potenziale & Risiken) und company_assessments genutzt.
    """
    db = get_supabase()
    try:
        result = db.table("signals").select(
            "id, event_type, event_date, summary, source, source_url, "
            "severity, raw_title, direction, signal_category"
        ).eq("company_id", company_id).eq("direction", direction).order(
            "event_date", desc=True
        ).limit(limit).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_directional_signals(%s, %s): %s", company_id, direction, e)
        return []


def fetch_signals_by_category(
    company_id: str,
    categories: list[str],
    limit: int = 10,
) -> list[dict]:
    """
    SE-11/SE-12: Gibt Signals nach signal_category gefiltert zurück.
    Nützlich für spezifische Risk/Potential Checks in Tab 5 + Scoring.
    Beispiel: categories=['regulatory', 'negative_earnings'] → alle Risiko-Signals
    """
    db = get_supabase()
    try:
        result = db.table("signals").select(
            "id, event_type, event_date, summary, source, source_url, "
            "severity, raw_title, direction, signal_category"
        ).eq("company_id", company_id).in_(
            "signal_category", categories
        ).order("event_date", desc=True).limit(limit).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_signals_by_category(%s): %s", company_id, e)
        return []
