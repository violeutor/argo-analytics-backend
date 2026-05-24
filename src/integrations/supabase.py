"""
Supabase client + DB service layer für Argo Analytics.

Alle DB-Operationen laufen hier durch — Controller bleiben frei von
Supabase-spezifischem Code.
"""

import logging
from supabase import create_client, Client
from src.config import settings

logger = logging.getLogger(__name__)


# ── ARCH-01 Shadow Promote ────────────────────────────────────────────────────

def promote_shadow_to_supabase(shadow: dict) -> dict | None:
    """
    ARCH-01: Schreibt Shadow-DB Daten in Supabase.
    Legt companies-Row an und schreibt Ownership aus Shareholdern.

    shadow: dict aus GET /shadow/company/{name} (BA-Bridge)
    Returns: companies-Row wenn erfolgreich, None bei Fehler.
    """
    db   = get_supabase()
    name = shadow.get("name", "")
    if not name:
        return None

    payload: dict = {
        "name":              name,
        "investment_path":   "Beobachten",
        "enrichment_status": "done",
    }
    if shadow.get("hq"):           payload["headquarters"]  = shadow["hq"]
    if shadow.get("founded_year"): payload["founding_year"] = shadow["founded_year"]
    if shadow.get("headcount"):    payload["headcount"]     = shadow["headcount"]

    try:
        result  = db.table("companies").insert(payload).execute()
        company = result.data[0] if result.data else None
        if not company:
            logger.warning("promote_shadow_to_supabase: insert lieferte kein Ergebnis für '%s'", name)
            return None
    except Exception as e:
        logger.warning("promote_shadow_to_supabase companies insert FAILED für '%s': %s", name, e)
        return None

    company_id = company["id"]

    # Ownership aus BA-Shareholdern
    for sh in (shadow.get("shareholders") or []):
        sh_name = (sh.get("name") or "").strip()
        if not sh_name:
            continue
        try:
            db.table("ownership_entries").insert({
                "company_id": company_id,
                "name":       sh_name,
                "role":       "shareholder",
                "share_pct":  sh.get("share_pct"),
                "source":     "bundesanzeiger_shadow",
            }).execute()
        except Exception as e:
            logger.debug("promote_shadow ownership skip '%s': %s", sh_name, e)

    logger.info(
        "promote_shadow_to_supabase OK: '%s' company_id=%s shareholders=%d",
        name, company_id, len(shadow.get("shareholders") or []),
    )
    return company


def upsert_ticker_yf(company_id: str, ticker_yf: str) -> None:
    """
    Schreibt den yfinance-Ticker (z.B. 'SIE.DE') in companies.ticker_yf.
    Wird vom BA-Bridge price_fetcher Cron aufgerufen.
    """
    if not company_id or not ticker_yf:
        return
    try:
        db = get_supabase()
        db.table("companies").update({"ticker_yf": ticker_yf}).eq("id", company_id).execute()
        logger.debug("ticker_yf=%s geschrieben für company_id=%s", ticker_yf, company_id)
    except Exception as e:
        logger.warning("upsert_ticker_yf fehlgeschlagen für %s: %s", company_id, e)


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
        "investment_path, proxy_ticker, ticker, ticker_yf, exchange, funding_total_usd_mn, funding_stage, "
        "funding_last_round, last_signal, last_signal_date, source, "
        "founding_year, headquarters, headcount, description, peers, region, "
        "patent_count, patent_granted_ratio, patent_ipc_codes, patents_fetched_at, "
        "peers_context, peers_resolved, peers_generated_at"
    ).limit(limit).order("name")

    if source:
        query = query.eq("source", source)

    result = query.execute()
    return result.data or []


def fetch_listed_companies_missing_ticker_yf() -> list[dict]:
    """
    BUG-42: Gibt alle listed Companies zurück die ticker_yf noch nicht gesetzt haben.
    Wird vom ticker_yf-Enrichment-Cron in main.py aufgerufen (vor Beta-Cron).
    Felder: id, name, ticker, exchange, ticker_yf
    """
    db = get_supabase()
    try:
        result = db.table("companies").select(
            "id, name, ticker, exchange, ticker_yf"
        ).eq("ipo_status", "listed").not_.is_("ticker", "null").execute()
        rows = result.data or []
        # Nur Companies ohne ticker_yf oder mit leerem ticker_yf zurückgeben
        return [r for r in rows if not r.get("ticker_yf") and r.get("ticker")]
    except Exception as e:
        logger.warning("fetch_listed_companies_missing_ticker_yf failed: %s", e)
        return []


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
            category, industry, ticker, exchange, ipo_status
            (BUG-47: ipo_status + ticker + exchange jetzt aus Wikipedia-Infobox)
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


def fetch_all_ownership_entries() -> list[dict]:
    """
    Gibt alle Ownership-Einträge zurück — für Signal-Engine ownership_map.
    Felder: company_id, name, role, share_pct, as_of_date, source
    """
    db = get_supabase()
    try:
        result = db.table("ownership_entries").select(
            "company_id, name, role, share_pct, as_of_date, source"
        ).order("as_of_date", desc=True).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_all_ownership_entries failed: %s", e)
        return []


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
            "severity, is_read, raw_title, direction, signal_category, "
            "relevance_score, source_domain, funding_amount_usd_mn, created_at"
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
    Duplikat-sicher via UNIQUE CONSTRAINT signals_dedup_v2
    (company_id, event_type, event_date, source, source_domain).
    Gibt Anzahl geschriebener Rows zurück.

    Erwartet dicts mit:
        company_id, event_type, event_date, summary,
        source, source_url, severity, raw_title,
        direction, signal_category,           ← SE-09/SE-11/SE-12/SE-13
        source_domain, relevance_score,       ← Session 10: Qualität + Deduplizierung
        funding_amount_usd_mn,               ← B-05 Funding Enrichment
    """
    if not events:
        return 0
    db = get_supabase()
    written = 0
    for event in events:
        try:
            payload = {
                **event,
                "direction":             event.get("direction", "neutral"),
                "signal_category":       event.get("signal_category", "general_news"),
                "source_domain":         event.get("source_domain"),
                "relevance_score":       event.get("relevance_score"),
                "funding_amount_usd_mn": event.get("funding_amount_usd_mn"),
            }
            db.table("signals").upsert(
                payload,
                on_conflict="company_id,event_type,event_date,source,source_domain",
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
            "severity, raw_title, direction, signal_category, relevance_score, source_domain"
        ).eq("company_id", company_id).in_(
            "signal_category", categories
        ).order("event_date", desc=True).limit(limit).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_signals_by_category(%s): %s", company_id, e)
        return []


# ── B-05: Funding Enrichment ─────────────────────────────────────────────────

def upsert_funding_round(company_id: str, data: dict) -> bool:
    """
    B-05: Schreibt eine Funding-Runde in funding_rounds.
    Duplikat-sicher via UNIQUE (company_id, date, type).
    Gibt True zurück wenn geschrieben, False bei Duplikat/Fehler.

    Erwartet:
        date, type, amount_usd_mn, lead_investor?, co_investors?,
        source_url?, raw_text?, enrichment_source
    """
    db = get_supabase()
    payload = {k: v for k, v in data.items() if v is not None}
    payload["company_id"] = company_id
    try:
        db.table("funding_rounds").upsert(
            payload,
            on_conflict="company_id,date,type",
            ignore_duplicates=True,
        ).execute()
        logger.info(
            "upsert_funding_round OK: %s %s %s %.1fM",
            company_id, data.get("date"), data.get("type"),
            data.get("amount_usd_mn") or 0,
        )
        return True
    except Exception as e:
        logger.warning("upsert_funding_round FAILED for %s: %s", company_id, e)
        return False


def update_last_funding_enriched_at(company_id: str) -> None:
    """B-05: Setzt companies.last_funding_enriched_at auf jetzt."""
    from datetime import timezone
    db = get_supabase()
    try:
        db.table("companies").update({
            "last_funding_enriched_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", company_id).execute()
    except Exception as e:
        logger.warning("update_last_funding_enriched_at FAILED for %s: %s", company_id, e)


def fetch_funding_signals(company_id: str, limit: int = 20) -> list[dict]:
    """
    B-05: Gibt Signals mit funding_amount_usd_mn zurück — für Funding-Enrichment-Pipeline.
    Nur Signals die einen extrahierten Betrag haben.
    """
    db = get_supabase()
    try:
        result = db.table("signals").select(
            "id, event_date, summary, source, source_url, "
            "raw_title, funding_amount_usd_mn, signal_category"
        ).eq("company_id", company_id).eq(
            "event_type", "funding_round"
        ).not_.is_("funding_amount_usd_mn", "null").order(
            "event_date", desc=True
        ).limit(limit).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_funding_signals failed for %s: %s", company_id, e)
        return []


def fetch_companies_for_funding_enrichment(days_since_last: int = 7) -> list[dict]:
    """
    B-05: Gibt Companies zurück die seit >N Tagen kein Funding-Enrichment hatten.
    Steuerung via companies.last_funding_enriched_at.
    """
    from datetime import timezone, timedelta
    db = get_supabase()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_since_last)).isoformat()
    try:
        # Companies ohne last_funding_enriched_at ODER mit altem Timestamp
        result = db.table("companies").select(
            "id, name, ticker, exchange, region, last_funding_enriched_at"
        ).or_(
            f"last_funding_enriched_at.is.null,last_funding_enriched_at.lt.{cutoff}"
        ).order("last_funding_enriched_at", desc=False, nullsfirst=True).limit(50).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_companies_for_funding_enrichment failed: %s", e)
        return []


# ── SC-01–SC-13 · Scores ──────────────────────────────────────────────────────

def upsert_company_scores(company_id: str, scores: dict) -> bool:
    """
    SC: Schreibt ScoreResult in company_scores — UPSERT auf company_id.
    Erwartet scores = ScoreResult.to_dict().
    """
    from datetime import datetime, timezone
    db = get_supabase()
    try:
        payload = {
            "company_id":         company_id,
            "financial_score":    scores.get("financial_score"),
            "strategic_score":    scores.get("strategic_score"),
            "market_score":       scores.get("market_score"),
            "risk_score":         scores.get("risk_score"),
            "ownership_score":    scores.get("ownership_score"),
            "value_driver_score": scores.get("value_driver_score"),
            "ipo_score":          scores.get("ipo_score"),
            "ma_score":           scores.get("ma_score"),
            "etf_score":          scores.get("etf_score"),
            "enabler_score":      scores.get("enabler_score"),
            "composite_score":    scores.get("composite_score"),
            "hero_path":          scores.get("hero_path"),
            "hero_score":         scores.get("hero_score"),
            "hero_path_label":    scores.get("hero_path_label"),
            "rating":             scores.get("rating"),
            "confidence":         scores.get("confidence", "auto"),
            "score_inputs":       scores.get("score_inputs"),
            "computed_at":        datetime.now(timezone.utc).isoformat(),
        }
        db.table("company_scores").upsert(payload, on_conflict="company_id").execute()
        logger.info("upsert_company_scores OK for %s — hero=%s rating=%s",
                    company_id, scores.get("hero_path"), scores.get("rating"))
        return True
    except Exception as e:
        logger.warning("upsert_company_scores FAILED for %s: %s", company_id, e)
        return False


def fetch_potential_buyers(company_id: str) -> list[dict]:
    """
    R-23: Gibt company-spezifische Käufer aus potential_buyers zurück.
    Sortiert nach generated_at DESC — stellt sicher dass is_cache_valid()
    den neuesten Eintrag prüft (BUG-32: war order("confidence") → unzuverlässig).
    Leere Liste wenn noch nicht generiert oder TTL abgelaufen.
    """
    db = get_supabase()
    try:
        result = db.table("potential_buyers").select("*").eq(
            "company_id", company_id
        ).order("generated_at", desc=True).execute()
        return result.data or []
    except Exception as e:
        logger.warning("fetch_potential_buyers failed für %s: %s", company_id, e)
        return []


def upsert_potential_buyers(rows: list[dict]) -> int:
    """
    R-23: Schreibt potential_buyers in DB.
    UNIQUE(company_id, name) → ON CONFLICT DO UPDATE (aktualisiert market_cap + generated_at).
    Gibt Anzahl geschriebener Rows zurück.
    """
    if not rows:
        return 0
    db = get_supabase()
    written = 0
    for row in rows:
        try:
            db.table("potential_buyers").upsert(
                row,
                on_conflict="company_id,name",
            ).execute()
            written += 1
        except Exception as e:
            logger.warning("upsert_potential_buyers failed für %s: %s", row.get("name"), e)
    logger.info("upsert_potential_buyers: %d/%d geschrieben", written, len(rows))
    return written


def fetch_company_scores(company_id: str) -> dict | None:
    """SC: Gibt gecachte Scores für eine Company zurück (None wenn nicht vorhanden)."""
    db = get_supabase()
    try:
        result = db.table("company_scores").select("*").eq(
            "company_id", company_id
        ).limit(1).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logger.warning("fetch_company_scores failed for %s: %s", company_id, e)
        return None


def fetch_recent_absence_categories(company_ids: list[str], days: int = 30) -> dict[str, set[str]]:
    """
    BUG-01: Gibt Absence-Signal-Kategorien zurück die in den letzten N Tagen
    pro Company bereits emittiert wurden.
    Verhindert täglich neue negative Absence-Signale (Cooldown-Mechanismus).

    Returns: {company_id: {'ownership', 'headcount', 'revenue', 'signal_stille'}}
    """
    if not company_ids:
        return {}
    from datetime import datetime, timezone, timedelta
    db = get_supabase()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    result_map: dict[str, set[str]] = {cid: set() for cid in company_ids}
    try:
        result = db.table("signals").select(
            "company_id, signal_category, raw_title"
        ).eq("source", "internal_absence").gte(
            "event_date", cutoff
        ).in_("company_id", company_ids).execute()

        for row in (result.data or []):
            cid  = row.get("company_id")
            cat  = row.get("signal_category") or ""
            title = (row.get("raw_title") or "").lower()
            if cid not in result_map:
                continue
            # Kategorie-Keys müssen mit check_absence_signals cooldown-Keys übereinstimmen
            if "ownership" in title:       result_map[cid].add("ownership")
            elif "headcount" in title:     result_map[cid].add("headcount")
            elif "revenue" in title:       result_map[cid].add("revenue")
            elif "signal-stille" in title: result_map[cid].add("signal_stille")
    except Exception as e:
        logger.warning("fetch_recent_absence_categories failed: %s", e)
    return result_map


# ── SE-14: Patents (EPO OPS) ──────────────────────────────────────────────────

def bulk_upsert_patents(patent_records: list[dict]) -> int:
    """
    SE-14: Schreibt EPO-Patent-Records in company_patents-Tabelle.
    Duplikat-sicher via UNIQUE (company_id, patent_number).
    Batch-Verarbeitung in Chunks à 50 — vermeidet zu große Payloads.

    Erwartet Records mit:
        company_id, patent_number, title, filing_date, grant_date,
        status, ipc_codes (list), citation_count, geo_coverage (list), source

    Gibt Anzahl erfolgreich geschriebener Records zurück.
    """
    if not patent_records:
        return 0

    db      = get_supabase()
    written = 0
    chunk_size = 50

    for i in range(0, len(patent_records), chunk_size):
        chunk = patent_records[i : i + chunk_size]
        try:
            db.table("company_patents").upsert(
                chunk,
                on_conflict="company_id,patent_number",
            ).execute()
            written += len(chunk)
        except Exception as e:
            # Fallback: einzeln versuchen um fehlerhafte Records zu isolieren
            logger.warning("bulk_upsert_patents chunk %d failed (%s) — Einzelversuch", i // chunk_size, e)
            for record in chunk:
                try:
                    db.table("company_patents").upsert(
                        record,
                        on_conflict="company_id,patent_number",
                    ).execute()
                    written += 1
                except Exception as re:
                    logger.warning(
                        "bulk_upsert_patents FAILED: %s / %s — %s",
                        record.get("company_id"), record.get("patent_number"), re,
                    )

    logger.info(
        "bulk_upsert_patents: %d/%d Records geschrieben",
        written, len(patent_records),
    )
    return written


def update_patent_aggregates(patent_records: list[dict]) -> None:
    """
    SE-14: Aggregiert Patent-Records pro Company und schreibt zurück in companies.
    Felder: patent_count, patent_granted_ratio, patent_ipc_codes, patents_fetched_at.

    Wird direkt nach bulk_upsert_patents() aufgerufen — kein separater DB-Read nötig
    da die Records bereits in memory vorliegen.

    Aggregationslogik:
        patent_count          = Anzahl Records (alle Status)
        patent_granted_ratio  = granted / gesamt (0.0–1.0)
        patent_ipc_codes      = deduplizierte IPC-Codes aller Patents der Company
        patents_fetched_at    = jetzt (UTC)
    """
    from datetime import datetime, timezone
    from collections import defaultdict

    if not patent_records:
        return

    db  = get_supabase()
    now = datetime.now(timezone.utc).isoformat()

    # Records nach company_id gruppieren
    by_company: dict[str, list[dict]] = defaultdict(list)
    for r in patent_records:
        cid = r.get("company_id")
        if cid:
            by_company[cid].append(r)

    for company_id, records in by_company.items():
        total   = len(records)
        granted = sum(1 for r in records if r.get("status") == "granted")

        # IPC-Codes aller Patents dedupliziert zusammenführen
        all_ipc: list[str] = []
        for r in records:
            codes = r.get("ipc_codes") or []
            all_ipc.extend(codes)
        unique_ipc = list(dict.fromkeys(all_ipc))   # Reihenfolge erhalten

        payload: dict = {
            "patent_count":         total,
            "patent_granted_ratio": round(granted / total, 4) if total > 0 else 0.0,
            "patents_fetched_at":   now,
        }
        if unique_ipc:
            payload["patent_ipc_codes"] = unique_ipc

        try:
            db.table("companies").update(payload).eq("id", company_id).execute()
            logger.debug(
                "update_patent_aggregates: %s → count=%d granted_ratio=%.2f ipc=%d codes",
                company_id, total, payload["patent_granted_ratio"], len(unique_ipc),
            )
        except Exception as e:
            logger.warning("update_patent_aggregates FAILED for %s: %s", company_id, e)

    logger.info(
        "update_patent_aggregates: %d Companies aktualisiert",
        len(by_company),
    )
