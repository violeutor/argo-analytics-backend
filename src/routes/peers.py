"""
R-10 · peers.py
Pfad: argo-analytics-backend/src/routes/peers.py

GET /api/v1/company/{name}/peers
  → Claude generiert 3-5 Wettbewerber
  → Peers werden als companies-Rows angelegt (falls nicht vorhanden)
  → peers_resolved UUID[] wird in companies geschrieben
  → Peer-Daten (aus DB) werden zurückgegeben

Pipeline:
  1. company aus DB laden
  2. peers_resolved vorhanden + < 30 Tage alt? → direkt aus DB zurückgeben
  3. Sonst: Claude generiert Peer-Namen
  4. Für jeden Peer: exists in DB? → id; sonst INSERT minimal row
  5. peers_resolved + peers_generated_at in companies schreiben
  6. Peer-Daten aus DB aggregieren + zurückgeben
"""

import logging
import json
import re
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from src.config import settings
from src.integrations.supabase import (
    get_supabase,
    fetch_company_by_name,
    fetch_companies,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["peers"])

_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
_CACHE_TTL_DAYS = 30   # Peer-Liste wird alle 30 Tage neu generiert


# ── Response Models ───────────────────────────────────────────────────────────

class PeerCompany(BaseModel):
    id: str
    name: str
    category: str | None = None
    industry: str | None = None
    region: str | None = None
    headquarters: str | None = None
    founding_year: int | None = None
    headcount: int | None = None
    funding_total_usd_mn: float | None = None
    funding_stage: str | None = None
    funding_last_round: str | None = None
    ipo_status: str | None = None
    ipo_potential: str | None = None
    investment_path: str | None = None
    revenue_usd_mn: float | None = None
    description: str | None = None
    website: str | None = None
    ticker: str | None = None
    exchange: str | None = None
    # Benchmark-Felder (berechnet)
    stage_normalized: str | None = None
    # Argo Scores (aus company_scores)
    composite_score: float | None = None
    rating: str | None = None          # A | B | C | D
    financial_score: float | None = None
    market_score: float | None = None
    # R-10: Positioning note (Claude-generiert, relativ zu Subject Company)
    positioning_note: str | None = None


class PeerBenchmark(BaseModel):
    metric: str
    company_value: str | None
    peer_median: str | None
    unit: str | None = None
    note: str | None = None


class PeersResponse(BaseModel):
    status: str                        # ready | generating | empty
    company_name: str
    peers: list[PeerCompany]
    benchmark: list[PeerBenchmark]
    generated_at: str | None = None
    from_cache: bool = False


# ── Haupt-Endpoint ────────────────────────────────────────────────────────────

async def _schedule_or_await(background_tasks, fn, *args) -> None:
    """
    Endpoint-Pfad (background_tasks gesetzt) → schedulen (schnelle Response).
    Cold-Path-Pfad (None) → inline awaiten (kein Response zu blockieren; der
    Buyer-Pool braucht die Peer-Financials zeitnah).
    """
    if background_tasks is not None:
        background_tasks.add_task(fn, *args)
        return
    try:
        await fn(*args)
    except Exception as e:
        logger.debug("_schedule_or_await inline failed (%s): %s",
                     getattr(fn, "__name__", fn), e)


async def ensure_peers(
    company: dict,
    *,
    background_tasks: BackgroundTasks | None = None,
) -> dict:
    """
    PEERS-CONSOLIDATE-01: idempotenter Peer-Generierungs-Kern, aus get_peers
    extrahiert. Cache-aware (30d) → bei gültigem Cache kein Claude-Call.
    Sonst: Namen generieren → resolve/create → peers_resolved schreiben.

    Aufrufpfade:
      - Endpoint get_peers (background_tasks gesetzt → Peer-Enrichment geschedult)
      - Cold-Path-Konsolidierung in company_detail (background_tasks=None →
        Peer-Enrichment inline, damit der Buyer-Pool die Peer-Financials bekommt)

    Returns: {"resolved_ids", "peer_notes", "generated_at", "from_cache"}.
    """
    db = get_supabase()
    company_id   = company.get("id")
    company_name = company.get("name", "")
    if not company_id:
        return {"resolved_ids": [], "peer_notes": {}, "generated_at": None, "from_cache": False}

    # Cache prüfen
    peers_resolved = company.get("peers_resolved") or []
    generated_at   = company.get("peers_generated_at")
    peers_context  = company.get("peers_context") or {}
    cache_valid    = False
    if peers_resolved and generated_at:
        try:
            gen_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            cache_valid = (datetime.now(timezone.utc) - gen_dt).days < _CACHE_TTL_DAYS
        except Exception:
            pass
    if cache_valid and peers_resolved:
        return {"resolved_ids": peers_resolved, "peer_notes": peers_context,
                "generated_at": generated_at, "from_cache": True}

    # Claude generiert Peer-Namen + Positioning Notes
    peer_names, peer_notes = await _claude_generate_peers(company)
    if not peer_names:
        return {"resolved_ids": [], "peer_notes": {}, "generated_at": None, "from_cache": False}

    # Peers in DB auflösen / anlegen
    all_companies = fetch_companies(limit=500)
    name_to_id: dict[str, str] = {c["name"].lower(): c["id"] for c in all_companies}

    # BUG-34: Self-reference guard — Company darf nicht ihr eigener Peer sein
    company_name_lower = company_name.lower()
    resolved_ids: list[str] = []
    for peer_name in peer_names:
        if peer_name.lower() == company_name_lower:
            logger.info("Self-reference skipped: peer '%s' matches source '%s'", peer_name, company_name)
            continue
        peer_id, is_new = await _resolve_or_create_peer(db, peer_name, name_to_id, company)
        if not peer_id:
            continue
        resolved_ids.append(peer_id)
        name_to_id[peer_name.lower()] = peer_id
        if is_new:
            # Neue Company: vollständiges Enrichment
            await _schedule_or_await(background_tasks, _enrich_new_peer, peer_id, peer_name)
        else:
            # Bestehende Company ohne Score: Enrichment nachholen (PEERS-01)
            try:
                _score_check = db.table("company_scores").select("composite_score").eq(
                    "company_id", peer_id
                ).limit(1).execute()
                _has_score = bool((_score_check.data or [{}])[0].get("composite_score"))
            except Exception:
                _has_score = False
            if not _has_score:
                await _schedule_or_await(background_tasks, _enrich_new_peer, peer_id, peer_name)

    # peers_resolved + peers_context + generated_at in companies schreiben
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        db.table("companies").update({
            "peers":              peer_names,
            "peers_resolved":     resolved_ids,
            "peers_generated_at": now_iso,
            "peers_context":      peer_notes,   # {peer_name: positioning_note}
        }).eq("id", company_id).execute()
        logger.info("peers_resolved + context geschrieben: %s → %d peers", company_name, len(resolved_ids))
    except Exception as e:
        logger.warning("peers_resolved upsert failed for %s: %s", company_name, e)

    return {"resolved_ids": resolved_ids, "peer_notes": peer_notes,
            "generated_at": now_iso, "from_cache": False}


@router.get("/company/{name}/peers", response_model=PeersResponse)
async def get_peers(name: str, background_tasks: BackgroundTasks) -> PeersResponse:
    """
    Gibt Peer-Companies zurück. Cache 30 Tage. Generierungs-Kern = ensure_peers
    (idempotent, auch vom Cold-Path in company_detail genutzt).
    """
    db = get_supabase()

    company = fetch_company_by_name(name)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' nicht gefunden.")

    company_name = company["name"]
    result = await ensure_peers(company, background_tasks=background_tasks)
    resolved_ids = result["resolved_ids"]

    if not resolved_ids:
        return PeersResponse(
            status="empty",
            company_name=company_name,
            peers=[],
            benchmark=[],
        )

    peer_rows = _fetch_peers_by_ids(db, resolved_ids)
    return PeersResponse(
        status="ready",
        company_name=company_name,
        peers=[_to_peer_model(p, result["peer_notes"]) for p in peer_rows],
        benchmark=_build_benchmark(company, peer_rows),
        generated_at=result["generated_at"],
        from_cache=result["from_cache"],
    )


# ── Claude Peer-Generierung ───────────────────────────────────────────────────

async def _claude_generate_peers(company: dict) -> tuple[list[str], dict[str, str]]:
    """
    Claude generiert 4-5 direkte Wettbewerber + Positioning Note je Peer.
    Gibt (peer_names, {peer_name: positioning_note}) zurück.
    """
    api_key = settings.anthropic_api_key
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY fehlt — Peer-Generierung nicht möglich")
        return [], {}

    subject = company.get("name", "")

    # Kontextfelder aufbereiten
    funding_total = company.get("funding_total_usd_mn")
    funding_str   = (f"${funding_total:.0f}M" if funding_total and funding_total < 1000
                     else f"${funding_total/1000:.1f}B" if funding_total else "—")
    tags          = company.get("tags") or []
    tags_str      = ", ".join(tags) if tags else "—"
    tam           = company.get("tam_2035_usd_bn") or company.get("tam_usd_bn")
    tam_str       = f"${tam:.0f}B TAM 2035" if tam else "—"

    prompt = f"""Du bist ein M&A-Analyst. Identifiziere 4-5 direkte Wettbewerber dieser Company.

Company: {subject}
Kategorie: {company.get('category') or '—'}
Industrie: {company.get('industry') or '—'}
Region: {company.get('region') or '—'}
Gegründet: {company.get('founding_year') or '—'}
Funding Stage: {company.get('funding_stage') or '—'}
Funding Gesamt: {funding_str}
Technologie-Tags: {tags_str}
Markt (TAM): {tam_str}
Beschreibung: {company.get('description') or company.get('summary') or '—'}
Investment Path: {company.get('investment_path') or '—'}

Regeln:
- Nur direkte Wettbewerber (gleiche Technologie / gleicher Zielmarkt)
- Bevorzuge Companies ähnlicher Größe und Stage
- Mische US + Europa wenn relevant
- Keine Konglomerate oder reine Investoren
- positioning_note: 1 präziser Satz — warum direkter Wettbewerber, worin liegt der Kernunterschied zu {subject}

Antworte NUR mit einem JSON-Array, keine Erklärung, kein Markdown:
[{{"name": "Company Name", "positioning_note": "Direkter Wettbewerber weil ... — unterscheidet sich von {subject} durch ..."}}]"""

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                _CLAUDE_API_URL,
                headers={
                    "x-api-key":         api_key,
                    "anthropic-version":  "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      "claude-haiku-4-5-20251001",
                    "max_tokens": 1000,
                    "messages":   [{"role": "user", "content": prompt}],
                },
            )

        if resp.status_code != 200:
            logger.warning("Claude Peer-Gen API %s — body: %s", resp.status_code, resp.text[:500])
            return [], {}

        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)

        names: list[str] = []
        notes: dict[str, str] = {}
        seen: set[str] = set()

        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    n    = (item.get("name") or "").strip()
                    note = (item.get("positioning_note") or "").strip()
                elif isinstance(item, str):
                    n, note = item.strip(), ""
                else:
                    continue
                if n and n.lower() not in seen:
                    seen.add(n.lower())
                    names.append(n)
                    if note:
                        notes[n] = note
                if len(names) >= 5:
                    break

        logger.info("Claude Peers für %s: %s", subject, names)
        return names, notes

    except Exception as e:
        logger.warning("_claude_generate_peers failed: %s", e)

    return [], {}


# ── Peer auflösen / anlegen ───────────────────────────────────────────────────

async def _resolve_or_create_peer(
    db,
    peer_name: str,
    name_to_id: dict[str, str],
    source_company: dict,
) -> tuple[str | None, bool]:
    """
    Gibt (UUID, is_new) zurück.
    is_new=True wenn der Peer neu in der DB angelegt wurde → Enrichment triggern.
    Wenn nicht in DB: minimalen Row anlegen (Enrichment läuft im Background).
    """
    # Exakter Match (case-insensitive)
    existing_id = name_to_id.get(peer_name.lower())
    if existing_id:
        return existing_id, False

    # Fuzzy: Substring-Match (z.B. "Climeworks AG" findet "Climeworks")
    for db_name, db_id in name_to_id.items():
        if peer_name.lower() in db_name or db_name in peer_name.lower():
            return db_id, False

    # Nicht gefunden → minimal anlegen
    for source_val in ("peer_generated", "manual"):
        try:
            payload = {
                "name":            peer_name,
                "source":          source_val,
                "investment_path": "Beobachten",
                # Industrie vom Source-Company übernehmen als Startwert
                "industry":        source_company.get("industry"),
                "category":        source_company.get("category"),
                "region":          source_company.get("region"),
            }
            result = db.table("companies").insert(payload).execute()
            if result.data:
                new_id = result.data[0]["id"]
                logger.info("Peer angelegt: %s → %s (source=%s)", peer_name, new_id, source_val)
                return new_id, True
            break
        except Exception as e:
            if source_val == "peer_generated" and "22P02" in str(e):
                logger.warning("Enum 'peer_generated' fehlt — Fallback auf 'manual' für %s", peer_name)
                continue
            logger.warning("Peer anlegen fehlgeschlagen für %s: %s", peer_name, e)
            break

    return None, False


# ── Background: Enrichment für neue Peers ────────────────────────────────────

async def _enrich_new_peer(peer_id: str, peer_name: str) -> None:
    """
    R-21: Vollständiges Enrichment für Peer-Companies (neu oder ohne Score).
    Stufe 1: Wikipedia + Crunchbase → vollständige Persistenz via upsert_company_enrichment
             (identischer Pfad wie company_detail.py — tags, category, industry, ipo_status,
             ticker, exchange, region werden korrekt geschrieben)
    Stufe 2: TAM-Lookup + Market Data Enrichment (async, non-blocking)
    Stufe 3: Scoring mit frischen Daten
    """
    import re as _re
    try:
        from src.services.enrichment import (
            enrich_company, infer_category_industry, normalize_ipo_status_for_db,
        )
        from src.integrations.supabase import upsert_company_enrichment

        # Peer-Row aus DB holen für Guards (ipo_status, ticker, category)
        db = get_supabase()
        peer_row_res = db.table("companies").select("*").eq("id", peer_id).limit(1).execute()
        peer_record  = (peer_row_res.data or [{}])[0]

        enriched = await enrich_company(peer_name, company_record=peer_record)
        if not enriched:
            logger.debug("_enrich_new_peer: kein Ergebnis für %s", peer_name)
            return

        # Headcount normalisieren (gleiche Logik wie company_detail.py)
        def _parse_headcount(value):
            if not value:
                return None
            try:
                match = _re.search(r"(\d[\d,]*)\s*[-–]\s*(\d[\d,]*)", str(value))
                if match:
                    lo = int(match.group(1).replace(",", ""))
                    hi = int(match.group(2).replace(",", ""))
                    n  = (lo + hi) // 2
                else:
                    match = _re.search(r"\d[\d,]*", str(value))
                    if not match:
                        return None
                    n = int(match.group().replace(",", ""))
                return n if 1 <= n <= 100_000 else None
            except Exception:
                return None

        def _parse_year(value):
            if not value:
                return None
            try:
                match = _re.search(r"\b(19|20)\d{2}\b", str(value))
                return int(match.group()) if match else None
            except Exception:
                return None

        # Vollständiger upsert_payload — identisch mit company_detail.py
        upsert_payload: dict = {
            "founding_year": _parse_year(enriched.founded_year),
            "headquarters":  enriched.headquarters or None,
            "headcount":     _parse_headcount(enriched.employee_count),
            "description":   (enriched.description or None),
            "website":       enriched.website or None,
            "tags":          enriched.tags if enriched.tags else None,
        }

        # IPO-STATUS-ENUM-01: enriched.ipo_status ist ein internes Listed/
        # Private-Binärsignal, kein DB-Enum-Wert — normalize_ipo_status_for_db
        # mappt "private" auf None statt eine pre_ipo_*-Stufe zu erraten.
        _ipo_status_for_db = normalize_ipo_status_for_db(enriched.ipo_status)
        if _ipo_status_for_db and not peer_record.get("ipo_status"):
            upsert_payload["ipo_status"] = _ipo_status_for_db

        # Ticker + Exchange: nur für börsennotierte Companies
        _is_listed = (
            peer_record.get("ipo_status") == "listed"
            or enriched.ipo_status == "listed"
            or bool(enriched.ticker)
        )
        if _is_listed:
            if enriched.ticker and not peer_record.get("ticker"):
                upsert_payload["ticker"] = enriched.ticker
            if enriched.exchange and not peer_record.get("exchange"):
                upsert_payload["exchange"] = enriched.exchange

        # category / industry aus Tag-Inferenz
        inferred_cat = enriched.category
        inferred_ind = enriched.industry
        if not inferred_cat and enriched.tags:
            inferred_cat, inferred_ind = infer_category_industry(enriched.tags)
        if inferred_cat and not peer_record.get("category"):
            upsert_payload["category"] = inferred_cat
        if inferred_ind and not peer_record.get("industry"):
            upsert_payload["industry"] = inferred_ind

        # None-Werte rausfiltern — kein versehentliches Überschreiben mit null
        upsert_payload = {k: v for k, v in upsert_payload.items() if v is not None}

        if upsert_payload:
            upsert_company_enrichment(peer_id, upsert_payload)
            logger.info(
                "Peer %s enriched (vollständig): %d Felder — %s",
                peer_name, len(upsert_payload), list(upsert_payload.keys()),
            )

    except Exception as e:
        logger.warning("_enrich_new_peer Enrichment failed für %s: %s", peer_name, e)

    # ── R-21 Stufe 2: TAM + Market Data ──────────────────────────────────────
    # Non-blocking — Fehler hier stoppen nicht die Peer-Generierung
    try:
        from src.services.tam import get_tam
        from src.integrations.supabase import (
            upsert_tam_cache, fetch_market_data, upsert_market_data,
            set_enrichment_status, fetch_all_funding_rounds, fetch_companies,
        )
        from src.services.market_data_enrichment import (
            enrich_market_data, enrich_market_data_sync_wrapper,
        )

        # Category aus DB holen (evtl. gerade geschrieben)
        db = get_supabase()
        peer_row = db.table("companies").select("category, peers_context").eq("id", peer_id).limit(1).execute()
        peer_category = (peer_row.data or [{}])[0].get("category") if peer_row.data else None

        # TAM
        tam = await get_tam(peer_name, peer_category)
        if tam.get("tam_usd_bn"):
            upsert_tam_cache(
                company_id=peer_id,
                tam_usd_bn=tam["tam_usd_bn"],
                cagr_pct=tam.get("cagr_pct"),
                source=tam.get("source", "scrape"),
            )
            logger.info("Peer %s TAM enriched: %.1f Bn", peer_name, tam["tam_usd_bn"])

        # Market Data — nur wenn noch nicht vorhanden
        existing_md = fetch_market_data(peer_id)
        if not existing_md or not existing_md.get("enriched_at"):
            set_enrichment_status(peer_id, "running")
            all_companies = fetch_companies(limit=500)
            all_rounds    = fetch_all_funding_rounds()
            async_result  = await enrich_market_data(
                company_id=peer_id,
                company_name=peer_name,
                category=peer_category,
                sector_tag=None,
                tam_usd_bn=tam.get("tam_usd_bn"),
            )
            sync_result = enrich_market_data_sync_wrapper(
                company_id=peer_id,
                company_name=peer_name,
                category=peer_category,
                sector_tag=None,
                tam_usd_bn=tam.get("tam_usd_bn"),
                all_companies=all_companies,
                all_funding_rounds=all_rounds,
                async_result=async_result,
            )
            upsert_payload = {
                **{k: v for k, v in async_result.items() if k != "_competition_signals"},
                **sync_result,
            }
            upsert_market_data(peer_id, upsert_payload)
            set_enrichment_status(peer_id, "done")
            logger.info("Peer %s market data enriched", peer_name)

    except Exception as e:
        logger.warning("_enrich_new_peer TAM/Market failed für %s: %s", peer_name, e)

    # ── R-21-ext Stufe 3: Scoring ─────────────────────────────────────────────
    # compute_all_scores() mit frisch angereicherten Daten — gibt Peer sofort
    # einen validen Score im Peer Review Benchmark.
    try:
        from src.services.score_calculator import compute_all_scores
        from src.integrations.supabase import (
            upsert_company_scores, fetch_market_data as _fetch_md,
        )

        db = get_supabase()
        peer_full = db.table("companies").select("*").eq("id", peer_id).limit(1).execute()
        peer_company = (peer_full.data or [{}])[0]
        if not peer_company:
            return

        market_data = _fetch_md(peer_id) or {}

        score_result = compute_all_scores(
            company=peer_company,
            market_data=market_data,
            signals=[],
            ownership_entries=[],
            buyers=[],
            value_drivers=[],
        )
        upsert_company_scores(peer_id, score_result.to_dict())
        logger.info(
            "Peer %s scored — hero=%s rating=%s composite=%.1f",
            peer_name,
            score_result.hero_path,
            score_result.rating,
            score_result.composite_score or 0.0,
        )

    except Exception as e:
        logger.warning("_enrich_new_peer Scoring failed für %s: %s", peer_name, e)


# ── DB Helpers ────────────────────────────────────────────────────────────────

def _fetch_peers_by_ids(db, ids: list[str]) -> list[dict]:
    """Lädt Peer-Companies aus DB anhand ihrer UUIDs — inkl. Argo Scores."""
    if not ids:
        return []
    try:
        result = db.table("companies").select(
            "id, name, category, industry, region, headquarters, founding_year, "
            "headcount, funding_total_usd_mn, funding_stage, funding_last_round, "
            "ipo_status, ipo_potential, investment_path, revenue_usd_mn, "
            "description, website, ticker, exchange, summary"
        ).in_("id", ids).execute()
        rows = result.data or []
    except Exception as e:
        logger.warning("_fetch_peers_by_ids companies failed: %s", e)
        return []

    # Argo Scores nachladen + in Peer-Row mergen
    try:
        scores_result = db.table("company_scores").select(
            "company_id, composite_score, rating, financial_score, market_score"
        ).in_("company_id", ids).execute()
        scores_by_id = {
            s["company_id"]: s for s in (scores_result.data or [])
        }
        for row in rows:
            s = scores_by_id.get(row["id"], {})
            row["composite_score"]  = s.get("composite_score")
            row["rating"]           = s.get("rating")
            row["financial_score"]  = s.get("financial_score")
            row["market_score"]     = s.get("market_score")
    except Exception as e:
        logger.warning("_fetch_peers_by_ids scores failed (non-fatal): %s", e)
        for row in rows:
            row.setdefault("composite_score", None)
            row.setdefault("rating", None)
            row.setdefault("financial_score", None)
            row.setdefault("market_score", None)

    return rows


_STAGE_LABEL: dict[str, str] = {
    "seed": "Seed", "pre_seed": "Pre-Seed",
    "series_a": "Series A", "series_b": "Series B",
    "series_c": "Series C", "series_d": "Series D",
    "series_d_plus": "Series D+", "growth": "Growth", "public": "Public",
}


def _to_peer_model(row: dict, peers_context: dict[str, str] | None = None) -> PeerCompany:
    raw_stage = row.get("funding_stage") or ""
    db_name   = row["name"]

    # Positioning-Note Lookup: exakter Match → normalisiert → leer
    # Normalisierung: lowercase + strip — verhindert stille Datenverluste wenn
    # Claude-generierter Name marginal vom DB-Namen abweicht
    note = None
    if peers_context:
        note = peers_context.get(db_name)
        if note is None:
            db_name_norm = db_name.lower().strip()
            for ctx_name, ctx_note in peers_context.items():
                if ctx_name.lower().strip() == db_name_norm:
                    note = ctx_note
                    break

    return PeerCompany(
        id=row["id"],
        name=db_name,
        category=row.get("category"),
        industry=row.get("industry"),
        region=row.get("region"),
        headquarters=row.get("headquarters"),
        founding_year=row.get("founding_year"),
        headcount=row.get("headcount"),
        funding_total_usd_mn=row.get("funding_total_usd_mn"),
        funding_stage=raw_stage,
        funding_last_round=row.get("funding_last_round"),
        ipo_status=row.get("ipo_status"),
        ipo_potential=row.get("ipo_potential"),
        investment_path=row.get("investment_path"),
        revenue_usd_mn=row.get("revenue_usd_mn"),
        description=row.get("description") or row.get("summary"),
        website=row.get("website"),
        ticker=row.get("ticker"),
        exchange=row.get("exchange"),
        stage_normalized=_STAGE_LABEL.get(raw_stage, raw_stage) or None,
        composite_score=row.get("composite_score"),
        rating=row.get("rating"),
        financial_score=row.get("financial_score"),
        market_score=row.get("market_score"),
        positioning_note=note,
    )


# ── Benchmark Berechnung ──────────────────────────────────────────────────────

def _build_benchmark(company: dict, peers: list[dict]) -> list[PeerBenchmark]:
    """
    Berechnet Benchmark-Metriken: Company-Wert vs. Peer-Median.
    Nur Metriken mit ausreichend Datenpunkten (≥2 Peers mit Wert).
    """
    benchmarks: list[PeerBenchmark] = []

    def _median(vals: list[float]) -> float | None:
        if not vals:
            return None
        s = sorted(vals)
        mid = len(s) // 2
        return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2

    def _fmt_m(v: float | None) -> str | None:
        if v is None:
            return None
        if v >= 1000:
            return f"${v/1000:.1f}B"
        return f"${v:.0f}M"

    def _fmt_k(v: int | None) -> str | None:
        return f"{v:,}" if v else None

    # ── Argo Scores (zuerst — wichtigster Vergleichspunkt für VC/PE/M&A) ────
    # Rating-Verteilung Peers
    peer_ratings = [p.get("rating") for p in peers if p.get("rating")]
    if peer_ratings:
        rating_counts: dict[str, int] = {}
        for r in peer_ratings:
            rating_counts[r] = rating_counts.get(r, 0) + 1
        rating_str = " · ".join(
            f"{r} ({n})" for r, n in sorted(rating_counts.items())
        )
        company_rating = company.get("rating") or "—"
        # rating aus company_scores falls vorhanden
        benchmarks.append(PeerBenchmark(
            metric="Argo Rating",
            company_value=company_rating,
            peer_median=rating_str,
            note="A=No-Brainer · B=Solide · C=Abwägen · D=Uninteressant",
        ))

    # Composite Score Median
    peer_scores = [
        float(p["composite_score"]) for p in peers
        if p.get("composite_score") is not None
    ]
    if len(peer_scores) >= 2:
        company_score = company.get("composite_score")
        benchmarks.append(PeerBenchmark(
            metric="Composite Score",
            company_value=f"{company_score:.1f}" if company_score is not None else None,
            peer_median=f"{_median(peer_scores):.1f}" if _median(peer_scores) is not None else None,
            unit="0–10",
            note="Argo Score Engine · SC-01–SC-13",
        ))
    peer_fundings = [p["funding_total_usd_mn"] for p in peers if p.get("funding_total_usd_mn")]
    if len(peer_fundings) >= 2:
        benchmarks.append(PeerBenchmark(
            metric="Funding Total",
            company_value=_fmt_m(company.get("funding_total_usd_mn")),
            peer_median=_fmt_m(_median(peer_fundings)),
            unit="USD",
            note="Peer-Median aus verfügbaren Daten",
        ))

    # Headcount
    peer_hc = [p["headcount"] for p in peers if p.get("headcount")]
    if len(peer_hc) >= 2:
        benchmarks.append(PeerBenchmark(
            metric="Mitarbeiter",
            company_value=_fmt_k(company.get("headcount")),
            peer_median=_fmt_k(int(_median(peer_hc) or 0)),
            note="Peer-Median",
        ))

    # Revenue (wo vorhanden)
    peer_rev = [p["revenue_usd_mn"] for p in peers if p.get("revenue_usd_mn")]
    if len(peer_rev) >= 2:
        benchmarks.append(PeerBenchmark(
            metric="Revenue",
            company_value=_fmt_m(company.get("revenue_usd_mn")),
            peer_median=_fmt_m(_median(peer_rev)),
            unit="USD",
            note="Peer-Median · nur wo Daten verfügbar",
        ))

    # BUG-26: Funding-Pace — Funding-Gesamt ÷ Jahre seit Gründung (USD Mn/Jahr)
    # Proxy für Wachstumsgeschwindigkeit — aussagekräftiger als absolutes Funding
    import datetime
    current_year = datetime.datetime.now().year

    def _funding_pace(c: dict) -> float | None:
        fy = c.get("founding_year")
        ft = c.get("funding_total_usd_mn")
        if not fy or not ft:
            return None
        years = max(current_year - int(fy), 1)
        return round(ft / years, 1)

    peer_pace = [p for p in [_funding_pace(peer) for peer in peers] if p is not None]
    company_pace = _funding_pace(company)
    if len(peer_pace) >= 2:
        pace_median = _median(peer_pace)
        benchmarks.append(PeerBenchmark(
            metric="Funding-Pace",
            company_value=f"${company_pace:.0f}M/Jahr" if company_pace else None,
            peer_median=f"${pace_median:.0f}M/Jahr" if pace_median else None,
            note="Funding-Gesamt ÷ Jahre seit Gründung · Wachstumsgeschwindigkeit",
        ))

    # BUG-26: TechReadiness-Stage-Proxy — grobe Einordnung der Reife
    # Echte TR-Werte aus scores fehlen für Peers → Stage als strukturierter Proxy
    _STAGE_TR: dict[str, float] = {
        "pre_seed": 0.15, "seed": 0.2, "series_a": 0.35,
        "series_b": 0.5, "series_c": 0.65, "series_d": 0.75,
        "series_d_plus": 0.8, "growth": 0.85, "public": 0.9,
    }
    peer_tr = [
        _STAGE_TR[p["funding_stage"]]
        for p in peers
        if p.get("funding_stage") and p["funding_stage"] in _STAGE_TR
    ]
    company_stage = company.get("funding_stage") or ""
    company_tr = _STAGE_TR.get(company_stage)
    if len(peer_tr) >= 2:
        tr_median = _median(peer_tr)
        benchmarks.append(PeerBenchmark(
            metric="TechReadiness (Stage-Proxy)",
            company_value=f"{company_tr:.2f}" if company_tr else None,
            peer_median=f"{tr_median:.2f}" if tr_median else None,
            note="Stage-basierte Näherung · 0.15 (Pre-Seed) → 0.9 (Public)",
        ))

    # Stage-Verteilung (kein Median — descriptiv)
    stage_counts: dict[str, int] = {}
    for p in peers:
        s = _STAGE_LABEL.get(p.get("funding_stage") or "", p.get("funding_stage") or "—")
        stage_counts[s] = stage_counts.get(s, 0) + 1
    if stage_counts:
        stage_str = " · ".join(f"{s} ({n})" for s, n in sorted(stage_counts.items()))
        benchmarks.append(PeerBenchmark(
            metric="Stage-Verteilung Peers",
            company_value=_STAGE_LABEL.get(company.get("funding_stage") or "", company.get("funding_stage") or "—"),
            peer_median=stage_str,
            note="Verteilung der Peer-Companies",
        ))

    return benchmarks
