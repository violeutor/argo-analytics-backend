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
from fastapi import APIRouter, HTTPException
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

@router.get("/company/{name}/peers", response_model=PeersResponse)
async def get_peers(name: str) -> PeersResponse:
    """
    Gibt Peer-Companies zurück.
    Cache: 30 Tage — danach neu generiert via Claude.
    """
    db = get_supabase()

    # 1. Company laden
    company = fetch_company_by_name(name)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{name}' nicht gefunden.")

    company_id   = company["id"]
    company_name = company["name"]

    # 2. Cache prüfen
    peers_resolved = company.get("peers_resolved") or []
    generated_at   = company.get("peers_generated_at")
    cache_valid    = False

    if peers_resolved and generated_at:
        try:
            gen_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            cache_valid = (datetime.now(timezone.utc) - gen_dt).days < _CACHE_TTL_DAYS
        except Exception:
            pass

    if cache_valid and peers_resolved:
        peer_rows = _fetch_peers_by_ids(db, peers_resolved)
        return PeersResponse(
            status="ready",
            company_name=company_name,
            peers=[_to_peer_model(p) for p in peer_rows],
            benchmark=_build_benchmark(company, peer_rows),
            generated_at=generated_at,
            from_cache=True,
        )

    # 3. Claude generiert Peer-Namen
    peer_names = await _claude_generate_peers(company)
    if not peer_names:
        return PeersResponse(
            status="empty",
            company_name=company_name,
            peers=[],
            benchmark=[],
        )

    # 4. Peers in DB auflösen / anlegen
    all_companies = fetch_companies(limit=500)
    name_to_id: dict[str, str] = {c["name"].lower(): c["id"] for c in all_companies}

    resolved_ids: list[str] = []
    for peer_name in peer_names:
        peer_id = await _resolve_or_create_peer(db, peer_name, name_to_id, company)
        if peer_id:
            resolved_ids.append(peer_id)
            name_to_id[peer_name.lower()] = peer_id   # für Folge-Iterationen

    # 5. peers_resolved + generated_at in companies schreiben
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        db.table("companies").update({
            "peers":              peer_names,
            "peers_resolved":     resolved_ids,
            "peers_generated_at": now_iso,
        }).eq("id", company_id).execute()
        logger.info("peers_resolved geschrieben: %s → %d peers", company_name, len(resolved_ids))
    except Exception as e:
        logger.warning("peers_resolved upsert failed for %s: %s", company_name, e)

    # 6. Peer-Daten aus DB aggregieren
    peer_rows = _fetch_peers_by_ids(db, resolved_ids)

    return PeersResponse(
        status="ready",
        company_name=company_name,
        peers=[_to_peer_model(p) for p in peer_rows],
        benchmark=_build_benchmark(company, peer_rows),
        generated_at=now_iso,
        from_cache=False,
    )


# ── Claude Peer-Generierung ───────────────────────────────────────────────────

async def _claude_generate_peers(company: dict) -> list[str]:
    """
    Claude generiert 3-5 direkte Wettbewerber.
    Gibt Liste von Company-Namen zurück (englisch, ohne Rechtssuffix).
    """
    api_key = settings.anthropic_api_key
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY fehlt — Peer-Generierung nicht möglich")
        return []

    prompt = f"""Du bist ein M&A-Analyst. Identifiziere die 4-5 direkten Wettbewerber dieser Company.

Company: {company.get('name')}
Kategorie: {company.get('category') or '—'}
Industrie: {company.get('industry') or '—'}
Region: {company.get('region') or '—'}
Beschreibung: {company.get('description') or company.get('summary') or '—'}
Funding Stage: {company.get('funding_stage') or '—'}
Investment Path: {company.get('investment_path') or '—'}

Regeln:
- Nur direkte Wettbewerber (gleiche Technologie / gleicher Markt)
- Bevorzuge Companies ähnlicher Größe und Stage
- Mische US + Europa wenn relevant
- Keine Konglomerate oder reine Investoren
- Antworte NUR mit einem JSON-Array von Company-Namen, keine Erklärung:
["Name 1", "Name 2", "Name 3", "Name 4"]"""

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                _CLAUDE_API_URL,
                headers={
                    "x-api-key":        api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type":     "application/json",
                },
                json={
                    "model":      "claude-haiku-4-5-20251001",
                    "max_tokens": 200,
                    "messages":   [{"role": "user", "content": prompt}],
                },
            )

        if resp.status_code != 200:
            logger.warning("Claude Peer-Gen API %s", resp.status_code)
            return []

        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        names = json.loads(raw)

        if isinstance(names, list):
            # Bereinigen: max 5, keine leeren Strings, keine Duplikate
            seen = set()
            result = []
            for n in names:
                if isinstance(n, str) and n.strip() and n.strip().lower() not in seen:
                    seen.add(n.strip().lower())
                    result.append(n.strip())
                if len(result) >= 5:
                    break
            logger.info("Claude Peers für %s: %s", company.get("name"), result)
            return result

    except Exception as e:
        logger.warning("_claude_generate_peers failed: %s", e)

    return []


# ── Peer auflösen / anlegen ───────────────────────────────────────────────────

async def _resolve_or_create_peer(
    db,
    peer_name: str,
    name_to_id: dict[str, str],
    source_company: dict,
) -> str | None:
    """
    Gibt UUID des Peers zurück.
    Wenn nicht in DB: minimalen Row anlegen (Enrichment läuft beim ersten Company-Aufruf).
    """
    # Exakter Match (case-insensitive)
    existing_id = name_to_id.get(peer_name.lower())
    if existing_id:
        return existing_id

    # Fuzzy: Substring-Match (z.B. "Climeworks AG" findet "Climeworks")
    for db_name, db_id in name_to_id.items():
        if peer_name.lower() in db_name or db_name in peer_name.lower():
            return db_id

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
                return new_id
            break
        except Exception as e:
            if source_val == "peer_generated" and "22P02" in str(e):
                logger.warning("Enum 'peer_generated' fehlt — Fallback auf 'manual' für %s", peer_name)
                continue
            logger.warning("Peer anlegen fehlgeschlagen für %s: %s", peer_name, e)
            break

    return None


# ── DB Helpers ────────────────────────────────────────────────────────────────

def _fetch_peers_by_ids(db, ids: list[str]) -> list[dict]:
    """Lädt Peer-Companies aus DB anhand ihrer UUIDs."""
    if not ids:
        return []
    try:
        result = db.table("companies").select(
            "id, name, category, industry, region, headquarters, founding_year, "
            "headcount, funding_total_usd_mn, funding_stage, funding_last_round, "
            "ipo_status, ipo_potential, investment_path, revenue_usd_mn, "
            "description, website, ticker, exchange, summary"
        ).in_("id", ids).execute()
        return result.data or []
    except Exception as e:
        logger.warning("_fetch_peers_by_ids failed: %s", e)
        return []


_STAGE_LABEL: dict[str, str] = {
    "seed": "Seed", "pre_seed": "Pre-Seed",
    "series_a": "Series A", "series_b": "Series B",
    "series_c": "Series C", "series_d": "Series D",
    "series_d_plus": "Series D+", "growth": "Growth", "public": "Public",
}


def _to_peer_model(row: dict) -> PeerCompany:
    raw_stage = row.get("funding_stage") or ""
    return PeerCompany(
        id=row["id"],
        name=row["name"],
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

    # Funding Total
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
