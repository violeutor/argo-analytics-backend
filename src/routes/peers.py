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

import asyncio
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
from src.services.score_calculator import _is_listed as _resolve_is_listed
from src.services.llm_name_validation import split_llm_company_name

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
    # PEER-ISLISTED-FIELD-01: kanonische Quelle statt Frontend-Ableitung aus ipo_status
    is_listed: bool | None = None
    # Benchmark-Felder (berechnet)
    stage_normalized: str | None = None
    # Argo Scores (aus company_scores)
    composite_score: float | None = None
    rating: str | None = None          # A | B | C | D
    financial_score: float | None = None
    market_score: float | None = None
    # R-10: Positioning note (Claude-generiert, relativ zu Subject Company)
    positioning_note: str | None = None
    # PEER-RELATION-01: strukturierte Sektornähe statt nur Freitext — Claude
    # klassifiziert ohnehin implizit für die positioning_note, hier explizit
    # gemacht für Sortierung/Gruppierung im Frontend.
    # direct = gleiche Technologie/Zielmarkt · adjacent = angrenzend, überlappender
    # Markt oder Kunde · analog = anderes Feld, vergleichbares Geschäftsmodell/Stage
    relation: str | None = None


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
        return {"resolved_ids": [], "peer_notes": {}, "peer_relations": {},
                "generated_at": None, "from_cache": False}

    # Cache prüfen
    peers_resolved  = company.get("peers_resolved") or []
    generated_at    = company.get("peers_generated_at")
    peers_context   = company.get("peers_context") or {}
    # PEER-RELATION-01: eigene Spalte statt peers_context umzuformen — bestehende
    # gecachte peers_context-Werte (reiner String je Peer) bleiben unangetastet,
    # kein Migrations-/Parsing-Sonderfall für altes vs. neues Format nötig.
    peers_relations = company.get("peers_relations") or {}
    cache_valid    = False
    if peers_resolved and generated_at:
        try:
            gen_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            cache_valid = (datetime.now(timezone.utc) - gen_dt).days < _CACHE_TTL_DAYS
        except Exception:
            pass
    if cache_valid and peers_resolved:
        return {"resolved_ids": peers_resolved, "peer_notes": peers_context,
                "peer_relations": peers_relations,
                "generated_at": generated_at, "from_cache": True}

    # Claude generiert Peer-Namen + Positioning Notes + Relation
    peer_names, peer_notes, peer_relations = await _claude_generate_peers(company)
    if not peer_names:
        return {"resolved_ids": [], "peer_notes": {}, "peer_relations": {},
                "generated_at": None, "from_cache": False}

    # PEERS-LOOKUP-SCALE-01 (04.07.): fetch_companies(limit=500) entfernt —
    # derselbe Skalierungs-Bug wie company_detail.py (Zeile ~1892, dort
    # bereits gefixt): fetch_companies() sortiert alphabetisch, bei >500
    # Companies in der DB fehlen Namen ab einem bestimmten Anfangsbuchstaben
    # im Snapshot komplett → jeder Peer dahinter gilt fälschlich als "neu",
    # _resolve_or_create_peer legt ein Duplikat an (kein Unique-Constraint,
    # roher .insert()). Wahrscheinlichste Ursache für PEERS-BUYERS-
    # PERSISTENCE-01. name_to_id bleibt als reiner Call-lokaler Cache (verhindert
    # doppelte Anlage, wenn derselbe Peer-Name zweimal in einer generierten
    # Liste auftaucht) — Existenz-Check läuft jetzt gezielt in
    # _resolve_or_create_peer() gegen die DB, nicht gegen einen Snapshot.
    name_to_id: dict[str, str] = {}

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
        # BUG-34 (Erweiterung, Session 83): der Name-Guard oben fängt nur exakte
        # String-Treffer. Liefert Claude eine Namensvariante (z.B. "Watershed
        # Climate" statt "Watershed"), matched der String-Vergleich nicht — aber
        # _resolve_or_create_peer kann trotzdem per Fuzzy-Match auf dieselbe
        # bestehende Company-Zeile zurückfallen. Deshalb zusätzlich auf die
        # tatsächlich aufgelöste ID prüfen, nicht nur auf den Rohnamen.
        if peer_id == company.get("id"):
            logger.warning(
                "Self-reference (ID-Match) übersprungen: Claude-Name '%s' löste auf Subject '%s' selbst auf",
                peer_name, company_name,
            )
            continue
        resolved_ids.append(peer_id)
        name_to_id[peer_name.lower()] = peer_id
        if is_new:
            # Neue Company: vollständiges Enrichment
            await _schedule_or_await(background_tasks, _enrich_new_peer, peer_id, peer_name)
        else:
            # Bestehende Company: zwei unabhängige Nachhol-Gründe.
            # (1) PEERS-01: kein Score → volles Enrichment nötig.
            # (2) CATEGORY-INHERIT-01: Score vorhanden, aber category_inherited
            #     noch true → NUR die schlanke Kategorie-Korrektur, nicht den
            #     ganzen Zyklus (TAM/Financials/Scoring sind bereits korrekt,
            #     das nochmal zu laufen wäre unnötige DDG/yfinance/Anthropic-Last).
            # Macht künftige Fälle dieses Bugmusters selbstheilend, sobald
            # irgendeine Subject-Company ihre Peer-Liste refresht — unabhängig
            # vom einmaligen Backfill für den aktuellen Bestand.
            try:
                _peer_check = db.table("companies").select("category_inherited").eq(
                    "id", peer_id
                ).limit(1).execute()
                _cat_inherited = bool((_peer_check.data or [{}])[0].get("category_inherited"))
            except Exception:
                _cat_inherited = False
            try:
                _score_check = db.table("company_scores").select("composite_score").eq(
                    "company_id", peer_id
                ).limit(1).execute()
                _has_score = bool((_score_check.data or [{}])[0].get("composite_score"))
            except Exception:
                _has_score = False
            if not _has_score:
                await _schedule_or_await(background_tasks, _enrich_new_peer, peer_id, peer_name)
            elif _cat_inherited:
                await _schedule_or_await(background_tasks, _backfill_category_only, peer_id, peer_name)

    # peers_resolved + peers_context + peers_relations + generated_at schreiben
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        db.table("companies").update({
            "peers":              peer_names,
            "peers_resolved":     resolved_ids,
            "peers_generated_at": now_iso,
            "peers_context":      peer_notes,       # {peer_name: positioning_note}
            "peers_relations":    peer_relations,   # {peer_name: direct|adjacent|analog}
        }).eq("id", company_id).execute()
        logger.info("peers_resolved + context geschrieben: %s → %d peers", company_name, len(resolved_ids))
    except Exception as e:
        logger.warning("peers_resolved upsert failed for %s: %s", company_name, e)

    return {"resolved_ids": resolved_ids, "peer_notes": peer_notes,
            "peer_relations": peer_relations,
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
        peers=[_to_peer_model(p, result["peer_notes"], result["peer_relations"]) for p in peer_rows],
        benchmark=_build_benchmark(company, peer_rows),
        generated_at=result["generated_at"],
        from_cache=result["from_cache"],
    )


# ── CATEGORY-INHERIT-01: einmaliger Backfill-Trigger ─────────────────────────
# Session 83: für den aktuellen Bestand (173 per Reset markierte Peers,
# source='peer_generated'). Kein Cron, kein Dauerbetrieb. Nach bestätigtem
# Abschluss (Monitoring-Query unten liefert 0) kann dieser Block wieder
# entfernt werden — bewusst NICHT dauerhaft im Router lassen, ist reine
# Migrationshilfe, kein Produktfeature.
#
# Einfacher Token-Schutz statt vollem Auth-Wiring: das ist ein POST-Endpoint
# mit echtem Effekt (Netzwerk-Calls, DB-Writes für bis zu ~200 Companies) —
# ungeschützt öffentlich erreichbar wäre ein Einfallstor für versehentliche
# oder böswillige Wiederholungsaufrufe. Token ist hart codiert, ausreichend
# für den kurzen Zeitraum zwischen Deploy und Backfill-Abschluss, KEIN Ersatz
# für echte Auth — Andreas' eigener Zugriff via curl direkt nach Deploy.
_BACKFILL_TOKEN = "s83-category-backfill-x7k2"


class BackfillResult(BaseModel):
    status: str
    queued: int


@router.post("/admin/peers/backfill-category", response_model=BackfillResult)
async def backfill_category_inherited(
    background_tasks: BackgroundTasks, token: str, limit: int = 250,
) -> BackfillResult:
    if token != _BACKFILL_TOKEN:
        raise HTTPException(status_code=403, detail="Ungültiges Token.")

    db = get_supabase()
    rows = db.table("companies").select("id, name").eq("category_inherited", True).limit(limit).execute()
    targets = rows.data or []
    for row in targets:
        background_tasks.add_task(_backfill_category_only, row["id"], row["name"])
    logger.info("CATEGORY-INHERIT-01 Backfill: %d Companies queued", len(targets))
    return BackfillResult(status="queued", queued=len(targets))


# ── Claude Peer-Generierung ───────────────────────────────────────────────────

async def _claude_generate_peers(company: dict) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """
    Claude generiert 4-5 Wettbewerber/Vergleichs-Companies + Positioning Note +
    strukturierte Relation (direct/adjacent/analog) je Peer.
    Gibt (peer_names, {peer_name: positioning_note}, {peer_name: relation}) zurück.
    """
    api_key = settings.anthropic_api_key
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY fehlt — Peer-Generierung nicht möglich")
        return [], {}, {}

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
- Bevorzuge Companies ähnlicher Größe und Stage
- Mische US + Europa wenn relevant
- Keine Konglomerate oder reine Investoren
- positioning_note: 1 präziser Satz — warum Wettbewerber/vergleichbar, worin liegt der Kernunterschied zu {subject}
- relation: genau einer von drei Werten —
  "direct" = gleiche Technologie UND gleicher Zielmarkt, austauschbares Angebot
  "adjacent" = überlappender Markt oder Kunde, aber andere Technologie/Teilsegment
  "analog" = anderes Feld, aber vergleichbares Geschäftsmodell/Stage (Referenzpunkt, kein echter Wettbewerber)
- Mische nicht nur "direct" — wenn es für {subject} weniger als 3 echte direkte Wettbewerber gibt, ergänze bewusst adjacent/analog statt sie zu erzwingen

Antworte NUR mit einem JSON-Array, keine Erklärung, kein Markdown:
[{{"name": "Company Name", "relation": "direct", "positioning_note": "Direkter Wettbewerber weil ... — unterscheidet sich von {subject} durch ..."}}]"""

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
        relations: dict[str, str] = {}
        seen: set[str] = set()
        _VALID_RELATIONS = {"direct", "adjacent", "analog"}

        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    raw_n    = (item.get("name") or "").strip()
                    note     = (item.get("positioning_note") or "").strip()
                    relation = (item.get("relation") or "").strip().lower()
                elif isinstance(item, str):
                    raw_n, note, relation = item.strip(), "", ""
                else:
                    continue

                # PEER-GEN-NAME-VALIDATION-01: "X / Y" → beide als eigene
                # Kandidaten, "X (nun Y)" → Rename-Notiz abgeschnitten. Note/
                # Relation gelten inhaltlich für beide Split-Hälften gleicher-
                # maßen (Haiku begründet meist das Paar gemeinsam, z.B. "beide
                # sind Nischenanbieter im selben Subsegment") — dieselbe
                # Zuordnung wie im unveränderten Einzelnamen-Fall.
                for n in split_llm_company_name(raw_n):
                    if n and n.lower() not in seen:
                        seen.add(n.lower())
                        names.append(n)
                        if note:
                            notes[n] = note
                        # Fail-loud statt stillem Falsch-Wert: unbekannte/fehlende
                        # Relation wird NICHT geraten (z.B. immer "adjacent"),
                        # sondern bleibt None — Frontend zeigt das als "unklassifiziert"
                        # statt eine falsche Sicherheit vorzutäuschen.
                        if relation in _VALID_RELATIONS:
                            relations[n] = relation
                        elif relation:
                            logger.warning(
                                "Claude Peer-Gen: unbekannter relation-Wert '%s' für %s (Subject %s) — verworfen",
                                relation, n, subject,
                            )
                    if len(names) >= 5:
                        break
                if len(names) >= 5:
                    break

        logger.info("Claude Peers für %s: %s", subject, names)
        return names, notes, relations

    except Exception as e:
        logger.warning("_claude_generate_peers failed: %s", e)

    return [], {}, {}


# ── Peer auflösen / anlegen ───────────────────────────────────────────────────

def _autopick_peer(result) -> "object | None":
    """
    PEER-IDENT-01 · Nicht-interaktiver Pick aus einem WikidataResolutionResult,
    analog zu buyer_enrichment._autopick_listed — aber OHNE Listed-Zwang: ein
    Peer darf (und ist meistens) privat sein, anders als ein Buyer. Nimmt den
    direkten Treffer wenn vorhanden, sonst den ersten Kandidaten — bewusst
    keine Disambiguierungs-Logik hier (DISAMBIG-REVIEW-01 ist der richtige,
    eigenständige Ort dafür, nicht dieser stille Background-Call).
    """
    resolved = getattr(result, "resolved", None)
    if resolved is not None:
        return resolved
    candidates = getattr(result, "candidates", None) or []
    return candidates[0] if candidates else None


async def _resolve_or_create_peer(
    db,
    peer_name: str,
    name_to_id: dict[str, str],
    source_company: dict,
) -> tuple[str | None, bool]:
    """
    Gibt (UUID, is_new) zurück.
    is_new=True wenn der Peer neu in der DB angelegt wurde → Enrichment triggern.

    PEER-IDENT-01: Vor dem Insert wird die Identität geprüft statt der Haiku-
    Behauptung blind zu vertrauen — Wikidata-Resolve (Auto-Pick, kein Modal,
    KEIN Listed-Zwang, anders als bei Buyern) → bei Fehlschlag DDG-Existenz-
    Check (Crunchbase-Site-Scope, dann generisch). Bleibt auch das leer, wird
    TROTZDEM angelegt — nur als identity_confidence='unverified' geflaggt,
    statt verworfen zu werden. Drop wie bei Buyern wäre hier verlustreich:
    kleine/private Wettbewerber, die schlicht nicht indexiert sind, sind
    trotzdem reale Companies — anders als ein unauflösbarer Buyer-Name, der
    per Definition kein tauglicher Buyer ist.
    """
    # Exakter Match — zuerst im Call-lokalen Cache (schnell, deckt Mehrfach-
    # nennung desselben Namens innerhalb dieser Peer-Liste ab).
    existing_id = name_to_id.get(peer_name.lower())
    if existing_id:
        return existing_id, False

    # PEERS-LOOKUP-SCALE-01 (04.07.): gezielter DB-Lookup statt In-Memory-Scan
    # über einen (bei >500 Companies potenziell unvollständigen) Snapshot.
    from src.integrations.supabase import fetch_company_by_name, search_companies_by_name

    exact_hit = fetch_company_by_name(peer_name)
    if exact_hit and exact_hit.get("id"):
        return exact_hit["id"], False

    # Fuzzy: Substring-Match (z.B. "Climeworks AG" findet "Climeworks")
    for row in search_companies_by_name(peer_name, limit=10):
        row_name = (row.get("name") or "").lower()
        if peer_name.lower() in row_name or row_name in peer_name.lower():
            return row["id"], False

    # ── PEER-IDENT-01: Identität prüfen, bevor wir eine companies-Row anlegen ──
    resolved_name:       str        = peer_name
    resolved_ticker:     str | None = None
    resolved_exchange:   str | None = None
    resolved_hq:         str | None = None
    resolved_founded:    int | None = None
    identity_confidence: str        = "unverified"

    try:
        from src.services.wikidata_resolver import resolve_entity
        async with httpx.AsyncClient(timeout=10.0) as _wd_client:
            wd_result = await resolve_entity(peer_name, client=_wd_client)
        picked = _autopick_peer(wd_result)
        if picked is not None:
            resolved_name     = getattr(picked, "display_name", None) or getattr(picked, "name", None) or peer_name
            resolved_ticker   = getattr(picked, "ticker", None)
            resolved_exchange = getattr(picked, "display_exchange", None)
            resolved_hq       = getattr(picked, "headquarters", None)
            _founded = getattr(picked, "founded_year", None)
            try:
                resolved_founded = int(_founded) if _founded else None
            except (ValueError, TypeError):
                resolved_founded = None
            identity_confidence = "verified_wikidata"
    except Exception as e:
        logger.debug("PEER-IDENT-01 Wikidata-Resolve failed für '%s': %s", peer_name, e)

    if identity_confidence == "unverified":
        try:
            from src.services.market_data_enrichment import check_entity_existence
            found, source = await check_entity_existence(peer_name)
            if found:
                identity_confidence = source
        except Exception as e:
            logger.debug("PEER-IDENT-01 DDG-Existenz-Check failed für '%s': %s", peer_name, e)

    if identity_confidence == "unverified":
        logger.info(
            "PEER-IDENT-01: '%s' weder via Wikidata noch DDG verifizierbar → "
            "wird trotzdem angelegt, aber als unverified geflaggt (kein Drop)",
            peer_name,
        )

    # Nicht in DB → anlegen (mit Resolve-Daten falls vorhanden + identity_confidence)
    for source_val in ("peer_generated", "manual"):
        try:
            payload = {
                "name":                resolved_name,
                "source":              source_val,
                "investment_path":     "Beobachten",
                # Industrie vom Source-Company übernehmen als Startwert
                "industry":            source_company.get("industry"),
                "category":            source_company.get("category"),
                "region":              source_company.get("region"),
                "identity_confidence": identity_confidence,
                # CATEGORY-INHERIT-01: markiert category/industry explizit als
                # geerbten Platzhalter, nicht als echten Wert — die spätere
                # Anreicherung (_enrich_new_peer) darf ihn überschreiben, auch
                # wenn das Feld technisch schon "gefüllt" ist. Ohne dieses Flag
                # verhindert der reine NULL-Check in _enrich_new_peer, dass die
                # echte, peer-eigene Kategorie je geschrieben wird (derselbe
                # Fehlertyp wie HAI-GATE-01, S82: Feldzustand als Ersatz für ein
                # eigenständiges Signal missbraucht).
                "category_inherited":  True,
            }
            if resolved_ticker:
                payload["ticker"] = resolved_ticker
            if resolved_exchange:
                payload["exchange"] = resolved_exchange
            if resolved_hq:
                payload["headquarters"] = resolved_hq
            if resolved_founded:
                payload["founding_year"] = resolved_founded
            result = db.table("companies").insert(payload).execute()
            if result.data:
                new_id = result.data[0]["id"]
                logger.info(
                    "Peer angelegt: %s → %s (source=%s, identity_confidence=%s)",
                    resolved_name, new_id, source_val, identity_confidence,
                )
                return new_id, True
            break
        except Exception as e:
            if source_val == "peer_generated" and "22P02" in str(e):
                logger.warning("Enum 'peer_generated' fehlt — Fallback auf 'manual' für %s", peer_name)
                continue
            _msg = str(e)
            if resolved_ticker and ("duplicate key" in _msg.lower() or "23505" in _msg):
                # PEER-GEN-NAME-VALIDATION-01-Folgefund (04.07.): analog zu
                # find_or_create_buyer_company()'s Race-Handling. Wenn zwei
                # Split-Kandidaten ("X / Y") beide auf denselben Ticker
                # auflösen (z.B. weil sie tatsächlich dieselbe Company sind),
                # verhindert companies_ticker_unique zwar korrekt die zweite
                # Row — ohne Re-Query ging der zweite Kandidat bisher aber
                # spurlos verloren statt mit der bereits existierenden Row
                # verknüpft zu werden (Andreas-Frage 04.07.).
                try:
                    hit = (db.table("companies").select("*")
                           .ilike("ticker", resolved_ticker).limit(1).execute().data)
                    if hit:
                        logger.info(
                            "Peer-Race erkannt für '%s' (Ticker %s bereits vergeben) — "
                            "verknüpft mit bestehender Row statt Duplikat",
                            peer_name, resolved_ticker,
                        )
                        return hit[0]["id"], False
                except Exception as e2:
                    logger.warning(
                        "Peer anlegen: Re-Query nach Ticker-Race failed für %s: %s",
                        peer_name, e2,
                    )
            logger.warning("Peer anlegen fehlgeschlagen für %s: %s", peer_name, e)
            break

    return None, False


# ── Background: schlanker Kategorie-Backfill (CATEGORY-INHERIT-01) ──────────

async def _backfill_category_only(peer_id: str, peer_name: str) -> None:
    """
    CATEGORY-INHERIT-01 · Session 83: leichte Variante von _enrich_new_peer
    Stufe 1 — nur Identität/Kategorie neu ermitteln, KEIN Financials-/TAM-/
    Market-/Scoring-Durchlauf. Für Companies gedacht, die bereits einen validen
    Score haben und nur wegen der geerbten Platzhalter-Kategorie betroffen
    sind — der volle Zyklus wäre unnötige DDG/yfinance/Anthropic-Last für
    Daten, die schon korrekt sind.
    """
    try:
        from src.services.enrichment import enrich_company, infer_category_industry
        from src.integrations.supabase import upsert_company_enrichment

        db = get_supabase()
        peer_row_res = db.table("companies").select("*").eq("id", peer_id).limit(1).execute()
        peer_record = (peer_row_res.data or [{}])[0]
        if not peer_record:
            return

        enriched = await enrich_company(peer_record.get("name") or peer_name, company_record=peer_record)
        if not enriched:
            logger.warning("CATEGORY-INHERIT-01 Backfill: kein Enrichment-Ergebnis für %s", peer_name)
            return

        inferred_cat = enriched.category
        inferred_ind = enriched.industry
        if not inferred_cat and enriched.tags:
            inferred_cat, inferred_ind = infer_category_industry(enriched.tags)

        if inferred_cat:
            upsert_payload: dict = {"category": inferred_cat, "category_inherited": False}
            if inferred_ind:
                upsert_payload["industry"] = inferred_ind
            upsert_company_enrichment(peer_id, upsert_payload)
            logger.info(
                "CATEGORY-INHERIT-01 Backfill: %s → category=%s industry=%s",
                peer_name, inferred_cat, inferred_ind,
            )
        else:
            # Fail-loud statt still: kein Fund wird NICHT als Erfolg maskiert,
            # category_inherited bleibt true — sichtbar in der Monitoring-Query,
            # nächster natürlicher Besuch der Company-Seite versucht es erneut
            # (R1-GUARD greift ohnehin, solange category NULL ist).
            logger.warning(
                "CATEGORY-INHERIT-01 Backfill: keine Kategorie ermittelbar für %s — Flag bleibt gesetzt",
                peer_name,
            )

    except Exception as e:
        logger.warning("CATEGORY-INHERIT-01 Backfill failed für %s: %s", peer_name, e)


# ── Background: Enrichment für neue Peers ────────────────────────────────────

async def _enrich_new_peer(peer_id: str, peer_name: str) -> None:
    """
    R-21: Vollständiges Enrichment für Peer-Companies (neu oder ohne Score).
    Stufe 1:   Wikipedia + Crunchbase → vollständige Persistenz via upsert_company_enrichment
               (identischer Pfad wie company_detail.py — tags, category, industry, ipo_status,
               ticker, exchange, region werden korrekt geschrieben)
    Stufe 1.5: FUND-PRIORITY-01 — Financials (market_cap/cash/debt_ebitda) via yfinance,
               VOR TAM/Market/Scoring. Gated auf identity_confidence (PEER-IDENT-01) —
               kein Financials-Call für frisch als 'unverified' geflaggte Peers.
    Stufe 2:   TAM-Lookup + Market Data Enrichment (async, non-blocking)
    Stufe 3:   Scoring mit frischen Daten
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

        # PEER-IDENT-01: Falls das Gate einen kanonischen Namen aufgelöst hat
        # (Wikidata-Resolve in _resolve_or_create_peer), ist der präziser als
        # der rohe Haiku-Name — bessere Trefferquote bei der Wikipedia-Suche.
        _lookup_name = peer_record.get("name") or peer_name
        enriched = await enrich_company(_lookup_name, company_record=peer_record)
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
        # CATEGORY-INHERIT-01: Guard prüft jetzt category_inherited statt nur
        # NULL — eine beim Anlegen kopierte Kategorie (Startwert von der
        # Source-Company) blockiert die echte, peer-eigene Klassifizierung
        # nicht mehr. Legacy-Rows ohne das Flag (vor diesem Fix angelegt,
        # category_inherited IS NULL) werden konservativ wie "nicht geerbt"
        # behandelt — die Reset-Migration holt diese separat nach.
        inferred_cat = enriched.category
        inferred_ind = enriched.industry
        if not inferred_cat and enriched.tags:
            inferred_cat, inferred_ind = infer_category_industry(enriched.tags)
        _cat_was_inherited = bool(peer_record.get("category_inherited"))
        if inferred_cat and (not peer_record.get("category") or _cat_was_inherited):
            upsert_payload["category"] = inferred_cat
        if inferred_ind and (not peer_record.get("industry") or _cat_was_inherited):
            upsert_payload["industry"] = inferred_ind
        # Sobald eine echte Kategorie geschrieben wird, ist sie nicht mehr
        # "nur geerbt" — Flag zurücksetzen, sonst würde eine spätere,
        # möglicherweise schlechtere Neu-Anreicherung sie wieder überschreiben.
        if "category" in upsert_payload or "industry" in upsert_payload:
            upsert_payload["category_inherited"] = False

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

    # ── FUND-PRIORITY-01 · Stufe 1.5: Financials VOR TAM/Market/Scoring ──────
    # Quantitative Kerndaten sind die Berechnungsgrundlage für Size-Gate/Scoring
    # (Prinzip Financial-Fundamentals-Priorität, S67) → deshalb hier, vor Stufe 2.
    # Gated auf identity_confidence (PEER-IDENT-01): ein als 'unverified'
    # geflaggter Peer bekommt noch keinen yfinance-Call spendiert. Legacy-Rows
    # ohne den Wert (NULL, vor der Migration angelegt) werden NICHT blockiert —
    # nur ein explizit geprüftes und leeres Ergebnis blockt.
    # Eigener, frischer DB-Read statt Wiederverwendung der Stufe-1-Variablen
    # (upsert_payload/enriched) — die könnten unbound sein, wenn Stufe 1 mitten
    # in ihrem try-Block gescheitert ist (SNIPPETS-UNBOUND-01-Muster, hier
    # bewusst nicht wiederholt).
    try:
        db = get_supabase()
        _fin_row = db.table("companies").select(
            "ticker, exchange, identity_confidence, market_cap_usd_bn"
        ).eq("id", peer_id).limit(1).execute()
        _fin_record = (_fin_row.data or [{}])[0]

        _fin_ticker   = _fin_record.get("ticker")
        _fin_exchange = _fin_record.get("exchange")
        _fin_id_conf  = _fin_record.get("identity_confidence")

        if _fin_id_conf == "unverified":
            logger.debug(
                "Peer %s: Financials-Fetch übersprungen (identity_confidence=unverified)",
                peer_name,
            )
        elif _fin_ticker and not _fin_record.get("market_cap_usd_bn"):
            from src.integrations.supabase import persist_company_financials
            from src.services.buyer_enrichment import (
                _fetch_company_financials_sync, _yahoo_ticker,
            )

            _yt = _yahoo_ticker(_fin_ticker, _fin_exchange)
            if _yt:
                fin = await asyncio.wait_for(
                    asyncio.to_thread(_fetch_company_financials_sync, _yt, _fin_exchange),
                    timeout=12.0,
                )
                if fin.get("market_cap_usd_bn") is not None or fin.get("debt_ebitda") is not None:
                    persist_company_financials(
                        peer_id,
                        fin.get("market_cap_usd_bn"),
                        fin.get("cash_usd_bn"),
                        fin.get("debt_ebitda"),
                    )
                    logger.info(
                        "Peer %s Financials enriched (FUND-PRIORITY-01): mcap=%s cash=%s debt_ebitda=%s",
                        peer_name, fin.get("market_cap_usd_bn"),
                        fin.get("cash_usd_bn"), fin.get("debt_ebitda"),
                    )

    except asyncio.TimeoutError:
        logger.warning("_enrich_new_peer Financials-Fetch Timeout für %s", peer_name)
    except Exception as e:
        logger.warning("_enrich_new_peer Financials-Fetch failed für %s: %s", peer_name, e)

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
            # PEERS-LOOKUP-SCALE-01 (04.07.) — Stopgap, kein vollständiger Fix:
            # anders als der Existenz-Check oben (jetzt gezielter Lookup) braucht
            # enrich_market_data_sync_wrapper() eine BREITE Kandidatenliste für
            # Competition-Signale, kein Einzel-Name-Match — ein gezielter Lookup
            # ist hier nicht 1:1 übertragbar. Limit angehoben (500→5000) als
            # Übergangslösung gegen den akuten Skalierungs-Bug; der eigentliche
            # Fix (serverseitige Filterung z.B. nach category/industry statt
            # Full-Table-Fetch + In-Memory-Filter) braucht Einsicht in
            # enrich_market_data_sync_wrapper() selbst — nicht Teil dieser Session.
            all_companies = fetch_companies(limit=5000)
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

        # PEERS-REWORK-01: compute_all_scores() kennt kein 'buyers'-Kwarg — der
        # echte Parameter heißt 'ma_aggregate' (verifiziert gegen den Aufruf in
        # company_detail.py). Peers durchlaufen die Buyer-Enrichment-Engine nicht,
        # deshalb hier derselbe "kein Aggregat vorhanden"-Fallback, den
        # compute_strategic_score() auch für Companies ohne Buyer-Match nutzt —
        # kein Sonderfall, sondern der bereits vorgesehene neutrale Default.
        score_result = compute_all_scores(
            company=peer_company,
            market_data=market_data,
            signals=[],
            ownership_entries=[],
            ma_aggregate={"aggregate_score": None, "basis": "none",
                          "deals_considered": 0, "feasible_count": 0, "contributors": []},
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
            "description, website, ticker, exchange, summary, is_listed"
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


def _to_peer_model(
    row: dict,
    peers_context: dict[str, str] | None = None,
    peers_relations: dict[str, str] | None = None,
) -> PeerCompany:
    raw_stage = row.get("funding_stage") or ""
    db_name   = row["name"]

    # Positioning-Note Lookup: exakter Match → normalisiert → leer
    # Normalisierung: lowercase + strip — verhindert stille Datenverluste wenn
    # Claude-generierter Name marginal vom DB-Namen abweicht
    def _lookup(ctx: dict[str, str] | None) -> str | None:
        if not ctx:
            return None
        val = ctx.get(db_name)
        if val is not None:
            return val
        db_name_norm = db_name.lower().strip()
        for ctx_name, ctx_val in ctx.items():
            if ctx_name.lower().strip() == db_name_norm:
                return ctx_val
        return None

    note     = _lookup(peers_context)
    relation = _lookup(peers_relations)

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
        # PEER-ISLISTED-FIELD-01: kanonische _is_listed()-Logik statt roher Spalte
        # (respektiert den Bayer/Monsanto-Schutzfall: explizites is_listed=False
        # schlägt einen evtl. noch gesetzten historischen Ticker)
        is_listed=_resolve_is_listed(row),
        stage_normalized=_STAGE_LABEL.get(raw_stage, raw_stage) or None,
        composite_score=row.get("composite_score"),
        rating=row.get("rating"),
        financial_score=row.get("financial_score"),
        market_score=row.get("market_score"),
        positioning_note=note,
        relation=relation,
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
