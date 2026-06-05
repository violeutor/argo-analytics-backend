"""
wikidata_resolver.py — Wikidata-first Entity Resolution (DISAMBIG-03-REVIVED)

Ersetzt OpenFIGI als primären Entity-Resolver. OpenFIGI kennt nur börsennotierte
Instrumente — private Töchter, GmbHs und pre-IPO Companies tauchen dort nicht auf.
Wikidata kennt alle Unternehmensformen inkl. P749 (subsidiary of) → Töchter werden
als eigene Kandidaten sichtbar.

Architektur:
    resolve_entity(name) → WikidataResolutionResult
        Schicht 1: direkte name-Matches (rdfs:label exakt)
        Schicht 2: Töchter von Treffern (wdt:P749 parent match)
        is_listed aus P414 (hat Börsenplatz) → kein OpenFIGI für Private nötig
        legal_name aus P1448 (offizieller Name) → kein Wikipedia-Infobox-Hop mehr
        ticker aus P249 (Börsenkürzel) → direkter Enrichment-Input

    OpenFIGI-Rolle danach:
        NUR noch Exchange-Resolution (resolve_exchange_from_composite bleibt)
        Kein Name→Equity-Lookup mehr

Trigger-Logik (unverändert):
    0 Kandidaten   → privater Flow, kein Modal (kein Wikidata-Eintrag)
    1 Kandidat     → still binden, kein Modal
    ≥2 Kandidaten  → Modal, User wählt
    User-Wahl      → is_listed direkt gesetzt (kein nachgelagerter Guess)

Einschränkung:
    Sehr kleine/neue GmbHs ohne Wikidata-Eintrag → 0 Kandidaten → Cold-Path via HAI.
    Aktualität: Wikidata hinkt bei frischen Spin-offs (akzeptabel für Phase 1).
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
_TIMEOUT = 8.0
_HEADERS = {
    "User-Agent": "argo-analytics/1.0 (entity-resolution; mailto:info@argo-analytics.io)",
    "Accept": "application/json",
}

# Wikidata entity types that qualify as a business/organisation
# Q4830453 = business, Q43229 = organization — both cover subsidiaries
_BUSINESS_CLASS = "wd:Q4830453"
_PUBLIC_CO_CLASS = "wd:Q6881511"   # publicly traded company


# ─── 24h In-Memory-Cache (WIKIDATA-CACHE-01) ─────────────────────────────────
# Motivation: Wikidata wdqs hat regelmäßige 429-Outages (1 req/min-Limit).
# Ohne Cache macht jede Suche nach "Bayer" o.ä. während eines Outage-Fensters
# den gesamten Lifecycle-Zweig unnutzbar → resolver_degraded → kein Modal.
# Lösung: erfolgreiche Auflösungen (ok=True, candidates vorhanden) für 24h cachen.
# Degraded-Ergebnisse (ok=False) werden NICHT gecacht — dann soll beim nächsten
# Request neu versucht werden. Single-Instance Render → einfaches dict, kein Redis.
#
# Wichtige Eigenschaften:
#   - Cache-Key: normalize(name) — case-insensitive, whitespace-normalisiert
#   - TTL: 86400s (24h) — Wikidata-Daten ändern sich selten, Stale ist akzeptabel
#   - Thread-Safety: asyncio single-thread, kein Lock nötig
#   - Memory: realistisch <500 Einträge = ~1MB (Argo Single-Instance, kein Problem)

_CACHE_TTL = 86_400  # 24h in Sekunden

@dataclass
class _CacheEntry:
    result: "WikidataResolutionResult"
    stored_at: float  # time.monotonic()

_resolve_cache: dict[str, _CacheEntry] = {}


def _cache_key(name: str) -> str:
    """Normalize für Cache-Key: lowercase + whitespace-kollaps."""
    return " ".join(name.strip().lower().split())


def _cache_get(name: str) -> "WikidataResolutionResult | None":
    key = _cache_key(name)
    entry = _resolve_cache.get(key)
    if entry is None:
        return None
    if time.monotonic() - entry.stored_at > _CACHE_TTL:
        del _resolve_cache[key]
        logger.debug("Wikidata cache EXPIRED für '%s'", name)
        return None
    logger.info("Wikidata cache HIT für '%s' (%d Kandidaten)", name, len(entry.result.candidates))
    return entry.result


def _cache_set(name: str, result: "WikidataResolutionResult") -> None:
    """Nur cachen wenn Ergebnis belastbar ist (kein degraded, mind. 1 Kandidat)."""
    if result.reason == "resolver_degraded":
        return
    # no_wikidata_match (echtes Leer) cachen wir NICHT — könnte sich ändern wenn
    # ein Unternehmen einen Wikidata-Eintrag bekommt. Kostet nichts außer einem
    # weiteren Outage-Fenster-Fehlversuch.
    if result.reason == "no_wikidata_match":
        return
    _resolve_cache[_cache_key(name)] = _CacheEntry(result=result, stored_at=time.monotonic())
    logger.debug("Wikidata cache SET für '%s' (%d Kandidaten, reason=%s)",
                 name, len(result.candidates), result.reason)


# ─── Data Models ─────────────────────────────────────────────────────────────

@dataclass
class WikidataCandidate:
    """Ein Unternehmens-Kandidat aus Wikidata — listed oder private."""
    wikidata_id: str           # z.B. "Q152"
    name: str                  # rdfs:label (EN)
    legal_name: str | None     # P1448 — offizieller Name ("Bayer AG")
    is_listed: bool            # True wenn P414 gesetzt (hat Börsenplatz)
    ticker: str | None         # P249 — Börsenkürzel ("BAYN")
    exchange_label: str | None # P414 label ("Frankfurt Stock Exchange")
    headquarters: str | None   # P159 label
    founded_year: str | None   # P571
    is_subsidiary: bool = False  # True wenn über P749 gefunden (Tochter)
    parent_name: str | None = None  # Name der Muttergesellschaft
    # DISAMBIG-03 Lifecycle: aus P576 (Auflösungsdatum) + P7888 (merged into).
    # Erkennt Entitäten, die nicht mehr eigenständig existieren — der Fall, in den
    # die listed/private-Binärlogik sonst hineinfällt (Monsanto → Bayer Crop Science).
    dissolved_year: str | None = None       # P576 — Auflösungsjahr
    consolidated_into: str | None = None     # P7888 label — überlebende Einheit
    consolidated_into_id: str | None = None  # P7888 QID — Pointer für Re-Resolve

    @property
    def display_name(self) -> str:
        """Bevorzugter Anzeigename: legal_name > name."""
        return self.legal_name or self.name

    @property
    def formerly_listed(self) -> bool:
        """
        Divergenz P414-vorhanden + isPublic-false = ehemals börsennotiert.
        Wikidata behält den historischen Börsenplatz (Monsanto: NYSE), auch wenn
        die Entity heute keine 'publicly traded company' (Q6881511) mehr ist.
        """
        return bool(self.exchange_label) and not self.is_listed

    @property
    def lifecycle_status(self) -> str:
        """
        Status AUF der Entität (≠ consolidated_into, dem Pointer auf die Nachfolge-Einheit).
        Priorität: P7888 (merged into) ist das stärkste Signal.
            acquired  → in eine andere Einheit aufgegangen (Pointer gesetzt)
            defunct   → aufgelöst, ohne benannte Nachfolge
            delisted  → ehemals börsennotiert, heute nicht mehr eigenständig listed
            active    → Default (listed ODER private ohne Auflösungssignal)
        Wichtig: 'acquired' unterdrückt den M&A-Pfad NICHT — eine übernommene Sparte
        kann per Carve-out/Spin-off wieder Target werden. Es kontextualisiert ihn nur.
        """
        if self.consolidated_into:
            return "acquired"
        if self.dissolved_year:
            return "defunct"
        if self.formerly_listed:
            return "delisted"
        return "active"

    @property
    def display_exchange(self) -> str | None:
        """Lesbare Exchange-Kurzform aus dem Label."""
        if not self.exchange_label:
            return None
        _map = {
            "frankfurt stock exchange": "XETRA/Frankfurt",
            "new york stock exchange": "NYSE",
            "nasdaq": "NASDAQ",
            "london stock exchange": "London",
            "euronext paris": "Paris",
            "borsa italiana": "Milano",
            "madrid stock exchange": "Madrid",
            "swiss exchange": "Zürich",
            "wiener börse": "Wien",
        }
        return _map.get(self.exchange_label.lower(), self.exchange_label)

    def to_dict(self) -> dict:
        return {
            "wikidata_id": self.wikidata_id,
            "name": self.name,
            "legal_name": self.legal_name,
            "display_name": self.display_name,
            "is_listed": self.is_listed,
            "ticker": self.ticker,
            "exchange_label": self.exchange_label,
            "display_exchange": self.display_exchange,
            "headquarters": self.headquarters,
            "founded_year": self.founded_year,
            "is_subsidiary": self.is_subsidiary,
            "parent_name": self.parent_name,
            "dissolved_year": self.dissolved_year,
            "consolidated_into": self.consolidated_into,
            "consolidated_into_id": self.consolidated_into_id,
            "formerly_listed": self.formerly_listed,
            "lifecycle_status": self.lifecycle_status,
        }


@dataclass
class WikidataResolutionResult:
    """Ergebnis der Wikidata Entity-Resolution + Modal-Trigger-Entscheidung."""
    query: str
    candidates: list[WikidataCandidate]
    show_modal: bool
    resolved: Optional[WikidataCandidate]
    reason: str

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "show_modal": self.show_modal,
            "resolved": self.resolved.to_dict() if self.resolved else None,
            "candidates": [c.to_dict() for c in self.candidates],
            "reason": self.reason,
        }


# ─── SPARQL Helpers ───────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _extract_wikidata_id(uri: str) -> str:
    """'http://www.wikidata.org/entity/Q152' → 'Q152'."""
    return uri.rsplit("/", 1)[-1]


def _extract_year(value: str) -> str | None:
    """'1863-01-01T00:00:00Z' → '1863'."""
    if value and len(value) >= 4 and value[:4].isdigit():
        return value[:4]
    return None


def _parse_binding(row: dict, is_subsidiary: bool = False,
                   parent_name: str | None = None) -> WikidataCandidate | None:
    """Einzelne SPARQL-Result-Row → WikidataCandidate."""
    try:
        item_uri = row.get("item", {}).get("value", "")
        if not item_uri:
            return None
        wikidata_id = _extract_wikidata_id(item_uri)

        name = row.get("itemLabel", {}).get("value", "").strip()
        if not name or name.startswith("Q") and name[1:].isdigit():
            # Label fehlt oder ist nur die QID → skip
            return None

        legal_name_raw = row.get("officialName", {}).get("value", "").strip()
        legal_name = legal_name_raw if legal_name_raw and 2 <= len(legal_name_raw) <= 120 else None

        is_listed = row.get("isPublic", {}).get("value", "false").lower() == "true"

        ticker_raw = row.get("ticker", {}).get("value", "").strip().upper()
        ticker = ticker_raw if ticker_raw and 1 <= len(ticker_raw) <= 12 else None

        exchange_label = row.get("exchangeLabel", {}).get("value", "").strip() or None

        hq = row.get("hqLabel", {}).get("value", "").strip() or None

        founded_year = _extract_year(row.get("founded", {}).get("value", ""))

        # DISAMBIG-03 Lifecycle: P576 (Auflösungsdatum) + P7888 (merged into).
        dissolved_year = _extract_year(row.get("dissolved", {}).get("value", ""))

        merged_uri = row.get("mergedInto", {}).get("value", "")
        consolidated_into_id = _extract_wikidata_id(merged_uri) if merged_uri else None
        merged_label = row.get("mergedIntoLabel", {}).get("value", "").strip()
        # Label-Service liefert bei fehlendem Label die nackte QID zurück → verwerfen.
        consolidated_into = (
            merged_label
            if merged_label and not (merged_label.startswith("Q") and merged_label[1:].isdigit())
            else None
        )

        return WikidataCandidate(
            wikidata_id=wikidata_id,
            name=name,
            legal_name=legal_name,
            is_listed=is_listed,
            ticker=ticker,
            exchange_label=exchange_label,
            headquarters=hq,
            founded_year=founded_year,
            is_subsidiary=is_subsidiary,
            parent_name=parent_name,
            dissolved_year=dissolved_year,
            consolidated_into=consolidated_into,
            consolidated_into_id=consolidated_into_id,
        )
    except Exception as exc:
        logger.debug("_parse_binding error: %s — row=%s", exc, row)
        return None


# ─── SPARQL Queries ───────────────────────────────────────────────────────────

_DIRECT_QUERY = """
SELECT DISTINCT ?item ?itemLabel ?officialName ?ticker ?exchangeLabel ?hqLabel ?founded
       ?dissolved ?mergedInto ?mergedIntoLabel
       (IF(EXISTS {{ ?item wdt:P31/wdt:P279* {public_class}. }}, true, false) AS ?isPublic)
WHERE {{
  ?item wdt:P31/wdt:P279* {business_class} ;
        rdfs:label "{name}"@en .
  OPTIONAL {{ ?item wdt:P571 ?founded. }}
  OPTIONAL {{ ?item wdt:P159 ?hq. }}
  OPTIONAL {{ ?item wdt:P1448 ?officialName. FILTER(LANG(?officialName) = "en") }}
  OPTIONAL {{ ?item wdt:P249 ?ticker. }}
  OPTIONAL {{ ?item wdt:P414 ?exchange. }}
  OPTIONAL {{ ?item wdt:P576 ?dissolved. }}
  OPTIONAL {{ ?item wdt:P7888 ?mergedInto. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY DESC(?isPublic)
LIMIT 10
"""

_SUBSIDIARY_QUERY = """
SELECT DISTINCT ?item ?itemLabel ?officialName ?ticker ?exchangeLabel ?hqLabel ?founded
                ?parentLabel ?dissolved ?mergedInto ?mergedIntoLabel
       (IF(EXISTS {{ ?item wdt:P31/wdt:P279* {public_class}. }}, true, false) AS ?isPublic)
WHERE {{
  ?parent wdt:P31/wdt:P279* {business_class} ;
          rdfs:label "{name}"@en .
  ?item wdt:P749 ?parent ;
        wdt:P31/wdt:P279* {business_class} .
  OPTIONAL {{ ?item wdt:P571 ?founded. }}
  OPTIONAL {{ ?item wdt:P159 ?hq. }}
  OPTIONAL {{ ?item wdt:P1448 ?officialName. FILTER(LANG(?officialName) = "en") }}
  OPTIONAL {{ ?item wdt:P249 ?ticker. }}
  OPTIONAL {{ ?item wdt:P414 ?exchange. }}
  OPTIONAL {{ ?item wdt:P576 ?dissolved. }}
  OPTIONAL {{ ?item wdt:P7888 ?mergedInto. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY DESC(?isPublic)
LIMIT 10
"""


async def _run_sparql(client: httpx.AsyncClient, query: str) -> tuple[list[dict], bool]:
    """
    SPARQL Query ausführen → (rows, ok).

    ok=True  : Query erfolgreich (auch bei 0 Treffern — echtes Leer-Ergebnis).
    ok=False : Transport-/HTTP-Fehler (Timeout, 429, Outage) — NICHT als 'kein
               Treffer' interpretierbar. Ermöglicht resolver_degraded → OpenFIGI-
               Fallback, statt eine börsennotierte Company stumm als private
               anzulegen oder den Mutterkonzern aus dem Modal zu verlieren.
    """
    try:
        resp = await client.get(
            _SPARQL_ENDPOINT,
            params={"query": query, "format": "json"},
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json().get("results", {}).get("bindings", []), True
        logger.warning("Wikidata SPARQL HTTP %s", resp.status_code)
        return [], False
    except Exception as exc:
        logger.debug("Wikidata SPARQL error: %s", exc)
        return [], False


def _deduplicate(candidates: list[WikidataCandidate]) -> list[WikidataCandidate]:
    """
    Selbe Entity kann aus Direct- und Subsidiary-Query kommen → dedup by wikidata_id.
    Direct-Treffer bevorzugt (is_subsidiary=False).
    """
    seen: dict[str, WikidataCandidate] = {}
    for c in candidates:
        if c.wikidata_id not in seen or not c.is_subsidiary:
            seen[c.wikidata_id] = c
    return list(seen.values())


def _sort_candidates(candidates: list[WikidataCandidate]) -> list[WikidataCandidate]:
    """
    Sortierung: listed vor private, direkte Treffer vor Töchtern, dann Name.
    """
    return sorted(
        candidates,
        key=lambda c: (
            not c.is_listed,       # listed first
            c.is_subsidiary,       # direct match before subsidiary
            c.display_name,        # stable alpha
        )
    )


# ─── Public API ──────────────────────────────────────────────────────────────

async def resolve_entity(
    name: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> WikidataResolutionResult:
    """
    Haupteinstieg. Löst 'name' via Wikidata in Unternehmens-Kandidaten auf —
    listed UND private, inkl. Töchter (P749).

    Parameters
    ----------
    name   : roher Suchstring vom User ("Bayer", "Bayer CropScience")
    client : optionaler httpx-Client (Wiederverwendung empfohlen)
    """
    query = (name or "").strip()
    if not query:
        return WikidataResolutionResult(query, [], False, None, "empty_query")

    # WIKIDATA-CACHE-01: Cache-Lookup vor dem ersten Netzwerk-Call.
    # Outage-Resilienz: war die Auflösung in den letzten 24h erfolgreich,
    # liefern wir das gecachte Ergebnis sofort zurück — kein Wikidata-Hit nötig.
    cached = _cache_get(query)
    if cached is not None:
        return cached

    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS)

    try:
        direct_q = _DIRECT_QUERY.format(
            name=query.replace('"', '\\"'),
            business_class=_BUSINESS_CLASS,
            public_class=_PUBLIC_CO_CLASS,
        )
        subsidiary_q = _SUBSIDIARY_QUERY.format(
            name=query.replace('"', '\\"'),
            business_class=_BUSINESS_CLASS,
            public_class=_PUBLIC_CO_CLASS,
        )

        # Beide Queries parallel — maximale Geschwindigkeit
        (direct_rows, direct_ok), (subsidiary_rows, subsidiary_ok) = await asyncio.gather(
            _run_sparql(client, direct_q),
            _run_sparql(client, subsidiary_q),
        )

        logger.info(
            "Wikidata resolve '%s': %d direct (ok=%s) + %d subsidiary (ok=%s) rows",
            query, len(direct_rows), direct_ok, len(subsidiary_rows), subsidiary_ok,
        )

        # Degraded-Erkennung (Option 2): Die Direct-Query ist die EINZIGE, die die
        # gesuchte Entität selbst (Mutterkonzern / Haupteintrag) liefern kann.
        # Scheitert sie (Timeout/429/Outage → ok=False), ist die Kandidatenmenge
        # strukturell unvollständig — der prominenteste Treffer (z.B. Bayer AG) könnte
        # fehlen. Dann NICHT stumm eine Tochter-only-Liste zeigen, sondern an den
        # OpenFIGI-Fallback in company_detail übergeben (bewährter Listed-Resolver).
        #
        # Abgrenzung: direct_ok=True + 0 Treffer = ECHTES Leer (legitimer Marken-/
        # Eigentümer-Fall, z.B. eine Marke die selbst keine Entität ist aber Töchter
        # hat) → kein degraded, Töchter werden gezeigt. Genau diese Unterscheidung
        # war vorher unmöglich (Fehler sah aus wie Leere → Bayer AG verschwand).
        if not direct_ok:
            logger.warning(
                "Wikidata resolve '%s': Direct-Query degraded (ok=False, subsidiary_ok=%s) "
                "→ resolver_degraded (OpenFIGI-Fallback)",
                query, subsidiary_ok,
            )
            return WikidataResolutionResult(query, [], False, None, "resolver_degraded")

        # Parse — direkte Treffer zuerst
        direct_cands = [c for row in direct_rows
                        if (c := _parse_binding(row, is_subsidiary=False)) is not None]

        # Für Subsidiaries: Parent-Name aus Query ableiten
        sub_cands = [c for row in subsidiary_rows
                     if (c := _parse_binding(
                         row,
                         is_subsidiary=True,
                         parent_name=row.get("parentLabel", {}).get("value"),
                     )) is not None]

        all_raw = direct_cands + sub_cands
        deduped = _deduplicate(all_raw)
        candidates = _sort_candidates(deduped)

        logger.info(
            "Wikidata resolve '%s': %d kandidaten nach dedup (%d listed, %d private, %d subsidiaries)",
            query,
            len(candidates),
            sum(1 for c in candidates if c.is_listed),
            sum(1 for c in candidates if not c.is_listed),
            sum(1 for c in candidates if c.is_subsidiary),
        )
        for c in candidates:
            logger.debug(
                "  Candidate: %r is_listed=%s is_subsidiary=%s ticker=%s exchange=%s lifecycle=%s consolidated_into=%s",
                c.display_name, c.is_listed, c.is_subsidiary, c.ticker, c.exchange_label,
                c.lifecycle_status, c.consolidated_into,
            )

        if not candidates:
            return WikidataResolutionResult(query, [], False, None, "no_wikidata_match")

        # Exakter Name-Match auf genau einen Kandidaten → still binden
        qn = _norm(query)
        exact = [
            c for c in candidates
            if _norm(c.display_name) == qn or _norm(c.name) == qn
        ]
        if len(exact) == 1:
            result = WikidataResolutionResult(
                query, candidates, False, exact[0], "exact_name_match"
            )
            _cache_set(query, result)
            return result

        # Genau ein Kandidat gesamt → still binden
        if len(candidates) == 1:
            result = WikidataResolutionResult(
                query, candidates, False, candidates[0], "single_candidate"
            )
            _cache_set(query, result)
            return result

        # ≥2 Kandidaten → Modal
        result = WikidataResolutionResult(
            query, candidates, True, None, "multiple_candidates"
        )
        _cache_set(query, result)
        return result

    finally:
        if owns_client:
            await client.aclose()
