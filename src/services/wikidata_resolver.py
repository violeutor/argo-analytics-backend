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

    @property
    def display_name(self) -> str:
        """Bevorzugter Anzeigename: legal_name > name."""
        return self.legal_name or self.name

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
        )
    except Exception as exc:
        logger.debug("_parse_binding error: %s — row=%s", exc, row)
        return None


# ─── SPARQL Queries ───────────────────────────────────────────────────────────

_DIRECT_QUERY = """
SELECT DISTINCT ?item ?itemLabel ?officialName ?ticker ?exchangeLabel ?hqLabel ?founded
       (IF(EXISTS {{ ?item wdt:P31/wdt:P279* {public_class}. }}, true, false) AS ?isPublic)
WHERE {{
  ?item wdt:P31/wdt:P279* {business_class} ;
        rdfs:label "{name}"@en .
  OPTIONAL {{ ?item wdt:P571 ?founded. }}
  OPTIONAL {{ ?item wdt:P159 ?hq. }}
  OPTIONAL {{ ?item wdt:P1448 ?officialName. FILTER(LANG(?officialName) = "en") }}
  OPTIONAL {{ ?item wdt:P249 ?ticker. }}
  OPTIONAL {{ ?item wdt:P414 ?exchange. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY DESC(?isPublic)
LIMIT 10
"""

_SUBSIDIARY_QUERY = """
SELECT DISTINCT ?item ?itemLabel ?officialName ?ticker ?exchangeLabel ?hqLabel ?founded
                ?parentLabel
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
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY DESC(?isPublic)
LIMIT 10
"""


async def _run_sparql(client: httpx.AsyncClient, query: str) -> list[dict]:
    """SPARQL Query ausführen → list of result rows. Fehler → []."""
    try:
        resp = await client.get(
            _SPARQL_ENDPOINT,
            params={"query": query, "format": "json"},
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json().get("results", {}).get("bindings", [])
        logger.warning("Wikidata SPARQL HTTP %s", resp.status_code)
        return []
    except Exception as exc:
        logger.debug("Wikidata SPARQL error: %s", exc)
        return []


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
        direct_rows, subsidiary_rows = await asyncio.gather(
            _run_sparql(client, direct_q),
            _run_sparql(client, subsidiary_q),
        )

        logger.info(
            "Wikidata resolve '%s': %d direct + %d subsidiary rows",
            query, len(direct_rows), len(subsidiary_rows),
        )

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
                "  Candidate: %r is_listed=%s is_subsidiary=%s ticker=%s exchange=%s",
                c.display_name, c.is_listed, c.is_subsidiary, c.ticker, c.exchange_label,
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
            return WikidataResolutionResult(
                query, candidates, False, exact[0], "exact_name_match"
            )

        # Genau ein Kandidat gesamt → still binden
        if len(candidates) == 1:
            return WikidataResolutionResult(
                query, candidates, False, candidates[0], "single_candidate"
            )

        # ≥2 Kandidaten → Modal
        return WikidataResolutionResult(
            query, candidates, True, None, "multiple_candidates"
        )

    finally:
        if owns_client:
            await client.aclose()
