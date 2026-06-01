"""
gleif_resolver.py — Entity-Resolution via GLEIF (DISAMBIG-01 / R25)

Zweck
-----
Löst einen Freitext-Firmennamen in eindeutige *gelistete* Entitäten auf,
BEVOR das Enrichment startet. Bindet pro Kandidat LEI + ISIN + Legal Name
+ Legal Form. Damit kollabiert "Siemens" auf die handvoll emissionsfähigen
Einheiten (Siemens AG / Siemens Energy AG / Siemens Healthineers AG) statt
auf Dutzende Konzern-GmbHs.

Gestufter Trigger (mit Andreas in Session 41 festgelegt):
    >=2 Kandidaten MIT ISIN   -> Modal (echte Listed-Ambiguität)
    genau 1 ISIN-Kandidat     -> still binden, kein Modal
    exakter Legal-Name-Match  -> still binden, kein Modal
    0 ISIN-Kandidaten         -> private/unlisted -> bestehender Flow

Designentscheidungen
--------------------
- Nur emissionsfähige Entitäten ins Modal: Filter = "hat ISIN". GmbHs ohne
  Wertpapier fallen raus (korrekt — nicht direkt investierbar). Das ist der
  Filter, nicht die Suche, der die Modal-Qualität bestimmt.
- ISIN ist der primäre DE-Identifier (ISIN-First, Session 40). GLEIF ist
  hier der Zweitweg neben EN-11 (Wikipedia-Infobox).
- Nur Cold-Path: Aufrufer ruft dies NUR auf, wenn die Company noch nicht in
  der DB ist. Warm-Path liefert bereits geklärte Entitäten in der Suchleiste.
- Kein API-Key, keine Auth. Free public API. JSON:API-Format.

Live-Verifikation steht aus (DISAMBIG-01-TEST): in der Sandbox ist
api.gleif.org nicht erreichbar (Domain-Allowlist). Auf Render testen mit
'Siemens', 'Deutsche Bank', 'SAP', 'LanzaTech'.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

GLEIF_BASE = "https://api.gleif.org/api/v1"
_HEADERS = {
    "Accept": "application/vnd.api+json",
    "User-Agent": "argo-analytics/1.0 (entity-resolution)",
}

# Konservative Defaults — GLEIF sitzt synchron im Cold-Path, daher kurze Timeouts.
_TIMEOUT = 8.0
_PAGE_SIZE = 50          # genug, um die gelisteten Einheiten sicher einzufangen
_MAX_ISIN_CHECKS = 15    # Obergrenze paralleler ISIN-Relationship-Calls pro Suche
_CONNECTED_CAP = 5       # max. verbundene (ISIN-lose) Treffer im Modal; darüber → Verfeinern-Hinweis

# Rechtsformen, die typischerweise emissionsfähig sind. KEIN harter Filter
# (ISIN-Vorhandensein ist der harte Filter) — nur als sekundäres Sortier-/
# Plausibilitätssignal. ISO-20275 ELF-Codes wären die robustere Quelle, aber
# der Legal-Form-String reicht für die Sortierung.
_LISTED_FORM_HINTS = ("AG", "SE", "KGAA", "N.V.", "NV", "PLC", "S.A.", "SA")


@dataclass
class EntityCandidate:
    """Ein aufgelöster Kandidat fürs Disambiguierungs-Modal."""
    lei: str
    legal_name: str
    legal_form: Optional[str] = None
    country: Optional[str] = None
    status: Optional[str] = None          # ACTIVE / INACTIVE (registration status)
    isins: list[str] = field(default_factory=list)

    @property
    def has_isin(self) -> bool:
        return bool(self.isins)

    @property
    def primary_isin(self) -> Optional[str]:
        if not self.isins:
            return None
        # DE-ISIN bevorzugen (ISIN-First, exchange-unabhängig), sonst erste.
        for isin in self.isins:
            if isin.startswith("DE"):
                return isin
        return self.isins[0]

    def to_dict(self) -> dict:
        return {
            "lei": self.lei,
            "legal_name": self.legal_name,
            "legal_form": self.legal_form,
            "country": self.country,
            "status": self.status,
            "isin": self.primary_isin,
            "isins": self.isins,
        }


@dataclass
class ResolutionResult:
    """Ergebnis der Entity-Resolution + Trigger-Entscheidung."""
    query: str
    candidates: list[EntityCandidate]      # gelistete (ISIN-tragende), sortiert
    show_modal: bool                       # True -> Frontend zeigt Auswahl
    resolved: Optional[EntityCandidate]    # gesetzt wenn eindeutig (kein Modal)
    reason: str                            # für Logging/Debugging
    # Variante 2 (Session 41): verbundene Namens-Treffer OHNE ISIN.
    # Bewusst "weitere Treffer", nicht "Töchter" — Parent-Filterung wäre DISAMBIG-03.
    connected: list[EntityCandidate] = field(default_factory=list)   # gecappt auf _CONNECTED_CAP
    connected_truncated: bool = False      # True -> es gab mehr als der Cap; Verfeinern-Hinweis

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "show_modal": self.show_modal,
            "resolved": self.resolved.to_dict() if self.resolved else None,
            "candidates": [c.to_dict() for c in self.candidates],
            "connected": [c.to_dict() for c in self.connected],
            "connected_truncated": self.connected_truncated,
            "reason": self.reason,
        }


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _parse_record(rec: dict) -> Optional[EntityCandidate]:
    """Ein JSON:API lei-record -> EntityCandidate (ohne ISINs, die kommen separat)."""
    try:
        lei = rec.get("id")
        attr = rec.get("attributes", {}) or {}
        ent = attr.get("entity", {}) or {}
        reg = attr.get("registration", {}) or {}
        legal_name = (ent.get("legalName", {}) or {}).get("name")
        if not lei or not legal_name:
            return None
        # legalForm kann als {id, other} kommen; 'other' ist der Klartext-Fallback.
        lf = ent.get("legalForm", {}) or {}
        legal_form = lf.get("other") or lf.get("id")
        country = (ent.get("legalAddress", {}) or {}).get("country")
        status = reg.get("status")
        return EntityCandidate(
            lei=lei,
            legal_name=legal_name,
            legal_form=legal_form,
            country=country,
            status=status,
        )
    except Exception as exc:  # defensiv — GLEIF-Shape kann variieren
        logger.debug("GLEIF record parse failed: %s", exc)
        return None


async def _fetch_isins(client: httpx.AsyncClient, lei: str) -> list[str]:
    """ISINs für eine LEI via Relationship-Endpoint. Leere Liste bei Fehler."""
    url = f"{GLEIF_BASE}/lei-records/{lei}/isins"
    try:
        resp = await client.get(url, params={"page[size]": 100})
        if resp.status_code != 200:
            return []
        data = resp.json().get("data", []) or []
        out = []
        for item in data:
            # ISIN steht je nach Shape in attributes.isin oder als id.
            isin = (item.get("attributes", {}) or {}).get("isin") or item.get("id")
            if isin:
                out.append(isin)
        return out
    except Exception as exc:
        logger.debug("GLEIF isin fetch failed for %s: %s", lei, exc)
        return []


async def _search_records(
    client: httpx.AsyncClient,
    name: str,
    country: Optional[str],
) -> list[EntityCandidate]:
    """lei-records-Suche nach legalName (+ optional country), nur ACTIVE."""
    params = {
        "filter[entity.legalName]": name,
        "filter[entity.registrationStatus]": "ISSUED",  # ACTIVE-Äquivalent in LEI-Daten
        "page[size]": _PAGE_SIZE,
    }
    if country:
        params["filter[entity.legalAddress.country]"] = country
    try:
        resp = await client.get(f"{GLEIF_BASE}/lei-records", params=params)
        if resp.status_code != 200:
            logger.warning("GLEIF search HTTP %s for %r", resp.status_code, name)
            return []
        records = resp.json().get("data", []) or []
    except Exception as exc:
        logger.warning("GLEIF search failed for %r: %s", name, exc)
        return []

    candidates = []
    for rec in records:
        cand = _parse_record(rec)
        if cand:
            candidates.append(cand)
    return candidates


def _sort_candidates(cands: list[EntityCandidate]) -> list[EntityCandidate]:
    """Plausibelste Listed-Einheiten zuerst: ACTIVE + Listed-Rechtsform bevorzugt."""
    def key(c: EntityCandidate):
        form_rank = 0 if (c.legal_form and any(
            h in c.legal_form.upper() for h in _LISTED_FORM_HINTS)) else 1
        status_rank = 0 if (c.status or "").upper() in ("ISSUED", "ACTIVE") else 1
        return (status_rank, form_rank, len(c.legal_name))
    return sorted(cands, key=key)


async def resolve_entity(
    name: str,
    *,
    country: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> ResolutionResult:
    """
    Haupteinstieg. Löst 'name' in gelistete Kandidaten auf und entscheidet
    den gestuften Trigger.

    Parameters
    ----------
    name : roher Suchstring vom User
    country : optionaler ISO-3166-1-alpha-2 Filter (z.B. 'DE'). None = global.
    client : optionaler httpx-Client (Wiederverwendung im Backend empfohlen).

    Returns
    -------
    ResolutionResult — siehe Trigger-Logik oben.
    """
    query = (name or "").strip()
    if not query:
        return ResolutionResult(query, [], False, None, "empty_query")

    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT)

    try:
        raw = await _search_records(client, query, country)
        if not raw:
            return ResolutionResult(query, [], False, None, "no_gleif_match")

        # ISINs für die Top-Kandidaten parallel auflösen (gekappt).
        ranked = _sort_candidates(raw)[:_MAX_ISIN_CHECKS]
        isin_lists = await asyncio.gather(
            *[_fetch_isins(client, c.lei) for c in ranked]
        )
        for cand, isins in zip(ranked, isin_lists):
            cand.isins = isins

        # Gelistete (emissionsfähig, ISIN) vs. verbundene (Namens-Treffer ohne ISIN).
        listed = _sort_candidates([c for c in ranked if c.has_isin])
        connected_all = _sort_candidates([c for c in ranked if not c.has_isin])

        # Variante 2: verbundene Gruppe cappen. >Cap -> unterdrücken + Hinweis-Flag.
        # Eine willkürlich abgeschnittene Teilliste wäre irreführend ("warum diese 5?"),
        # daher alles-oder-Hinweis: bei Überlauf gar keine verbundenen zeigen.
        connected_truncated = len(connected_all) > _CONNECTED_CAP
        connected = [] if connected_truncated else connected_all

        if not listed:
            # Kein gelisteter Treffer -> private/unlisted -> bestehender Flow.
            # (Verbundene allein lösen kein Modal aus — ohne Anker keine sinnvolle Auswahl.)
            return ResolutionResult(query, [], False, None, "no_listed_entity")

        # Exakter Legal-Name-Match auf GENAU eine gelistete Entität -> still binden.
        # Volle Firmierung = klare Absicht; verbundene Treffer NICHT dazwischenfunken.
        qn = _norm(query)
        exact = [c for c in listed if _norm(c.legal_name) == qn]
        if len(exact) == 1:
            return ResolutionResult(
                query, listed, False, exact[0], "exact_legal_name_match"
            )

        # Trigger: Modal wenn >=2 gelistete ODER überhaupt verbundene Treffer existieren
        # (gezeigt ODER per Hinweis unterdrückt). So erscheint Uniper SE + Kraftwerke GmbH,
        # während eindeutige Single-Listed ohne verbundene (z.B. LanzaTech) direkt durchgehen.
        has_connected = bool(connected_all)

        if len(listed) == 1 and not has_connected:
            # Genau ein gelisteter Kandidat, keine verbundenen -> still binden.
            return ResolutionResult(
                query, listed, False, listed[0], "single_listed_candidate"
            )

        reason = (
            "multiple_listed_candidates" if len(listed) >= 2
            else "listed_plus_connected"
        )
        return ResolutionResult(
            query, listed, True, None, reason,
            connected=connected,
            connected_truncated=connected_truncated,
        )
    finally:
        if owns_client:
            await client.aclose()
