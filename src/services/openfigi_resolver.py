"""
openfigi_resolver.py — Entity-Resolution via OpenFIGI (DISAMBIG-01 / R25)

Ersetzt gleif_resolver.py. OpenFIGI ist eine kostenlose Bloomberg-API die
Firmennamen direkt in FIGI + ISIN + Ticker + Exchange auflöst — mit exzellenter
DE/EU-Coverage und strukturiertem securityType-Filter.

Vorteile gegenüber GLEIF:
- Direkte ISIN + Ticker im selben Call (kein separater ISIN-Relationship-Fetch)
- securityType trennt Equity von Bonds (DISAMBIG-02 kostenlos gelöst)
- Stabile DE-Coverage (Bayer AG, Mercedes-Benz, Daimler Truck etc.)
- Kein API-Key nötig für <25 req/min (Cold-Path reicht locker)
- Einfachere Response-Struktur → weniger Code

Gestufter Trigger (unverändert gegenüber GLEIF-Version):
    >=2 Equity-Kandidaten  → Modal
    genau 1 Kandidat       → still binden, kein Modal
    exakter Name-Match     → still binden, kein Modal
    0 Kandidaten           → privater Flow (kein Modal)

Töchter/GmbHs:
    Nicht börsennotierte Entitäten erscheinen nicht in OpenFIGI.
    Frontend zeigt Hinweis: "Nicht dabei? Vollständige Firmierung eingeben."
    Das ist korrekt — private Töchter gehen direkt in den privaten Enrichment-Flow.

API: POST https://api.openfigi.com/v3/search
     Body: {"query": "<name>", "securityType": "Common Stock"}
     Kein Auth-Header nötig (anonym: 25 req/min).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

OPENFIGI_BASE = "https://api.openfigi.com/v3"
_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "argo-analytics/1.0 (entity-resolution)",
}

_TIMEOUT = 6.0
_MAX_RESULTS = 10        # OpenFIGI gibt max 10 pro Search-Call zurück
_OPENFIGI_API_KEY = None  # Optional: setzen für 250 req/min statt 25

# Relevante Security-Types für Investment-Screening.
# Bonds/Notes explizit ausgeschlossen — nur direkt investierbare Equity.
_EQUITY_TYPES = frozenset({
    "Common Stock",
    "Depositary Receipt",    # ADRs/GDRs — gleiche Konzernmutter, anderer Market
    "ETP",                   # ETFs die einzelne Companies tracken (Randfall)
})


@dataclass
class FigiCandidate:
    """Ein aufgelöster börsennotierter Kandidat für das Disambiguierungs-Modal."""
    figi: str
    name: str
    ticker: str | None = None
    exchange: str | None = None      # exchCode z.B. "GY" (XETRA), "US" (NYSE)
    security_type: str | None = None
    isin: str | None = None
    composite_figi: str | None = None

    @property
    def is_equity(self) -> bool:
        return self.security_type in _EQUITY_TYPES

    @property
    def display_exchange(self) -> str | None:
        """Lesbarer Exchange-Name für das Modal."""
        _map = {
            "GY": "XETRA", "GF": "Frankfurt", "GM": "München",
            "US": "NYSE/NASDAQ", "UN": "NYSE", "UQ": "NASDAQ",
            "LN": "London", "FP": "Paris", "SM": "Madrid",
            "IM": "Milano", "AV": "Wien", "SW": "Zürich",
        }
        return _map.get(self.exchange or "", self.exchange)

    def to_dict(self) -> dict:
        return {
            "figi": self.figi,
            "name": self.name,
            "ticker": self.ticker,
            "exchange": self.exchange,
            "display_exchange": self.display_exchange,
            "security_type": self.security_type,
            "isin": self.isin,
            "composite_figi": self.composite_figi,
        }


@dataclass
class ResolutionResult:
    """Ergebnis der Entity-Resolution + Trigger-Entscheidung."""
    query: str
    candidates: list[FigiCandidate]   # Equity-Kandidaten, sortiert
    show_modal: bool
    resolved: Optional[FigiCandidate]  # gesetzt wenn eindeutig (kein Modal)
    reason: str

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "show_modal": self.show_modal,
            "resolved": self.resolved.to_dict() if self.resolved else None,
            "candidates": [c.to_dict() for c in self.candidates],
            "reason": self.reason,
        }


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _parse_result(item: dict) -> Optional[FigiCandidate]:
    """Einzelnes OpenFIGI-Result-Item → FigiCandidate."""
    try:
        figi = item.get("figi")
        name = item.get("name")
        if not figi or not name:
            return None
        return FigiCandidate(
            figi=figi,
            name=name,
            ticker=item.get("ticker"),
            exchange=item.get("exchCode"),
            security_type=item.get("securityType"),
            isin=item.get("isin"),              # OpenFIGI liefert ISIN direkt
            composite_figi=item.get("compositeFIGI"),
        )
    except Exception as exc:
        logger.debug("OpenFIGI parse failed: %s", exc)
        return None


async def _search_figi(
    client: httpx.AsyncClient,
    query: str,
    security_type: str | None = "Common Stock",
) -> list[FigiCandidate]:
    """OpenFIGI /v3/search → Liste von FigiCandidates."""
    body: dict = {"query": query}
    if security_type:
        body["securityType"] = security_type

    headers = dict(_HEADERS)
    if _OPENFIGI_API_KEY:
        headers["X-OPENFIGI-APIKEY"] = _OPENFIGI_API_KEY

    try:
        resp = await client.post(
            f"{OPENFIGI_BASE}/search",
            json=body,
            headers=headers,
            timeout=_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning("OpenFIGI HTTP %s für %r", resp.status_code, query)
            return []
        data = resp.json()
        items = data.get("data", []) or []
        candidates = []
        for item in items[:_MAX_RESULTS]:
            c = _parse_result(item)
            if c:
                candidates.append(c)
        return candidates
    except Exception as exc:
        logger.warning("OpenFIGI search failed für %r: %s", query, exc)
        return []


def _sort_candidates(cands: list[FigiCandidate]) -> list[FigiCandidate]:
    """
    Sortierung: DE-Primärlistung zuerst, dann andere Exchanges.
    ADRs/GDRs nach hinten (Duplikat der Konzernmutter, anderer Market).
    """
    def key(c: FigiCandidate):
        # Primärlistung (XETRA/Frankfurt/NYSE/NASDAQ) vor Sekundär
        is_primary = c.exchange in ("GY", "GF", "UN", "UQ", "LN", "FP", "SM", "IM")
        is_dr = c.security_type == "Depositary Receipt"
        return (int(is_dr), int(not is_primary), c.name)
    return sorted(cands, key=key)


async def resolve_entity(
    name: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> ResolutionResult:
    """
    Haupteinstieg. Löst 'name' via OpenFIGI in börsennotierte Kandidaten auf
    und entscheidet den gestuften Modal-Trigger.

    Parameters
    ----------
    name    : roher Suchstring vom User
    client  : optionaler httpx-Client (Wiederverwendung empfohlen)
    """
    query = (name or "").strip()
    if not query:
        return ResolutionResult(query, [], False, None, "empty_query")

    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=_TIMEOUT)

    try:
        # Primärsuche: Common Stock (Equity).
        # Parallel: auch Depositary Receipts laden (ADRs für nicht-US Companies).
        equity, drs = await asyncio.gather(
            _search_figi(client, query, "Common Stock"),
            _search_figi(client, query, "Depositary Receipt"),
        )

        # Deduplizieren: DR für einen Kandidaten der schon als Common Stock da ist
        # nicht doppelt zeigen (gleicher composite_figi → gleiche Konzernmutter).
        equity_composites = {c.composite_figi for c in equity if c.composite_figi}
        unique_drs = [
            c for c in drs
            if not c.composite_figi or c.composite_figi not in equity_composites
        ]

        all_candidates = _sort_candidates(equity + unique_drs)

        if not all_candidates:
            return ResolutionResult(query, [], False, None, "no_figi_match")

        # Exakter Name-Match auf genau einen Kandidaten → still binden.
        qn = _norm(query)
        exact = [c for c in all_candidates if _norm(c.name) == qn]
        if len(exact) == 1:
            return ResolutionResult(
                query, all_candidates, False, exact[0], "exact_name_match"
            )

        # Genau ein Kandidat gesamt → still binden.
        if len(all_candidates) == 1:
            return ResolutionResult(
                query, all_candidates, False, all_candidates[0], "single_candidate"
            )

        # >=2 Kandidaten → Modal.
        return ResolutionResult(
            query, all_candidates, True, None, "multiple_candidates"
        )

    finally:
        if owns_client:
            await client.aclose()
