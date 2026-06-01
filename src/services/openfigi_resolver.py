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
import re
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
    def legal_name(self) -> str:
        """Bloomberg-Instrumentenname → Legal Name ("BAYER AG-REG" → "Bayer AG")."""
        return _normalize_legal_name(self.name, fallback=self.name)

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
            "legal_name": self.legal_name,
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


# Bloomberg-Instrumentennamen → Legal Name.
# Bloomberg liefert "BAYER AG-REG", "ALPHABET INC-CL A", "VOLKSWAGEN AG-PFD" —
# Instrumentennamen mit Share-Class-/Registrierungs-Qualifier, KEINE Legal Names.
# Enrichment-Quellen (Wikipedia/Wikidata/HAI) finden damit nichts. Wir schneiden
# an der Rechtsform-Grenze ab (alles nach der Rechtsform ist Qualifier) + title-casen.
_LEGAL_FORM_ALT = (
    r"AG|SE|GMBH|KGAA|MBH|KG|EG|INC|CORP|CORPORATION|CO|LLC|LLP|LP|LTD|"
    r"PLC|NV|BV|SAS|SARL|SA|SPA|SRL|ASA|OYJ|AB|OY|A/S|AS"
)
_LEGAL_FORM_CANON: dict[str, str] = {
    "AG": "AG", "SE": "SE", "GMBH": "GmbH", "KGAA": "KGaA", "MBH": "mbH",
    "KG": "KG", "EG": "eG", "INC": "Inc", "CORP": "Corp",
    "CORPORATION": "Corporation", "CO": "Co", "LLC": "LLC", "LLP": "LLP",
    "LP": "LP", "LTD": "Ltd", "PLC": "plc", "NV": "NV", "BV": "BV",
    "SAS": "SAS", "SARL": "SARL", "SA": "SA", "SPA": "SpA", "SRL": "Srl",
    "ASA": "ASA", "OYJ": "Oyj", "AB": "AB", "OY": "Oy", "A/S": "A/S", "AS": "AS",
}


def _normalize_legal_name(bloomberg_name: str | None, fallback: str = "") -> str:
    """
    "BAYER AG-REG" → "Bayer AG", "ALPHABET INC-CL A" → "Alphabet Inc",
    "MERCEDES-BENZ GROUP AG" → "Mercedes-Benz Group AG", "SAP SE" → "SAP SE".

    Schneidet an der ERSTEN Rechtsform-Grenze ab (non-greedy), wirft den
    Bloomberg-Qualifier weg, title-cased wortweise und stellt die kanonische
    Rechtsform-Schreibweise wieder her. Fällt auf `fallback` (i.d.R. User-Input)
    zurück, wenn keine Rechtsform erkennbar ist.

    Casing-Hinweis: All-Caps-Akronyme ("SAP", "BASF") werden hier zu "Sap"/"Basf"
    title-gecased — das korrigiert der BUG-34-Wikipedia-Kanonik-Pfad im Enrichment
    (Wikipedia-Titel "SAP" überschreibt). Der Guard dort verhindert nur die
    Verkürzung von Legal-Form-Namen (Bayer AG ↛ Bayer).
    """
    if not bloomberg_name or not bloomberg_name.strip():
        return fallback
    raw = bloomberg_name.strip()
    m = re.match(rf"^(.*?\b(?:{_LEGAL_FORM_ALT}))\b", raw, re.I)
    core = m.group(1) if m else None
    if not core:
        return fallback

    def _title(word: str) -> str:
        # Bindestrich-Namen erhalten (Mercedes-Benz → Mercedes-Benz)
        return "-".join(p.capitalize() for p in word.split("-"))

    out_words: list[str] = []
    for w in core.split():
        canon = _LEGAL_FORM_CANON.get(w.upper().strip("."))
        out_words.append(canon if canon else _title(w))
    result = " ".join(out_words).strip()
    return result or fallback


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


# Exchange-Priorität: höherer Rank = bevorzugte Primärlistung.
_EXCHANGE_RANK: dict[str, int] = {
    "GY": 100,   # XETRA (DE Primär)
    "QT": 95,    # Bloomberg Global Composite — kanonischer Ticker (BAYN, AAPL etc.)
    "GF": 90,    # Frankfurt
    "UN": 85,    # NYSE
    "UQ": 85,    # NASDAQ
    "LN": 80,    # London
    "FP": 75,    # Paris (Euronext)
    "SM": 70,    # Madrid
    "IM": 70,    # Milano
    "AV": 65,    # Wien
    "SW": 65,    # Zürich
    "SS": 60,    # Stockholm
    "NA": 60,    # Amsterdam
}


def _deduplicate(cands: list[FigiCandidate]) -> list[FigiCandidate]:
    """
    Zwei Stufen:
    1. Nur bekannte Primär-Exchanges behalten (filtert EO/E1/EB/XE/XU/XA/QT/PO etc.)
    2. Pro normalisiertem Namen den besten verbleibenden Exchange-Eintrag behalten.

    Hintergrund: OpenFIGI gibt für "Bayer AG" 10 Einträge mit verschiedenen
    compositeFIGIs zurück — jedes Exchange-Listing hat eine eigene Bloomberg-ID.
    Der einzige reliable Deduplizierungs-Key ist Name + Exchange-Filter.
    """
    # Stufe 1: nur bekannte Exchanges (Primär-Listings).
    known = [c for c in cands if c.exchange in _EXCHANGE_RANK]
    # Wenn keine bekannten Exchanges: alle behalten (Fallback, verhindert leeres Modal).
    pool = known if known else cands

    # Stufe 2: pro normalisiertem Namen besten Exchange-Rank behalten.
    best: dict[str, FigiCandidate] = {}
    for c in pool:
        key = _norm(c.name)
        rank = _EXCHANGE_RANK.get(c.exchange or "", 0)
        if key not in best or rank > _EXCHANGE_RANK.get(best[key].exchange or "", 0):
            best[key] = c
    return list(best.values())


def _sort_candidates(cands: list[FigiCandidate]) -> list[FigiCandidate]:
    """
    Sortierung nach Exchange-Priorität: DE-Primärlistung zuerst.
    ADRs/GDRs nach hinten (gleiche Konzernmutter, anderer Market).
    """
    def key(c: FigiCandidate):
        is_dr = c.security_type == "Depositary Receipt"
        rank = _EXCHANGE_RANK.get(c.exchange or "", 0)
        return (int(is_dr), -rank, c.name)
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

        raw_all = equity + unique_drs
        logger.info("OpenFIGI raw: %d equity + %d drs = %d total",
                    len(equity), len(unique_drs), len(raw_all))
        for c in raw_all:
            logger.info("  FIGI candidate: name=%r ticker=%r exchange=%r composite=%r",
                        c.name, c.ticker, c.exchange, c.composite_figi)
        deduped = _deduplicate(raw_all)
        logger.info("OpenFIGI after dedup: %d candidates", len(deduped))
        all_candidates = _sort_candidates(deduped)

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
