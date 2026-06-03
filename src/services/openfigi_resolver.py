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
    share_class_figi: str | None = None   # universeller Cross-Venue-Key (S45 Exchange-Resolution)

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


# RESOLVE-PRIMARY-01: Bloomberg-Platzhalter-Ticker für private/pre-IPO Entities.
# Bloomberg vergibt für nicht (öffentlich) handelbare Entities Pseudo-Ticker im Muster
# <6+ Ziffern><1 Buchstabe>, z.B. "1531142D" (Northvolt AB an SS). Das sind KEINE
# echten Listings — sie haben keinen Yahoo/TwelveData-Kurs und dürfen is_listed nicht
# auf True setzen, sonst läuft eine private Company fälschlich durch den Listed-Pfad
# (kein Funding-Enrichment, Phantom-Preis-Lookups → 404).
_PHANTOM_TICKER_RE = re.compile(r"^\d{6,}[A-Z]$")


def _is_phantom_ticker(ticker: str | None) -> bool:
    """True wenn der Ticker ein Bloomberg-Platzhalter ist (private/pre-IPO Entity)."""
    return bool(ticker and _PHANTOM_TICKER_RE.match(ticker.strip()))


def _parse_result(item: dict) -> Optional[FigiCandidate]:
    """Einzelnes OpenFIGI-Result-Item → FigiCandidate."""
    try:
        figi = item.get("figi")
        name = item.get("name")
        if not figi or not name:
            return None
        ticker = item.get("ticker")
        # RESOLVE-PRIMARY-01: Platzhalter-Ticker verwerfen — keine echte Notierung.
        if _is_phantom_ticker(ticker):
            logger.info(
                "OpenFIGI: Platzhalter-Ticker %r für %r verworfen (private/pre-IPO Entity)",
                ticker, name,
            )
            return None
        return FigiCandidate(
            figi=figi,
            name=name,
            ticker=ticker,
            exchange=item.get("exchCode"),
            security_type=item.get("securityType"),
            isin=item.get("isin"),              # OpenFIGI liefert ISIN direkt
            composite_figi=item.get("compositeFIGI"),
            share_class_figi=item.get("shareClassFIGI"),
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
# RESOLVE-PRIMARY-01 (Option A): Liquiditäts-/Heimatbörsen-Rang statt Geo-Rang.
# Vorher stand GY (XETRA) auf 100 über US-Börsen (85) — das machte aus JEDER Company
# mit XETRA-Zweitnotiz eine deutsche (NVIDIA → NVD/GY statt NVDA/UQ). Der Rang war
# für DE-Primärnotierungen (Bayer) gebaut, wirkte aber global.
# Neue Logik: Heimatbörsen aller Hauptmärkte sind gleichrangig (100). Die Trennung
# "US-Company mit DE-Zweitnotiz" vs. "echte DE-Company" leistet NICHT der Geo-Rang,
# sondern der DR/ADR-Filter in _sort_candidates: Bayer hat an US-Börsen nur eine ADR
# (Depositary Receipt, depriorisiert) → GY-Common-Stock gewinnt. NVIDIA hat an UQ ein
# echtes Common-Stock-Listing (NVDA) → gewinnt gegen die NVD/GY-Zweitnotiz.
# QT (Bloomberg Global Composite) bleibt mittig — kanonischer Ticker, aber kein Venue;
# resolve_exchange_from_composite löst QT über shareClass in echte Venues auf.
_EXCHANGE_RANK: dict[str, int] = {
    "UN": 100,   # NYSE          ┐
    "UQ": 100,   # NASDAQ        │ US-Heimatbörsen
    "UA": 100,   # NYSE American ┘
    "GY": 100,   # XETRA (DE Heimat)
    "LN": 100,   # London (UK Heimat)
    "FP": 100,   # Paris / Euronext (FR Heimat)
    "SM": 100,   # Madrid (ES Heimat)
    "IM": 100,   # Milano (IT Heimat)
    "SW": 100,   # Zürich / SIX (CH Heimat)
    "SS": 100,   # Stockholm (SE Heimat)
    "NA": 100,   # Amsterdam (NL Heimat)
    "AV": 100,   # Wien (AT Heimat)
    "GF": 80,    # Frankfurt (DE Sekundär neben XETRA)
    "GM": 80,    # München (DE regional)
    "QT": 50,    # Bloomberg Global Composite — kanonischer Ticker, kein echtes Venue
}


def _candidate_priority(c: FigiCandidate) -> tuple[int, int]:
    """
    RESOLVE-PRIMARY-01: Einheitlicher Prioritäts-Schlüssel für Dedup UND Sort.
    Höher = besser. Zwei Stufen, damit bei rang-gleichen Heimatbörsen (alle 100)
    der DR/ADR-Status entscheidet — NICHT alphabetisch/zufällig:
      1. Common Stock vor Depositary Receipt (ADRs nie als Primär wählen).
         Das ist der Mechanismus, der "US-Company mit DE-Zweitnotiz" von "DE-Company
         mit US-ADR" trennt: NVIDIA hat US-Common-Stock (NVDA) → gewinnt; Bayer hat an
         US-Börsen nur eine ADR (BAYRY, DR) → verliert gegen GY-Common-Stock (BAYN).
      2. Exchange-Rank (Heimatbörsen 100, QT/regional niedriger).
    """
    is_common = 1 if c.security_type == "Common Stock" else 0
    rank = _EXCHANGE_RANK.get(c.exchange or "", 0)
    return (is_common, rank)


def _deduplicate(cands: list[FigiCandidate]) -> list[FigiCandidate]:
    """
    Zwei Stufen:
    1. Nur bekannte Primär-Exchanges behalten (filtert EO/E1/EB/XE/XU/XA/QT/PO etc.)
    2. Pro normalisiertem Namen den besten verbleibenden Eintrag behalten — nach
       _candidate_priority (Common Stock > DR, dann Exchange-Rank).

    Hintergrund: OpenFIGI gibt für "Bayer AG" 10 Einträge mit verschiedenen
    compositeFIGIs zurück — jedes Exchange-Listing hat eine eigene Bloomberg-ID.
    Der einzige reliable Deduplizierungs-Key ist Name + Exchange-Filter.
    """
    # Stufe 1: nur bekannte Exchanges (Primär-Listings).
    known = [c for c in cands if c.exchange in _EXCHANGE_RANK]
    # Wenn keine bekannten Exchanges: alle behalten (Fallback, verhindert leeres Modal).
    pool = known if known else cands

    # Stufe 2: pro normalisiertem Namen besten Kandidaten behalten (DR-bewusst).
    best: dict[str, FigiCandidate] = {}
    for c in pool:
        key = _norm(c.name)
        if key not in best or _candidate_priority(c) > _candidate_priority(best[key]):
            best[key] = c
    return list(best.values())


def _sort_candidates(cands: list[FigiCandidate]) -> list[FigiCandidate]:
    """
    Sortierung nach _candidate_priority (Common Stock > DR, dann Exchange-Rank),
    absteigend; Name als stabiler Tiebreaker. Konsistent mit _deduplicate.
    """
    return sorted(cands, key=lambda c: (-_candidate_priority(c)[0], -_candidate_priority(c)[1], c.name))


async def resolve_exchange_from_composite(
    composite_figi: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> str | None:
    """
    compositeFIGI → echtes Primär-Exchange (z.B. "GY" für XETRA).

    WARUM ZWEI HOPS (Befund Session 45):
    OpenFIGI /v3/search liefert für DE/EU-Konzerne als Top-Kandidat den Bloomberg-
    Global-Composite (exchCode "QT"). Dessen compositeFIGI ist NICHT der nationale
    Composite — und `COMPOSITE_ID_BB_GLOBAL` verknüpft per Definition nur Venues
    INNERHALB desselben Landes-Composites. Vom QT-Global aus erreicht man GY=XETRA
    also nie (Sackgasse — der alte Ein-Hop-Pfad gab immer None zurück).

    Korrekter Pfad über die shareClassFIGI-Ebene:
      Hop 1: mapping(COMPOSITE_ID_BB_GLOBAL, composite_figi) → liest shareClassFIGI
             (jedes Equity-FIGI trägt die universelle Share-Class-ID).
      Hop 2: mapping(ID_BB_GLOBAL_SHARE_CLASS_LEVEL, shareClassFIGI) → ALLE Venue-
             Listings derselben Aktiengattung über alle Börsen — inkl. GY=XETRA,
             das in der /v3/search-Top-10 fehlt.
      → bestes echtes Venue via _EXCHANGE_RANK (QT excluded).

    Läuft in Phase A (vor fast_only return), muss vor Phase B completed sein — damit
    Yahoo in company_detail.py den korrekten Suffix (.DE/.L/.PA) bekommt.

    Gibt None zurück bei Fehler oder wenn kein bekannter Primär-Exchange gefunden.
    """
    if not composite_figi:
        return None

    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=_TIMEOUT)

    headers = dict(_HEADERS)
    if _OPENFIGI_API_KEY:
        headers["X-OPENFIGI-APIKEY"] = _OPENFIGI_API_KEY

    async def _map(id_type: str, id_value: str) -> list[dict]:
        resp = await client.post(
            f"{OPENFIGI_BASE}/mapping",
            json=[{"idType": id_type, "idValue": id_value}],
            headers=headers,
            timeout=_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning(
                "resolve_exchange_from_composite HTTP %s (%s=%s)",
                resp.status_code, id_type, id_value,
            )
            return []
        out: list[dict] = []
        for batch in resp.json():
            out.extend(batch.get("data") or [])
        return out

    try:
        # Hop 1: Composite → shareClassFIGI
        comp_items = await _map("COMPOSITE_ID_BB_GLOBAL", composite_figi)
        share_class = next(
            (it.get("shareClassFIGI") for it in comp_items if it.get("shareClassFIGI")),
            None,
        )
        if not share_class:
            logger.debug(
                "resolve_exchange_from_composite: keine shareClassFIGI für compositeFIGI=%s",
                composite_figi,
            )
            return None

        # Hop 2: shareClassFIGI → alle Venue-Listings der Aktiengattung
        venue_items = await _map("ID_BB_GLOBAL_SHARE_CLASS_LEVEL", share_class)

        best_exchange: str | None = None
        best_rank = -1
        for item in venue_items:
            exch = item.get("exchCode") or ""
            if exch == "QT":   # Composite-Pseudo-Code — nie ein echtes Venue
                continue
            rank = _EXCHANGE_RANK.get(exch, -1)
            if rank > best_rank:
                best_rank = rank
                best_exchange = exch

        if best_exchange:
            logger.info(
                "resolve_exchange_from_composite: compositeFIGI=%s → shareClass=%s → exchange=%s",
                composite_figi, share_class, best_exchange,
            )
        else:
            logger.debug(
                "resolve_exchange_from_composite: kein Primär-Exchange (shareClass=%s, %d venues)",
                share_class, len(venue_items),
            )
        return best_exchange

    except Exception as exc:
        logger.warning(
            "resolve_exchange_from_composite failed für compositeFIGI=%s: %s",
            composite_figi, exc,
        )
        return None
    finally:
        if owns_client:
            await client.aclose()


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
            logger.info("  FIGI candidate: name=%r ticker=%r exchange=%r composite=%r shareClass=%r",
                        c.name, c.ticker, c.exchange, c.composite_figi, c.share_class_figi)
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
