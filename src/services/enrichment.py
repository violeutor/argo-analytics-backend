"""
Company Enrichment Pipeline — v2.0
====================================
Sources:
  - DDG Instant Answer API  → employee_count, HQ (ersetzt Crunchbase BUG-04)
  - Wikidata SPARQL          → founding_year, HQ als strukturierter Fallback
  - Bundesanzeiger          → financials + ownership for private DE companies
  - Wikipedia API           → founding year, description

Bundesanzeiger-Strategie:
  - Öffentlich zugängliche Jahresabschlüsse (§ 325 HGB Pflichtveröffentlichung)
  - Suche: https://www.bundesanzeiger.de/pub/de/suchergebnis
  - Kurzansicht + Metadaten ohne Login; Volltext hinter Login
  - Wir ziehen: Rechtsform, HRB, letztes Geschäftsjahr, strukturierte Finanzkennzahlen
    aus dem öffentlichen Listing-Snippet + verlinkter Detailseite
"""

import re
import json
import logging
import asyncio
from datetime import datetime, timezone
from dataclasses import dataclass, field

import httpx
from bs4 import BeautifulSoup
from src.config import settings

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

DE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ─── Data Models ─────────────────────────────────────────────────────────────

@dataclass
class InvestorEntry:
    name: str
    type: str           # "VC" | "Corporate" | "Impact VC" | "Government" | "Fund" | "Unknown"
    role: str           # "Lead" | "Strategic" | "Early" | "Co-Investor" | "Shareholder"
    rounds: list[str] = field(default_factory=list)
    notes: str | None = None


@dataclass
class FundingRound:
    round_name: str
    amount_mn: float | None
    date: str | None
    investors: list[str] = field(default_factory=list)


@dataclass
class BundesanzeigerData:
    company_name: str
    legal_form: str | None = None
    registered_at: str | None = None          # "HRB 12345 AG München"
    last_annual_report_year: str | None = None
    revenue_mn: float | None = None           # EUR Mio
    ebitda_mn: float | None = None
    ebit_mn: float | None = None
    net_income_mn: float | None = None
    equity_mn: float | None = None            # Eigenkapital
    total_assets_mn: float | None = None      # Bilanzsumme
    employees: int | None = None
    shareholders: list[InvestorEntry] = field(default_factory=list)
    source_url: str | None = None
    found: bool = False


@dataclass
class EnrichmentResult:
    name: str
    description: str | None = None
    wikipedia_url: str | None = None
    website: str | None = None
    founded_year: str | None = None
    headquarters: str | None = None
    employee_count: str | None = None
    ticker: str | None = None      # EN-06: Börsen-Ticker aus Wikipedia-Infobox
    exchange: str | None = None    # EN-06: Börsenplatz aus Wikipedia-Infobox
    isin: str | None = None        # EN-11: ISIN aus Wikipedia-Infobox
    ipo_status: str | None = None  # BUG-47: "listed" | "private" aus Wikipedia-Infobox
    category: str | None = None   # abgeleitet aus Tags oder Claude-Fallback
    industry: str | None = None   # abgeleitet aus Tags oder Claude-Fallback
    bundesanzeiger: BundesanzeigerData | None = None
    investors: list[InvestorEntry] = field(default_factory=list)
    funding_rounds: list[FundingRound] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    enriched_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ─── Investor classifier ──────────────────────────────────────────────────────

_VC_KW     = ["ventures", "venture", "capital", "partners", "fund", "equity", "growth", "invest"]
_CORP_KW   = ["ag", "inc", "corp", "gmbh", "se ", "plc", "ltd", "technologies",
              "energy", "chemicals", "materials", "systems", "industries"]
_GOV_KW    = ["government", "federal", "national", "ministry", "department",
              "bundesministerium", "kfw", "eib", "beis", "doe", "arena"]
_IMPACT_KW = ["impact", "climate", "green", "sustainability", "esg",
              "breakthrough energy", "amazon climate", "pledge"]


def _classify_investor(name: str) -> str:
    n = name.lower()
    if any(k in n for k in _GOV_KW):    return "Government"
    if any(k in n for k in _IMPACT_KW): return "Impact VC"
    if any(k in n for k in _VC_KW):     return "VC"
    if any(k in n for k in _CORP_KW):   return "Corporate"
    return "Fund"


def _classify_role(context: str) -> str:
    c = context.lower()
    if any(k in c for k in ["lead", "leading"]):              return "Lead"
    if any(k in c for k in ["strategic", "partner"]):         return "Strategic"
    if any(k in c for k in ["seed", "early", "angel"]):       return "Early"
    return "Co-Investor"


# ─── Wikipedia ───────────────────────────────────────────────────────────────

async def _fetch_wikipedia(company: str) -> dict:
    """
    Holt Wikipedia Summary + Wikidata-Infobox-Felder.
    Zwei Requests:
      1. /api/rest_v1/page/summary/{name}  → description, founded_year (aus Extract)
         Fallback: company + " Technologies" wenn 404.
      2. /w/api.php?action=query&prop=revisions (Wikitext) → founding year, HQ, employees
         als Fallback wenn Summary-Regex nichts findet.
    """
    out: dict = {}

    async def _summary_request(client: httpx.AsyncClient, title: str) -> httpx.Response:
        return await client.get(
            "https://en.wikipedia.org/api/rest_v1/page/summary/" + title.replace(" ", "_"),
        )

    try:
        async with httpx.AsyncClient(timeout=8, headers=HEADERS) as client:
            resp = await _summary_request(client, company)
            # Fallback 1: "Company Technologies"
            if resp.status_code == 404 and not company.lower().endswith("technologies"):
                resp = await _summary_request(client, company + " Technologies")
            # Fallback 2: "Company Inc"
            if resp.status_code == 404:
                resp = await _summary_request(client, company + " Inc")

        if resp.status_code != 200:
            return out

        data = resp.json()

        # Fix B: Disambiguation-Guard — Wikipedia-Disambig-Seiten erkennen und überspringen
        # Erkennungsmuster: type="disambiguation" oder extract enthält "may refer to"
        page_type = data.get("type", "")
        desc_raw  = data.get("extract", "")
        is_disambig = (
            page_type == "disambiguation"
            or "may refer to" in desc_raw[:200].lower()
            or "can refer to" in desc_raw[:200].lower()
            or desc_raw.strip().endswith("may refer to:")
        )
        if is_disambig:
            logger.info("Wikipedia disambiguation erkannt für '%s' — überspringe", company)
            # Kein return — weiter mit Wikitext-Fallback (Step 2) und DDG-Fallback
            desc_raw = ""

        desc = desc_raw
        out["description"]    = desc[:500] if desc else None
        out["wikipedia_url"]  = data.get("content_urls", {}).get("desktop", {}).get("page")
        # BUG-02: Kanonischer Name aus Wikipedia-Titel (z.B. "spacex" → "SpaceX")
        wiki_title = data.get("title", "")
        if wiki_title and wiki_title.lower() != "not found":
            out["canonical_name"] = wiki_title

        # ── Founding year — erweitertes Muster ───────────────────────────────
        year_patterns = [
            r"(?:was\s+)?founded\s+in\s+(\d{4})",
            r"(?:was\s+)?established\s+in\s+(\d{4})",
            r"(?:was\s+)?incorporated\s+in\s+(\d{4})",
            r"(?:was\s+)?launched\s+in\s+(\d{4})",
            r"(?:was\s+)?started\s+in\s+(\d{4})",
            r"(?:was\s+)?formed\s+in\s+(\d{4})",
            r"founded\s+in\s+(\d{4})",
            r"(\d{4})[,\s]+(?:as\s+a\s+)?(?:startup|company|corporation|venture)",
            r"in\s+(\d{4})[,\s]+(?:the\s+)?company",
            r"in\s+(\d{4})[,\s]+\w+\s+(?:founded|established|launched|started)",
            r"company\s+(?:was\s+)?founded\s+in\s+(\d{4})",
        ]
        for pat in year_patterns:
            m = re.search(pat, desc, re.I)
            if m:
                out["founded_year"] = m.group(1)
                break

        # ── HQ aus Summary-Text extrahieren (Fallback vor Wikitext) ─────────────
        if not out.get("headquarters") and desc:
            hq_patterns = [
                r"headquartered\s+in\s+([A-Z][^.]+?)(?:\.|$)",
                r"headquarters\s+(?:are\s+|is\s+)?(?:located\s+)?in\s+([A-Z][^.]+?)(?:\.|$)",
                r"based\s+in\s+([A-Z][^.]+?)(?:\.|$)",
                r"offices?\s+in\s+([A-Z][^.]+?)(?:\.|$)",
            ]
            for pat in hq_patterns:
                m = re.search(pat, desc)
                if m:
                    hq = m.group(1).strip().rstrip(",")
                    # Plausibilitätscheck: max 50 Zeichen, kein ganzer Satz
                    if hq and len(hq) < 50:
                        out["headquarters"] = hq
                    break

        # ── Wikitext-Fallback: Infobox parsen ─────────────────────────────────
        # Die Infobox trägt STRUKTURELLE Felder (ISIN, Ticker, Exchange, ipo_status),
        # die der Prosa-Summary NIE liefert. Daher öffnen wir das Gate auch wenn nur
        # eines dieser Felder fehlt — nicht nur bei Gründung/HQ. Sonst überspringen
        # EN-06 (Ticker) + EN-11 (ISIN) still für gut dokumentierte Companies, deren
        # Prosa bereits Gründung+HQ liefert (Bayer-Effekt: Lede nennt "founded 1863
        # in Barmen" + "Headquartered in Leverkusen" → Gate schloss → Infobox nie geholt).
        if (not out.get("founded_year") or not out.get("headquarters")
                or not out.get("ticker") or not out.get("isin")
                or not out.get("ipo_status")):
            try:
                async with httpx.AsyncClient(timeout=6, headers=HEADERS) as client:
                    wt = await client.get(
                        "https://en.wikipedia.org/w/api.php",
                        params={
                            "action": "query",
                            "titles": company.replace(" ", "_"),
                            "prop": "revisions",
                            "rvprop": "content",
                            "rvslots": "main",
                            "formatversion": "2",
                            "format": "json",
                        },
                    )
                if wt.status_code == 200:
                    pages = wt.json().get("query", {}).get("pages", [])
                    wikitext = ""
                    for page in pages:
                        wikitext = (
                            page.get("revisions", [{}])[0]
                            .get("slots", {}).get("main", {}).get("content", "")
                        )
                        break

                    if wikitext:
                        # founded — Infobox-Felder
                        if not out.get("founded_year"):
                            for pat in [
                                r"\|\s*(?:founded|foundation|formation)\s*=\s*(?:.*?(\d{4}))",
                                r"\|\s*founding_year\s*=\s*(\d{4})",
                                r"\|\s*start_date\s*=.*?(\d{4})",
                            ]:
                                m = re.search(pat, wikitext, re.I)
                                if m:
                                    out["founded_year"] = m.group(1)
                                    break

                        # headquarters
                        if not out.get("headquarters"):
                            for pat in [
                                r"\|\s*(?:headquarters|hq_location|location_city)\s*=\s*([^\|\n]{3,120})",
                                r"\|\s*location\s*=\s*([^\|\n]{3,120})",
                            ]:
                                m = re.search(pat, wikitext, re.I)
                                if m:
                                    hq = m.group(1)
                                    # BUG-45: robuster Wikitext-Cleanup
                                    # 1. {{X|[[Y]]}} → [[Y]] retten vor Template-Strip
                                    hq = re.sub(r"\{\{\w[^|{}]*\|(\[\[[^\]]+\]\])\}\}", r"\1", hq)
                                    # 2. Restliche Templates iterativ strippen (flagicon, nowrap etc.)
                                    for _ in range(3):
                                        hq = re.sub(r"\{\{[^{}]*\}\}", "", hq)
                                    # 3. [[Link|Display]] → Display
                                    hq = re.sub(r"\[\[[^\]|]+\|([^\]]+)\]\]", r"\1", hq)
                                    # 4. [[Link]] → Link
                                    hq = re.sub(r"\[\[([^\]]+)\]\]", r"\1", hq)
                                    # 5. <ref>...</ref> — geschlossen und nicht-geschlossen
                                    hq = re.sub(r"<ref[^>]*>.*?</ref>", "", hq, flags=re.S)
                                    hq = re.sub(r"<ref[^>]*/?>.*", "", hq, flags=re.S)
                                    hq = re.sub(r"<[^>]+>", "", hq)
                                    # 6. Rest-Klammern
                                    hq = re.sub(r"[\[\]\{\}]", "", hq)
                                    hq = hq.strip(" ,\n")
                                    if hq and len(hq) < 80:
                                        out["headquarters"] = hq
                                    break

                        # employees
                        if not out.get("employee_count"):
                            for pat in [
                                r"\|\s*(?:num_employees|employees|employee_count)\s*=\s*([^\|\n\]]{1,30})",
                            ]:
                                m = re.search(pat, wikitext, re.I)
                                if m:
                                    raw = re.sub(r"<[^>]+>", "", m.group(1)).strip()
                                    # Zahlen extrahieren, Template-Markup ({{…}}) ignorieren
                                    raw_clean = re.sub(r"\{\{[^}]*\}\}", "", raw).strip(" ,+~")
                                    digits = re.sub(r"[,\.]", "", raw_clean)
                                    if digits.isdigit():
                                        n = int(digits)
                                        if 1 <= n <= 100_000:
                                            out["employee_count"] = str(n)
                                break

                        # website
                        if not out.get("website"):
                            for pat in [
                                r"\|\s*(?:website|url|homepage)\s*=\s*(?:\{\{URL\|)?([^\|\n\}\]]{5,80})",
                            ]:
                                m = re.search(pat, wikitext, re.I)
                                if m:
                                    url = m.group(1).strip().strip("{}").strip()
                                    url = re.sub(r"^https?://", "", url).strip("/")
                                    if "." in url and len(url) < 60:
                                        out["website"] = "https://" + url
                                    break

                        # ticker + exchange (EN-06)
                        # BUG-47: entkoppelt von is_listed — Ticker-Vorhandensein impliziert listed
                        if not out.get("ticker"):
                            # Muster 1: | traded_as = {{NASDAQ|FRVO}} oder {{NYSE|CRH}}
                            m = re.search(
                                r"\|\s*traded_as\s*=\s*(.*?)(?:\n\||\Z)",
                                wikitext, re.I | re.S,
                            )
                            if m:
                                block = m.group(1)
                                # {{EXCHANGE|TICKER}} — z.B. {{Nasdaq|FRVO}}
                                tm = re.search(
                                    r"\{\{(NYSE|Nasdaq|NASDAQ|LSE|Euronext|BMV|Frankfurt|Xetra|SIX|TSX|ASX)\s*\|\s*([A-Z0-9\.\-]{1,12})\}\}",
                                    block, re.I,
                                )
                                if tm:
                                    out["exchange"] = tm.group(1).upper()
                                    out["ticker"]   = tm.group(2).upper()

                        # Muster 2: | symbol = FRVO  (einfaches Feld ohne Exchange-Template)
                        if not out.get("ticker"):
                            m = re.search(
                                r"\|\s*(?:stock_symbol|symbol|ticker_symbol|stock_code)\s*=\s*([A-Z0-9\.\-]{1,12})\s*(?:\n|\|)",
                                wikitext, re.I,
                            )
                            if m:
                                out["ticker"] = m.group(1).strip().upper()

                        # Muster 3: Exchange direkt als Infobox-Feld
                        if out.get("ticker") and not out.get("exchange"):
                            m = re.search(
                                r"\|\s*(?:exchange|stock_exchange|listed_on)\s*=\s*([^\|\n\]]{2,40})",
                                wikitext, re.I,
                            )
                            if m:
                                raw_ex = re.sub(r"\{\{[^}]+\}\}", "", m.group(1)).strip()
                                raw_ex = re.sub(r"\[\[([^\|]+\|)?([^\]]+)\]\]", r"\2", raw_ex).strip()
                                if raw_ex and len(raw_ex) < 30:
                                    out["exchange"] = raw_ex

                        # BUG-47: ipo_status aus Infobox-Typ-Feld
                        # | type = Public company / Private company / Subsidiary / Non-profit
                        if not out.get("ipo_status"):
                            m = re.search(
                                r"\|\s*type\s*=\s*([^\|\n]{3,60})",
                                wikitext, re.I,
                            )
                            if m:
                                raw_type = m.group(1).lower()
                                # Wikitext-Markup entfernen
                                raw_type = re.sub(r"\[\[([^\]|]+\|)?([^\]]+)\]\]", r"\2", raw_type)
                                raw_type = re.sub(r"\{\{[^}]+\}\}", "", raw_type).strip()
                                if "public" in raw_type:
                                    out["ipo_status"] = "listed"
                                elif "private" in raw_type or "privately" in raw_type:
                                    out["ipo_status"] = "private"
                                elif "subsidiary" in raw_type:
                                    out["ipo_status"] = "private"
                            # Fallback: Ticker vorhanden → listed
                            if not out.get("ipo_status") and out.get("ticker"):
                                out["ipo_status"] = "listed"

                        # EN-11: ISIN aus Wikipedia Infobox
                        # | isin = DE000BASF111  oder  | ISIN = DE000BASF111
                        if not out.get("isin"):
                            m = re.search(
                                r"\|\s*isin\s*=\s*([A-Z]{2}[A-Z0-9]{9}[0-9])",
                                wikitext, re.I,
                            )
                            if m:
                                out["isin"] = m.group(1).strip().upper()
                                logger.info(
                                    "EN-11: ISIN aus Wikipedia-Infobox für '%s': %s",
                                    company, out["isin"],
                                )
            except Exception as e:
                logger.debug("Wikipedia Wikitext fallback failed for '%s': %s", company, e)

    except Exception as e:
        logger.debug("Wikipedia failed for '%s': %s", company, e)
    return out


# ─── DDG + Wikidata Company Facts ────────────────────────────────────────────
# Ersetzt Crunchbase (BUG-04: JS-Rendering, 403/429 Fehlermeldungen, keine SSR-Daten mehr)
# DDG Instant Answer API: employee_count, HQ aus Knowledge Graph
# Wikidata SPARQL: founding_year, HQ als strukturierte Fallback-Quelle

async def _fetch_ddg_company_facts(company: str) -> dict:
    """
    Holt Company-Fakten via DDG Instant Answer API + Wikidata SPARQL.
    Liefert: founded_year, headquarters, employee_count.
    Kein Scraping, kein Rate-Limit, keine 403/429.

    Quellen (Reihenfolge):
      1. DDG Instant Answer API (Knowledge Graph) → employee_count, HQ
      2. Wikidata SPARQL → founding_year, HQ als Fallback
    """
    out: dict = {}

    # ── 1. DDG Instant Answer API ────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=8, headers=HEADERS) as client:
            resp = await client.get(
                "https://api.duckduckgo.com/",
                params={
                    "q":            f"{company} company",
                    "format":       "json",
                    "no_html":      "1",
                    "skip_disambig":"1",
                },
            )
        if resp.status_code == 200:
            data = resp.json()

            # Infobox-Felder aus DDG Knowledge Graph
            for item in data.get("Infobox", {}).get("content", []):
                label = (item.get("label") or "").lower()
                value = (item.get("value") or "").strip()
                if not value:
                    continue

                if "employee" in label or "staff" in label:
                    # Normalisieren: "1,200" → "1200", "~500" → "500"
                    clean = re.sub(r"[^0-9]", "", value.split()[0])
                    if clean.isdigit() and 1 <= int(clean) <= 500_000:
                        out["employee_count"] = clean

                elif "headquarter" in label or "location" in label or "founded in" not in label and "city" in label:
                    if len(value) < 100:
                        out.setdefault("headquarters", value)

                elif "founded" in label or "inception" in label:
                    m = re.search(r"(\d{4})", value)
                    if m:
                        out.setdefault("founded_year", m.group(1))

            if out:
                logger.debug("DDG facts OK für '%s': %s", company, out)

    except Exception as e:
        logger.debug("DDG company facts failed für '%s': %s", company, e)

    # ── 2. Wikidata SPARQL — Fallback + Entity Resolution (EN-09) ───────────
    #
    # Erweiterte Query — holt in einem einzigen Call:
    #   ?founded      P571  — Gründungsjahr
    #   ?hqLabel      P159  — Hauptsitz
    #   ?officialName P1448 — offizieller Firmenname (z.B. "Uniper SE" statt "Uniper")
    #   ?ticker       P249  — Börsenkürzel
    #   ?exchangeLabel P414 — Börse (z.B. "Frankfurt Stock Exchange")
    #
    # Entity Resolution: P31/wdt:P279* erfasst Subklassen von Q4830453
    # → Q6881511 (publicly traded company), Q891723 (public company) etc.
    # ORDER BY DESC(?isPublic) bevorzugt listed Entities bei mehreren Treffern.
    #
    # Gate: läuft immer wenn ticker oder canonical_name noch fehlen — nicht nur bei
    # fehlenden founded_year/HQ — damit Ticker-Detection auch im Warm-Path greift.
    _wikidata_needed = (
        not out.get("founded_year")
        or not out.get("headquarters")
        or not out.get("ticker")
        or not out.get("canonical_name")
    )
    if _wikidata_needed:
        try:
            sparql = f"""
SELECT ?founded ?hqLabel ?officialName ?ticker ?exchangeLabel
       (IF(EXISTS {{ ?item wdt:P31/wdt:P279* wd:Q6881511. }}, true, false) AS ?isPublic)
WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q4830453 ;
        rdfs:label "{company}"@en .
  OPTIONAL {{ ?item wdt:P571 ?founded. }}
  OPTIONAL {{ ?item wdt:P159 ?hq. }}
  OPTIONAL {{ ?item wdt:P1448 ?officialName. FILTER(LANG(?officialName) = "en") }}
  OPTIONAL {{ ?item wdt:P249 ?ticker. }}
  OPTIONAL {{ ?item wdt:P414 ?exchange. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
ORDER BY DESC(?isPublic)
LIMIT 1
"""
            async with httpx.AsyncClient(timeout=8, headers={**HEADERS, "Accept": "application/json"}) as client:
                wd_resp = await client.get(
                    "https://query.wikidata.org/sparql",
                    params={"query": sparql, "format": "json"},
                )
            if wd_resp.status_code == 200:
                bindings = wd_resp.json().get("results", {}).get("bindings", [])
                if bindings:
                    row = bindings[0]
                    # founded_year
                    if not out.get("founded_year") and "founded" in row:
                        year_str = row["founded"]["value"][:4]
                        if year_str.isdigit():
                            out["founded_year"] = year_str
                    # headquarters
                    if not out.get("headquarters") and "hqLabel" in row:
                        out["headquarters"] = row["hqLabel"]["value"]
                    # EN-09: offizieller Name (P1448) — "Uniper" → "Uniper SE"
                    if "officialName" in row:
                        _official = row["officialName"]["value"].strip()
                        if _official and 2 <= len(_official) <= 120:
                            out["canonical_name"] = _official
                    # EN-09: Ticker (P249) — nur wenn noch nicht aus Wikipedia-Infobox
                    if not out.get("ticker") and "ticker" in row:
                        _tk = row["ticker"]["value"].strip().upper()
                        if _tk and 1 <= len(_tk) <= 12:
                            out["ticker"] = _tk
                            out["ticker_source"] = "wikidata"
                    # EN-09: Börse (P414)
                    if not out.get("exchange") and "exchangeLabel" in row:
                        out["exchange"] = row["exchangeLabel"]["value"]
                    logger.debug(
                        "Wikidata EN-09 OK für '%s': ticker=%s official=%s hq=%s",
                        company,
                        out.get("ticker"), out.get("canonical_name"), out.get("headquarters"),
                    )
        except Exception as e:
            logger.debug("Wikidata SPARQL failed für '%s': %s", company, e)

    return out


# ─── Bundesanzeiger ──────────────────────────────────────────────────────────

_BA_BASE   = "https://www.bundesanzeiger.de"
_BA_SEARCH = f"{_BA_BASE}/pub/de/suchergebnis"


def _parse_de_amount(amount_str: str, unit: str) -> float | None:
    """Parse German number format '1.234,56' + unit to float Mio EUR."""
    try:
        clean = amount_str.replace(".", "").replace(",", ".")
        val   = float(clean)
        u     = unit.upper()
        if "MIO" in u:         return round(val, 2)
        if "T" in u or "TSD" in u: return round(val / 1_000, 4)   # TEUR → Mio
        return round(val / 1_000_000, 4)                            # EUR → Mio
    except (ValueError, AttributeError):
        return None


async def _fetch_bundesanzeiger(company: str) -> BundesanzeigerData:
    result = BundesanzeigerData(company_name=company)

    # Strip legal suffixes for broader search match
    search_name = re.sub(
        r"\s+(GmbH & Co\.?\s*KG|GmbH|AG|SE|KG|UG|OHG|Inc\.?|Ltd\.?|Corp\.?)$",
        "", company, flags=re.I,
    ).strip()

    params = {
        "suchenach":  search_name,
        "kategorie":  "Jahresabschluss",
        "rechtsform": "",
        "land":       "",
    }

    try:
        async with httpx.AsyncClient(
            timeout=15, headers=DE_HEADERS, follow_redirects=True
        ) as client:
            resp = await client.get(_BA_SEARCH, params=params)

        if resp.status_code != 200:
            logger.debug("Bundesanzeiger %s for '%s'", resp.status_code, company)
            return result

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find result rows — multiple possible selectors across BA redesigns
        rows = (
            soup.select("table.result_container tr")
            or soup.select(".result-table tr")
            or soup.select("tr.publication-row")
            or [
                tr for tr in soup.find_all("tr")
                if search_name.lower() in tr.get_text().lower()
            ]
        )

        if not rows:
            logger.debug("Bundesanzeiger: no results for '%s'", company)
            return result

        result.found = True
        row = rows[0]
        row_text = row.get_text(" ", strip=True)

        # Legal form
        for lf in ["GmbH & Co. KG", "GmbH", "AG", "SE", "UG", "KG", "OHG", "GbR"]:
            if lf.lower() in row_text.lower():
                result.legal_form = lf
                break

        # Register number
        hrb = re.search(r"(HRB|HRA|VR|PR)\s*(\d+)", row_text)
        if hrb:
            result.registered_at = f"{hrb.group(1)} {hrb.group(2)}"

        # Filing year
        year_m = re.search(r"(20\d{2})", row_text)
        if year_m:
            result.last_annual_report_year = year_m.group(1)

        # Follow detail link
        link = row.find("a", href=True)
        if link:
            detail_url = link["href"]
            if not detail_url.startswith("http"):
                detail_url = _BA_BASE + detail_url
            result.source_url = detail_url

            try:
                async with httpx.AsyncClient(
                    timeout=12, headers=DE_HEADERS, follow_redirects=True
                ) as client:
                    dr = await client.get(detail_url)

                if dr.status_code == 200:
                    dtxt = BeautifulSoup(dr.text, "html.parser").get_text(" ", strip=True)

                    # Revenue
                    for pat in [
                        r"Umsatzerlöse[^0-9]*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)\s*(T?EUR|Tsd\.?\s*EUR|Mio\.?\s*EUR)",
                        r"Umsatz[^0-9]*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)\s*(T?EUR|Tsd\.?\s*EUR|Mio\.?\s*EUR)",
                    ]:
                        m = re.search(pat, dtxt, re.I)
                        if m:
                            result.revenue_mn = _parse_de_amount(m.group(1), m.group(2))
                            break

                    # Equity
                    m = re.search(
                        r"Eigenkapital[^0-9]*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)\s*(T?EUR|Tsd\.?\s*EUR|Mio\.?\s*EUR)",
                        dtxt, re.I,
                    )
                    if m:
                        result.equity_mn = _parse_de_amount(m.group(1), m.group(2))

                    # Balance sheet total
                    m = re.search(
                        r"Bilanzsumme[^0-9]*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)\s*(T?EUR|Tsd\.?\s*EUR|Mio\.?\s*EUR)",
                        dtxt, re.I,
                    )
                    if m:
                        result.total_assets_mn = _parse_de_amount(m.group(1), m.group(2))

                    # Employees
                    emp_m = re.search(
                        r"(?:Mitarbeiter|Arbeitnehmer|Beschäftigte)[:\s]*([0-9\.]{1,7})",
                        dtxt,
                    )
                    if emp_m:
                        try:
                            result.employees = int(emp_m.group(1).replace(".", ""))
                        except ValueError:
                            pass

                    # Shareholders / Gesellschafter
                    for pat in [
                        r"Gesellschafter[:\s]+([^.]{10,200})",
                        r"Anteilseigner[:\s]+([^.]{10,200})",
                        r"Alleiniger Gesellschafter[:\s]+([^.]{5,100})",
                    ]:
                        m = re.search(pat, dtxt, re.I)
                        if m:
                            known = {inv.name.lower() for inv in result.shareholders}
                            for raw in re.split(r",\s*|\bund\b|;\s*", m.group(1)):
                                n = raw.strip().rstrip(".")
                                if 3 < len(n) < 80 and n.lower() not in known:
                                    result.shareholders.append(InvestorEntry(
                                        name=n,
                                        type=_classify_investor(n),
                                        role="Shareholder",
                                    ))
                            break

            except Exception as e:
                logger.debug("BA detail fetch failed: %s", e)

    except Exception as e:
        logger.warning("Bundesanzeiger lookup failed for '%s': %s", company, e)

    logger.info(
        "Bundesanzeiger: %s — found=%s rev_mn=%s equity_mn=%s",
        company, result.found, result.revenue_mn, result.equity_mn,
    )
    return result


# ─── DE-company heuristic ────────────────────────────────────────────────────

_SKIP_BA = {"Climeworks", "Micropep", "Amini"}   # non-DE despite European origin

_DE_HINTS = ["gmbh", "ag", " se", " kg", "germany", "deutschland",
             "berlin", "munich", "münchen", "hamburg", "frankfurt",
             "stuttgart", "düsseldorf", "köln"]


# ─── EDGAR Basics (US-HQ) ────────────────────────────────────────────────────

_EDGAR_ENR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_HEADERS    = {"User-Agent": "ArgoAnalytics/1.0 (research; contact@argo-analytics.io)"}


def _normalize_for_edgar(name: str) -> str:
    """Entfernt rechtliche Suffixe für EDGAR-Suche."""
    return re.sub(
        r"\s+(GmbH\s*&\s*Co\.?\s*KG|GmbH|AG|SE|KG|UG|OHG|Inc\.?|Ltd\.?|Corp\.?|LLC|PLC|NV|BV|SAS)$",
        "", name, flags=re.I,
    ).strip()


async def _fetch_edgar_basics(company_name: str) -> dict:
    """
    EDGAR HQ-Lookup für US-Companies via CIK → submissions JSON.

    Flow:
      1. EDGAR Full-Text Search (forms=10-K,S-1,D) → erster Hit → CIK aus Accession-Number
      2. data.sec.gov/submissions/CIK{cik}.json → addresses.business (city + state)

    Liefert: {"headquarters": "Austin, TX"} oder {}
    Rate-Limit: EDGAR toleriert ~10 req/s mit korrektem User-Agent.
    """
    out: dict = {}
    norm = _normalize_for_edgar(company_name)

    try:
        async with httpx.AsyncClient(
            timeout=8, headers=_EDGAR_HEADERS, follow_redirects=True
        ) as client:
            # Schritt 1: CIK via EDGAR Search
            resp = await client.get(
                _EDGAR_ENR_SEARCH,
                params={
                    "q":       f'"{norm}"',
                    "forms":   "10-K,S-1,D",
                    "_source": "entity_name,display_names",
                },
            )
            if resp.status_code != 200:
                return out

            hits = resp.json().get("hits", {}).get("hits", [])
            if not hits:
                logger.debug("EDGAR basics: kein Treffer für '%s'", company_name)
                return out

            # CIK aus Accession-Number: "0001876789-22-000001" → "0001876789"
            acc = hits[0].get("_id", "")
            cik = acc.split("-")[0] if "-" in acc else acc[:10]
            if not cik or not cik.isdigit():
                return out

            # Schritt 2: Submissions-JSON → business address
            sub = await client.get(f"https://data.sec.gov/submissions/CIK{cik}.json")
            if sub.status_code != 200:
                return out

            biz   = sub.json().get("addresses", {}).get("business", {})
            city  = (biz.get("city") or "").strip()
            state = (biz.get("stateOrCountryDescription") or biz.get("stateOrCountry") or "").strip()

            if city:
                out["headquarters"] = f"{city}, {state}".strip(", ") if state else city
                logger.debug("EDGAR basics: %s → HQ=%s", company_name, out["headquarters"])

    except Exception as e:
        logger.debug("_fetch_edgar_basics failed for '%s': %s", company_name, e)

    return out


# ─── DuckDuckGo Instant Answer (letzter Fallback) ────────────────────────────

_DDG_API = "https://api.duckduckgo.com/"

_DDG_FOUNDED_LABELS: frozenset[str] = frozenset({
    "founded", "founded date", "foundation", "formation", "year founded",
    "founded in", "incorporated",
})
_DDG_HQ_LABELS: frozenset[str] = frozenset({
    "headquarters", "hq location", "hq", "location", "base", "office",
})


async def _fetch_duckduckgo_basics(company_name: str) -> dict:
    """
    DuckDuckGo Instant Answer API — letzter Fallback für founded_year + headquarters.
    Gibt strukturierte Infobox-Daten aus Wikipedia/Wikidata zurück.
    Kein API-Key, kostenlos, weltweite Abdeckung.

    Liefert: {"founded_year": "2009", "headquarters": "Austin, TX"} oder {}
    """
    out: dict = {}
    try:
        async with httpx.AsyncClient(timeout=6, headers=HEADERS) as client:
            resp = await client.get(
                _DDG_API,
                params={
                    "q":             company_name,
                    "format":        "json",
                    "no_html":       "1",
                    "skip_disambig": "1",
                },
            )
        if resp.status_code != 200:
            return out

        data    = resp.json()
        content = data.get("Infobox", {}).get("content", [])

        for item in content:
            label = (item.get("label") or "").lower().strip()
            value = (item.get("value") or "").strip()
            if not value:
                continue

            if label in _DDG_FOUNDED_LABELS and not out.get("founded_year"):
                m = re.search(r"\b(19|20)\d{2}\b", value)
                if m:
                    out["founded_year"] = m.group()

            elif label in _DDG_HQ_LABELS and not out.get("headquarters"):
                hq = re.sub(r"\[\d+\]", "", value).strip()
                if hq and len(hq) < 60:
                    out["headquarters"] = hq

        # Fallback: Abstract-Text wenn Infobox kein Jahr liefert
        if not out.get("founded_year"):
            m = re.search(r"founded\s+in\s+((?:19|20)\d{2})", data.get("Abstract", ""), re.I)
            if m:
                out["founded_year"] = m.group(1)

        if out:
            logger.debug("DuckDuckGo basics: %s → %s", company_name, out)

    except Exception as e:
        logger.debug("_fetch_duckduckgo_basics failed for '%s': %s", company_name, e)

    return out


def _is_likely_german(company_record: dict) -> bool:
    name = company_record.get("name", "")
    if name in _SKIP_BA:
        return False
    haystack = " ".join(filter(None, [
        name,
        company_record.get("headquarters", ""),
        company_record.get("website", ""),
        company_record.get("funding_last_round", ""),
    ])).lower()
    return any(h in haystack for h in _DE_HINTS)


# ─── Tag inference ────────────────────────────────────────────────────────────

TAG_KEYWORDS: dict[str, list[str]] = {
    "carbon-capture":        ["co2 capture", "carbon capture", "ccs", "dac", "direct air"],
    "low-carbon-cement":     ["cement", "concrete", "clinker", "calcite", "cementitious"],
    "battery":               ["battery", "lithium", "cell", "bess", "energy storage"],
    "long-duration-storage": ["iron air", "long duration", "ldes"],
    "solid-state-battery":   ["solid state", "solid-state", "solid electrolyte"],
    "grid":                  ["grid", "microgrid", "transmission", "utility"],
    "solar":                 ["solar", "photovoltaic", "pv"],
    "hydrogen":              ["hydrogen", "electrolyzer", "fuel cell", "h2"],
    "geothermal":            ["geothermal", "enhanced geothermal", "egs"],
    "agritech":              ["agriculture", "crop", "farm", "irrigation", "precision ag"],
    "bioengineering":        ["crispr", "gene edit", "genomic", "synthetic biology"],
    "co2-to-fuels":          ["saf", "sustainable aviation", "e-fuel", "syngas"],
    "datacenter-cooling":    ["cooling", "hvac", "data center", "datacenter", "thermal"],
    "waste-to-energy":       ["waste", "biogas", "landfill", "anaerobic"],
    "climate-risk-saas":     ["climate risk", "climate analytics", "esg platform"],
    "carbon-credits":        ["carbon credit", "carbon market", "offset"],
    "soil-carbon":           ["soil carbon", "microbial", "soil sequestration"],
    "irrigation":            ["irrigation", "drip", "water management"],
    "sustainable-materials": ["packaging", "bioplastic", "algae", "biodegradable"],
}


def _infer_tags(text: str) -> list[str]:
    t = text.lower()
    return [tag for tag, kws in TAG_KEYWORDS.items() if any(k in t for k in kws)]


# ── Tag → category / industry Mapping ────────────────────────────────────────
# Alle Werte sind kanonische Taxonomy-Strings aus src/taxonomy.py.
# Neue Tags hier eintragen → Werte aus TAXONOMY übernehmen.

from src.taxonomy import normalize_sector, normalize_category

_TAG_TO_CATEGORY: dict[str, str] = {
    # Carbon & Climate
    "carbon-capture":        "Carbon Capture & Storage",
    "direct-air-capture":    "Direct Air Capture",
    "ocean-cdr":             "Nature-Based Solutions",
    "co2-to-fuels":          "CO₂ Utilization",
    "carbon-credits":        "Carbon Markets & Credits",
    "climate-risk-saas":     "Climate Analytics & ESG",
    # Energy & Power
    "battery":               "Energy Storage",
    "long-duration-storage": "Energy Storage",
    "solid-state-battery":   "Energy Storage",
    "grid":                  "Grid & Smart Energy",
    "ai-grid-software":      "Grid & Smart Energy",
    "solar":                 "Solar PV",
    "hydrogen":              "Hydrogen & Fuel Cells",
    "geothermal":            "Geothermal",
    "waste-to-energy":       "Waste-to-Energy",
    # Materials & Chemicals
    "low-carbon-cement":     "Advanced Materials",
    "sustainable-materials": "Advanced Materials",
    "bio-based-chemicals":   "Green Chemicals",
    # Agriculture & Food
    "agritech":              "Precision Farming",
    "bioengineering":        "Alternative Proteins",
    "soil-carbon":           "Precision Farming",
    # Built Environment
    "datacenter-cooling":    "Smart Buildings",
    # Water
    "irrigation":            "Water Infrastructure",
    "water-tech":            "Water Technology",
}

_TAG_TO_INDUSTRY: dict[str, str] = {
    # Carbon & Climate
    "carbon-capture":        "Carbon & Climate",
    "direct-air-capture":    "Carbon & Climate",
    "ocean-cdr":             "Carbon & Climate",
    "co2-to-fuels":          "Carbon & Climate",
    "carbon-credits":        "Carbon & Climate",
    "climate-risk-saas":     "Carbon & Climate",
    # Energy & Power
    "battery":               "Energy & Power",
    "long-duration-storage": "Energy & Power",
    "solid-state-battery":   "Energy & Power",
    "grid":                  "Energy & Power",
    "ai-grid-software":      "Energy & Power",
    "solar":                 "Energy & Power",
    "hydrogen":              "Energy & Power",
    "geothermal":            "Energy & Power",
    "waste-to-energy":       "Industrial & Manufacturing",
    # Materials & Chemicals
    "low-carbon-cement":     "Materials & Chemicals",
    "sustainable-materials": "Materials & Chemicals",
    "bio-based-chemicals":   "Materials & Chemicals",
    # Agriculture & Food
    "agritech":              "Agriculture & Food",
    "bioengineering":        "Agriculture & Food",
    "soil-carbon":           "Agriculture & Food",
    # Built Environment
    "datacenter-cooling":    "Built Environment",
    # Water
    "irrigation":            "Water & Circular Economy",
    "water-tech":            "Water & Circular Economy",
}


def infer_category_industry(tags: list[str]) -> tuple[str | None, str | None]:
    """Leitet category/industry aus Tags ab — gibt Taxonomy-konforme Strings zurück."""
    for tag in tags:
        cat = _TAG_TO_CATEGORY.get(tag)
        ind = _TAG_TO_INDUSTRY.get(tag)
        if cat or ind:
            return cat, ind
    return None, None


async def _claude_infer_category(company: str, description: str) -> tuple[str | None, str | None]:
    """
    Claude-Fallback wenn TAG_KEYWORDS keinen Treffer liefert.
    Klassifiziert die Company in Sektor + Kategorie anhand der Description.
    Werte werden gegen src/taxonomy.py normalisiert — kein Freitext in die DB.
    Wird nur aufgerufen wenn tags leer — kein Timeout-Problem im Hauptpfad.
    """
    if not description:
        return None, None

    from src.taxonomy import ENRICHMENT_TAXONOMY_BLOCK, normalize_sector, normalize_category

    prompt = f"""Classify this company for an M&A investment database. Return ONLY valid JSON, no preamble, no markdown.

Company: {company}
Description: {description[:500]}

{ENRICHMENT_TAXONOMY_BLOCK}

Return JSON with exactly these two keys:
{{"sector": "<exact sector name from list>", "category": "<exact category name from list>"}}"""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",   # COST-01: Haiku für Kategorie-Inferenz
                    "max_tokens": 120,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        if resp.status_code != 200:
            logger.warning("_claude_infer_category HTTP %s for %s: %s", resp.status_code, company, resp.text[:200])
            return None, None
        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"```(?:json)?|```", "", raw).strip()
        parsed = json.loads(raw)
        raw_sector   = parsed.get("sector")
        raw_category = parsed.get("category")
        # Normalisierung gegen Taxonomy — verhindert Freitext in der DB
        sector   = normalize_sector(raw_sector)
        category = normalize_category(raw_category, sector)
        if not sector or not category:
            logger.warning(
                "_claude_infer_category NO_MATCH for %s: sector=%r category=%r",
                company, raw_sector, raw_category,
            )
        else:
            logger.info(
                "_claude_infer_category OK for %s: sector=%r category=%r",
                company, sector, category,
            )
        return category, sector
    except Exception as e:
        logger.warning("_claude_infer_category EXCEPTION for %s: %s — %s", company, type(e).__name__, e)
        return None, None


# ─── Website-URL Heuristik ───────────────────────────────────────────────────

def _guess_website_candidates(company_name: str) -> list[str]:
    """
    Leitet wahrscheinliche Website-URLs aus dem Company-Namen ab.
    Gibt Liste von Kandidaten zurück — erster der antwortet wird verwendet.
    """
    name = company_name.lower().strip()
    # Suffixe entfernen für slug-Variante
    short = name
    for suffix in [" technologies", " technology", " energy", " systems",
                   " solutions", " analytics", " sciences", " materials",
                   " ag", " inc", " gmbh", " ltd", " corp", " se", " plc", " llc"]:
        short = short.replace(suffix, "").strip()

    slug       = re.sub(r"[^a-z0-9]", "", short)
    slug_full  = re.sub(r"[^a-z0-9]", "", name)
    slug_dash  = re.sub(r"[^a-z0-9]+", "-", short).strip("-")

    candidates = []
    for s in [slug, slug_dash, slug_full]:
        if s:
            candidates += [f"https://{s}.com", f"https://{s}.io"]
    # Deduplizieren, Reihenfolge erhalten
    seen = set()
    result = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            result.append(c)
    return result


async def _resolve_website(company_name: str, known_url: str | None = None) -> str | None:
    """
    Gibt die erste erreichbare Website-URL zurück.
    Prüft zuerst bekannte URL, dann Heuristik-Kandidaten.
    """
    candidates = []
    if known_url:
        candidates.append(known_url)
    candidates += _guess_website_candidates(company_name)

    try:
        async with httpx.AsyncClient(
            timeout=5, headers=HEADERS, follow_redirects=True
        ) as client:
            for url in candidates:
                try:
                    r = await client.head(url)
                    if r.status_code < 400:
                        logger.debug("Website resolved for %s: %s", company_name, url)
                        return str(r.url)  # finale URL nach Redirect
                except Exception:
                    continue
    except Exception as e:
        logger.debug("Website resolution failed for %s: %s", company_name, e)
    return None


# ─── Company Website ─────────────────────────────────────────────────────────

async def _fetch_company_website(website: str) -> dict:
    """
    Scrapt die Company-Website nach Headcount-Angaben.
    Sucht in JSON-LD (numberOfEmployees) und im Seitentext.
    Wird aufgerufen wenn Wikipedia + DDG keinen Headcount liefern.
    """
    out: dict = {}
    if not website:
        return out
    # Normalisieren
    if not website.startswith("http"):
        website = "https://" + website
    try:
        async with httpx.AsyncClient(
            timeout=8, headers=HEADERS, follow_redirects=True
        ) as client:
            resp = await client.get(website)
        if resp.status_code != 200:
            return out

        soup = BeautifulSoup(resp.text, "html.parser")

        # 1. JSON-LD — numberOfEmployees (Schema.org Organization)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if not isinstance(data, dict):
                    continue
                emp = data.get("numberOfEmployees")
                if emp:
                    val = emp.get("value", emp) if isinstance(emp, dict) else emp
                    out["employee_count"] = str(val)
                    return out
            except Exception:
                pass

        # 2. Meta-Tags (og:description, description)
        for attr in [{"name": "description"}, {"property": "og:description"}]:
            tag = soup.find("meta", attrs=attr)
            if tag:
                content_val = tag.get("content", "")
                m = re.search(
                    r"(\d[\d,\.]*)\s*\+?\s*(?:employees?|team members?|people|Mitarbeiter|collaborators?)",
                    content_val, re.I,
                )
                if m:
                    out["employee_count"] = m.group(1).replace(",", "").replace(".", "")
                    return out

        # 3. Seitentext — Patterns für "X employees", "team of X", "X people"
        text = soup.get_text(" ", strip=True)[:8000]
        headcount_patterns = [
            r"(\d[\d,\.]*)\s*\+?\s*(?:full[- ]time\s+)?employees",
            r"(\d[\d,\.]*)\s*\+?\s*team\s+members",
            r"team\s+of\s+(?:over\s+|more\s+than\s+)?(\d[\d,\.]*)",
            r"(\d[\d,\.]*)\s*\+?\s*people\s+(?:strong|worldwide|globally|across)",
            r"(?:over|more than|nearly|about|approximately)\s+(\d[\d,\.]*)\s+(?:employees|people|staff)",
            r"(\d[\d,\.]*)\s*(?:Mitarbeiter|Beschäftigte)",
        ]
        for pat in headcount_patterns:
            m = re.search(pat, text, re.I)
            if m:
                # Sicher die erste nicht-leere Gruppe nehmen
                val = next((m.group(i) for i in range(1, (m.lastindex or 0) + 1) if m.group(i)), None)
                if not val:
                    continue
                # Plausibilitätscheck: zwischen 1 und 100.000, explizit 0 ausschließen
                try:
                    n = int(val.replace(",", "").replace(".", ""))
                    if 1 <= n <= 100_000:
                        out["employee_count"] = str(n)
                        return out
                except ValueError:
                    pass

    except Exception as e:
        logger.debug("Company website headcount scrape failed for %s: %s", website, e)
    return out


# ─── Public API ───────────────────────────────────────────────────────────────

async def _ddg_description_fallback(company_name: str) -> str | None:
    """
    Fix B: DuckDuckGo-Fallback für Company-Description.
    Greift wenn Wikipedia Disambig-Seite liefert oder 404.
    Sucht '{company_name} company' und extrahiert AbstractText aus DDG Instant Answer API.
    Fallback auf ersten organischen Snippet wenn AbstractText leer.
    """
    try:
        async with httpx.AsyncClient(timeout=8, headers=HEADERS) as client:
            # DDG Instant Answer API — kein Scraping, kein Rate-Limit
            resp = await client.get(
                "https://api.duckduckgo.com/",
                params={
                    "q":      f"{company_name} company",
                    "format": "json",
                    "no_html": "1",
                    "skip_disambig": "1",
                },
            )
            if resp.status_code != 200:
                return None
            data = resp.json()

            # AbstractText: direkte Zusammenfassung (Wikipedia-Extrakt via DDG)
            abstract = data.get("AbstractText", "").strip()
            if abstract and len(abstract) > 50:
                logger.debug("DDG AbstractText für '%s': %s…", company_name, abstract[:80])
                return abstract[:500]

            # Fallback: RelatedTopics erster Eintrag
            topics = data.get("RelatedTopics", [])
            if topics and isinstance(topics[0], dict):
                text = topics[0].get("Text", "").strip()
                if text and len(text) > 50:
                    return text[:500]

    except Exception as e:
        logger.debug("_ddg_description_fallback error für '%s': %s", company_name, e)
    return None


async def enrich_company(
    company_name: str,
    company_record: dict | None = None,
    existing_tags: list[str] | None = None,
    fast_only: bool = False,
) -> EnrichmentResult:
    """
    Async enrichment. Runs Wikipedia + DDG/Wikidata concurrently (Phase A: Identität
    inkl. EN-09 Ticker/canonical, Tags, category/industry), dann optionale Fallbacks
    (Phase B: description/headcount/HQ parallel + Bundesanzeiger).

    fast_only=True  → nur Phase A (Identität), kehrt vor den langsamen Fallbacks zurück.
                      Für den blockierenden On-Demand-Pfad: liefert Identität + Ticker
                      sofort, Phase B läuft beim Caller als BackgroundTask.
    fast_only=False → vollständig (Phase A + B). Default — Rolling Refresh / Cron.

    Returns EnrichmentResult — caller persists to Supabase.
    """
    company_record = company_record or {}
    # ISIN-First: listed wenn ticker ODER ISIN vorhanden — unabhängig von investment_path-Feldern
    is_listed = (
        company_record.get("investment_path") == "IPO-direkt"
        or company_record.get("ipo_potential") == "IPO erfolgt"
        or bool(company_record.get("ticker"))
        or bool(company_record.get("isin"))
        or company_record.get("ipo_status") == "listed"
    )

    # ── Verlustfreie Basis ───────────────────────────────────────────────────
    # result wird mit dem DB-Record VORBELEGT, statt leer zu starten.
    # Damit kann enrich_company keinen vorhandenen Wert mehr verlieren: findet
    # keine Quelle in diesem Lauf z.B. den Ticker, bleibt der DB-Wert stehen,
    # statt als None zurückzukommen. Gilt für JEDEN Caller (On-Demand, Rolling
    # Refresh, Cron) — der Schutz sitzt an der Wurzel, nicht in jedem Persist.
    # Namens-Mapping DB → Result: founding_year→founded_year, headcount→employee_count.
    result = EnrichmentResult(
        name=company_name,
        description=company_record.get("description"),
        website=company_record.get("website"),
        founded_year=company_record.get("founding_year"),
        headquarters=company_record.get("headquarters"),
        employee_count=company_record.get("headcount"),
        ticker=company_record.get("ticker"),
        exchange=company_record.get("exchange"),
        ipo_status=company_record.get("ipo_status"),
    )

    def _set_if_better(field_name: str, new_value) -> None:
        """Überschreibt result.<field> nur mit einem echten Neuwert.
        Leere Werte (None, '', leere Liste) lassen den Bestand unberührt —
        verlustfreie Merge-Regel für alle Quellen unten."""
        if new_value is None:
            return
        if isinstance(new_value, str) and not new_value.strip():
            return
        if isinstance(new_value, (list, dict)) and not new_value:
            return
        setattr(result, field_name, new_value)

    # Concurrent: Wikipedia (primär) + DDG/Wikidata (Fakten-Fallback)
    # Crunchbase entfernt: JS-Rendering seit ~2024, 403/429, keine SSR-Daten (BUG-04)
    # DDG Instant Answer API + Wikidata SPARQL als saubere, kostenfreie Alternative
    # Wikipedia Wikitext-Infobox bleibt primäre Quelle für founded_year, HQ, headcount
    wiki, ddg = await asyncio.gather(
        _fetch_wikipedia(company_name),
        _fetch_ddg_company_facts(company_name),
        return_exceptions=True,
    )

    if isinstance(wiki, dict):
        # BUG-02 / BUG-34: Kanonischen Namen aus Wikipedia-Titel übernehmen
        # "spacex" → "SpaceX", "lanzatech" → "LanzaTech"
        #
        # Drei Schutzbedingungen gegen falsche Umbenennungen:
        #  1. Listed companies (Ticker in DB) niemals umbenennen — Börsenname ist kanonisch
        #  2. Wortüberlappung: mindestens 1 Token aus company_name muss in canonical enthalten sein
        #     → verhindert "LanzaTech" → "Aviation biofuel" (kein gemeinsamer Token)
        #  3. Länge 2–120 (bestehende Bedingung)
        wiki_canonical = wiki.get("canonical_name", "")
        _has_ticker_in_db = bool(company_record.get("ticker"))
        if wiki_canonical and 2 <= len(wiki_canonical) <= 120 and not _has_ticker_in_db:
            # Token-Overlap: min. 1 signifikantes Wort (>2 Zeichen) muss übereinstimmen
            _orig_tokens = {t.lower() for t in company_name.split() if len(t) > 2}
            _wiki_tokens = {t.lower() for t in wiki_canonical.split() if len(t) > 2}
            if _orig_tokens & _wiki_tokens:
                result.name = wiki_canonical
            else:
                logger.warning(
                    "BUG-34 Guard: canonical_name '%s' für '%s' abgelehnt — "
                    "kein Token-Overlap (falsche Wikipedia-Weiterleitung?)",
                    wiki_canonical, company_name,
                )
        # Verlustfrei: nur mit echtem Neuwert überschreiben, sonst DB-Wert halten
        if wiki.get("wikipedia_url"):
            result.wikipedia_url = wiki.get("wikipedia_url")
        _set_if_better("description",    wiki.get("description"))
        _set_if_better("website",        wiki.get("website"))
        _set_if_better("founded_year",   wiki.get("founded_year"))
        _set_if_better("headquarters",   wiki.get("headquarters"))
        _set_if_better("employee_count", wiki.get("employee_count"))
        # BUG-47: ipo_status aus Wikipedia-Infobox (type-Feld + traded_as)
        # Ticker-Extraktion entkoppelt von is_listed — Ticker impliziert listed
        # Guard: listed-Status (Ticker in DB) niemals durch Wikipedia überschreiben
        _db_ipo_status = company_record.get("ipo_status")
        if _db_ipo_status != "listed":
            _set_if_better("ipo_status", wiki.get("ipo_status"))
        _set_if_better("ticker",   wiki.get("ticker"))
        _set_if_better("exchange", wiki.get("exchange"))
        _set_if_better("isin",     wiki.get("isin"))   # EN-11: ISIN-First
        # is_listed aktualisieren wenn Wikipedia es klar sagt
        if result.ipo_status == "listed" or result.ticker or result.isin:
            is_listed = True
        elif result.ipo_status == "private":
            is_listed = False

    # DDG/Wikidata-Fakten als Fallback — füllt nur was Wikipedia leer ließ
    if isinstance(ddg, dict) and ddg:
        result.founded_year   = result.founded_year   or ddg.get("founded_year")
        result.headquarters   = result.headquarters   or ddg.get("headquarters")
        result.employee_count = result.employee_count or ddg.get("employee_count")
        # EN-09: Ticker + Exchange aus Wikidata (P249/P414) — Fallback wenn Infobox leer
        if not result.ticker and ddg.get("ticker"):
            result.ticker   = ddg.get("ticker")
            result.exchange = result.exchange or ddg.get("exchange")
            if not result.ipo_status:
                result.ipo_status = "listed"
            logger.info(
                "EN-09: Wikidata Ticker für '%s': %s (%s)",
                company_name, result.ticker, result.exchange or "—",
            )
        # EN-09: Offizieller Name aus Wikidata P1448 — Fallback wenn Wikipedia kein canonical
        if ddg.get("canonical_name") and result.name == company_name:
            _official = ddg["canonical_name"]
            _orig_tokens = {t.lower() for t in company_name.split() if len(t) > 2}
            _off_tokens  = {t.lower() for t in _official.split() if len(t) > 2}
            if _orig_tokens & _off_tokens:
                result.name = _official
                logger.info(
                    "EN-09: Wikidata P1448 canonical name für '%s': '%s'",
                    company_name, _official,
                )

    # ── Optionale Fallbacks: drei UNABHÄNGIGE Gruppen, jetzt PARALLEL ──────────
    # Vorher sequenziell: description(8s) → website(6+6s) → edgar+ddg(8+6s) = bis 34s
    # Wall-Clock, was den 8s-Kill des Callers garantiert riss und ALLES verwarf.
    # Jetzt: die drei Gruppen laufen gleichzeitig (gather), Wall-Clock = langsamste
    # Einzelgruppe statt Summe. Reihenfolge INNERHALB einer Gruppe bleibt erhalten
    # (Website: resolve→scrape; Basics: EDGAR→DDG-Fallback). Identität (Wikipedia+DDG
    # oben) ist zu diesem Zeitpunkt bereits gemerged — diese Gruppen füllen nur Lücken.
    # Jede Gruppe schreibt nur via _set_if_better → kein Überschreiben mit leer,
    # Teilresultate überleben auch wenn eine Gruppe in den Timeout läuft.

    async def _fill_description() -> None:
        # Fix B: DDG-Fallback für description — wenn Wikipedia Disambig/404
        if result.description:
            return
        try:
            ddg_desc = await asyncio.wait_for(
                _ddg_description_fallback(company_name), timeout=6.0,
            )
            if ddg_desc:
                _set_if_better("description", ddg_desc)
                logger.info("DDG description fallback OK für '%s'", company_name)
        except asyncio.TimeoutError:
            logger.warning("DDG description fallback timeout für '%s'", company_name)
        except Exception as e:
            logger.warning("DDG description fallback failed für '%s': %s", company_name, e)

    async def _fill_headcount() -> None:
        # Company-Website: Headcount-Fallback. resolve → scrape (interne Reihenfolge).
        if result.employee_count:
            return
        _known_url = company_record.get("website") or result.website
        try:
            _website_url = await asyncio.wait_for(
                _resolve_website(company_name, _known_url), timeout=5.0,
            )
            if _website_url:
                _set_if_better("website", _website_url)
                website_data = await asyncio.wait_for(
                    _fetch_company_website(_website_url), timeout=5.0,
                )
                if website_data.get("employee_count"):
                    _set_if_better("employee_count", website_data["employee_count"])
        except asyncio.TimeoutError:
            logger.debug("Company website timeout for %s", company_name)
        except Exception as e:
            logger.debug("Company website failed for %s: %s", company_name, e)

    async def _fill_hq_founded() -> None:
        # EDGAR (US-Adresse) → DDG-Basics (weltweit). Interne Fallback-Reihenfolge.
        if result.headquarters and result.founded_year:
            return
        _region = (company_record.get("region") or "").upper()
        _needs_hq = not result.headquarters
        if _needs_hq and _region in ("US", ""):
            try:
                edgar_data = await asyncio.wait_for(
                    _fetch_edgar_basics(company_name), timeout=6.0,
                )
                if edgar_data.get("headquarters"):
                    _set_if_better("headquarters", edgar_data["headquarters"])
                    _needs_hq = False
            except asyncio.TimeoutError:
                logger.debug("EDGAR basics timeout for %s", company_name)
            except Exception as e:
                logger.debug("EDGAR basics failed for %s: %s", company_name, e)

        if _needs_hq or not result.founded_year:
            try:
                ddg_data = await asyncio.wait_for(
                    _fetch_duckduckgo_basics(company_name), timeout=6.0,
                )
                _set_if_better("founded_year", ddg_data.get("founded_year"))
                _set_if_better("headquarters", ddg_data.get("headquarters"))
            except asyncio.TimeoutError:
                logger.debug("DuckDuckGo basics timeout for %s", company_name)
            except Exception as e:
                logger.debug("DuckDuckGo basics failed for %s: %s", company_name, e)

    await asyncio.gather(_fill_description(), _fill_headcount(), _fill_hq_founded())

    # ── Phase A abschließen: Tags + category/industry (kein I/O, schnell) ──────
    # Vorgezogen vor die Fallbacks, damit fast_only eine identitäts-vollständige
    # Antwort liefert (Name, Ticker, Kategorie) — genau das Set für den ersten Paint.
    def _derive_tags_and_category() -> None:
        text_for_tags = " ".join(filter(None, [
            result.description,
            company_record.get("category", ""),
            company_record.get("industry", ""),
        ]))
        result.tags = list(set((existing_tags or []) + _infer_tags(text_for_tags)))
        if not company_record.get("category") or not company_record.get("industry"):
            inferred_cat, inferred_ind = infer_category_industry(result.tags)
            result.category = inferred_cat if not company_record.get("category") else None
            result.industry = inferred_ind if not company_record.get("industry") else None
        # Taxonomy-Normalisierung als Write-Guard — kein Freitext in die DB
        if result.industry:
            result.industry = normalize_sector(result.industry) or result.industry
        if result.category:
            result.category = normalize_category(result.category, result.industry) or result.category

    _derive_tags_and_category()

    if fast_only:
        # Phase A komplett — Identität + Ticker + Kategorie. Phase B (langsam) läuft
        # beim Caller als BackgroundTask via enrich_company(fast_only=False).
        return result

    # ── Phase B: optionale Fallbacks (drei UNABHÄNGIGE Gruppen, PARALLEL) ──────
    # Füllt Lücken (description/headcount/HQ). Identität steht bereits.
    # Jede Gruppe schreibt nur via _set_if_better → kein Überschreiben mit leer,
    # Teilresultate überleben auch wenn eine Gruppe in den Timeout läuft.

    async def _fill_description() -> None:
        # Fix B: DDG-Fallback für description — wenn Wikipedia Disambig/404
        if result.description:
            return
        try:
            ddg_desc = await asyncio.wait_for(
                _ddg_description_fallback(company_name), timeout=6.0,
            )
            if ddg_desc:
                _set_if_better("description", ddg_desc)
                logger.info("DDG description fallback OK für '%s'", company_name)
        except asyncio.TimeoutError:
            logger.warning("DDG description fallback timeout für '%s'", company_name)
        except Exception as e:
            logger.warning("DDG description fallback failed für '%s': %s", company_name, e)

    async def _fill_headcount() -> None:
        # Company-Website: Headcount-Fallback. resolve → scrape (interne Reihenfolge).
        if result.employee_count:
            return
        _known_url = company_record.get("website") or result.website
        try:
            _website_url = await asyncio.wait_for(
                _resolve_website(company_name, _known_url), timeout=5.0,
            )
            if _website_url:
                _set_if_better("website", _website_url)
                website_data = await asyncio.wait_for(
                    _fetch_company_website(_website_url), timeout=5.0,
                )
                if website_data.get("employee_count"):
                    _set_if_better("employee_count", website_data["employee_count"])
        except asyncio.TimeoutError:
            logger.debug("Company website timeout for %s", company_name)
        except Exception as e:
            logger.debug("Company website failed for %s: %s", company_name, e)

    async def _fill_hq_founded() -> None:
        # EDGAR (US-Adresse) → DDG-Basics (weltweit). Interne Fallback-Reihenfolge.
        if result.headquarters and result.founded_year:
            return
        _region = (company_record.get("region") or "").upper()
        _needs_hq = not result.headquarters
        if _needs_hq and _region in ("US", ""):
            try:
                edgar_data = await asyncio.wait_for(
                    _fetch_edgar_basics(company_name), timeout=6.0,
                )
                if edgar_data.get("headquarters"):
                    _set_if_better("headquarters", edgar_data["headquarters"])
                    _needs_hq = False
            except asyncio.TimeoutError:
                logger.debug("EDGAR basics timeout for %s", company_name)
            except Exception as e:
                logger.debug("EDGAR basics failed for %s: %s", company_name, e)

        if _needs_hq or not result.founded_year:
            try:
                ddg_data = await asyncio.wait_for(
                    _fetch_duckduckgo_basics(company_name), timeout=6.0,
                )
                _set_if_better("founded_year", ddg_data.get("founded_year"))
                _set_if_better("headquarters", ddg_data.get("headquarters"))
            except asyncio.TimeoutError:
                logger.debug("DuckDuckGo basics timeout for %s", company_name)
            except Exception as e:
                logger.debug("DuckDuckGo basics failed for %s: %s", company_name, e)

    await asyncio.gather(_fill_description(), _fill_headcount(), _fill_hq_founded())

    # Bundesanzeiger: private DE companies only.
    # Bleibt in Phase B (Background) — echter Tempo-Hebel ist HAI-01 in der Bridge.
    if not is_listed and _is_likely_german(company_record):
        ba = await _fetch_bundesanzeiger(company_name)
        result.bundesanzeiger = ba
        known = {inv.name.lower() for inv in result.investors}
        for sh in ba.shareholders:
            if sh.name.lower() not in known:
                result.investors.append(sh)

    # category/industry erneut prüfen: Phase B kann description nachgeliefert haben,
    # aus der sich jetzt Tags/Kategorie ableiten lassen (wenn Phase A noch leer war).
    if (not result.category or not result.industry) and result.description:
        _derive_tags_and_category()

    # ── Headcount Snapshot ────────────────────────────────────────────────────
    # Nur wenn company_id bekannt (Rolling Refresh) + employee_count vorhanden.
    # Schreibt nur bei Wertänderung gegenüber letztem Snapshot (write_headcount_snapshot
    # prüft selbst) — akkumulierende Zeitreihe für CAGR + Frontend-Verlauf.
    _company_id = company_record.get("id")
    if _company_id and result.employee_count:
        try:
            _hc_raw = str(result.employee_count).replace(",", "").replace(".", "").strip()
            _hc_int = int("".join(filter(str.isdigit, _hc_raw)) or "0") or None
            if _hc_int:
                from src.integrations.supabase import write_headcount_snapshot
                # Quelle: Wikipedia wenn founding_year aus Wiki stammt (Proxy),
                # sonst 'enrichment' als generischer Label für DDG/Website-Mix.
                _hc_source = "wikipedia" if company_record.get("headcount") else "enrichment"
                write_headcount_snapshot(
                    company_id=_company_id,
                    headcount=_hc_int,
                    source=_hc_source,
                )
        except Exception as _e:
            logger.debug("headcount snapshot skip for %s: %s", company_name, _e)

    return result
