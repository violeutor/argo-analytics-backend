"""
Company Enrichment Pipeline — v2.0
====================================
Sources:
  - Crunchbase public HTML  → funding rounds + investor list
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
class CrunchbaseData:
    url: str
    description: str | None = None
    funding_total_mn: float | None = None
    stage: str | None = None
    investors: list[InvestorEntry] = field(default_factory=list)
    funding_rounds: list[FundingRound] = field(default_factory=list)
    founded_year: str | None = None
    headquarters: str | None = None
    employee_count: str | None = None


@dataclass
class BundesanzeigerData:
    company_name: str
    legal_form: str | None = None
    registered_at: str | None = None          # "HRB 12345 AG München"
    last_annual_report_year: str | None = None
    revenue_mn: float | None = None           # EUR Mio
    ebitda_mn: float | None = None
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
    crunchbase: CrunchbaseData | None = None
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
        desc = data.get("extract", "")
        out["description"]   = desc[:500] if desc else None
        out["wikipedia_url"] = data.get("content_urls", {}).get("desktop", {}).get("page")

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
        # Nur wenn founding_year noch fehlt oder HQ/Mitarbeiter gebraucht werden
        if not out.get("founded_year") or not out.get("headquarters"):
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
                                r"\|\s*(?:headquarters|hq_location|location_city)\s*=\s*([^\|\n\]]{3,80})",
                                r"\|\s*location\s*=\s*([^\|\n\]]{3,80})",
                            ]:
                                m = re.search(pat, wikitext, re.I)
                                if m:
                                    # Wikitext-Markup entfernen
                                    hq = re.sub(r"\[\[([^\|]+\|)?([^\]]+)\]\]", r"\2", m.group(1))
                                    hq = re.sub(r"\{\{[^}]+\}\}", "", hq).strip(" ,")
                                    if hq:
                                        out["headquarters"] = hq
                                    break

                        # employees
                        if not out.get("employee_count"):
                            for pat in [
                                r"\|\s*(?:num_employees|employees|employee_count)\s*=\s*([^\|\n\]]{1,30})",
                            ]:
                                m = re.search(pat, wikitext, re.I)
                                if m:
                                    raw = m.group(1).strip()
                                    if re.search(r"\d", raw):
                                        out["employee_count"] = re.sub(r"<[^>]+>", "", raw).strip()
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
            except Exception as e:
                logger.debug("Wikipedia Wikitext fallback failed for '%s': %s", company, e)

    except Exception as e:
        logger.debug("Wikipedia failed for '%s': %s", company, e)
    return out


# ─── Crunchbase ───────────────────────────────────────────────────────────────

async def _fetch_crunchbase(company: str) -> CrunchbaseData:
    slug = re.sub(r"[^a-z0-9]+", "-", company.lower()).strip("-")
    url  = f"https://www.crunchbase.com/organization/{slug}"
    result = CrunchbaseData(url=url)

    try:
        async with httpx.AsyncClient(
            timeout=12, headers=HEADERS, follow_redirects=True
        ) as client:
            resp = await client.get(url)

        if resp.status_code != 200:
            return result

        soup = BeautifulSoup(resp.text, "html.parser")

        # Meta description (SSR, always present)
        meta = soup.find("meta", attrs={"name": "description"})
        if meta:
            desc = meta.get("content", "")
            result.description = desc[:400]
            # Funding total
            m = re.search(r"\$([0-9,.]+)\s*(M|B|million|billion)", desc, re.I)
            if m:
                val  = float(m.group(1).replace(",", ""))
                unit = m.group(2).upper()
                result.funding_total_mn = val * 1000 if unit in ("B", "BILLION") else val
            # Stage
            for s in ["Series E", "Series D", "Series C", "Series B", "Series A", "Seed", "IPO"]:
                if s.lower() in desc.lower():
                    result.stage = s
                    break

        # JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    if "foundingDate" in data:
                        result.founded_year = str(data["foundingDate"])[:4]
                    emp = data.get("numberOfEmployees")
                    if emp:
                        result.employee_count = str(
                            emp.get("value", emp) if isinstance(emp, dict) else emp
                        )
                    addr = data.get("address")
                    if isinstance(addr, dict):
                        city    = addr.get("addressLocality", "")
                        country = addr.get("addressCountry", "")
                        result.headquarters = f"{city}, {country}".strip(", ")
            except Exception:
                pass

        # Investor mentions from page text
        page_text = soup.get_text(" ", strip=True)
        for pat in [
            r"(?:Lead investors?|Notable investors?)[:\s]+([^.]{10,200})",
            r"(?:backed by|funded by)[:\s]+([^.]{10,150})",
        ]:
            m = re.search(pat, page_text, re.I)
            if m:
                raw_text = m.group(1)
                for raw in re.split(r",\s*|\band\b|\s+&\s+", raw_text):
                    name = raw.strip().rstrip(".")
                    if 3 < len(name) < 60:
                        result.investors.append(InvestorEntry(
                            name=name,
                            type=_classify_investor(name),
                            role=_classify_role(raw_text),
                        ))
                break

        # Funding rounds from page text (heuristic)
        round_re = re.compile(
            r"(Seed|Series [A-F]|Growth|Venture)\s+[\·\-–]?\s*\$?([\d,.]+)\s*(M|B|million|billion)?",
            re.I,
        )
        seen_rounds: set[str] = set()
        for m in round_re.finditer(page_text[:6000]):
            rname = m.group(1).strip().title()
            if rname in seen_rounds:
                continue
            seen_rounds.add(rname)
            try:
                val  = float(m.group(2).replace(",", ""))
                unit = (m.group(3) or "M").upper()
                if unit in ("B", "BILLION"):
                    val *= 1000
                result.funding_rounds.append(FundingRound(
                    round_name=rname, amount_mn=val, date=None,
                ))
            except ValueError:
                pass

    except Exception as e:
        logger.warning("Crunchbase scrape failed for '%s': %s", company, e)

    return result


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


# ─── Company Website ─────────────────────────────────────────────────────────

async def _fetch_company_website(website: str) -> dict:
    """
    Scrapt die Company-Website nach Headcount-Angaben.
    Sucht in JSON-LD (numberOfEmployees) und im Seitentext.
    Wird aufgerufen wenn Wikipedia + Crunchbase keinen Headcount liefern.
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
                # Gruppe 1 oder 2 je nach Pattern
                val = m.group(1) if m.lastindex == 1 or not m.group(2) else m.group(2)
                # Plausibilitätscheck: zwischen 1 und 100.000
                try:
                    n = int(val.replace(",", "").replace(".", ""))
                    if 1 <= n <= 100000:
                        out["employee_count"] = str(n)
                        return out
                except ValueError:
                    pass

    except Exception as e:
        logger.debug("Company website headcount scrape failed for %s: %s", website, e)
    return out


# ─── Public API ───────────────────────────────────────────────────────────────

async def enrich_company(
    company_name: str,
    company_record: dict | None = None,
    existing_tags: list[str] | None = None,
) -> EnrichmentResult:
    """
    Full async enrichment. Runs Wikipedia + Crunchbase concurrently;
    adds Bundesanzeiger if company is likely German-registered and private.

    Returns EnrichmentResult — caller persists to Supabase.
    """
    company_record = company_record or {}
    is_listed = (
        company_record.get("investment_path") == "IPO-direkt"
        or company_record.get("ipo_potential") == "IPO erfolgt"
    )

    result = EnrichmentResult(name=company_name)

    # Concurrent: Wikipedia (primär) + Crunchbase (opportunistisch)
    # Crunchbase liefert keine strukturierten Daten mehr via SSR (JS-Rendering, BUG-04).
    # JSON-LD gelegentlich noch brauchbar für foundingDate + numberOfEmployees.
    # Wikipedia Wikitext-Infobox ist primäre Quelle für founded_year, headquarters, headcount.
    # Crunchbase-Timeout: 12s — wird via return_exceptions abgefangen, blockiert nicht.
    wiki, cb = await asyncio.gather(
        _fetch_wikipedia(company_name),
        _fetch_crunchbase(company_name),
        return_exceptions=True,
    )

    if isinstance(wiki, dict):
        result.description    = wiki.get("description")
        result.wikipedia_url  = wiki.get("wikipedia_url")
        result.website        = wiki.get("website")
        result.founded_year   = wiki.get("founded_year")
        result.headquarters   = wiki.get("headquarters")
        result.employee_count = wiki.get("employee_count")

    if isinstance(cb, CrunchbaseData):
        result.crunchbase     = cb
        result.description    = result.description or cb.description
        result.founded_year   = result.founded_year or cb.founded_year
        # Crunchbase-Werte überschreiben Wikipedia nur wenn vorhanden
        result.headquarters   = cb.headquarters or result.headquarters
        result.employee_count = cb.employee_count or result.employee_count
        result.investors      = list(cb.investors)
        result.funding_rounds = list(cb.funding_rounds)

    # Company-Website: Headcount-Fallback wenn Wikipedia + Crunchbase leer
    # Website-URL: aus DB-Record ODER aus Wikipedia-Wikitext extrahiert
    _website_url = company_record.get("website") or result.website
    if not result.employee_count and _website_url:
        try:
            website_data = await asyncio.wait_for(
                _fetch_company_website(_website_url),
                timeout=6.0,
            )
            if website_data.get("employee_count"):
                result.employee_count = website_data["employee_count"]
        except asyncio.TimeoutError:
            logger.debug("Company website timeout for %s", company_name)
        except Exception as e:
            logger.debug("Company website failed for %s: %s", company_name, e)

    # Bundesanzeiger: private DE companies only
    if not is_listed and _is_likely_german(company_record):
        ba = await _fetch_bundesanzeiger(company_name)
        result.bundesanzeiger = ba
        known = {inv.name.lower() for inv in result.investors}
        for sh in ba.shareholders:
            if sh.name.lower() not in known:
                result.investors.append(sh)

    # Tags
    text_for_tags = " ".join(filter(None, [
        result.description,
        company_record.get("category", ""),
        company_record.get("industry", ""),
    ]))
    result.tags = list(set((existing_tags or []) + _infer_tags(text_for_tags)))

    return result
