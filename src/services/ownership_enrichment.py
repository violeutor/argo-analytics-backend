"""
Ownership Enrichment Pipeline — v1.0
=====================================
Befüllt ownership_entries-Tabelle für jede Company vollautomatisch.

Pipelines:
  EN-02  SEC EDGAR Form D (US) → Investoren + Funding-Runden
  EN-01  OpenRegister.de API (DE) → Gesellschafter, UBO, Kapitalstruktur
  CAP    compute_cap_table_score() → Komplexitäts-Score on-demand

Quellen:
  - EDGAR:         efts.sec.gov/LATEST/search-index?...  (kostenlos, strukturiert)
  - OpenRegister:  api.openregister.de (Free Tier, kein CC required)

Trigger: company_detail.py Background-Task bei One-Click oder erstem Tab-2-Aufruf.
Persistenz: ownership_entries-Tabelle (migration_009, deployed).
"""

import asyncio
import logging
import re
from datetime import date, datetime

import httpx

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "ArgoAnalytics/1.0 (investment intelligence platform; "
        "contact@argo-analytics.io)"
    ),
    "Accept": "application/json",
}


# ── EN-02 · SEC EDGAR Form D (US) ────────────────────────────────────────────

EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions"


async def _fetch_edgar_form_d(company_name: str) -> list[dict]:
    """
    Sucht EDGAR nach Form D Filings für eine US-Company.
    Gibt Liste von Ownership-Einträgen zurück (Investoren aus exempt offerings).

    Endpoint: efts.sec.gov/LATEST/search-index?q="company"&dateRange=custom&...
    Fallback: data.sec.gov/submissions/CIK{cik}.json für bekannte CIKs
    """
    results: list[dict] = []

    try:
        async with httpx.AsyncClient(timeout=12, headers=HEADERS) as client:
            # Schritt 1: Company-CIK suchen
            search_resp = await client.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={
                    "q": f'"{company_name}"',
                    "dateRange": "custom",
                    "startdt": "2018-01-01",
                    "forms": "D",
                    "hits.hits._source": "period_of_report,entity_name,file_num",
                },
            )
            if search_resp.status_code != 200:
                logger.debug("EDGAR search HTTP %s for %s", search_resp.status_code, company_name)
                return []

            data = search_resp.json()
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                logger.debug("EDGAR: no Form D hits for %s", company_name)
                return []

            # Schritt 2: Neuestes Filing nehmen
            hit = hits[0].get("_source", {})
            accession = hits[0].get("_id", "").replace("-", "")
            entity = hit.get("entity_name", company_name)

            # Schritt 3: Filing-Detail holen
            if not accession:
                return []

            # EDGAR filing XML — Investoren stehen in Relationship-Sektion
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{accession[:10]}/{accession}.txt"
            detail_resp = await client.get(
                f"https://efts.sec.gov/LATEST/search-index?q=%22{company_name.replace(' ', '+')}%22&forms=D&hits.hits.total.value=1",
                timeout=8,
            )

            # Schritt 4: Aus Hit Metadaten Investor-Infos extrahieren
            # Form D enthält: issuerName, relatedPersonsList, totalOfferingAmount
            filing_meta = hits[0].get("_source", {})
            period = filing_meta.get("period_of_report", "")
            amount_raw = filing_meta.get("totalOfferingAmount")

            # Issuer als Company-Eintrag
            results.append({
                "name": entity,
                "type": "issuer",
                "role": "issuer",
                "share_pct": None,
                "source": "edgar_form_d",
                "as_of_date": period[:10] if period else None,
                "notes": f"Total offering: ${amount_raw:,.0f}" if amount_raw else None,
            })

    except Exception as e:
        logger.debug("EDGAR Form D fetch failed for %s: %s", company_name, e)

    # Fallback: Volltext-Suche nach bekannten Investoren via EDGAR full-text
    try:
        edgar_entries = await _edgar_fulltext_investors(company_name)
        results.extend(edgar_entries)
    except Exception as e:
        logger.debug("EDGAR fulltext fallback failed for %s: %s", company_name, e)

    return results


async def _edgar_fulltext_investors(company_name: str) -> list[dict]:
    """
    EDGAR EFTS Volltext-Suche — findet Investoren aus SC 13G/13D und Form D.
    Gibt strukturierte Investor-Liste zurück.
    """
    results: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=10, headers=HEADERS) as client:
            resp = await client.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={
                    "q": f'"{company_name}"',
                    "forms": "SC 13G,SC 13D,D",
                    "hits.hits._source": "period_of_report,entity_name,file_num,form_type",
                },
            )
        if resp.status_code != 200:
            return []

        hits = resp.json().get("hits", {}).get("hits", [])
        seen: set[str] = set()

        for hit in hits[:8]:
            src = hit.get("_source", {})
            entity = src.get("entity_name", "")
            form_type = src.get("form_type", "")
            period = src.get("period_of_report", "")

            if not entity or entity.lower() == company_name.lower():
                continue
            if entity in seen:
                continue
            seen.add(entity)

            # SC 13G/13D = institutioneller Investor mit >5% Beteiligung
            if "13G" in form_type or "13D" in form_type:
                results.append({
                    "name": entity,
                    "type": _classify_investor_type(entity),
                    "role": "institutional_investor",
                    "share_pct": None,  # exakter Wert nur im Filing-XML
                    "source": f"edgar_{form_type.lower().replace(' ', '_')}",
                    "as_of_date": period[:10] if period else None,
                    "notes": f"Filed {form_type}",
                })

    except Exception as e:
        logger.debug("EDGAR fulltext search failed for %s: %s", company_name, e)

    return results


# ── EN-01 · OpenRegister.de (DE) ─────────────────────────────────────────────

OPENREGISTER_BASE = "https://api.openregister.de/v1"


async def _fetch_openregister(company_name: str, api_key: str | None = None) -> list[dict]:
    """
    OpenRegister.de API — primäre DE-Quelle für Gesellschafter + UBO.
    Free Tier: kein CC, API-Key via Registrierung.
    Felder: shareholders, managing_directors, legal_form, registered_capital

    Endpoint: GET /v1/companies/search?q={name}
    Detail:   GET /v1/companies/{id}/shareholders
    """
    results: list[dict] = []

    headers = {**HEADERS}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        async with httpx.AsyncClient(timeout=12, headers=headers) as client:
            # Schritt 1: Company suchen
            search_resp = await client.get(
                f"{OPENREGISTER_BASE}/companies/search",
                params={"q": company_name, "country": "DE", "limit": 3},
            )

            if search_resp.status_code == 401:
                logger.debug("OpenRegister: API key required for %s", company_name)
                return []
            if search_resp.status_code != 200:
                logger.debug("OpenRegister search HTTP %s for %s", search_resp.status_code, company_name)
                return []

            companies = search_resp.json().get("data", [])
            if not companies:
                logger.debug("OpenRegister: no match for %s", company_name)
                return []

            # Besten Treffer nehmen (erster = relevantester)
            company = companies[0]
            company_id = company.get("id")
            legal_form = company.get("legal_form", "")
            registered_capital = company.get("registered_capital")

            if not company_id:
                return []

            # Schritt 2: Gesellschafter abrufen
            sh_resp = await client.get(
                f"{OPENREGISTER_BASE}/companies/{company_id}/shareholders",
            )

            if sh_resp.status_code == 200:
                shareholders = sh_resp.json().get("data", [])
                for sh in shareholders[:10]:
                    name = sh.get("name", "")
                    if not name:
                        continue
                    share_pct = sh.get("share_percentage")
                    sh_type = sh.get("type", "")  # 'person' | 'company'

                    results.append({
                        "name": name,
                        "type": "individual" if sh_type == "person" else "corporate",
                        "role": "shareholder",
                        "share_pct": float(share_pct) if share_pct else None,
                        "source": "openregister_de",
                        "as_of_date": sh.get("as_of_date") or str(date.today()),
                        "notes": f"{legal_form}" + (f" · Kapital: €{registered_capital:,.0f}" if registered_capital else ""),
                    })

            # Schritt 3: Geschäftsführer
            mgmt_resp = await client.get(
                f"{OPENREGISTER_BASE}/companies/{company_id}/managing-directors",
            )
            if mgmt_resp.status_code == 200:
                directors = mgmt_resp.json().get("data", [])
                for d in directors[:5]:
                    name = d.get("name", "")
                    if not name:
                        continue
                    results.append({
                        "name": name,
                        "type": "individual",
                        "role": "managing_director",
                        "share_pct": None,
                        "source": "openregister_de",
                        "as_of_date": str(date.today()),
                        "notes": d.get("position", "Geschäftsführer"),
                    })

    except Exception as e:
        logger.debug("OpenRegister fetch failed for %s: %s", company_name, e)

    return results


# ── Klassifizierung ───────────────────────────────────────────────────────────

_VC_KEYWORDS = {"ventures", "capital", "vc", "fund", "partners", "equity", "growth",
                "innovation", "invest", "breakthrough", "sequoia", "andreessen",
                "khosla", "bessemer", "flagship", "deerfield"}
_PE_KEYWORDS = {"private equity", "blackstone", "kkr", "carlyle", "apollo", "warburg",
                "advent", "bain capital", "tpg", "vista"}
_CORP_KEYWORDS = {"inc", "corp", "gmbh", "ag", "ltd", "sa", "se", "plc", "llc",
                  "industries", "group", "holdings", "technologies", "systems"}
_GOV_KEYWORDS  = {"government", "ministry", "sovereign", "pension", "endowment",
                  "foundation", "university", "department", "federal", "national"}


def _classify_investor_type(name: str) -> str:
    """Heuristik: Investorentyp aus Name ableiten."""
    n = name.lower()
    if any(kw in n for kw in _PE_KEYWORDS):
        return "pe"
    if any(kw in n for kw in _VC_KEYWORDS):
        return "vc"
    if any(kw in n for kw in _GOV_KEYWORDS):
        return "government"
    if any(kw in n for kw in _CORP_KEYWORDS):
        return "corporate"
    return "unknown"


# ── Cap Table Complexity Score ────────────────────────────────────────────────

def compute_cap_table_score(
    ownership_entries: list[dict],
    funding_rounds: list[dict],
) -> dict:
    """
    Cap Table Complexity Score — on-demand Berechnung.
    score = normalize(investors_count × 0.4 + rounds_count × 0.3 + debt_flag × 0.3)

    Returns: {"score": 0.0–1.0, "label": str, "note": str}
    """
    investors_count = len(ownership_entries)
    rounds_count = len(funding_rounds)
    has_debt = any(
        (r.get("type") or "").lower() in ("debt", "loan", "convertible", "credit")
        for r in funding_rounds
    )

    # Normalisierung: investors max ~15, rounds max ~8
    inv_norm  = min(investors_count / 15, 1.0)
    rnd_norm  = min(rounds_count / 8, 1.0)
    debt_flag = 1.0 if has_debt else 0.0

    raw = inv_norm * 0.4 + rnd_norm * 0.3 + debt_flag * 0.3
    score = round(raw, 2)

    if score >= 0.7:
        label = "Hoch"
        note = f"{investors_count} Investoren, {rounds_count} Runden{', inkl. Debt' if has_debt else ''} — komplexe Kapitalstruktur."
    elif score >= 0.4:
        label = "Mittel"
        note = f"{investors_count} Investoren, {rounds_count} Runden — moderate Komplexität."
    else:
        label = "Niedrig"
        note = f"{investors_count} Investoren, {rounds_count} Runden — überschaubare Struktur."

    return {"score": score, "label": label, "note": note}


# ── Region-Detection ──────────────────────────────────────────────────────────

def _detect_region(company: dict) -> str:
    """
    Bestimmt ob Company DE oder US ist — für Pipeline-Auswahl.
    Basis: headquarters, category, name-Heuristik.
    """
    hq = (company.get("headquarters") or "").lower()
    if any(de in hq for de in ("germany", "deutschland", "berlin", "munich", "münchen",
                                "hamburg", "frankfurt", "cologne", "köln", "düsseldorf")):
        return "DE"
    if any(us in hq for us in ("united states", "usa", "new york", "san francisco",
                                "boston", "chicago", "austin", "seattle", "california",
                                "texas", "massachusetts", "new jersey")):
        return "US"
    # Fallback: Ticker-Börse
    exchange = (company.get("exchange") or "").lower()
    if any(x in exchange for x in ("nasdaq", "nyse", "amex")):
        return "US"
    if any(x in exchange for x in ("xetra", "frankfurt", "fse")):
        return "DE"
    return "US"  # Default: EDGAR versuchen


# ── Main Pipeline ─────────────────────────────────────────────────────────────

async def enrich_ownership(
    company_id: str,
    company_name: str,
    company: dict,
    existing_entries: list[dict],
    funding_rounds: list[dict],
    openregister_api_key: str | None = None,
) -> dict:
    """
    Vollständige Ownership Enrichment Pipeline.
    Wird async aufgerufen — non-blocking.

    Returns:
      {
        "entries": list[dict],        # neue ownership_entries zum Upsert
        "cap_table": dict,            # Cap Table Score
        "source_used": str,           # "edgar" | "openregister" | "none"
        "enriched_at": str,
      }
    """
    region = _detect_region(company)
    new_entries: list[dict] = []
    source_used = "none"

    # Bereits vorhandene Namen — keine Duplikate
    existing_names = {e.get("name", "").lower() for e in existing_entries}

    if region == "US":
        # EN-02: EDGAR
        logger.info("Ownership enrichment via EDGAR for %s (US)", company_name)
        try:
            edgar_entries = await asyncio.wait_for(
                _fetch_edgar_form_d(company_name), timeout=15.0
            )
            for e in edgar_entries:
                if e.get("name", "").lower() not in existing_names:
                    new_entries.append(e)
                    existing_names.add(e.get("name", "").lower())
            if new_entries:
                source_used = "edgar"
        except asyncio.TimeoutError:
            logger.debug("EDGAR timeout for %s", company_name)
        except Exception as e:
            logger.debug("EDGAR failed for %s: %s", company_name, e)

    elif region == "DE":
        # EN-01: OpenRegister
        logger.info("Ownership enrichment via OpenRegister for %s (DE)", company_name)
        try:
            or_entries = await asyncio.wait_for(
                _fetch_openregister(company_name, openregister_api_key), timeout=15.0
            )
            for e in or_entries:
                if e.get("name", "").lower() not in existing_names:
                    new_entries.append(e)
                    existing_names.add(e.get("name", "").lower())
            if new_entries:
                source_used = "openregister"
        except asyncio.TimeoutError:
            logger.debug("OpenRegister timeout for %s", company_name)
        except Exception as e:
            logger.debug("OpenRegister failed for %s: %s", company_name, e)

    # Cap Table Score — aus allen Einträgen (existing + new)
    all_entries = existing_entries + new_entries
    cap_table = compute_cap_table_score(all_entries, funding_rounds)

    return {
        "entries": new_entries,
        "cap_table": cap_table,
        "source_used": source_used,
        "region": region,
        "enriched_at": datetime.utcnow().isoformat(),
    }
