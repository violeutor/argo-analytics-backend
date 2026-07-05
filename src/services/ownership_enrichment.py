"""
Ownership Enrichment Pipeline — v1.1
=====================================
Befüllt ownership_entries-Tabelle für jede Company vollautomatisch.

Pipelines:
  EN-02  SEC EDGAR Form D + SC 13G/13D (US) → Investoren
  EN-01  Wikipedia Management-Extraktion (DE) → Gründer + Management
         North Data (DE) → Gesellschafter/UBO — aktivieren wenn Revenue steht
  CAP    compute_cap_table_score() → Komplexitäts-Score on-demand

Quellen:
  - EDGAR:     efts.sec.gov/LATEST/search-index (kostenlos, strukturiert)
  - Wikipedia: REST API (kostenlos, kein Key) — Infobox + Summary für DE-Mgmt

Trigger: company_detail.py Background-Task bei One-Click oder erstem Tab-2-Aufruf.
Persistenz: ownership_entries-Tabelle (migration_009, deployed).
"""

import asyncio
import logging
import re
from datetime import datetime

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

    BUG-48: War: Issuer (= Company selbst) als einzigen Eintrag geschrieben.
    Fix: relatedPersonsList aus Filing-JSON holen — das sind die echten Investoren/Directors.
    Issuer wird explizit gefiltert (Company ist nicht ihr eigener Investor).
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
            hit        = hits[0]
            source     = hit.get("_source", {})
            accession  = hit.get("_id", "")
            entity     = source.get("entity_name", company_name)
            period     = source.get("period_of_report", "")
            cik        = source.get("entity_id") or accession.split("-")[0] if accession else ""
            as_of_date = period[:10] if period else None

            # Schritt 3: Filing-JSON von EDGAR holen (enthält relatedPersonsList)
            # URL-Schema: https://data.sec.gov/submissions/CIK{cik:010d}.json
            # Alternativ: Filing-Index JSON via accession number
            accession_clean = accession.replace("-", "")
            related_persons: list[dict] = []

            if cik:
                try:
                    cik_padded = str(cik).zfill(10)
                    sub_resp = await client.get(
                        f"https://data.sec.gov/submissions/CIK{cik_padded}.json",
                        timeout=8,
                    )
                    if sub_resp.status_code == 200:
                        sub_data = sub_resp.json()
                        # relatedPersonsList aus letztem Form D Filing
                        filings = sub_data.get("filings", {}).get("recent", {})
                        forms   = filings.get("form", [])
                        accessions = filings.get("accessionNumber", [])
                        dates   = filings.get("filingDate", [])
                        # Neuestes Form D finden
                        for i, form in enumerate(forms):
                            if form in ("D", "D/A"):
                                acc = accessions[i].replace("-", "") if i < len(accessions) else ""
                                filing_date = dates[i] if i < len(dates) else as_of_date
                                if acc:
                                    # Form D XML/JSON holen
                                    fd_url = (
                                        f"https://www.sec.gov/Archives/edgar/data/"
                                        f"{cik}/{acc}/{acc}-index.json"
                                    )
                                    fd_resp = await client.get(fd_url, timeout=8)
                                    if fd_resp.status_code == 200:
                                        fd_idx = fd_resp.json()
                                        # primaryDocument aus Index
                                        for doc in fd_idx.get("directory", {}).get("item", []):
                                            if doc.get("type") == "primary_doc":
                                                doc_url = (
                                                    f"https://www.sec.gov/Archives/edgar/data/"
                                                    f"{cik}/{acc}/{doc['name']}"
                                                )
                                                doc_resp = await client.get(doc_url, timeout=8)
                                                if doc_resp.status_code == 200:
                                                    import xml.etree.ElementTree as ET
                                                    try:
                                                        root = ET.fromstring(doc_resp.text)
                                                        ns = {"d": "http://www.sec.gov/edgar/document/formd"}
                                                        for rp in root.findall(".//d:relatedPerson", ns) or root.findall(".//relatedPerson"):
                                                            name_el = rp.find("d:relatedPersonName", ns) or rp.find("relatedPersonName")
                                                            rel_el  = rp.find("d:relatedPersonRelationshipList", ns) or rp.find("relatedPersonRelationshipList")
                                                            if name_el is not None:
                                                                first = (name_el.findtext("d:relatedPersonFirstName", namespaces=ns) or name_el.findtext("relatedPersonFirstName") or "").strip()
                                                                last  = (name_el.findtext("d:relatedPersonLastName",  namespaces=ns) or name_el.findtext("relatedPersonLastName")  or "").strip()
                                                                full  = f"{first} {last}".strip()
                                                                rels  = []
                                                                if rel_el is not None:
                                                                    rels = [r.text.strip() for r in list(rel_el) if r.text]
                                                                if full:
                                                                    related_persons.append({
                                                                        "name": full,
                                                                        "relationships": rels,
                                                                        "as_of_date": filing_date,
                                                                    })
                                                    except ET.ParseError:
                                                        pass
                                break
                except Exception as e:
                    logger.debug("EDGAR submissions JSON failed for %s: %s", company_name, e)

            # Schritt 4: relatedPersons → Ownership-Einträge
            # BUG-48: Issuer explizit NICHT eintragen — Company ist nicht ihr eigener Investor
            company_name_lower = company_name.lower()
            for rp in related_persons:
                name = rp["name"]
                rels = rp.get("relationships", [])
                # Skip wenn Name wie die Company selbst klingt (Issuer-Duplikat)
                if name.lower() in company_name_lower or company_name_lower in name.lower():
                    continue
                # Rolle aus Relationship ableiten
                role = "director" if any("Director" in r for r in rels) else \
                       "officer"  if any("Officer"  in r for r in rels) else \
                       "significant_shareholder" if any("10%" in r for r in rels) else \
                       "related_person"
                inv_type = "individual" if role in ("director", "officer") else \
                           _classify_investor_type(name)
                results.append({
                    "name":       name,
                    "type":       inv_type,
                    "role":       role,
                    "share_pct":  None,
                    "source":     "edgar_form_d",
                    "as_of_date": rp.get("as_of_date"),
                    "notes":      ", ".join(rels) if rels else None,
                })

            # Fallback wenn XML-Parse nichts ergab: Volltext-Suche
            if not results:
                logger.debug("EDGAR Form D XML leer für %s — Volltext-Fallback", company_name)

    except Exception as e:
        logger.debug("EDGAR Form D fetch failed for %s: %s", company_name, e)

    # Fallback: Volltext-Suche nach bekannten Investoren via EDGAR full-text
    if not results:
        try:
            edgar_entries = await _edgar_fulltext_investors(company_name)
            results.extend(edgar_entries)
        except Exception as e:
            logger.debug("EDGAR fulltext fallback failed for %s: %s", company_name, e)

    return results


async def _fetch_yahoo_ownership(
    ticker: str,
    bridge_url: str,
    bridge_key: str | None,
) -> list[dict]:
    """
    EN-08: Institutionelle Investoren via yfinance institutional_holders (BA-Bridge).
    GET /yahoo/ownership/{ticker} → holders-Liste mit name, share_pct, shares,
    value_usd, date_reported.

    Nur für listed Companies — kein EDGAR/BA/Wikipedia-Fallback.
    Gibt leere Liste zurück bei Fehler — kein Hard-Fail.
    """
    results: list[dict] = []
    headers = {"X-API-Key": bridge_key} if bridge_key else {}

    try:
        async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
            resp = await client.get(
                f"{bridge_url.rstrip('/')}/yahoo/ownership/{ticker}",
            )
        if resp.status_code != 200:
            logger.debug("Yahoo ownership HTTP %s for ticker=%s", resp.status_code, ticker)
            return []

        data = resp.json()
        # Bridge liefert {"holders": [...]} oder flat list
        holders = data.get("holders") if isinstance(data, dict) else data
        if not isinstance(holders, list):
            logger.debug("Yahoo ownership: unerwartetes Format für %s", ticker)
            return []

        for h in holders:
            name = (h.get("name") or "").strip()
            if not name:
                continue
            shares = h.get("shares")
            notes: str | None = None
            if shares:
                try:
                    notes = f"{int(shares):,} Shares"
                except (ValueError, TypeError):
                    pass
            results.append({
                "name":       name,
                "type":       _classify_investor_type(name),
                "role":       "institutional_investor",
                "share_pct":  h.get("share_pct"),
                "source":     "yahoo_institutional",
                "as_of_date": h.get("date_reported"),
                "notes":      notes,
            })

        logger.info(
            "Yahoo ownership OK: ticker=%s → %d institutionelle Investoren",
            ticker, len(results),
        )

    except Exception as e:
        logger.debug("_fetch_yahoo_ownership failed for %s: %s", ticker, e)

    return results


# SEC-13D-13G-FULL-PARSE-01 / 13DG-XML-PARSE-01 (S89): Verifiziert per Live-
# Filing (Atlassian/Baillie Gifford, Accession 0001088875-26-000006,
# primary_doc.xml, Feb 2026) statt geraten:
#   1. Seit 18.12.2024 sind Schedule 13D/13G SEC-weit PFLICHT im strukturierten
#      XML-Format. Der neue EDGAR-Formtyp heißt exakt "SCHEDULE 13G"/
#      "SCHEDULE 13D" (Großschreibung, mit Leerzeichen) — unterscheidbar vom
#      alten "SC 13G"/"SC 13D" (HTML/Text-Ära, bis 17.12.2024). Beide Varianten
#      koexistieren aktuell in EDGAR. Der bisherige `forms`-Parameter an EFTS
#      listete NUR die Alt-Formtypen — jedes Filing seit Dez. 2024 (>1 Jahr)
#      lief damit am Suchfilter vorbei, unabhängig vom "13G"/"13D"-Substring-
#      Check weiter unten (der greift erst NACH dem EFTS-Request).
#   2. Investor-Name sitzt eindeutig unter
#      coverPageHeaderReportingPersonDetails/reportingPersonName — keine
#      Rollen-Ambiguität wie bei ESMA party_name (Issuer/Offeror/Guarantor).
#   3. classPercent existiert ZWEIMAL im Dokument mit leicht abweichenden
#      Werten: Cover Page (1 Nachkommastelle, deckt sich mit der historischen
#      SEC-Anweisung "round to nearest tenth") vs. Item 4 (2 Nachkommastellen,
#      keine erkennbare Rundungsvorschrift für dieses Feld). Cover-Page-Wert
#      gewählt — regelkonform UND die erste Fundstelle in Document-Order.
#   4. Gilt nur für Filings ab 18.12.2024 — ältere SC 13G/13D-Filings haben
#      kein primary_doc.xml (reines HTML/Text). share_pct bleibt dort weiterhin
#      None — kein HTML-Freitext-Parsing in diesem Schritt, bewusster Scope-
#      Cut statt Blindfix.
_13DG_XML_ELIGIBLE_FORMS = ("SCHEDULE 13G", "SCHEDULE 13D", "SCHEDULE 13G/A", "SCHEDULE 13D/A")


def _normalize_13dg_source(form_type: str) -> str:
    """
    Alte (SC 13G/13D) und neue (SCHEDULE 13G/13D) Formtypen auf denselben
    Source-Key mappen. SC-08 (compute_ownership_score, score_calculator.py)
    hat eine feste Transparenz-Tier-Tabelle auf "edgar_sc_13g"/"edgar_sc_13d"
    verdrahtet (Tier 10.0, Pflicht-Offenlegung). Ohne diese Normalisierung
    würden alle Filings seit Dez. 2024 unter einem neuen, dort unbekannten
    Source-Key laufen und still auf eine niedrigere Tier zurückfallen —
    gleiches Konsistenz-Risiko wie FRONTEND-COMPOSITE-STRATEGIC-01, nur
    Backend-Backend statt Backend-Frontend.
    """
    ft = form_type.upper()
    if "13G" in ft:
        return "edgar_sc_13g"
    if "13D" in ft:
        return "edgar_sc_13d"
    return f"edgar_{form_type.lower().replace(' ', '_').replace('/', '_')}"


async def _fetch_13dg_xml_details(client: httpx.AsyncClient, hit_id: str) -> dict | None:
    """
    Holt primary_doc.xml für ein strukturiertes SCHEDULE 13G/13D-Filing und
    extrahiert reportingPersonName + classPercent (Cover Page). Nur für die
    neuen Formtypen aufrufbar, s. Modul-Kommentar oben.

    hit_id: EFTS-`_id`-Feld, Format "{accession-mit-bindestrichen}:{dateiname}"
    (verifiziert gegen zwei unabhängige EFTS-Doku-Quellen — _id trägt immer
    die volle Accession-Number, nie nur den Dateinamen). CIK wird wie bei
    _fetch_edgar_form_d aus dem ersten Accession-Segment abgeleitet (= CIK
    des Filers, nicht des Issuers — für die URL-Konstruktion unerheblich,
    beide leben unter derselben Accession).
    """
    accession = hit_id.split(":")[0] if hit_id else ""
    if not accession or "-" not in accession:
        return None
    cik = accession.split("-")[0].lstrip("0") or "0"
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/"
        f"{accession.replace('-', '')}/primary_doc.xml"
    )
    try:
        resp = await client.get(url, headers=HEADERS, timeout=12.0)
        if resp.status_code != 200:
            logger.debug("13G/13D primary_doc.xml HTTP %s — %s", resp.status_code, url)
            return None

        import xml.etree.ElementTree as ET
        root = ET.fromstring(resp.text)

        def _find_by_suffix(parent, suffix):
            # Namespace-agnostisches Parsing (Tag-Suffix statt vollem Tag-
            # Namen) — gleiches Muster wie _fetch_form_d_amount in
            # discovery_engine.py, fängt Spec-Versions-Drift ab.
            for el in parent.iter():
                if el.tag.split("}")[-1] == suffix:
                    return el
            return None

        cover = _find_by_suffix(root, "coverPageHeaderReportingPersonDetails")
        if cover is None:
            return None
        name_el = _find_by_suffix(cover, "reportingPersonName")
        pct_el  = _find_by_suffix(cover, "classPercent")
        reporting_name = name_el.text.strip() if name_el is not None and name_el.text else None
        share_pct = None
        if pct_el is not None and pct_el.text:
            try:
                share_pct = float(pct_el.text.strip())
            except ValueError:
                share_pct = None
        return {"reporting_person_name": reporting_name, "share_pct": share_pct}
    except Exception as e:
        logger.debug(
            "13G/13D primary_doc.xml Fetch/Parse fehlgeschlagen: %s — %r (%s)",
            type(e).__name__, e, url,
        )
        return None


async def _edgar_fulltext_investors(company_name: str) -> list[dict]:
    """
    EDGAR EFTS Volltext-Suche — findet Investoren aus SC 13G/13D + den neuen
    strukturierten SCHEDULE 13G/13D (Pflicht seit 18.12.2024) und Form D.
    Gibt strukturierte Investor-Liste zurück.

    SEC-13D-13G-FULL-PARSE-01 (S89): s. Modul-Kommentar bei
    _normalize_13dg_source für Formtyp-Historie und verifizierte XML-Struktur.
    """
    results: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=10, headers=HEADERS) as client:
            resp = await client.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={
                    "q": f'"{company_name}"',
                    "forms": "SC 13G,SC 13D,SCHEDULE 13G,SCHEDULE 13D,D",
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
                    share_pct = None
                    reporting_name = entity
                    if form_type.upper() in _13DG_XML_ELIGIBLE_FORMS:
                        detail = await _fetch_13dg_xml_details(client, hit.get("_id", ""))
                        await asyncio.sleep(0.15)  # SEC Fair-Access, ≈6.6 req/s
                        if detail:
                            share_pct = detail.get("share_pct")
                            reporting_name = detail.get("reporting_person_name") or entity

                    results.append({
                        "name": reporting_name,
                        "type": _classify_investor_type(reporting_name),
                        "role": "institutional_investor",
                        "share_pct": share_pct,
                        "source": _normalize_13dg_source(form_type),
                        "as_of_date": period[:10] if period else None,
                        "notes": f"Filed {form_type}",
                    })

    except Exception as e:
        logger.debug("EDGAR fulltext search failed for %s: %s", company_name, e)

    return results


# ── EN-01 · Wikipedia Management-Extraktion (DE) ─────────────────────────────

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_REST = "https://en.wikipedia.org/api/rest_v1/page/summary"

# Wikitext-Patterns für Management/Founder-Felder
_WIKI_PEOPLE_FIELDS = [
    "founder", "founders", "key_people", "chairman", "ceo",
    "president", "director", "directors", "board_member",
    "geschäftsführer", "gründer", "vorstand",
]


async def _fetch_wikipedia_management(company_name: str) -> list[dict]:
    """
    EN-01 Fallback: Extrahiert Gründer + Management aus Wikipedia-Infobox.
    Kostenlos, kein Key, kein Rate Limit für normale Nutzung.

    Strategie:
      1. Wikipedia REST Summary → prüfen ob Infobox-Daten im Wikitext
      2. MediaWiki API → Wikitext der Seite
      3. Regex auf Infobox-Felder: founder, key_people, chairman, ceo etc.
      4. Namen normalisieren + als ownership_entries zurückgeben
    """
    results: list[dict] = []

    try:
        async with httpx.AsyncClient(timeout=12, headers={
            "User-Agent": "ArgoAnalytics/1.0 (investment intelligence; contact@argo-analytics.io)",
            "Accept": "application/json",
        }) as client:

            # Schritt 1: Seite finden via MediaWiki search
            search_resp = await client.get(
                WIKIPEDIA_API,
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": company_name,
                    "srlimit": 3,
                    "format": "json",
                },
            )
            if search_resp.status_code != 200:
                return []

            search_results = search_resp.json().get("query", {}).get("search", [])
            if not search_results:
                logger.debug("Wikipedia: no results for %s", company_name)
                return []

            # Besten Treffer nehmen — Titel muss Company-Name enthalten
            page_title = None
            for r in search_results:
                title = r.get("title", "")
                if any(w.lower() in title.lower() for w in company_name.split()):
                    page_title = title
                    break
            if not page_title:
                page_title = search_results[0].get("title", "")

            # Schritt 2: Wikitext holen
            wikitext_resp = await client.get(
                WIKIPEDIA_API,
                params={
                    "action": "query",
                    "titles": page_title,
                    "prop": "revisions",
                    "rvprop": "content",
                    "rvslots": "main",
                    "format": "json",
                },
            )
            if wikitext_resp.status_code != 200:
                return []

            pages = wikitext_resp.json().get("query", {}).get("pages", {})
            page = next(iter(pages.values()), {})
            wikitext = (
                page.get("revisions", [{}])[0]
                .get("slots", {}).get("main", {})
                .get("*", "")
            )
            if not wikitext:
                return []

            # Schritt 3: Infobox-Felder extrahieren
            # BUG-50: founding_year übergeben — Gründer alter Companies werden gefiltert
            founding_year = None
            fy_match = re.search(r"\|\s*founded\s*=.*?(\d{4})", wikitext, re.I)
            if fy_match:
                try:
                    founding_year = int(fy_match.group(1))
                except ValueError:
                    pass
            results.extend(_parse_infobox_people(wikitext, company_name, founding_year))

    except asyncio.TimeoutError:
        logger.debug("Wikipedia timeout for %s", company_name)
    except Exception as e:
        logger.debug("Wikipedia management fetch failed for %s: %s", company_name, e)

    return results


def _parse_infobox_people(wikitext: str, company_name: str, founding_year: int | None = None) -> list[dict]:
    """
    Extrahiert Personen aus Wikipedia-Infobox-Feldern.
    Patterns: | founder = [[Name]] oder | key_people = Name (Role)

    BUG-50: founder-Felder nur aufnehmen wenn Company jünger als ~20 Jahre.
    Für alte Companies (Siemens 1847, BASF 1865 etc.) ist der Gründer kein
    relevanter Ownership-Eintrag — nur aktuelles Management ist sinnvoll.
    """
    results: list[dict] = []
    seen: set[str] = set()
    current_year = datetime.utcnow().year
    company_is_old = founding_year is not None and (current_year - founding_year) > 20

    for field in _WIKI_PEOPLE_FIELDS:
        # BUG-50: founder-Felder bei alten Companies überspringen
        if company_is_old and field in ("founder", "founders", "gründer"):
            continue

        # Infobox-Zeile: | field = value (bis zur nächsten Zeile mit |)
        pattern = rf"\|\s*{field}\s*=\s*([^\|{{}}]+?)(?=\n\s*\||\n\s*}}}})"
        matches = re.findall(pattern, wikitext, re.IGNORECASE | re.DOTALL)

        for match in matches:
            # Wiki-Links entfernen: [[Name|Display]] → Display oder Name
            text = re.sub(r"\[\[(?:[^\|\]]*\|)?([^\]]+)\]\]", r"\1", match)
            # HTML + Refs raus
            text = re.sub(r"<[^>]+>", "", text)
            text = re.sub(r"\{\{[^}]+\}\}", "", text)  # Templates
            text = re.sub(r"'''?", "", text)           # Bold/italic
            text = text.strip()

            # Mehrere Namen (durch <br>, Komma, Newline getrennt)
            names_raw = re.split(r"[,\n]|<br\s*/?>", text)

            for raw in names_raw:
                # Klammern-Inhalt = Rolle
                role_match = re.search(r"\(([^)]+)\)", raw)
                role_hint = role_match.group(1).strip() if role_match else None
                name = re.sub(r"\([^)]*\)", "", raw).strip()
                name = re.sub(r"\s+", " ", name).strip()

                # Qualitätsfilter: nur echte Namen (mind. 2 Wörter oder bekannte Struktur)
                if not name or len(name) < 3 or name.lower() == company_name.lower():
                    continue
                # Keine Jahreszahlen, keine reinen Zahlen
                if re.match(r"^\d+$", name) or re.search(r"\b(19|20)\d{2}\b", name):
                    continue
                if name in seen:
                    continue
                seen.add(name)

                # Rolle aus Feldname ableiten wenn keine explizite Rolle
                role = role_hint or _role_from_field(field)

                results.append({
                    "name": name,
                    "type": "individual",
                    "role": role,
                    "share_pct": None,
                    "source": "wikipedia_infobox",
                    "as_of_date": None,
                    "notes": f"Wikipedia: {field}",
                })

    return results[:10]  # Max 10 Einträge aus Wikipedia


def _role_from_field(field: str) -> str:
    """Mappt Infobox-Feldname auf strukturierte Rolle."""
    mapping = {
        "founder": "founder", "founders": "founder",
        "ceo": "ceo", "chairman": "chairman", "president": "president",
        "key_people": "key_person", "director": "board_member",
        "directors": "board_member", "board_member": "board_member",
        "geschäftsführer": "managing_director", "gründer": "founder",
        "vorstand": "board_member",
    }
    return mapping.get(field.lower(), "key_person")


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
    openregister_api_key: str | None = None,  # reserviert für North Data (Phase 3)
) -> dict:
    """
    Vollständige Ownership Enrichment Pipeline.
    Wird async aufgerufen — non-blocking.

    US: EDGAR Form D + SC 13G/13D
    DE: Wikipedia Infobox (Gründer, Management) — North Data wenn Revenue steht

    Returns:
      {
        "entries": list[dict],        # neue ownership_entries zum Upsert
        "cap_table": dict,            # Cap Table Score
        "source_used": str,           # "edgar" | "wikipedia" | "none"
        "region": str,
        "enriched_at": str,
      }
    """
    region = _detect_region(company)
    new_entries: list[dict] = []
    source_used = "none"

    # Bereits vorhandene Namen — keine Duplikate
    existing_names = {e.get("name", "").lower() for e in existing_entries}

    # Listed Companies — EN-07/EN-08
    # BUG-50: Keine Wikipedia-Founders für börsennotierte Companies.
    # Quellen-Hierarchie:
    #   US listed  → EDGAR SC 13G/13D (EN-07, gov API, kein CAPTCHA) → Yahoo Fallback (EN-08)
    #   DE listed  → BaFin Cron schreibt direkt in ownership_entries (EN-07)
    #                → Yahoo Fallback ad-hoc beim ersten Aufruf vor dem Cron (EN-08)
    #   other      → Yahoo (EN-08)
    is_listed = (company.get("ipo_status") or "").lower() == "listed"

    if is_listed:
        ticker_yf = company.get("ticker_yf") or company.get("ticker")

        # EN-07: EDGAR SC 13G/13D — primary für listed US (gov API, kein CAPTCHA-Risiko)
        if region == "US":
            logger.info(
                "Ownership enrichment via EDGAR SC 13G/13D for %s (listed US)",
                company_name,
            )
            try:
                edgar_entries = await asyncio.wait_for(
                    _edgar_fulltext_investors(company_name), timeout=15.0
                )
                for e in edgar_entries:
                    if e.get("name", "").lower() not in existing_names:
                        new_entries.append(e)
                        existing_names.add(e["name"].lower())
                if new_entries:
                    source_used = "edgar_sc13g"
                    logger.info(
                        "EDGAR SC 13G/13D: %d Investoren für %s (listed US)",
                        len(new_entries), company_name,
                    )
            except asyncio.TimeoutError:
                logger.debug("EDGAR SC 13G/13D timeout for %s", company_name)
            except Exception as e:
                logger.debug("EDGAR SC 13G/13D failed for %s: %s", company_name, e)

        # EN-07: DE listed → BaFin-Cron (bafin_ownership.py, 03:15 UTC) schreibt
        # direkt in ownership_entries — kein ad-hoc BaFin-Call nötig.
        # Beim ersten Aufruf vor dem Cron: Yahoo-Fallback unten greift.

        # EN-08: Yahoo institutional_holders — Fallback für alle listed Companies
        # US:    wenn EDGAR leer (kein SC 13G/13D Filing gefunden)
        # DE:    immer als ad-hoc Erstbefüllung vor BaFin-Cron
        # other: immer
        if not new_entries and ticker_yf:
            logger.info(
                "Ownership fallback via Yahoo institutional for %s "
                "(listed, ticker=%s, region=%s)",
                company_name, ticker_yf, region,
            )
            try:
                from src.config import settings as argo_settings
                bridge_url = getattr(argo_settings, "ba_bridge_url", None)
                bridge_key = getattr(argo_settings, "ba_bridge_api_key", None)
                if bridge_url:
                    yahoo_entries = await asyncio.wait_for(
                        _fetch_yahoo_ownership(ticker_yf, bridge_url, bridge_key),
                        timeout=10.0,
                    )
                    for e in yahoo_entries:
                        if e.get("name", "").lower() not in existing_names:
                            new_entries.append(e)
                            existing_names.add(e["name"].lower())
                    if new_entries:
                        source_used = "yahoo_institutional"
                else:
                    logger.debug("BA-Bridge nicht konfiguriert — kein Yahoo-Ownership-Fallback")
            except asyncio.TimeoutError:
                logger.debug("Yahoo ownership timeout for %s", company_name)
            except Exception as e:
                logger.debug("Yahoo ownership failed for %s: %s", company_name, e)
        elif not ticker_yf:
            logger.debug(
                "Listed Company %s ohne ticker_yf/ticker — kein Ownership-Fallback möglich",
                company_name,
            )

    elif region == "US":
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
        # BA-05: BA-Bridge (primär) → Wikipedia (Fallback)
        # BA-Bridge liefert Gesellschafter + Geschäftsführer aus Bundesanzeiger via Claude NER
        logger.info("Ownership enrichment via BA-Bridge for %s (DE)", company_name)
        ba_success = False
        try:
            from src.config import settings as argo_settings
            bridge_url = getattr(argo_settings, "ba_bridge_url", None)
            bridge_key = getattr(argo_settings, "ba_bridge_api_key", None)

            if bridge_url:
                async with httpx.AsyncClient(timeout=10) as client:
                    headers = {"X-API-Key": bridge_key} if bridge_key else {}
                    resp = await client.get(
                        f"{bridge_url.rstrip('/')}/ba/company/{company_name}",
                        headers=headers,
                    )

                if resp.status_code == 200:
                    ba_data = resp.json()
                    # 202 = fetching, noch nicht im Cache → Fallback
                    if ba_data.get("cached"):
                        # Gesellschafter
                        for sh in ba_data.get("shareholders", []):
                            name = (sh.get("name") or "").strip()
                            if not name or name.lower() in existing_names:
                                continue
                            new_entries.append({
                                "name":       name,
                                "type":       "corporate" if sh.get("is_company") else "individual",
                                "role":       "shareholder",
                                "share_pct":  sh.get("share_pct"),
                                "source":     "ba_bridge",
                                "as_of_date": None,
                                "notes":      f"Anteil: {sh['share_pct']}%" if sh.get("share_pct") else None,
                            })
                            existing_names.add(name.lower())

                        # Geschäftsführer / Aufsichtsrat
                        for ex in ba_data.get("executives", []):
                            name = (ex.get("name") or "").strip()
                            if not name or name.lower() in existing_names:
                                continue
                            new_entries.append({
                                "name":       name,
                                "type":       "individual",
                                "role":       ex.get("role", "executive"),
                                "share_pct":  None,
                                "source":     "ba_bridge",
                                "as_of_date": None,
                                "notes":      None,
                            })
                            existing_names.add(name.lower())

                        if new_entries:
                            source_used = "ba_bridge"
                            ba_success = True
                            logger.info(
                                "BA-Bridge: %d Einträge für %s",
                                len(new_entries), company_name,
                            )

                elif resp.status_code == 202:
                    # Bridge fetcht gerade — beim nächsten Pipeline-Run verfügbar
                    logger.info("BA-Bridge fetching %s — kein Cache noch", company_name)

        except asyncio.TimeoutError:
            logger.debug("BA-Bridge timeout for %s", company_name)
        except Exception as e:
            logger.debug("BA-Bridge failed for %s: %s", company_name, e)

        # Fallback: Wikipedia Management-Extraktion
        # BUG-50: listed Companies nicht via Wikipedia anreichern —
        # Founders als Ownership-Einträge sind für börsennotierte Konzerne irrelevant.
        # BA-Bridge oder North Data (Phase 3) sind die korrekten Quellen.
        is_listed = (company.get("ipo_status") or "").lower() == "listed"
        if not ba_success and not is_listed:
            logger.info("Ownership fallback via Wikipedia for %s (DE)", company_name)
            try:
                wiki_entries = await asyncio.wait_for(
                    _fetch_wikipedia_management(company_name), timeout=15.0
                )
                for e in wiki_entries:
                    if e.get("name", "").lower() not in existing_names:
                        new_entries.append(e)
                        existing_names.add(e.get("name", "").lower())
                if new_entries:
                    source_used = "wikipedia"
            except asyncio.TimeoutError:
                logger.debug("Wikipedia timeout for %s", company_name)
            except Exception as e:
                logger.debug("Wikipedia failed for %s: %s", company_name, e)

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
