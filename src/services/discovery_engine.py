"""
DISCOVERY-PREIPO-01 · discovery_engine.py
Pfad: argo-analytics-backend/src/services/discovery_engine.py

Continuous-Discovery-Pipeline für Pre-IPO-/Funding-Kandidaten — findet NEUE,
der DB noch unbekannte Companies. Eigenständiges Modul, KEINE Erweiterung von
signal_engine.py: run_signal_engine() ist strikt auf bereits bekannte
company_name/ticker geschlüsselt (jede Quelle baut ihre Query AUS dem Namen,
statt einen Namen ERST aufzulösen) — Discovery braucht den umgekehrten Pfad.

DRY-RUN (Default, S72-Erstbau): Schreibt NICHTS in companies/signals — loggt
nur Kandidaten + Gate-Ergebnisse inkl. Roh-Antworten der noch nicht vollständig
verifizierten Quelle (ESMA PRIII-Feldnamen, s. unten). Scharfstellen erst nach
einem Render-Testlauf: DISCOVERY_DRY_RUN=false (Env-Var).

Vier Quellen, zwei Caps nach KONTEXT (nicht Quellen-Mechanismus), je 2/Tag
Decke, kein Soll — Qualität vor Quantität ist explizite Vorgabe, das Gate wird
nie gelockert um die Zahl zu erreichen:

  IPO-Intent-Topf:
    (a) EDGAR S-1/S-11   — volle Form-Indizes (/edgar/daily-index/.../form.idx),
                            NICHT efts.sec.gov (laut SEC-FAQ für Stichwortsuche
                            gedacht, kein "alle Filings eines Typs ohne Suchbegriff").
    (b) ESMA PRIII       — esma_registers_priii_documents Solr-Core (pan-EWR-
                            Pendant zu S-1). Core-Name UND Parent-Feldnamen
                            jetzt gegen die offizielle A2A-Doku verifiziert
                            (ESMA-SCHEMA-01, S84 — registers.esma.europa.eu/
                            publication/helpApp, Abschnitt 11): Core ist
                            Parent/Child (Block-Join), Query korrigiert auf
                            {!parent which="type_s:parent"}. Issuer-Name sitzt
                            auf separaten Child-Records (party_type="ISSR") —
                            ob der Parent-Response ihn dennoch mitliefert, ist
                            NICHT verifiziert (ESMA-SCHEMA-02, offen). Zusätzlich
                            fehlte bisher jeder Header am Request (ESMA-HEADER-01,
                            S84) — Diagnose-Fetches gegen denselben Endpoint
                            schlugen von hier aus konsequent mit 400 fehl, auch
                            bei q=*:*, was für einen UA-sensiblen WAF spricht.
                            Dry-Run loggt weiterhin die Roh-Keys der ersten
                            Antwort bei fehlendem Issuer-Namen (_log_unknown_
                            schema) — der nächste Render-Lauf zeigt, ob Header+
                            Query-Fix reichen oder ob ESMA-SCHEMA-02 (Child-Doc-
                            Join/[child]-Transformer) nachgezogen werden muss.

  Funding-Topf:
    (c) EDGAR Form D/D-A — gleicher Index-Mechanismus wie (a). WICHTIG: die
                            Indexdatei selbst enthält KEINEN Betrag —
                            total_offering_amount kommt aus der eigenen
                            primary_doc.xml jeder Filing (FORMD-DIRECT-XML-01,
                            S78 — löst den ursprünglichen efts.sec.gov-Namens-
                            Search ab, dessen _source-Schema das Feld nie
                            geführt hat, s. Funktion-Docstring). CIK +
                            Accession-Number bereits aus dem form.idx-Eintrag
                            bekannt, kein zweiter Suchindex-Call mehr nötig.
                            Floor ≥$10M.
    (d) Kuratierte EU-News (Google News DE+EN, sektor- statt company-geschlüsselt,
                            + bestehender TechCrunch-Globalfeed), Domain-Whitelist,
                            Floor ≥€5M via _extract_eur_funding_amount() (NEU,
                            signal_engine.py, additiv — s. dortiger Patch).

Gates (alle vier Quellen, NACH dem jeweiligen Cap/Floor-Filter):
  1. DB-Dedupe       — normalisierter Namens-Check (_normalize_name, reused)
  2. Sektor-Gate     — Claude-Haiku-Klassifikation gegen die 5 Konviktion-
                        Sektoren (Climate Tech, Circular Economy, Modern
                        Agriculture, Biotech, Defense). Schreibt NICHT
                        category/industry — das übernimmt die bestehende
                        Rolling-Refresh-Pipeline beim ersten regulären
                        Enrichment-Pass (infer_category_industry, taxonomy.py-
                        SSOT). Sektor-Gate hier ist reiner Kosten-/Qualitäts-
                        Filter, kein Taxonomy-Autor.
  3. Identitäts-Gate — 1:1 Reuse aus PEER-IDENT-01: resolve_entity()
                        (wikidata_resolver.py, Auto-Pick, kein Modal) → bei
                        Leerlauf check_entity_existence() (market_data_
                        enrichment.py) → identity_confidence-Flag, KEIN Drop
                        bei doppeltem Leerlauf (exakt wie peers.py::
                        _resolve_or_create_peer).

Write (nur scharf, DISCOVERY_DRY_RUN=false):
  companies-Row — OHNE enrichment_status, OHNE category/industry. enriched_at
  bleibt NULL → main.py::_cron_rolling_refresh holt die neue Row beim nächsten
  stündlichen Lauf automatisch ab (sortiert enriched_at IS NULL zuerst) — KEIN
  Discovery-eigener Enrichment-Sonderpfad, exakt wie in der Spec verlangt
  ("danach normale Cold-Path-Enrichment").
  + EIN SignalEvent (signal_engine.SignalEvent) mit dem Filing/Artikel als
  sichtbare Begründung, über upsert_signals() — identischer Pfad wie
  signal_engine.py, damit Notifications/Signal-Feed nichts Neues lernen müssen.

Cron-Slot: main.py, 03:45 UTC, vor Signal-Engine (04:00) — s. main.py-Patch.

Offene Verifikation (bewusst nicht blockierend für den Dry-Run-Bau, s. MODUL06
S71/S72/S84): ESMA-SCHEMA-02 (liefert der korrigierte Parent-Query den Issuer-
Namen mit, oder braucht es einen Child-Doc-Join — s. ESMA PRIII oben), EDGAR-
form.idx-Spaltentrennung am echten Live-File (Bot-Detection verhinderte den
direkten Abruf aus der Sandbox; Doku-basierte Spalten-Semantik ist verifiziert,
das exakte Whitespace-Layout nicht).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Literal
from xml.etree import ElementTree as ET

import httpx

from src.services.signal_engine import (
    SignalEvent,
    _normalize_name,
    _extract_eur_funding_amount,
)
# DEALCOMPS-TRANSACTIONS-01: dieselbe Sektor-/Stage-Taxonomie wie
# compute_target_valuation() — Comp-Transaktionen muessen gegen denselben
# Bucket-Schluessel klassifiziert werden, den die spaetere EXIT_ADJUSTMENT-
# Kalibrierung verwendet, sonst passt keine Zeile zur anderen.
from src.services.valuation import VERTICAL_DELTA, STAGE_MULT

logger = logging.getLogger(__name__)

# ── Konfiguration ─────────────────────────────────────────────────────────────

# Default DRY: keine DB-Writes, nur Logging. Scharf erst nach Render-Testlauf.
DRY_RUN: bool = os.environ.get("DISCOVERY_DRY_RUN", "true").lower() != "false"

DAILY_CAP_PER_POT = 2   # Decke, kein Soll — pro Topf (IPO-Intent / Funding)

FORM_D_FLOOR_USD_MN = 10.0    # ≥$10M, strukturiert aus EDGAR vorhanden
EU_NEWS_FLOOR_EUR_MN = 5.0    # ≥€5M, aus Artikel-Freitext extrahiert

DOMAIN_WHITELIST = {"sifted.eu", "tech.eu", "techcrunch.com", "gruenderszene.de"}

CONVICTION_SECTORS = (
    "Climate Tech", "Circular Economy", "Modern Agriculture", "Biotech", "Defense",
)

# SEC verlangt einen deklarierten User-Agent (sonst Bot-Detection/403) UND
# dokumentiert ein hartes Rate-Limit von 10 req/s — kein "kein Limit gefunden"
# mehr (Korrektur ggü. der ursprünglichen S71-Spezifikation, per Web-Diagnose
# vor diesem Build verifiziert). Gleicher UA-String wie signal_engine.py, damit
# SEC nicht zwei unterschiedliche Absender für dasselbe Produkt sieht.
_SEC_USER_AGENT = "ArgoAnalytics/1.0 (research; contact@argo-analytics.io)"
_SEC_HEADERS = {"User-Agent": _SEC_USER_AGENT}
_SEC_MIN_DELAY_S = 0.15   # ≈6.6 req/s — sicher unter dem 10-req/s-Limit

_EDGAR_DAILY_INDEX_BASE = "https://www.sec.gov/Archives/edgar/daily-index"

_ESMA_PRIII_SELECT = "https://registers.esma.europa.eu/solr/esma_registers_priii_documents/select"

# ESMA-HEADER-01 (S84): Anders als der EDGAR-Client sendete dieser Call bisher
# GAR KEINEN Header (kein User-Agent) — plausibelster Timeout-Verdächtiger
# (Diagnose-Fetches gegen denselben Endpoint schlugen von hier aus konsequent
# mit 400 fehl, selbst bei q=*:*; die menschliche Registers-UI zeigt zudem ein
# Captcha-Cookie-Gate — beides spricht für einen WAF, der Requests ohne
# plausiblen Absender abweist/verzögert statt sauber abzulehnen). Gleicher
# deklarierter UA wie SEC, aus demselben Grund: ein Produkt, ein Absender.
_ESMA_HEADERS = {"User-Agent": _SEC_USER_AGENT}

_TC_FEED = "https://techcrunch.com/feed/"
_GNEWS_BASE = "https://news.google.com/rss/search"

_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-haiku-4-5-20251001"

# DEALCOMPS-TRANSACTIONS-01 (S78): bewusst SCHMALERER Item-Filter als
# signal_engine.py's _8K_MA_ITEMS ({1.01,2.01,2.02,5.02,8.01} — dort fuer
# allgemeine M&A-SIGNALE gedacht). Hier zaehlt nur, was tatsaechlich einen
# Kaufpreis/eine Transaktion belegt: 1.01 = Entry into Material Definitive
# Agreement (Vertragsabschluss), 2.01 = Completion of Acquisition (Closing).
# 2.02/5.02/8.01 sind Earnings/Exec-Wechsel/Sonstiges — fuer Comps nur Rauschen.
_8K_COMPTX_ITEMS = {"1.01", "2.01"}
_8K_ITEM_RE = re.compile(r"Item\s+(\d\.\d{2})\b", re.IGNORECASE)

COMPTX_DAILY_CAP = 20   # hoeher als DAILY_CAP_PER_POT=2 -- hier keine Company-
                        # Qualitaetsverwaesserung wie bei IPO-Intent/Funding,
                        # mehr Vergleichstransaktionen sind fuer die spaetere
                        # EXIT_ADJUSTMENT-Kalibrierung schlicht mehr Stichprobe.

# DISCOVERY-FUND-VEHICLE-FILTER-01 (S84, im Dry-Run-Log vom 05.07. erneut
# bestätigt: "Octagon Biotech Opportunities Fund II LP", $175M — derselbe
# Fall wie S84, kein Einzelfund). Form D deckt jede private Wertpapier-
# emission ab, auch Fonds-Closings — nicht nur Startup-Runden. Muster laut
# Andreas (S84: " Fund", " LP", " L.P.", "Partners II") + eigene Erweiterung
# auf "Partners <Zahl>" allgemein sowie "SPV" — bewusst wortgrenzenbasiert
# (\b), damit eine echte Portfolio-Company mit "Fund"/"Partners" im Namen
# (z.B. "FundApp Inc") NICHT fälschlich geblockt wird.
_FUND_VEHICLE_NAME_RE = re.compile(
    r"\bfund\b|\bL\.?P\.?\b|\bpartners?\s+(?:i{1,3}|iv|v|vi{1,3})\b|\bSPV\b",
    re.IGNORECASE,
)


def _looks_like_fund_vehicle(name: str) -> bool:
    """True wenn der Name eher nach Investment-Vehikel (Fonds, GP/LP-
    Struktur) klingt als nach Portfolio-Company. S. Modul-Kommentar oben."""
    return bool(_FUND_VEHICLE_NAME_RE.search(name))


# ── Hilfsfunktionen: EDGAR Full-Index ────────────────────────────────────────

def _edgar_index_url(target_date: date) -> str:
    quarter = (target_date.month - 1) // 3 + 1
    return (
        f"{_EDGAR_DAILY_INDEX_BASE}/{target_date.year}/QTR{quarter}/"
        f"form.{target_date.strftime('%Y%m%d')}.idx"
    )


def _parse_form_idx(raw_text: str) -> list[dict]:
    """
    Parst form.idx (Plain-Text, Spalten via 2+ Leerzeichen getrennt — etablierter
    Standard für EDGAR-Idx-Dateien). Spalten gemäß offizieller SEC-Doku
    (Accessing-EDGAR-Data): Form Type, Company Name, CIK, Date Filed, File Name.

    Nicht aus einem Live-File verifiziert (Bot-Detection in der Sandbox, s.
    Moduldocstring) — defensiv: Zeilen, die nicht in genau 5 Felder zerfallen,
    werden geloggt statt stillschweigend verworfen (IMPORT-GUARD-01-Prinzip:
    Format-Drift soll laut auffallen, nicht als leere Liste maskiert werden).
    """
    lines = raw_text.splitlines()
    start_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("---"):
            start_idx = i + 1
            break
    if start_idx is None:
        logger.warning("form.idx: kein Dashes-Separator gefunden — leeres Ergebnis")
        return []

    rows: list[dict] = []
    malformed = 0
    for line in lines[start_idx:]:
        if not line.strip():
            continue
        fields = re.split(r"\s{2,}", line.strip())
        if len(fields) != 5:
            malformed += 1
            continue
        form_type, company_name, cik, date_filed, file_name = fields
        rows.append({
            "form_type": form_type.strip(),
            "company_name": company_name.strip(),
            "cik": cik.strip(),
            "date_filed": date_filed.strip(),
            "file_name": file_name.strip(),
        })

    if malformed:
        logger.warning(
            "form.idx: %d/%d Zeilen nicht in 5 Felder zerlegbar — geprüft werden "
            "nur die restlichen %d (Format-Drift möglich, nicht stillschweigend ignorieren)",
            malformed, len(lines) - start_idx, len(rows),
        )
    return rows


async def _fetch_edgar_daily_index_raw(
    client: httpx.AsyncClient,
    max_lookback_days: int = 5,
) -> list[dict]:
    """
    EDGAR-FORMIDX-TRIPLEFETCH-01 (S84 gefunden, S85 (04.07.) gefixt): holt die
    form.idx-Datei des letzten Handelstags EINMAL pro Cron-Lauf (mit Rückwärts-
    Fallback für Wochenenden/Feiertage, max_lookback_days Versuche) und gibt
    ALLE Zeilen ungefiltert zurück. Vorher rief jeder der drei Töpfe (S-1/S-11,
    D/D-A, 8-K) eine eigene Fetch+Parse-Funktion auf — 3x dieselbe Datei vom
    Netz, plus je 1x der vorab abgelehnte 403-Versuch auf den noch nicht
    veröffentlichten Vortages-Index macht 6 HTTP-Requests für eine einzige
    Datei pro Lauf. Filterung passiert jetzt separat in _filter_edgar_rows()
    (reine In-Memory-Funktion, kein weiterer Netzwerk-Call).
    """
    today = date.today()
    for delta in range(1, max_lookback_days + 1):
        target = today - timedelta(days=delta)
        url = _edgar_index_url(target)
        try:
            resp = await client.get(url, headers=_SEC_HEADERS, timeout=15.0)
            await asyncio.sleep(_SEC_MIN_DELAY_S)
            if resp.status_code == 404:
                continue   # kein Handelstag — nächsten Tag zurück versuchen
            if resp.status_code != 200:
                logger.warning("EDGAR full-index HTTP %s für %s", resp.status_code, url)
                continue
            rows = _parse_form_idx(resp.text)
            logger.info(
                "EDGAR full-index %s: %d Zeilen gesamt (ungefiltert, einmaliger Fetch)",
                target.isoformat(), len(rows),
            )
            return rows
        except Exception as e:
            logger.warning("EDGAR full-index Fetch fehlgeschlagen für %s: %s", url, e)
    logger.warning("EDGAR full-index: kein Treffer in %d Tagen Rückwärtssuche", max_lookback_days)
    return []


def _filter_edgar_rows(rows: list[dict], forms: set[str]) -> list[dict]:
    """Reine In-Memory-Filterung der einmalig geholten form.idx-Zeilen — kein Netzwerk-Call."""
    filtered = [r for r in rows if r["form_type"] in forms]
    logger.info("EDGAR full-index Form-Filter %s: %d von %d Zeilen", sorted(forms), len(filtered), len(rows))
    return filtered


_ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")


def _extract_accession_from_file_name(file_name: str) -> str | None:
    """form.idx 'File Name'-Spalte, z.B. 'edgar/data/1750153/0001750153-26-000123.txt'."""
    m = _ACCESSION_RE.search(file_name)
    return m.group(1) if m else None


async def _fetch_form_d_amount(
    client: httpx.AsyncClient,
    cik: str,
    file_name: str,
    company_name: str,
) -> float | None:
    """
    FORMD-DIRECT-XML-01 (S78): ersetzt den Namens-Search gegen efts.sec.gov.
    DIAGNOSE (S77-Dry-Run, 248/248 Filer ohne Treffer): das offizielle
    _source-Schema von efts.sec.gov (SEC EDGAR Full-Text-Search) liefert nur
    file_date/period_of_report/form_type/entity_name/file_num/film_num/
    file_description — total_offering_amount ist dort KEIN indexiertes Feld,
    sondern strukturierte Form-D-XML-Information (offeringData.offering
    SalesAmounts.totalOfferingAmount laut SEC EDGAR Form-D-XML-Tech-Spec).
    Identisches Muster wie der source_type-Fund aus TR-RELATIONAL-CALIBRATION-01
    (S77): kein Kalibrierungsproblem, eine nicht existierende Datenquelle.

    Holt stattdessen direkt die primary_doc.xml der eigenen Filing — CIK +
    Accession-Number stammen schon aus der form.idx-Zeile (kein Name-Match-
    Risiko mehr, kein zweiter Index-Scan). Konvention: .../edgar/data/{cik}/
    {accession_ohne_bindestriche}/primary_doc.xml. Nicht gegen ein Live-File
    verifiziert (gleiche Sandbox-Bot-Detection-Einschränkung wie form.idx
    selbst, s. Moduldocstring) — Namespace-agnostisches Parsing (Tag-Suffix
    statt vollem Tag-Namen) fängt Spec-Versions-Drift ab, 404/Parse-Fehler
    werden geloggt statt stillschweigend verschluckt (IMPORT-GUARD-01).
    """
    accession = _extract_accession_from_file_name(file_name)
    if not accession:
        logger.warning(
            "Form D: Accession-Number nicht aus file_name extrahierbar — '%s' (%s)",
            file_name, company_name,
        )
        return None
    cik_clean = cik.lstrip("0") or "0"
    url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/"
        f"{accession.replace('-', '')}/primary_doc.xml"
    )
    try:
        resp = await client.get(url, headers=_SEC_HEADERS, timeout=12.0)
        await asyncio.sleep(_SEC_MIN_DELAY_S)
        if resp.status_code != 200:
            logger.debug(
                "Form D primary_doc.xml HTTP %s — %s (%s)",
                resp.status_code, company_name, url,
            )
            return None
        root = ET.fromstring(resp.text)
        element_found = False
        for el in root.iter():
            tag = el.tag.split("}")[-1]   # Namespace-Präfix abstreifen
            if tag == "totalOfferingAmount" and el.text:
                element_found = True
                try:
                    return round(float(el.text.strip()) / 1_000_000, 2)
                except (ValueError, TypeError):
                    # Form-D-Spec erlaubt den Sentinel "Indefinite" statt einer
                    # Zahl (unbegrenzte Offering-Größe) — kein Parse-Bug.
                    logger.debug(
                        "Form D: totalOfferingAmount nicht-numerisch ('%s', vermutlich "
                        "'Indefinite'-Sentinel) — %s (%s)",
                        el.text.strip(), company_name, url,
                    )
                    continue
        if not element_found:
            logger.debug(
                "Form D: kein totalOfferingAmount-Element in primary_doc.xml — %s (%s)",
                company_name, url,
            )
    except Exception as e:
        logger.debug(
            "Form D primary_doc.xml Fetch fehlgeschlagen für %s: %s — %r",
            company_name, type(e).__name__, e,
        )
    return None


# ── ESMA PRIII ────────────────────────────────────────────────────────────────

# ESMA-SCHEMA-01 (S84): Feldnamen jetzt gegen die offizielle A2A-Doku
# (registers.esma.europa.eu/publication/helpApp, Abschnitt 11 "Prospectus III")
# verifiziert statt geraten. Zwei Befunde:
#   1. approval_filing_date ist der bestätigte Datumsfeld-Name auf den
#      Parent-Docs (document_type-Einträgen) — an erster Stelle.
#   2. issuer_name/issuer_lei/issuer_residency sitzen NICHT auf dem Parent-
#      Doc, sondern auf separaten Child-Records (party_type="ISSR") — der
#      Core ist Parent/Child (Block-Join), kein flaches Schema. issuer_name
#      bleibt trotzdem als Kandidat stehen (ESMA-SCHEMA-02, S84, offen): falls
#      der Parent-Response ihn NICHT liefert, bestätigt das die Child-Doc-
#      Hypothese und _log_unknown_schema_once zeigt die echten Parent-Felder
#      für den nächsten Schritt (Block-Join-Query oder [child]-Transformer,
#      bewusst noch nicht gebaut — erst am echten Response-Schema verifizieren).
#   Alte geratene Kandidaten bleiben als Fallback, falls die Doku veraltet ist.
_ESMA_ISSUER_NAME_KEYS = (
    "issuer_name", "pd_issuerName", "issuerName", "pd_issuer_name", "name",
)
_ESMA_DATE_KEYS = ("approval_filing_date", "pd_approvalDate", "approvalDate", "approval_date", "pd_publicationDate")
_ESMA_LOGGED_UNKNOWN_SCHEMA = False   # nur einmal pro Prozess loggen, nicht pro Kandidat

# ESMA-SCHEMA-02 (S89): Roh-Keys der ersten Antwort (S84) enthielten kein
# issuer_name/issuerName, dafür party_name — plausibelster Kandidat für den
# Emittenten-Namen, aber NICHT verifiziert: könnte laut Doku (ESMA-HEADER-01/
# ESMA-SCHEMA-01, Child-Records party_type=ISSR/OFFR/GNTR) auch Underwriter/
# Zahlstelle sein, oder als Solr-Multivalue-Feld mehrere Parteien (Issuer+
# Offeror+Guarantor) undifferenziert zusammenfassen. Bisher wurde nur der
# FeldNAME geloggt, nie der tatsächliche Feldwert — Diagnose ohne Daten war
# Rätselraten. Diese Liste sind die plausibelsten Kandidaten für den nächsten
# Dry-Run, keine Behauptung, dass einer davon bereits die Lösung ist.
_ESMA_DIAGNOSTIC_VALUE_FIELDS = (
    "party_name", "prospectus_id", "document_type_descr",
    "home_member_state_descr", "national_document_id",
)


def _extract_esma_field(doc: dict, candidates: tuple[str, ...]) -> str | None:
    for key in candidates:
        if doc.get(key):
            return str(doc[key])
    return None


def _log_unknown_schema_once(doc: dict) -> None:
    global _ESMA_LOGGED_UNKNOWN_SCHEMA
    if _ESMA_LOGGED_UNKNOWN_SCHEMA:
        return
    _ESMA_LOGGED_UNKNOWN_SCHEMA = True
    # ESMA-SCHEMA-02: Wert UND Typ mitloggen (nicht nur Vorhandensein) — ein
    # str vs. list-Typ bei party_name entscheidet, ob ein einfacher Key-
    # Zusatz reicht oder ob eine Rollen-Disambiguierung (Issuer vs. Offeror/
    # Guarantor) nötig ist, bevor der Name blind übernommen werden darf.
    sample_values = {
        key: (doc.get(key), type(doc.get(key)).__name__)
        for key in _ESMA_DIAGNOSTIC_VALUE_FIELDS
    }
    logger.warning(
        "ESMA PRIII: keiner der bekannten Issuer-Name-Keys %s im Dokument gefunden. "
        "Roh-Keys der ersten Antwort (für Feld-Abgleich, einmalig geloggt): %s. "
        "Werte der plausibelsten Kandidaten, Format (Wert, Python-Typ) — "
        "ESMA-SCHEMA-02, noch nicht verifiziert ob Issuer oder andere Rolle: %s",
        _ESMA_ISSUER_NAME_KEYS, sorted(doc.keys()), sample_values,
    )


async def _fetch_esma_priii_documents(
    client: httpx.AsyncClient,
    lookback_days: int = 3,
    rows: int = 50,
) -> list[dict]:
    """
    Holt aktuelle PRIII-Dokumente (Prospekt-Genehmigungen, pan-EWR). Kein API-
    Key, öffentlicher Solr-Select-Endpoint. Datumsfilter weiterhin clientseitig
    statt via `fq` — approval_filing_date ist jetzt doku-bestätigt (s.o.), aber
    das exakte Datumsformat/-verhalten im `fq`-Range-Syntax ist ungetestet;
    client-seitig bleibt robuster gegen einen falsch formatierten fq, der sonst
    still 0 Treffer liefern würde statt einen Fehler zu zeigen.

    ESMA-SCHEMA-01 (S84): Core ist Parent/Child (Block-Join), kein flaches
    Schema — Prospekt-Dokumente (document_type: URGN/REGN/SECN/SMRY/BPFT/
    BPWO/STDA) sind Parent-Docs, Issuer/Offeror/Guarantor sind separate
    Child-Records (party_type: ISSR/OFFR/GNTR). q=*:* ohne Parent-Filter
    durchsuchte bisher den kompletten verschachtelten Index ungefiltert —
    korrigiert auf {!parent which="type_s:parent"} + fq=document_type:(...),
    exakt wie in der offiziellen A2A-Doku (Abschnitt 11) vorgegeben.

    ESMA-HEADER-01 (S84): Request sendete bisher keinen User-Agent — jetzt
    gleicher deklarierter UA wie SEC (s. _ESMA_HEADERS).

    ESMA-TIMEOUT-DIAG-01 (04.07.): trotz bestätigtem S84-Fix-Deploy (03.07.,
    13:06 UTC — vor diesem Lauf) weiterhin ReadTimeout, exakt bei ~15s
    (altes Timeout) nach dem EDGAR-Call. Deploy-Timing scheidet damit als
    Erklärung aus. Ein Timeout (kein sofortiger 4xx) spricht eher für einen
    echten langsamen/hängenden Request als für einen harten WAF-Reject —
    Konstruktion der Solr-Query stimmt mit dem offiziell dokumentierten
    Block-Join-Muster anderer ESMA-Register überein (upreg, saris_new,
    mifid_shsexs nutzen dieselbe {!parent which="type_s:parent"}-Syntax),
    ist also keine erfundene Syntax. NICHT verifizierbar von hier aus: ob
    genau DIESER Core (esma_registers_priii_documents) einen ungebundenen
    Block-Join (kein Such-Kriterium nach dem Parent-Operator, nur ein
    fq-Filter) teuer macht, oder ob es sich um ein IP-/Traffic-Muster-
    basiertes Rate-Limiting auf Render-Infrastruktur handelt.
    Zwei rein diagnostische, risikoarme Änderungen (KEINE behauptete
    Lösung): (1) voller Request wird jetzt VOR dem Call geloggt — bisher
    nur der Fehler danach, nie der tatsächlich gesendete Request selbst;
    (2) Timeout testweise auf 45s angehoben, um zu unterscheiden ob der
    Request bei 45s durchgeht (→ echt langsam, Query-Optimierung nötig)
    oder weiterhin hängt (→ Netzwerk-/WAF-Block, andere Lösung nötig).
    """
    params = {
        "q": '{!parent which="type_s:parent"}',
        "fq": 'document_type:("URGN" or "REGN" or "SECN" or "SMRY" '
              'or "BPFT" or "BPWO" or "STDA")',
        "rows": rows,
        "wt": "json",
        "sort": "approval_filing_date desc",
        # ESMA-FL-PARAM-01 (S83) bleibt in Kraft: ohne fl=* liefert
        # Solr nur den Request-Handler-Default-Feldsatz.
        "fl": "*",
    }
    logger.info("ESMA PRIII Request: GET %s params=%s", _ESMA_PRIII_SELECT, params)
    candidates: list[dict] = []
    _t0 = asyncio.get_event_loop().time()
    try:
        resp = await client.get(
            _ESMA_PRIII_SELECT,
            headers=_ESMA_HEADERS,
            params=params,
            timeout=45.0,
        )
        if resp.status_code != 200:
            logger.warning("ESMA PRIII HTTP %s", resp.status_code)
            return []
        data = resp.json()
        docs = data.get("response", {}).get("docs", [])
        logger.info("ESMA PRIII: %d Dokumente in Rohantwort", len(docs))
        for doc in docs:
            name = _extract_esma_field(doc, _ESMA_ISSUER_NAME_KEYS)
            if not name:
                _log_unknown_schema_once(doc)
                continue
            date_str = _extract_esma_field(doc, _ESMA_DATE_KEYS)
            candidates.append({
                "company_name": name,
                "date_filed": (date_str or "")[:10],
                "raw": doc,
            })
    except Exception as e:
        # ESMA-EXC-TYPE-LOG-01: str(e) ist bei manchen Exceptions (u.a.
        # asyncio.TimeoutError) leer — ohne Typ+repr nicht diagnostizierbar
        # (S77-Dry-Run-Log zeigte exakt das: "Fetch fehlgeschlagen: " ohne Inhalt).
        # ESMA-TIMEOUT-DIAG-01: Elapsed-Time zeigt, ob der 45s-Timeout auch
        # greift (→ hängt weiterhin, wahrscheinlich Netzwerk-/WAF-Block) oder
        # ob der Request irgendwo dazwischen durchgeht/schneller fehlschlägt.
        elapsed = asyncio.get_event_loop().time() - _t0
        logger.warning(
            "ESMA PRIII Fetch fehlgeschlagen nach %.1fs: %s — %r",
            elapsed, type(e).__name__, e,
        )
    return candidates


# ── Kuratierte EU-News ────────────────────────────────────────────────────────

async def _fetch_curated_eu_news(client: httpx.AsyncClient, lookback_days: int = 3) -> list[dict]:
    """
    Sektor-geschlüsselte Google-News-Queries (DE+EN, je Konviktion-Sektor) +
    bestehender TechCrunch-Globalfeed — gefiltert auf Domain-Whitelist. Anders
    als parse_google_news()/parse_techcrunch() in signal_engine.py (company-
    geschlüsselt, kein NER nötig) braucht dieser Pfad eine echte Entitäts-
    Extraktion aus Freitext — genau die Lücke, die laut MODUL06 (S71) in der
    bestehenden Signal-Engine fehlt. Claude-Haiku-Extraktion s. _extract_company_from_news.
    """
    cutoff = date.today() - timedelta(days=lookback_days)
    raw_items: list[dict] = []

    # (1) Sektor-geschlüsselte Google-News-Queries
    for sector in CONVICTION_SECTORS:
        queries = [
            f"{sector} Finanzierungsrunde OR Millionen Euro",
            f"{sector} funding round OR raises million",
        ]
        for q in queries:
            try:
                resp = await client.get(
                    _GNEWS_BASE, params={"q": q, "hl": "de", "gl": "DE", "ceid": "DE:de"}, timeout=8.0,
                )
                if resp.status_code != 200:
                    continue
                from xml.etree import ElementTree as ET
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item")[:6]:
                    raw_items.append({
                        "title": (item.findtext("title") or "").strip(),
                        "description": (item.findtext("description") or "").strip(),
                        "link": (item.findtext("link") or "").strip(),
                        "pub_date": item.findtext("pubDate"),
                        "sector_hint": sector,
                    })
            except Exception as e:
                logger.debug("EU-News Google-Query fehlgeschlagen (%s): %s", sector, e)
            await asyncio.sleep(0.3)

    # (2) TechCrunch-Globalfeed (eigener Fetch — eigener Cron-Slot, kein
    # In-Memory-Cache aus run_signal_engine() verfügbar)
    try:
        resp = await client.get(_TC_FEED, timeout=8.0)
        if resp.status_code == 200:
            from xml.etree import ElementTree as ET
            root = ET.fromstring(resp.text)
            for item in root.findall(".//item"):
                raw_items.append({
                    "title": (item.findtext("title") or "").strip(),
                    "description": (item.findtext("description") or "").strip(),
                    "link": (item.findtext("link") or "").strip(),
                    "pub_date": item.findtext("pubDate"),
                    "sector_hint": None,
                })
    except Exception as e:
        logger.debug("EU-News TechCrunch-Fetch fehlgeschlagen: %s", e)

    # Domain-Whitelist + Lookback-Filter
    from src.services.signal_engine import _extract_domain, _parse_rss_date
    filtered: list[dict] = []
    for it in raw_items:
        domain = _extract_domain(it["link"]) or ""
        in_whitelist = any(domain == d or domain.endswith("." + d) for d in DOMAIN_WHITELIST)
        if not in_whitelist:
            continue
        ev_date = _parse_rss_date(it["pub_date"])
        if ev_date < cutoff:
            continue
        it["event_date"] = ev_date
        it["domain"] = domain
        filtered.append(it)

    logger.info("EU-News: %d Roh-Items, %d nach Domain-Whitelist+Lookback", len(raw_items), len(filtered))
    return filtered


# ── Claude Haiku: Sektor-Gate + Entitäts-Extraktion (News-Pfad) ──────────────

def _claude_headers() -> dict | None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    return {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}


async def _classify_sector(client: httpx.AsyncClient, name: str, context: str) -> str | None:
    """Sektor-Gate — gegen die 5 Konviktion-Sektoren, sonst None (kein Fit)."""
    headers = _claude_headers()
    if not headers:
        logger.warning("Sektor-Gate: ANTHROPIC_API_KEY fehlt — Kandidat '%s' ohne Gate-Klassifikation, wird NICHT verworfen (Fail-Open wäre hier riskanter als geloggter Fail-Closed) — siehe Rückgabewert None", name)
        return None
    sectors = ", ".join(CONVICTION_SECTORS)
    prompt = (
        f"Firma: {name}\nKontext: {context[:300]}\n\n"
        f"Passt diese Firma in genau einen dieser Sektoren: {sectors}? "
        f"Antworte NUR mit einem JSON-Objekt: {{\"sector\": \"<einer der 5 Namen>\"}} "
        f"oder {{\"sector\": null}} falls kein Fit. Kein Fließtext."
    )
    try:
        resp = await client.post(
            _CLAUDE_API_URL, headers=headers,
            json={"model": _CLAUDE_MODEL, "max_tokens": 64, "messages": [{"role": "user", "content": prompt}]},
            timeout=15.0,
        )
        if resp.status_code != 200:
            logger.warning("Sektor-Gate Claude HTTP %s für '%s'", resp.status_code, name)
            return None
        import json as _json
        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = _json.loads(raw)
        sector = parsed.get("sector")
        return sector if sector in CONVICTION_SECTORS else None
    except Exception as e:
        logger.warning("Sektor-Gate fehlgeschlagen für '%s': %s", name, e)
        return None


async def _extract_company_from_news(client: httpx.AsyncClient, title: str, description: str) -> dict | None:
    """
    NEU (anders als alles in signal_engine.py): extrahiert eine Firma + Fund-
    ing-Signal aus Freitext, statt einen bereits bekannten Namen nur zu klassi-
    fizieren. Genau die Lücke, die laut MODUL06 (S71) in run_signal_engine()/
    _claude_ner_pass() fehlt ("klassifiziert nur, extrahiert keine neuen Entitäten").
    """
    headers = _claude_headers()
    if not headers:
        return None
    prompt = (
        f"Titel: {title}\nBeschreibung: {description[:300]}\n\n"
        f"Ist das eine Meldung über eine Finanzierungsrunde/Funding-Round einer "
        f"einzelnen, konkret benannten Firma? Falls ja, antworte NUR mit JSON: "
        f"{{\"is_funding\": true, \"company_name\": \"<Firmenname>\"}}. "
        f"Falls nein oder keine klar benannte Firma: {{\"is_funding\": false}}."
    )
    try:
        resp = await client.post(
            _CLAUDE_API_URL, headers=headers,
            json={"model": _CLAUDE_MODEL, "max_tokens": 100, "messages": [{"role": "user", "content": prompt}]},
            timeout=15.0,
        )
        if resp.status_code != 200:
            return None
        import json as _json
        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = _json.loads(raw)
        if not parsed.get("is_funding") or not parsed.get("company_name"):
            return None
        return {"company_name": parsed["company_name"]}
    except Exception as e:
        logger.debug("Entitäts-Extraktion fehlgeschlagen für '%s': %s", title[:60], e)
        return None


# ── DEALCOMPS-TRANSACTIONS-01: 8-K Volltext + Deal-Extraktion ───────────────

async def _fetch_filing_text(client: httpx.AsyncClient, file_name: str) -> str | None:
    """
    Holt die komplette Submission als Rohtext. SEC bestaetigt offiziell
    (sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data):
    form.idx 'File Name' zeigt direkt auf die ".txt"-Datei mit der "raw text
    version of the complete disseminated filing content" — keine zweite
    Index-Auflösung noetig, gleicher Pfad wird schon fuer S-1/Form-D-Zeilen
    aus form.idx gelesen. Enthaelt SGML/HTML-Markup, ungestrippt — Stripping
    passiert separat in _strip_filing_markup(), getrennt von der Item-Erkennung
    (die braucht den Rohtext, "Item 1.01" steht oft in einer Tag-Struktur).
    """
    url = f"https://www.sec.gov/Archives/{file_name}"
    try:
        resp = await client.get(url, headers=_SEC_HEADERS, timeout=15.0)
        await asyncio.sleep(_SEC_MIN_DELAY_S)
        if resp.status_code != 200:
            logger.debug("Filing-Text HTTP %s — %s", resp.status_code, url)
            return None
        return resp.text
    except Exception as e:
        logger.debug("Filing-Text Fetch fehlgeschlagen für %s: %s — %r", url, type(e).__name__, e)
        return None


def _8k_item_numbers(filing_text: str) -> set[str]:
    """
    Item-Nummern per Regex aus dem 8-K-Rohtext — bewusst KEIN efts.sec.gov-
    Call fuer das 'items'-Feld, obwohl laut SEC-Webmaster-FAQ offiziell
    real (anders als total_offering_amount, s. FORMD-DIRECT-XML-01). Grund:
    Filing-Text wird fuer die Deal-Extraktion sowieso gebraucht — ein
    zweiter API-Call nur fuers Item-Vorfiltern waere eine zusaetzliche
    Abhaengigkeit ohne Nutzen. Items stehen immer im Kopfbereich vor den
    Exhibits, 20k Zeichen Suchfenster reicht mit Marge.
    """
    return set(_8K_ITEM_RE.findall(filing_text[:20000]))


_TAG_STRIP_RE = re.compile(r"<[^>]+>")
_ENTITY_STRIP_RE = re.compile(r"&[a-zA-Z]+;")


def _strip_filing_markup(filing_text: str, max_chars: int = 4000) -> str:
    """
    Grobe SGML/HTML-Bereinigung fuer den Haiku-Kontext — kein vollstaendiger
    Parser noetig (gleiche "minimum viable"-Linie wie an anderen Stellen
    des Moduls). Deal-Beschreibung + Item-Ueberschriften stehen vor den
    Exhibits, daher reicht ein Fenster aus dem oberen Teil des Dokuments.
    """
    body = filing_text[:20000]
    body = _TAG_STRIP_RE.sub(" ", body)
    body = _ENTITY_STRIP_RE.sub(" ", body)
    body = re.sub(r"\s+", " ", body).strip()
    return body[:max_chars]


async def _extract_deal_details(
    client: httpx.AsyncClient,
    filing_text_clean: str,
    items: set[str],
    filing_date: str,
) -> dict | None:
    """
    Extrahiert Käufer/Ziel/Preis/Sektor/Funding-Snapshot aus einem bereits
    auf Items 1.01/2.01 vorgefilterten 8-K-Filing-Text. Industrie- und
    Stage-Werte werden gegen dieselbe Taxonomie wie valuation.py validiert
    (VERTICAL_DELTA/STAGE_MULT) — kein drittes Vokabular, sonst passt die
    spaetere EXIT_ADJUSTMENT-Kalibrierung nicht zu den Buckets aus
    compute_target_valuation().
    """
    headers = _claude_headers()
    if not headers:
        return None
    vertical_keys = ", ".join(VERTICAL_DELTA.keys())
    stage_keys = ", ".join(STAGE_MULT.keys())
    prompt = (
        f"SEC-8-K-Filing-Auszug (Items: {', '.join(sorted(items))}, Datum {filing_date}):\n"
        f"{filing_text_clean}\n\n"
        f"Das ist ein Filing zu einer Unternehmensübernahme. Extrahiere NUR was "
        f"explizit im Text steht — nichts erraten oder schätzen. Antworte NUR mit "
        f"einem JSON-Objekt mit genau diesen Feldern:\n"
        f'{{"acquirer_name": "<Name oder null>", "target_name": "<Name oder null>", '
        f'"deal_price_usd_mn": <Zahl oder null>, "deal_date": "<YYYY-MM-DD oder null>", '
        f'"industry": "<genau einer von [{vertical_keys}] oder null>", '
        f'"target_funding_total_usd_mn_at_sale": <Zahl oder null — NUR falls der Text '
        f'die bisherige Funding-Historie des Targets explizit nennt>, '
        f'"target_funding_stage_at_sale": "<genau einer von [{stage_keys}] oder null>"}}\n'
        f'Falls kein klar benanntes Target oder kein erkennbarer Kaufpreis im Text steht: '
        f'{{"acquirer_name": null}}.'
    )
    try:
        resp = await client.post(
            _CLAUDE_API_URL, headers=headers,
            json={"model": _CLAUDE_MODEL, "max_tokens": 300, "messages": [{"role": "user", "content": prompt}]},
            timeout=20.0,
        )
        if resp.status_code != 200:
            # COMPTX-EXTRACTION-DIAGNOSTIC-01 (04.07.): war debug — ein Claude-
            # API-Fehler ist keine Routine-Ablehnung wie "kein klarer Deal im
            # Text", sondern ein Infrastrukturproblem. Gleiches Prinzip wie
            # ESMA-EXC-TYPE-LOG-01: technische Fehler laut, fachliche Non-
            # Matches leise.
            logger.warning("Deal-Extraktion Claude HTTP %s", resp.status_code)
            return None
        import json as _json
        raw = resp.json()["content"][0]["text"].strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = _json.loads(raw)
        if not parsed.get("acquirer_name") or not parsed.get("target_name"):
            # Legitimer, erwarteter Fall (Claude fand keinen klaren Deal) —
            # bewusst debug, keine technische Störung.
            logger.debug("Deal-Extraktion: kein klarer Deal erkannt (kein acquirer/target im Filing-Text)")
            return None
        # Taxonomie-Validierung: Haiku haelt sich i.d.R. an die Vorgabe, aber
        # nicht garantiert — ein ungueltiger Wert wird zu None statt einem
        # stillen Fehlwert in einer Spalte, die spaeter als harter Bucket-Key
        # dient (EXIT_ADJUSTMENT[industry][stage]).
        if parsed.get("industry") not in VERTICAL_DELTA:
            parsed["industry"] = None
        if parsed.get("target_funding_stage_at_sale") not in STAGE_MULT:
            parsed["target_funding_stage_at_sale"] = None
        return parsed
    except Exception as e:
        # COMPTX-EXTRACTION-DIAGNOSTIC-01 (04.07.): war debug ohne Typ/Repr —
        # identisches Muster zum ESMA-EXC-TYPE-LOG-01-Fund (S77): str(e) ist
        # bei manchen Exceptions leer, ohne Typ+repr nicht diagnostizierbar.
        # 17/17 comp_tx-Extraktions-Rejects am 04.07. liefen alle unsichtbar
        # durch genau diesen Pfad oder den obigen Non-Match-Pfad — bisher
        # nicht unterscheidbar, welcher der beiden dominiert.
        logger.warning("Deal-Extraktion fehlgeschlagen: %s — %r", type(e).__name__, e)
        return None


async def _lookup_company_by_name(name: str) -> dict | None:
    """
    Duenne Wrapper-Funktion statt direktem Import im Orchestrator — macht
    die Abhaengigkeit an einer Stelle sichtbar und faengt einen ggf.
    fehlenden Import sauber ab statt mit ImportError im Cron zu crashen.
    fetch_company_by_name() existiert bereits in supabase.py (S78 geprueft) —
    ABER: nutzt .ilike() ohne Wildcards = exaktes Case-Insensitive-Match,
    keine Rechtsform-Normalisierung wie _normalize_name() hier im Modul.
    Reale Praezisions-Luecke: "BiomX" (Haiku-Extraktion) matcht NICHT
    "BiomX, Inc." (DB-Eintrag) — bewusst nicht an der gemeinsam genutzten
    Funktion geaendert (andere Caller koennten vom exakten Verhalten
    abhaengen), hier nur dokumentiert statt verschwiegen.
    """
    try:
        from src.integrations.supabase import fetch_company_by_name
        return fetch_company_by_name(name)
    except ImportError:
        logger.warning(
            "fetch_company_by_name fehlt in supabase.py — Snapshot-Capture "
            "für '%s' übersprungen, target_company_id bleibt leer", name,
        )
        return None


async def _insert_comparable_transaction(row: dict) -> None:
    """
    Gleiche Wrapper-Logik wie _lookup_company_by_name. insert_comparable_
    transaction() wurde S78 in supabase.py ergänzt (Upsert-Key: (source,
    source_url), s. schema_patch_comparable_transactions.sql) — try/except
    bleibt als Sicherheitsnetz, falls Backend- und Frontend-Push je auseinanderlaufen.
    """
    try:
        from src.integrations.supabase import insert_comparable_transaction
        insert_comparable_transaction(row)
    except ImportError:
        logger.warning(
            "insert_comparable_transaction fehlt in supabase.py — "
            "Transaktion '%s' nicht geschrieben (lieber laut scheitern als "
            "stiller Datenverlust)", row.get("target_name"),
        )




# ── Identitäts-Gate (1:1 Reuse aus PEER-IDENT-01) ────────────────────────────

def _autopick(result) -> "object | None":
    """
    Wortgleiche Logik zu peers.py::_autopick_peer — bewusst hier dupliziert
    statt aus src.routes.peers importiert: services importieren nicht aus
    routes (Layering), und die Funktion ist 4 Zeilen lang. Kein Modal, kein
    Listed-Zwang (anders als bei Buyern) — Discovery-Kandidaten sind über-
    wiegend pre-IPO/privat, ein Hard-Drop bei fehlendem Wikidata-Hit wäre
    verlustreich.
    """
    resolved = getattr(result, "resolved", None)
    if resolved is not None:
        return resolved
    candidates = getattr(result, "candidates", None) or []
    return candidates[0] if candidates else None


async def _identity_gate(client: httpx.AsyncClient, name: str) -> tuple[str, dict]:
    """
    Gibt (identity_confidence, resolved_fields) zurück. resolved_fields ist
    leer falls nichts aufgelöst werden konnte — identity_confidence ist dann
    'unverified', die Company wird TROTZDEM angelegt (kein Drop), exakt wie
    in peers.py::_resolve_or_create_peer (PEER-IDENT-01).
    """
    from src.services.wikidata_resolver import resolve_entity
    from src.services.market_data_enrichment import check_entity_existence

    resolved_fields: dict = {}
    identity_confidence = "unverified"

    try:
        wd_result = await resolve_entity(name, client=client)
        picked = _autopick(wd_result)
        if picked is not None:
            resolved_fields["name"] = getattr(picked, "display_name", None) or getattr(picked, "name", None) or name
            resolved_fields["ticker"] = getattr(picked, "ticker", None)
            resolved_fields["exchange"] = getattr(picked, "display_exchange", None)
            resolved_fields["headquarters"] = getattr(picked, "headquarters", None)
            identity_confidence = "verified_wikidata"
    except Exception as e:
        logger.debug("Identitäts-Gate Wikidata-Resolve failed für '%s': %s", name, e)

    if identity_confidence == "unverified":
        try:
            found, source = await check_entity_existence(name)
            if found:
                identity_confidence = source
        except Exception as e:
            logger.debug("Identitäts-Gate DDG-Check failed für '%s': %s", name, e)

    return identity_confidence, resolved_fields


# ── DB-Dedupe ─────────────────────────────────────────────────────────────────

def _is_known(name: str, known_normalized: set[str]) -> bool:
    norm = _normalize_name(name)
    if norm in known_normalized:
        return True
    return any(norm in k or k in norm for k in known_normalized if norm and k)


# ── Write-Pfad (DB) ───────────────────────────────────────────────────────────

def _insert_discovery_company(db, name: str, discovery_source: str, identity_confidence: str, resolved_fields: dict) -> str | None:
    """
    Insert mit Enum-Retry-Fallback — exakt das in peers.py::_resolve_or_create_peer
    etablierte Muster (S66/BUYER-IDENT-02-Trap: company_source-Enum braucht u.U.
    eine Migration für den neuen Wert; bis die läuft, fällt der Insert auf
    'manual' zurück statt zu crashen — LAUT geloggt, nicht stillschweigend).
    """
    payload = {
        "name": resolved_fields.get("name") or name,
        "discovery_source": discovery_source,
        "identity_confidence": identity_confidence,
    }
    if resolved_fields.get("ticker"):
        payload["ticker"] = resolved_fields["ticker"]
    if resolved_fields.get("exchange"):
        payload["exchange"] = resolved_fields["exchange"]
    if resolved_fields.get("headquarters"):
        payload["headquarters"] = resolved_fields["headquarters"]

    for source_val in ("discovery_engine", "manual"):
        try:
            payload["source"] = source_val
            result = db.table("companies").insert(payload).execute()
            if result.data:
                new_id = result.data[0]["id"]
                logger.info(
                    "Discovery: '%s' angelegt → %s (discovery_source=%s, source=%s, identity_confidence=%s)",
                    payload["name"], new_id, discovery_source, source_val, identity_confidence,
                )
                return new_id
            break
        except Exception as e:
            if source_val == "discovery_engine" and "22P02" in str(e):
                logger.warning(
                    "Enum-Wert 'discovery_engine' auf company_source fehlt (Migration "
                    "schema_patch_discovery_source_enum.sql noch nicht gelaufen?) — "
                    "Fallback auf 'manual' für '%s'", name,
                )
                continue
            logger.warning("Discovery: Insert fehlgeschlagen für '%s': %s", name, e)
            break
    return None


async def _backfill_acquired_company(db, client: httpx.AsyncClient, deal: dict) -> str | None:
    """
    DEALCOMPS-BACKFILL-01 (S78): legt ein akquiriertes Target nachträglich
    als Company an, wenn es nicht schon bei uns getrackt ist (Andreas, S78:
    "jüngst akquirierte Unternehmen ... nachträglich schreiben"). Nutzt
    lifecycle_status='acquired' — Enum-Wert existiert bereits (DISAMBIG-03,
    company_detail.py Zeile 257), kein neues Vokabular.

    Bewusst OHNE Beitrag zur EXIT_ADJUSTMENT-Kalibrierung: zum Backfill-
    Zeitpunkt ist die Company schon akquiriert, ihr "aktueller" Funding-
    Stand sagt nichts mehr über den Zustand vor dem Verkauf (s. Snapshot-
    Diskussion S78) — komplett unabhängig vom *_at_sale-Snapshot der
    Transaktion. Wert liegt in Sektor-/Peer-/Supply-Chain-Vollständigkeit,
    nicht im Multiple-Pfad.
    """
    identity_confidence, resolved = await _identity_gate(client, deal["target_name"])
    new_id = _insert_discovery_company(
        db, deal["target_name"], "dealcomps_backfill", identity_confidence, resolved,
    )
    if new_id:
        from src.integrations.supabase import upsert_company_enrichment
        upsert_company_enrichment(new_id, {
            "lifecycle_status": "acquired",
            "industry": deal.get("industry"),
        })
        logger.info(
            "DEALCOMPS-BACKFILL-01: '%s' nachträglich angelegt → %s "
            "(lifecycle_status=acquired, industry=%s)",
            deal["target_name"], new_id, deal.get("industry") or "—",
        )
    return new_id


# ── Orchestrator ──────────────────────────────────────────────────────────────

async def run_discovery_pipeline() -> dict:
    """
    Haupteinstieg, aufgerufen von main.py::_cron_discovery() um 03:45 UTC.
    Gibt eine Zusammenfassung zurück (für Logging/Monitoring).
    """
    from src.integrations.supabase import get_supabase, fetch_companies, upsert_signals

    db = get_supabase()
    existing = fetch_companies(limit=500)
    known_normalized = {_normalize_name(c.get("name", "")) for c in existing if c.get("name")}

    stats = {
        "dry_run": DRY_RUN,
        "ipo_intent_written": 0, "funding_written": 0,
        "ipo_intent_seen": 0, "funding_seen": 0,
        # STATS-ATTRIBUTION-01: vorher ein geteiltes rejected_sector/
        # rejected_dedupe über BEIDE Töpfe — machte nicht rekonstruierbar,
        # wie viele Rejections aus IPO-Intent vs. Funding kamen. Jetzt pro
        # Topf getrennt + Fallback-Summe für Rückwärts-Kompatibilität mit
        # bestehenden Log-Auswertungen.
        "ipo_intent_rejected_sector": 0, "ipo_intent_rejected_dedupe": 0,
        "ipo_intent_skipped_cap": 0,
        "funding_rejected_sector": 0, "funding_rejected_dedupe": 0,
        "funding_skipped_cap": 0, "funding_rejected_fund_vehicle": 0,
        # EU-NEWS-FUNDING-GRANULARITY-01: unterscheidet, ob ein EU-News-Item
        # am EUR-Floor oder an der Claude-Haiku-Namensextraktion scheitert —
        # vorher beides im selben "funding_seen=0" nicht unterscheidbar.
        # EU-NEWS-AMOUNT-DIAGNOSTIC-01 (04.07.): "kein Betrag im Text erkannt"
        # und "Betrag erkannt, aber < Floor" liefen bisher in DENSELBEN
        # eu_news_rejected_floor-Zähler — macht die Andreas-Hypothese
        # (EU-Runden kommunizieren Beträge seltener/anders als US) empirisch
        # nicht von einem Extraktionsbug unterscheidbar. Jetzt getrennt.
        "eu_news_seen": 0, "eu_news_rejected_no_amount": 0,
        "eu_news_rejected_floor": 0, "eu_news_rejected_extraction": 0,
        # DEALCOMPS-TRANSACTIONS-01: dritter Topf, kein DAILY_CAP_PER_POT-Limit
        # (eigene Decke COMPTX_DAILY_CAP, andere Logik s. dort).
        # COMPTX-EXTRACTION-DIAGNOSTIC-01 (04.07.): comp_tx_rejected_extraction
        # vermischte bisher zwei fachlich verschiedene Fälle (kein Deal vs.
        # Deal ohne Preis) — bei 17/17 Rejects am 04.07. nicht rekonstruierbar,
        # welcher Fall vorlag. Aufgeteilt, alter Key bleibt als Summe für
        # Rückwärts-Kompatibilität mit bestehenden Log-Auswertungen.
        "comp_tx_seen": 0, "comp_tx_written": 0,
        "comp_tx_rejected_no_item": 0, "comp_tx_rejected_extraction": 0,
        "comp_tx_rejected_no_deal": 0, "comp_tx_rejected_no_price": 0,
        "comp_tx_backfilled": 0,
    }
    new_signals: list[SignalEvent] = []

    timeout = httpx.Timeout(15.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:

        # EDGAR-FORMIDX-TRIPLEFETCH-01: EINMAL geholt, dreifach in-memory
        # gefiltert (S-1/S-11, D/D-A, 8-K) — vorher 3x (effektiv 6x) derselbe
        # Netzwerk-Fetch für dieselbe Datei.
        edgar_index_raw = await _fetch_edgar_daily_index_raw(client)

        # ── IPO-Intent-Topf: EDGAR S-1/S-11 + ESMA PRIII ─────────────────────
        ipo_intent_candidates: list[dict] = []

        edgar_s1 = _filter_edgar_rows(edgar_index_raw, forms={"S-1", "S-11"})
        for row in edgar_s1:
            ipo_intent_candidates.append({
                "name": row["company_name"], "source": "edgar_s1",
                "context": f"{row['form_type']} Filing, CIK {row['cik']}, {row['date_filed']}",
                "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={row['cik']}",
                "raw_title": f"{row['form_type']} — {row['company_name']}",
                "event_date": row["date_filed"],
            })

        esma_docs = await _fetch_esma_priii_documents(client)
        for doc in esma_docs:
            ipo_intent_candidates.append({
                "name": doc["company_name"], "source": "esma_prospectus",
                "context": f"ESMA PRIII Prospektus, {doc.get('date_filed') or 'Datum unbekannt'}",
                "source_url": None,
                "raw_title": f"PRIII — {doc['company_name']}",
                "event_date": doc.get("date_filed") or date.today().isoformat(),
            })

        stats["ipo_intent_seen"] = len(ipo_intent_candidates)

        for idx, cand in enumerate(ipo_intent_candidates):
            if stats["ipo_intent_written"] >= DAILY_CAP_PER_POT:
                # STATS-ATTRIBUTION-01: Cap erreicht ist by-design (Decke,
                # kein Soll) — aber ohne diesen Zähler sehen die restlichen,
                # nie evaluierten Kandidaten in den Stats wie spurloser
                # Datenverlust aus statt wie beabsichtigtes Cap-Verhalten.
                stats["ipo_intent_skipped_cap"] += len(ipo_intent_candidates) - idx
                break
            if _is_known(cand["name"], known_normalized):
                stats["ipo_intent_rejected_dedupe"] += 1
                continue
            sector = await _classify_sector(client, cand["name"], cand["context"])
            if sector is None:
                stats["ipo_intent_rejected_sector"] += 1
                continue
            identity_confidence, resolved = await _identity_gate(client, cand["name"])

            if DRY_RUN:
                logger.info(
                    "[DRY-RUN] IPO-Intent-Kandidat akzeptiert: '%s' (Quelle=%s, Sektor=%s, "
                    "Identität=%s) — würde geschrieben mit ipo_status=pre_ipo_high",
                    cand["name"], cand["source"], sector, identity_confidence,
                )
            else:
                new_id = _insert_discovery_company(db, cand["name"], cand["source"], identity_confidence, resolved)
                if new_id:
                    from src.integrations.supabase import upsert_company_enrichment
                    upsert_company_enrichment(new_id, {"ipo_status": "pre_ipo_high"})
                    new_signals.append(SignalEvent(
                        company_id=new_id, company_name=resolved.get("name") or cand["name"],
                        event_type="ipo_status_change",
                        event_date=date.fromisoformat(cand["event_date"][:10]) if cand["event_date"] else date.today(),
                        summary=f"Neu entdeckt via {cand['source']}: {cand['context']}",
                        source=cand["source"], source_url=cand["source_url"],
                        severity="high", raw_title=cand["raw_title"],
                        direction="positive", signal_category="ipo_progress",
                    ))
            stats["ipo_intent_written"] += 1

        # ── Funding-Topf: EDGAR Form D/D-A + kuratierte EU-News ──────────────
        # DISCOVERY-FUND-VEHICLE-FILTER-01 (S84, im Log vom 05.07. erneut
        # bestätigt: 'Octagon Biotech Opportunities Fund II LP', $175M —
        # derselbe Fall wie S84, kein Einzelfund). Form D deckt jede private
        # Wertpapieremission ab, auch Fonds-Closings — nicht nur Startup-
        # Runden. Ein Investmentfonds als "Investment-Target" im Explore-Feed
        # widerspricht "Datentransparenz als Plattform-Logik" (Andreas, S84).
        # Gate sitzt VOR _fetch_form_d_amount (spart einen SEC-Request pro
        # Fund-Treffer) und vor dem späteren Sektor-Gate (spart einen Claude-
        # Haiku-Call). s. _FUND_VEHICLE_NAME_RE weiter oben für die Muster.
        funding_candidates: list[dict] = []

        edgar_formd = _filter_edgar_rows(edgar_index_raw, forms={"D", "D/A"})
        for row in edgar_formd:
            if _looks_like_fund_vehicle(row["company_name"]):
                stats["funding_rejected_fund_vehicle"] += 1
                logger.debug(
                    "Form D Reject (Namensmuster deutet auf Fonds/Investment-"
                    "Vehikel statt Portfolio-Company): '%s'",
                    row["company_name"],
                )
                continue
            amount = await _fetch_form_d_amount(client, row["cik"], row["file_name"], row["company_name"])
            if amount is None or amount < FORM_D_FLOOR_USD_MN:
                continue
            funding_candidates.append({
                "name": row["company_name"], "source": "edgar_formd",
                "context": f"Form {row['form_type']}, ${amount:.1f}M, CIK {row['cik']}",
                "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={row['cik']}",
                "raw_title": f"{row['form_type']} — {row['company_name']}",
                "event_date": row["date_filed"],
                "funding_amount_usd_mn": amount,
            })

        eu_news = await _fetch_curated_eu_news(client)
        stats["eu_news_seen"] = len(eu_news)
        for item in eu_news:
            text = f"{item['title']} {item['description']}"
            eur_amount = _extract_eur_funding_amount(text)
            if eur_amount is None:
                stats["eu_news_rejected_no_amount"] += 1
                logger.debug(
                    "EU-News Reject (kein EUR-Betrag im Text erkennbar): '%s'",
                    item["title"][:120],
                )
                continue
            if eur_amount < EU_NEWS_FLOOR_EUR_MN:
                stats["eu_news_rejected_floor"] += 1
                logger.debug(
                    "EU-News Floor-Reject (€%.1fM erkannt, < €%.1fM Floor): '%s'",
                    eur_amount, EU_NEWS_FLOOR_EUR_MN, item["title"][:120],
                )
                continue
            extracted = await _extract_company_from_news(client, item["title"], item["description"])
            if not extracted:
                stats["eu_news_rejected_extraction"] += 1
                logger.debug(
                    "EU-News Extraction-Reject (kein Company-Name trotz €%.1fM-Treffer): '%s'",
                    eur_amount, item["title"][:120],
                )
                continue
            funding_candidates.append({
                "name": extracted["company_name"], "source": "news_curated_eu",
                "context": f"{item['domain']}, €{eur_amount:.1f}M — {item['title'][:120]}",
                "source_url": item["link"] or None,
                "raw_title": item["title"],
                "event_date": item["event_date"].isoformat(),
                "funding_amount_usd_mn": None,   # bewusst nicht in USD-Feld — EUR-Wert separat
            })

        stats["funding_seen"] = len(funding_candidates)

        for idx, cand in enumerate(funding_candidates):
            if stats["funding_written"] >= DAILY_CAP_PER_POT:
                stats["funding_skipped_cap"] += len(funding_candidates) - idx
                break
            if _is_known(cand["name"], known_normalized):
                stats["funding_rejected_dedupe"] += 1
                continue
            sector = await _classify_sector(client, cand["name"], cand["context"])
            if sector is None:
                stats["funding_rejected_sector"] += 1
                continue
            identity_confidence, resolved = await _identity_gate(client, cand["name"])

            if DRY_RUN:
                logger.info(
                    "[DRY-RUN] Funding-Kandidat akzeptiert: '%s' (Quelle=%s, Sektor=%s, "
                    "Identität=%s) — %s",
                    cand["name"], cand["source"], sector, identity_confidence, cand["context"],
                )
            else:
                new_id = _insert_discovery_company(db, cand["name"], cand["source"], identity_confidence, resolved)
                if new_id:
                    new_signals.append(SignalEvent(
                        company_id=new_id, company_name=resolved.get("name") or cand["name"],
                        event_type="funding_round",
                        event_date=date.fromisoformat(cand["event_date"][:10]) if cand["event_date"] else date.today(),
                        summary=f"Neu entdeckt via {cand['source']}: {cand['context']}",
                        source=cand["source"], source_url=cand["source_url"],
                        severity="medium", raw_title=cand["raw_title"],
                        direction="positive", signal_category="funding",
                        funding_amount_usd_mn=cand.get("funding_amount_usd_mn"),
                    ))
            stats["funding_written"] += 1

        # ── Vergleichbare-Transaktionen-Topf: EDGAR 8-K Items 1.01/2.01 ──────
        # DEALCOMPS-TRANSACTIONS-01: kein known_normalized-Dedupe wie bei den
        # anderen Töpfen — eine Comp-Transaktion referenziert kein neues
        # Company-Discovery-Objekt, sondern eine Sektor-Referenz (Andreas, S78:
        # "eher Referenz innerhalb eines Sektors als fest an einer Company").
        # DEALCOMPS-BACKFILL-01 (Company-Nachholung für unbekannte Targets)
        # ist hier bewusst NICHT mitgebaut — eigenes, separat zu spezifizierendes
        # Ticket, kein Vermischen von Schema-Aufbau und Backfill-Logik.
        edgar_8k = _filter_edgar_rows(edgar_index_raw, forms={"8-K"})
        stats["comp_tx_seen"] = len(edgar_8k)

        for row in edgar_8k:
            if stats["comp_tx_written"] >= COMPTX_DAILY_CAP:
                break

            filing_text = await _fetch_filing_text(client, row["file_name"])
            if not filing_text:
                continue

            items = _8k_item_numbers(filing_text)
            if not (items & _8K_COMPTX_ITEMS):
                stats["comp_tx_rejected_no_item"] += 1
                continue

            clean_text = _strip_filing_markup(filing_text)
            deal = await _extract_deal_details(client, clean_text, items, row["date_filed"])
            if not deal:
                stats["comp_tx_rejected_no_deal"] += 1
                stats["comp_tx_rejected_extraction"] += 1
                continue

            deal_price = deal.get("deal_price_usd_mn")
            if deal_price is None:
                # Ohne Preis kein Comp — der einzige zwingende Wert (Andreas,
                # S78: "Kaufpreise reichen, um Transaktionen abzubilden").
                stats["comp_tx_rejected_no_price"] += 1
                stats["comp_tx_rejected_extraction"] += 1
                continue

            deal_date = deal.get("deal_date") or row["date_filed"]

            # Snapshot-Versuch: existiert das Target schon bei uns? Dann den
            # AKTUELLEN DB-Stand einfrieren (8-K liegt nah am Deal-Close —
            # s. Snapshot-Diskussion S78). Sonst bleibt, was Haiku ggf. direkt
            # aus dem Filing-Text gezogen hat (oder None — zwei Praezisions-
            # stufen in derselben Tabelle, kein Alles-oder-nichts).
            target_funding_total = deal.get("target_funding_total_usd_mn_at_sale")
            target_funding_stage = deal.get("target_funding_stage_at_sale")
            target_company_id    = None

            existing_target = await _lookup_company_by_name(deal["target_name"])
            if existing_target:
                target_company_id    = existing_target.get("id")
                target_funding_total = existing_target.get("funding_total_usd_mn") or target_funding_total
                target_funding_stage = existing_target.get("funding_stage") or target_funding_stage
            elif not DRY_RUN:
                # DEALCOMPS-BACKFILL-01: nur scharf anlegen, kein Dry-Run-
                # Zweigpfad für eine Company-Erstellung mit echten Web-Calls
                # (Identitäts-Gate) — im Dry-Run bleibt es bei der Log-Zeile
                # unten, die das fehlende target_company_id schon transparent macht.
                target_company_id = await _backfill_acquired_company(db, client, deal)
                if target_company_id:
                    stats["comp_tx_backfilled"] += 1

            if DRY_RUN:
                logger.info(
                    "[DRY-RUN] Vergleichbare Transaktion erkannt: '%s' → '%s', "
                    "$%.1fM, %s, Sektor=%s, Snapshot=%s — würde geschrieben "
                    "(target_company_id=%s)",
                    deal.get("acquirer_name"), deal["target_name"], deal_price,
                    deal_date, deal.get("industry") or "—",
                    "ja" if (target_funding_total and target_funding_stage) else "nein",
                    target_company_id or "kein Match",
                )
            else:
                await _insert_comparable_transaction({
                    "target_name": deal["target_name"],
                    "target_company_id": target_company_id,
                    "acquirer_name": deal.get("acquirer_name"),
                    "industry": deal.get("industry"),
                    "deal_price_usd_mn": deal_price,
                    "deal_date": deal_date,
                    "source": "edgar_8k",
                    "source_url": f"https://www.sec.gov/Archives/{row['file_name']}",
                    "target_funding_total_usd_mn_at_sale": target_funding_total,
                    "target_funding_stage_at_sale": target_funding_stage,
                })
            stats["comp_tx_written"] += 1

    if new_signals and not DRY_RUN:
        written = upsert_signals([e.to_dict() for e in new_signals])
        stats["signals_written"] = written

    logger.info("Discovery-Cron fertig: %s", stats)
    return stats
