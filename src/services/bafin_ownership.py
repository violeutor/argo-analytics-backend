"""
EN-07 · bafin_ownership.py — BA-Bridge
========================================
BaFin Stimmrechtsmitteilungen Scraper für listed DE Companies.

Quelle:     BaFin-Portal (portal.mvp.bafin.de) — öffentlich, kein Auth
Cron:       Rolling alle 6h — alle listed DE Companies in einem Lauf
On-Demand:  run_bafin_on_demand(company_name, ...) — direkt bei User-Anfrage
Rate-Limit: 1 Request / 65s → kein CAPTCHA-Risiko (~20 min für alle listed DE)
Persistenz: Argo Supabase ownership_entries (direkter Write via supabase-py)

Einstiegspunkte:
  run_bafin_ownership_cron(supabase_url, supabase_key)  — Rolling-Cron (alle 6h)
  run_bafin_on_demand(company_name, supabase_url, supabase_key)  — One-Click-Trigger
"""

import logging
import re
import time
from html.parser import HTMLParser
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── Konstanten ────────────────────────────────────────────────────────────────

BAFIN_URL = (
    "https://portal.mvp.bafin.de/database/MeldepflichtigeVorhabenInfo/list.do"
)
BAFIN_HEADERS = {
    "User-Agent": (
        "ArgoAnalytics/1.0 (investment intelligence; contact@argo-analytics.io)"
    ),
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Connection":      "keep-alive",
}
RATE_LIMIT_SECS = 65    # 1 Request/Minute + 5s Puffer → kein CAPTCHA
TIMEOUT_SECS    = 25
_ALLOWED_FIELDS = {"name", "type", "role", "share_pct", "source", "as_of_date"}


# ── HTML-Parser ───────────────────────────────────────────────────────────────

class _TableParser(HTMLParser):
    """
    Minimalparser für BaFin-Ergebnistabelle.
    Liest die erste <table> im Response und extrahiert alle Zeilen.
    Robustness: ignoriert verschachtelte Tables (depth-Guard).
    """

    def __init__(self):
        super().__init__()
        self._table_depth  = 0
        self._in_row       = False
        self._in_cell      = False
        self._cell_buf: str            = ""
        self._row_buf: list[str]       = []
        self.rows: list[list[str]]     = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "table":
            self._table_depth += 1
        if self._table_depth != 1:
            return
        if tag == "tr":
            self._in_row  = True
            self._row_buf = []
        elif tag in ("td", "th") and self._in_row:
            self._in_cell  = True
            self._cell_buf = ""

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "table":
            self._table_depth -= 1
        if self._table_depth != 1 and not (tag == "table" and self._table_depth == 0):
            if self._in_cell and tag in ("td", "th"):
                pass  # handled below
            else:
                return
        if tag in ("td", "th") and self._in_cell:
            self._row_buf.append(self._cell_buf.strip())
            self._in_cell = False
        elif tag == "tr" and self._in_row:
            if any(c for c in self._row_buf):
                self.rows.append(self._row_buf[:])
            self._in_row = False

    def handle_data(self, data):
        if self._in_cell:
            self._cell_buf += data

    def handle_entityref(self, name):
        # Häufige HTML-Entities normalisieren
        _ent = {"amp": "&", "lt": "<", "gt": ">", "nbsp": " ", "auml": "ä",
                "ouml": "ö", "uuml": "ü", "Auml": "Ä", "Ouml": "Ö", "Uuml": "Ü",
                "szlig": "ß"}
        if self._in_cell:
            self._cell_buf += _ent.get(name, "")

    def handle_charref(self, name):
        if self._in_cell:
            try:
                ch = chr(int(name[1:], 16) if name.startswith("x") else int(name))
                self._cell_buf += ch
            except (ValueError, OverflowError):
                pass


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def _parse_share_pct(text: str) -> Optional[float]:
    """
    Extrahiert Prozentzahl aus BaFin-Zellentext.
    Formate: '5,12 %' | '5.12%' | '5,12' | '>5 %' | '≥ 10 %'
    Gibt None zurück wenn kein valider Wert erkannt.
    """
    # Führende Vergleichsoperatoren entfernen
    text = re.sub(r"^[≥≤<>]=?\s*", "", text.strip())
    # Deutsches Komma → Punkt
    text = text.replace(",", ".")
    match = re.search(r"(\d{1,3}(?:\.\d+)?)\s*%?", text)
    if match:
        try:
            val = float(match.group(1))
            # Plausibilitätscheck: 0–100 %
            if 0.0 < val <= 100.0:
                return round(val, 4)
        except ValueError:
            pass
    return None


def _parse_date(text: str) -> Optional[str]:
    """Konvertiert DD.MM.YYYY → YYYY-MM-DD (ISO 8601)."""
    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text.strip())
    if match:
        return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
    return None


def _classify_holder(name: str) -> str:
    """
    Heuristik: Investorentyp aus Name ableiten.
    Reihenfolge: PE → Government → Institutional → Corporate → unknown
    """
    n = name.lower()
    _PE  = ("blackstone", "kkr", "carlyle", "apollo", "warburg pincus",
             "advent international", "bain capital", "tpg", "permira",
             "bridgepoint", "cinven")
    _GOV = ("sovereign", "pension", "stiftung", "sparkasse", "landesbank",
             "bundesland", "freistaat", "republic", "bundesregierung",
             "ministry", "central bank", "government", "state of ",
             "kfw", "kreditanstalt", "förderbank", "förderung")
    _INST = ("fund", "asset management", "investment management",
              "blackrock", "vanguard", "fidelity", "dimensional",
              "allianz", "dws", "union invest", "deka", "amundi",
              "norges bank", "ubs asset", "deutsche asset",
              "flossbach", "capital group", "t. rowe", "schroders",
              "jpmorgan asset", "goldman sachs asset")
    if any(kw in n for kw in _PE):
        return "pe"
    if any(kw in n for kw in _GOV):
        return "government"
    if any(kw in n for kw in _INST):
        return "institutional"
    _CORP_EXACT = re.compile(
        r"\b(ag|se|gmbh|plc|inc|corp|holding|group|industries)\b"
    )
    if _CORP_EXACT.search(n):
        return "corporate"
    return "unknown"


def _is_de_company(company: dict) -> bool:
    """Prüft ob eine Company eine listed DE-Company ist (Exchange oder HQ)."""
    exchange = (company.get("exchange") or "").lower()
    if any(x in exchange for x in ("xetra", "frankfurt", "fse", "xfra", "m:access")):
        return True
    hq = (company.get("headquarters") or "").lower()
    _DE = ("germany", "deutschland", "berlin", "munich", "münchen",
           "hamburg", "frankfurt", "cologne", "köln", "düsseldorf",
           "stuttgart", "hannover", "dortmund", "essen", "leipzig",
           "bremen", "dresden", "nuremberg", "nürnberg", "bonn",
           "mannheim", "karlsruhe", "augsburg", "wiesbaden", "mainz")
    return any(h in hq for h in _DE)


# ── BaFin Scraper ─────────────────────────────────────────────────────────────

def fetch_bafin_stimmrechte(company_name: str) -> list[dict]:
    """
    Scrapt BaFin-Portal nach Stimmrechtsmitteilungen für einen Emittenten.
    Gibt Liste von Ownership-Einträgen zurück (kompatibel mit ownership_entries Schema).

    Rate-Limit liegt beim Aufrufer (RATE_LIMIT_SECS zwischen Calls einhalten).
    Gibt leere Liste zurück bei Fehler — kein Hard-Fail.

    Gesuchte Spalten im BaFin-Response-HTML:
        Datum | Meldepflichtiger | Emittent | Schwelle | Anteil in %
    """
    params = {
        "emittent.name": company_name,
        "cmd":           "search",
        "zeitraum":      "0",    # alle Zeiträume (nicht nur letztes Jahr)
        "d-4012147-s":   "1",    # Sortierung nach Datum DESC
    }
    results: list[dict] = []

    try:
        with httpx.Client(
            timeout=TIMEOUT_SECS,
            headers=BAFIN_HEADERS,
            follow_redirects=True,
        ) as client:
            resp = client.get(BAFIN_URL, params=params)

        if resp.status_code != 200:
            logger.debug(
                "BaFin HTTP %s für '%s'", resp.status_code, company_name
            )
            return []

        parser = _TableParser()
        parser.feed(resp.text)

        if not parser.rows:
            logger.debug("BaFin: keine Tabellendaten für '%s'", company_name)
            return []

        # Erste Zeile = Header
        header = [h.lower() for h in (parser.rows[0] if parser.rows else [])]
        data_rows = parser.rows[1:]

        if not data_rows:
            logger.debug("BaFin: keine Datenzeilen für '%s'", company_name)
            return []

        # Spalten-Index aus Header ableiten (BaFin-Layout-Varianten abfangen)
        def _col(keywords: list[str], default: int) -> int:
            for i, h in enumerate(header):
                if any(kw in h for kw in keywords):
                    return i
            return default

        col_date   = _col(["datum", "date"],              0)
        col_holder = _col(["meldepflichtiger", "meldend", "holder"], 1)
        col_emit   = _col(["emittent", "issuer", "unternehmen"],    2)
        col_pct    = _col(["anteil", "prozent", "%", "pct"],        4)

        seen:          set[str] = set()
        company_lower: str      = company_name.lower()

        for row in data_rows:
            if len(row) < 2:
                continue

            holder_name = row[col_holder].strip() if col_holder < len(row) else ""
            holder_name = re.sub(r"\s+", " ", holder_name)
            if not holder_name or len(holder_name) < 3:
                continue
            if holder_name.lower() in seen:
                continue

            # Emittent-Abgleich: Zeile muss zum gesuchten Unternehmen gehören.
            # Tolerant: Substring-Match in beide Richtungen.
            if col_emit < len(row):
                emittent = row[col_emit].lower()
                if (emittent
                        and company_lower not in emittent
                        and emittent not in company_lower
                        # Kurzname-Abgleich (erste zwei Wörter)
                        and not any(
                            w in emittent
                            for w in company_lower.split()[:2]
                            if len(w) > 3
                        )):
                    logger.debug(
                        "BaFin: Emittent-Mismatch '%s' ≠ '%s' — übersprungen",
                        emittent, company_lower,
                    )
                    continue

            share_pct  = _parse_share_pct(row[col_pct]) if col_pct < len(row) else None
            as_of_date = _parse_date(row[col_date])      if col_date < len(row) else None

            seen.add(holder_name.lower())
            results.append({
                "name":       holder_name,
                "type":       _classify_holder(holder_name),
                "role":       "significant_shareholder",
                "share_pct":  share_pct,
                "source":     "bafin_stimmrechte",
                "as_of_date": as_of_date,
            })

        logger.info(
            "BaFin Stimmrechte: '%s' → %d Einträge", company_name, len(results)
        )

    except httpx.TimeoutException:
        logger.debug("BaFin timeout für '%s'", company_name)
    except Exception as e:
        logger.debug("fetch_bafin_stimmrechte failed für '%s': %s", company_name, e)

    return results


# ── Normalisierung für Self-Reference-Check (BUG-53) ─────────────────────────

_LEGAL_SUFFIXES = re.compile(
    r"\b(ag|se|gmbh|kg|ohg|gbr|ev|eg|ug|plc|inc|corp|ltd|llc|sa|nv|bv)\b\.?$",
    re.IGNORECASE,
)

def _normalize_name(name: str) -> str:
    """Lowercase + Rechtsform-Suffixe entfernen für Self-Ref-Vergleich."""
    n = name.lower().strip()
    n = _LEGAL_SUFFIXES.sub("", n).strip(" ,.")
    return n


# ── Kern-Funktion: eine Company fetchen + schreiben ───────────────────────────

def _fetch_and_store_company(company: dict, db) -> int:
    """
    Fetcht BaFin-Daten für eine Company und upserted Einträge in ownership_entries.

    Beinhaltet:
      - BUG-53: Self-Reference-Filter (company selbst als Holder überspringen)
      - Upsert via Unique Constraint (company_id, name, source) —
        aktualisiert share_pct + as_of_date wenn Holder bereits existiert
      - Rate-Limit liegt beim Aufrufer

    Gibt Anzahl upserted Einträge zurück.
    """
    cid  = company.get("id") or ""
    name = company.get("name") or ""
    if not cid or not name:
        return 0

    company_normalized = _normalize_name(name)
    entries = fetch_bafin_stimmrechte(name)
    written = 0

    for e in entries:
        entry_name = (e.get("name") or "").strip()
        if not entry_name:
            continue

        # BUG-53: Self-Reference-Filter
        if _normalize_name(entry_name) == company_normalized:
            logger.debug(
                "BaFin: Self-Reference übersprungen — '%s' für '%s'",
                entry_name, name,
            )
            continue

        try:
            payload = {
                k: v for k, v in e.items()
                if k in _ALLOWED_FIELDS and v is not None
            }
            payload["company_id"] = cid
            payload["name"]       = entry_name
            # Upsert: Unique Constraint (company_id, name, source) —
            # aktualisiert share_pct + as_of_date bei geänderter Beteiligung
            db.table("ownership_entries").upsert(
                payload,
                on_conflict="company_id,name,source",
            ).execute()
            written += 1
        except Exception as ins_e:
            logger.debug(
                "BaFin upsert skip '%s' für '%s': %s", entry_name, name, ins_e
            )

    if written:
        logger.info("BaFin: '%s' → %d neue Einträge geschrieben", name, written)
    else:
        logger.debug("BaFin: '%s' — keine neuen Einträge", name)

    return written


# ── On-Demand: eine einzelne Company sofort befüllen ──────────────────────────

def run_bafin_on_demand(
    company_name: str,
    supabase_url: str,
    supabase_key: str,
) -> dict:
    """
    One-Click-Trigger — wird von BA-Bridge POST /bafin/enrich/{name} aufgerufen.

    Sucht die Company in Supabase, prüft ob sie eine listed DE-Company ist,
    fetcht BaFin-Daten und schreibt direkt in ownership_entries.
    Kein Rate-Limit-Sleep — single Request, keine Kollision mit Cron.

    Gibt dict zurück: entries_written, skipped (not_found / not_de_listed)
    """
    from supabase import create_client

    db     = create_client(supabase_url, supabase_key)
    result = db.table("companies").select(
        "id, name, ticker, exchange, headquarters, ipo_status"
    ).ilike("name", company_name).limit(1).execute()

    companies = result.data or []
    if not companies:
        logger.info("BaFin on-demand: '%s' nicht in Supabase", company_name)
        return {"entries_written": 0, "skipped": "not_found"}

    company = companies[0]
    if not _is_de_company(company) or company.get("ipo_status") != "listed":
        logger.info(
            "BaFin on-demand: '%s' ist keine listed DE-Company", company_name
        )
        return {"entries_written": 0, "skipped": "not_de_listed"}

    written = _fetch_and_store_company(company, db)
    return {"entries_written": written, "skipped": None}


# ── Rolling Cron: alle listed DE Companies (alle 6h) ─────────────────────────

def run_bafin_ownership_cron(supabase_url: str, supabase_key: str) -> dict:
    """
    Rolling-Cron — alle 6h via bridge_main.py.
    Verarbeitet alle listed DE-Companies in einem Lauf (bounded set, ~20 min).

    Ablauf:
      1. Alle listed Companies aus Argo Supabase laden
      2. DE-Companies herausfiltern (_is_de_company)
      3. Pro Company: BaFin scrapen + schreiben (Rate-Limit: RATE_LIMIT_SECS)
      4. Statistik zurückgeben

    Gibt dict zurück: companies_processed, companies_with_data, entries_written, errors
    """
    from supabase import create_client

    stats = {
        "companies_processed": 0,
        "companies_with_data": 0,
        "entries_written":     0,
        "errors":              0,
    }

    try:
        db = create_client(supabase_url, supabase_key)

        result = db.table("companies").select(
            "id, name, ticker, exchange, headquarters, ipo_status"
        ).eq("ipo_status", "listed").execute()

        all_listed   = result.data or []
        de_companies = [c for c in all_listed if _is_de_company(c)]

        logger.info(
            "BaFin Cron: %d listed gesamt, %d DE-Companies",
            len(all_listed), len(de_companies),
        )

        for company in de_companies:
            try:
                written = _fetch_and_store_company(company, db)
                stats["companies_processed"] += 1
                if written:
                    stats["companies_with_data"] += 1
                    stats["entries_written"]     += written
            except Exception as ce:
                stats["errors"] += 1
                logger.warning(
                    "BaFin Cron: '%s' failed — %s", company.get("name"), ce
                )

            time.sleep(RATE_LIMIT_SECS)

    except Exception as e:
        logger.exception("BaFin Ownership Cron kritischer FEHLER: %s", e)
        stats["errors"] += 1

    logger.info(
        "BaFin Cron abgeschlossen — processed=%d with_data=%d written=%d errors=%d",
        stats["companies_processed"],
        stats["companies_with_data"],
        stats["entries_written"],
        stats["errors"],
    )
    return stats
