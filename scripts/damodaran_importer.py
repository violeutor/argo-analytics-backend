"""
YH-04 · damodaran_importer.py
Pfad: argo-analytics-backend/scripts/damodaran_importer.py

Jährlicher Import der NYU Damodaran Beta-Datenbank → SUPABASE.

Architektur (Session 49): Damodaran ist statische Referenz (kein yfinance, keine
Berechnung) und gehört zu market_data in Supabase, NICHT in die Bridge. Die Bridge
bleibt reiner yfinance-Beta-Dienst. Dieser Importer schreibt direkt nach Supabase
(damodaran_beta). Kein Bridge-Hop mehr beim Lesen.

Quelle: https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/betas.html
Excel-Download: https://pages.stern.nyu.edu/~adamodar/pc/datasets/betas.xls

Standalone-Batch (NICHT im Request-Pfad — pandas/xlrd gehören nicht in den
FastAPI-Prozess). Läuft 1×/Jahr (Januar) oder ad-hoc manuell:
    python -m scripts.damodaran_importer              # aktuelles Jahr
    python -m scripts.damodaran_importer --dry-run    # nur ausgeben, nichts schreiben

Voraussetzung: Supabase-Tabelle damodaran_beta existiert
(siehe migration_damodaran_beta.sql).

Mapping: Argo-Kategorie → Damodaran-Sektor (hardcoded, ändert sich kaum).
Mehrere Argo-Kategorien können auf denselben Damodaran-Sektor mappen.
"""

import argparse
import io
import logging
from datetime import datetime, timezone

import httpx
import pandas as pd

import os
from supabase import create_client as _create_client

def get_supabase():
    url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL und SUPABASE_SERVICE_KEY müssen als Env-Vars gesetzt sein.")
    return _create_client(url, key)

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------------------------------------------------------------------
# Damodaran Excel URL
# ---------------------------------------------------------------------------
DAMODARAN_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/betas.xls"

# ---------------------------------------------------------------------------
# Mapping: Argo-SEKTOR → Damodaran-Sektor  (Taxonomy v1.0, 14 Sektoren)
#
# Session 49: von Kategorie-Ebene (68 Einträge, brüchig) auf Sektor-Ebene
# umgestellt. Damodaran ist selbst Sektor-Ebene → ein Match je Argo-Sektor reicht.
# Robuster + wartungsarm: Company.industry (= Argo-Sektor) ist stabiler als die
# Feinkategorie, und der Lookup wird ein klarer Match statt ILIKE-Gefummel.
#
# WICHTIG: Die Damodaran-Sektornamen müssen EXAKT mit der betas.xls übereinstimmen
# (z.B. "Farming/Agriculture" OHNE Leerzeichen, "Aerospace/Defense" mit Slash).
# Die Validierung in run() meldet im Dry-Run jeden Namen, der nicht in der Datei
# gefunden wurde — dort Tippfehler korrigieren.
#
# Mehrere Argo-Sektoren dürfen auf denselben Damodaran-Sektor mappen.
# ---------------------------------------------------------------------------
ARGO_TO_DAMODARAN: dict[str, str] = {
    "Energy & Power":             "Green & Renewable Energy",
    "Mobility & Transport":       "Auto & Truck",
    "Carbon & Climate":           "Environmental & Waste Services",
    "Industrial & Manufacturing": "Machinery",
    "Materials & Chemicals":      "Chemical (Specialty)",
    "Agriculture & Food":         "Farming/Agriculture",
    "Built Environment":          "Engineering/Construction",
    "Life Sciences & Health":     "Drugs (Pharmaceutical)",
    "Digital Infrastructure":     "Software (System & Application)",
    "Financial Services":         "Financial Svcs. (Non-bank & Insurance)",
    "Consumer & Commerce":        "Retail (Online)",
    "Space & Defense":            "Aerospace/Defense",
    "Water & Circular Economy":   "Environmental & Waste Services",
    "Mining & Resources":         "Metals & Mining",
}

# ---------------------------------------------------------------------------
# Excel laden + parsen
# ---------------------------------------------------------------------------

def _download_excel(url: str) -> bytes:
    log.info(f"Lade Damodaran Excel: {url}")
    # NYU (pages.stern.nyu.edu) blockt UA-lose Requests teils mit 403 — Browser-UA setzen.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
    }
    resp = httpx.get(url, timeout=30, follow_redirects=True, headers=headers)
    resp.raise_for_status()
    log.info(f"Download OK — {len(resp.content) / 1024:.0f} KB")
    return resp.content


def _parse_betas(raw: bytes) -> pd.DataFrame:
    """
    Findet automatisch das richtige Sheet in Damodarans betas.xls.
    Sheet 0 ist oft eine Einleitungsseite ('End Game') — daher alle Sheets
    laden und das erste nehmen, das 'Industry Name' + 'Unlevered beta' enthält.

    Relevante Spalten:
        - Industry Name
        - Unlevered beta corrected for cash  (= unlevered_beta)
        - Beta / Average Beta                (= levered_beta, Branchen-Ø)
        - D/E Ratio                          (= d_e_ratio)

    Spaltennamen variieren leicht je Jahrgang — daher flexible Suche.
    """
    # header=None: Damodaran hat oft Leerzeilen am Anfang, daher Header manuell finden.
    all_sheets = pd.read_excel(io.BytesIO(raw), sheet_name=None, header=None)
    log.info(f"Sheets im Excel: {list(all_sheets.keys())}")

    df = None
    for sheet_name, sheet_raw in all_sheets.items():
        # Header-Zeile: erste Zeile wo eine Zelle EXAKT (oder fast) "Industry Name"
        # heißt UND eine andere Zelle mit "Unlevered" beginnt (kein Substring-Match
        # auf langen Beschreibungstexten — Damodaran hat Einleitungszeilen oben).
        header_row = None
        for i, row in sheet_raw.iterrows():
            vals = [str(v).strip() for v in row.values]
            has_industry = any(v.lower() in ("industry name", "industry", "sector") for v in vals)
            has_unlevered = any(v.lower().startswith("unlevered") for v in vals)
            if has_industry and has_unlevered:
                header_row = i
                break
        if header_row is not None:
            sheet_df = sheet_raw.iloc[header_row:].reset_index(drop=True)
            sheet_df.columns = [str(c).strip() for c in sheet_df.iloc[0]]
            sheet_df = sheet_df.iloc[1:].reset_index(drop=True)
            log.info(f"Datentabelle gefunden: Sheet '{sheet_name}', Header-Zeile {header_row}")
            df = sheet_df
            break

    if df is None:
        raise ValueError(
            f"Kein Sheet mit Industry + Unlevered Beta gefunden. "
            f"Sheets: {list(all_sheets.keys())}"
        )

    log.info(f"Spalten im Excel: {list(df.columns)}")
    # Debug: erste Datenzeile ausgeben um echte Werte zu sehen
    log.info(f"Erste Zeile: {df.iloc[0].tolist()}")

    # Spaltennamen flexibel matchen
    col_sector    = _find_col(df, ["Industry Name", "Industry", "Sector"])
    col_unlevered = _find_col(df, ["Unlevered beta corrected for cash", "Unlevered Beta", "Unlevered beta"])
    col_levered   = _find_col(df, ["Levered Beta", "Average Levered Beta", "Average Beta", "levered beta"])
    col_de        = _find_col(df, ["D/E Ratio", "Debt/Equity"])

    if not col_sector or not col_unlevered:
        raise ValueError(
            f"Pflicht-Spalten nicht gefunden. "
            f"Verfügbar: {list(df.columns)}"
        )

    result = df[[col_sector, col_unlevered]].copy()
    result.columns = ["sector", "unlevered_beta"]

    if col_levered:
        result["levered_beta"] = df[col_levered]
    else:
        result["levered_beta"] = None

    if col_de:
        result["d_e_ratio"] = df[col_de]
    else:
        result["d_e_ratio"] = None

    # Bereinigen
    result = result.dropna(subset=["sector", "unlevered_beta"])
    result = result[result["sector"].str.strip() != ""]
    result["sector"]        = result["sector"].str.strip()
    result["unlevered_beta"] = pd.to_numeric(result["unlevered_beta"], errors="coerce")
    result["levered_beta"]   = pd.to_numeric(result["levered_beta"],   errors="coerce")
    result["d_e_ratio"]      = pd.to_numeric(result["d_e_ratio"],      errors="coerce")
    result = result.dropna(subset=["unlevered_beta"])

    log.info(f"{len(result)} Sektoren nach Bereinigung.")
    return result


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Findet die erste passende Spalte (case-insensitive, substring)."""
    for cand in candidates:
        for col in df.columns:
            if cand.lower() in col.lower():
                return col
    return None


# ---------------------------------------------------------------------------
# Argo-Mapping anwenden + upsert
# ---------------------------------------------------------------------------

def _build_argo_category_map(df: pd.DataFrame) -> dict[str, str]:
    """
    Invertiert ARGO_TO_DAMODARAN: Damodaran-Sektor → alle mappenden Argo-Sektoren
    (kommagetrennt). Für den DB-Eintrag: argo_category enthält die Argo-Sektornamen
    (Spaltenname historisch 'argo_category', hält jetzt aber Sektoren — Sektor-Ebene
    seit Session 49). Der Backend-Lookup matcht company.industry dagegen.
    """
    mapping: dict[str, list[str]] = {}
    for argo_cat, dam_sector in ARGO_TO_DAMODARAN.items():
        mapping.setdefault(dam_sector, []).append(argo_cat)
    return {k: ", ".join(sorted(v)) for k, v in mapping.items()}


def run(dry_run: bool = False) -> None:
    updated_year = datetime.now(timezone.utc).year

    # Download + Parse
    raw = _download_excel(DAMODARAN_URL)
    df  = _parse_betas(raw)

    # ── Mapping-Validierung ────────────────────────────────────────────────
    # Prüft, welche Damodaran-Sektornamen aus ARGO_TO_DAMODARAN NICHT in der
    # Datei existieren (Tippfehler → stiller Miss, kein Beta für den Argo-Sektor).
    file_sectors    = set(df["sector"].tolist())
    mapped_sectors  = set(ARGO_TO_DAMODARAN.values())
    matched_sectors = sorted(mapped_sectors & file_sectors)
    missing_sectors = sorted(mapped_sectors - file_sectors)
    log.info(
        "MAPPING-VALIDIERUNG: %d/%d Damodaran-Sektoren in der Datei gefunden",
        len(matched_sectors), len(mapped_sectors),
    )
    if missing_sectors:
        log.warning("NICHT gefunden — diese Argo-Sektoren bekommen KEIN Beta:")
        for dam in missing_sectors:
            argo = [a for a, d in ARGO_TO_DAMODARAN.items() if d == dam]
            log.warning("  '%s'  ←  %s", dam, ", ".join(argo))
        log.warning("→ Exakten Damodaran-Namen in ARGO_TO_DAMODARAN korrigieren.")

    argo_map = _build_argo_category_map(df)

    if dry_run:
        log.info("=== DRY RUN — keine DB-Schreibvorgänge ===")
        for _, row in df.iterrows():
            argo = argo_map.get(row["sector"], "—")
            log.info(
                f"  {row['sector']:<45} "
                f"unlevered={row['unlevered_beta']:.3f}  "
                f"levered={row['levered_beta'] if pd.notna(row.get('levered_beta')) else float('nan'):.3f}  "
                f"argo={argo}"
            )
        return

    # Supabase-Batch-Upsert (on_conflict=sector — Natural Key). Ein einziger Call
    # statt Zeile-für-Zeile, kein Session-Handling nötig.
    sb = get_supabase()
    now_iso = datetime.now(timezone.utc).isoformat()

    rows: list[dict] = []
    skipped = 0
    for _, row in df.iterrows():
        sector = row["sector"]
        argo   = argo_map.get(sector)  # None wenn kein Argo-Mapping
        if not argo:
            skipped += 1
        rows.append({
            "sector":         sector,
            "argo_category":  argo,
            "unlevered_beta": float(row["unlevered_beta"]),
            "levered_beta":   float(row["levered_beta"]) if pd.notna(row.get("levered_beta")) else None,
            "d_e_ratio":      float(row["d_e_ratio"])    if pd.notna(row.get("d_e_ratio"))    else None,
            "updated_year":   updated_year,
            "source_url":     DAMODARAN_URL,
            "imported_at":    now_iso,
        })

    try:
        sb.table("damodaran_beta").upsert(rows, on_conflict="sector").execute()
        log.info(
            f"=== Damodaran Import · Fertig · {len(rows)} Sektoren upserted · "
            f"{skipped} ohne Argo-Mapping (trotzdem gespeichert) ==="
        )
    except Exception as e:
        log.error(f"Supabase-Upsert fehlgeschlagen: {e}")
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Damodaran Beta Import → Supabase")
    parser.add_argument("--dry-run", action="store_true", help="Nur ausgeben, nichts schreiben")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
