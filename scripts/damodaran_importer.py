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
# Mapping: Argo-Kategorie → Damodaran-Sektor
#
# Quelle: Damodaran Industry List (Januar 2025)
# Regel: unlevered_beta bevorzugt — leverage-bereinigt, da private
#        Company-Kapitalstruktur unbekannt.
# ---------------------------------------------------------------------------
ARGO_TO_DAMODARAN: dict[str, str] = {
    # Carbon Removal / CDR
    "Carbon Removal (DAC)":         "Chemical (Basic)",
    "Biomass CDR":                  "Environmental & Waste Services",
    "Mineralization":               "Chemical (Basic)",
    "Ocean CDR":                    "Environmental & Waste Services",
    "Modular Capture":              "Chemical (Basic)",
    "Mobile Capture":               "Chemical (Basic)",
    "Industrial Capture":           "Chemical (Basic)",
    "Electrochemical Capture":      "Chemical (Diversified)",

    # CO₂-Utilisation
    "CO₂-to-Chemicals":             "Chemical (Diversified)",
    "CO₂-to-Fuels":                 "Chemical (Diversified)",
    "CO₂-to-Fuels / SAF":          "Chemical (Diversified)",

    # Materials / Cement
    "Low-Carbon Concrete":          "Building Materials",
    "Low-Carbon Cement":            "Building Materials",
    "Electrified Cement":           "Building Materials",
    "Sustainable Materials":        "Chemical (Diversified)",

    # Energy / Storage
    "Geothermal / EGS":             "Power",
    "Long-Duration Storage":        "Power",
    "Distributed Battery / Grid":   "Power",
    "Distributed Power Infrastructure": "Power",
    "Solid-State Battery":          "Electronics (General)",
    "Battery Innovation":           "Electronics (General)",
    "Circular Battery Materials":   "Metals & Mining",
    "Circular Battery / Second-Life BESS": "Electronics (General)",

    # Hydrogen
    "Hydrogen":                     "Chemical (Basic)",

    # Grid / Software
    "AI × Grid Software":           "Software (System & Application)",
    "AI × Water / Cooling":         "Software (System & Application)",
    "Datacenter Cooling / HVAC":    "Electronics (General)",

    # Agriculture / Food
    "Agritech":                     "Farming / Agriculture",
    "Agritech SaaS":                "Software (System & Application)",
    "Vertical Farming":             "Farming / Agriculture",
    "Soil Carbon":                  "Farming / Agriculture",
    "Agroforestry":                 "Farming / Agriculture",
    "Carbon Credits":               "Environmental & Waste Services",
    "Bioengineering":               "Biotechnology",
    "Biotech":                      "Biotechnology",

    # Climate Risk / SaaS
    "Climate-Risk / Satelliten":    "Software (System & Application)",
    "Climate-Risk SaaS":            "Software (System & Application)",
    "Climate Adaptation / AI":      "Software (System & Application)",
    "Bio-based Chemicals":          "Chemical (Basic)",

    # Irrigation / Water
    "Irrigation":                   "Farming / Agriculture",
    "Solar Irrigation":             "Power",

    # Waste / Energy
    "Waste-to-Energy":              "Environmental & Waste Services",
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
        # Header-Zeile: erste Zeile die "industry"/"sector" UND "unlevered" enthält
        header_row = None
        for i, row in sheet_raw.iterrows():
            vals = [str(v).lower() for v in row.values]
            if any("industry" in v or "sector" in v for v in vals) and \
               any("unlevered" in v for v in vals):
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

    # Spaltennamen flexibel matchen
    col_sector    = _find_col(df, ["Industry Name", "Industry", "Sector"])
    col_unlevered = _find_col(df, ["Unlevered beta corrected for cash", "Unlevered Beta", "Unlevered beta"])
    col_levered   = _find_col(df, ["Beta", "Levered Beta", "Average Beta"])
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
    Invertiert ARGO_TO_DAMODARAN: Damodaran-Sektor → alle Argo-Kategorien (kommagetrennt).
    Für den DB-Eintrag: argo_category enthält alle mappenden Argo-Kategorien.
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
