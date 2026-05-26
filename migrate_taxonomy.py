#!/usr/bin/env python3
"""
migrate_taxonomy.py — Argo Analytics
=====================================
Normalisiert bestehende `category` + `industry` Werte in der companies-Tabelle
gegen die neue Argo Taxonomy v1.0 (src/taxonomy.py).

Ausführen aus dem Backend-Root:

  # Dry-Run — zeigt alle Änderungen, schreibt nichts
  python migrate_taxonomy.py

  # Apply — schreibt normalisierte Werte in die DB
  python migrate_taxonomy.py --apply

  # Nur bestimmte Companies (komma-separiert)
  python migrate_taxonomy.py --apply --names "LanzaTech,Fervo Energy"

  # Detailliertes Logging aller unveränderten Companies
  python migrate_taxonomy.py --verbose
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass

# Taxonomy aus src/ laden
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.taxonomy import normalize_sector, normalize_category

# ── Legacy-Mapping ────────────────────────────────────────────────────────────
# Bekannte alte Freitext-Werte aus enrichment.py v1 + Claude-Fallbacks
# die nicht automatisch gegen die Taxonomy matchen.
# Exakter Case-insensitive Match — vor normalize_sector/category geprüft.

_LEGACY_SECTOR: dict[str, str] = {
    # Alte Claude-Industrie-Liste (enrichment.py v1)
    "carbon removal":           "Carbon & Climate",
    "industrial decarbonization": "Industrial & Manufacturing",
    "energy storage":           "Energy & Power",
    "grid & infrastructure":    "Energy & Power",
    "renewable energy":         "Energy & Power",
    "fuels & chemicals":        "Materials & Chemicals",
    "digital infrastructure":   "Digital Infrastructure",
    "climate intelligence":     "Carbon & Climate",
    "carbon markets":           "Carbon & Climate",
    "water & irrigation":       "Water & Circular Economy",
    "diversified industrial":   "Industrial & Manufacturing",
    "agriculture & food":       "Agriculture & Food",
    "battery / energy storage":  "Energy & Power",
    "battery storage":           "Energy & Power",
    # Sonstige bekannte Freitext-Varianten
    "cleantech":                "Energy & Power",
    "climate tech":             "Carbon & Climate",
    "industrial tech":          "Industrial & Manufacturing",
}

_LEGACY_CATEGORY: dict[str, str] = {
    # Alte _TAG_TO_CATEGORY Werte (enrichment.py v1)
    "carbon removal (dac)":          "Direct Air Capture",
    "ocean cdr":                     "Nature-Based Solutions",
    "low-carbon cement":             "Advanced Materials",
    "sustainable materials":         "Advanced Materials",
    "battery / energy storage":      "Energy Storage",
    "long-duration storage":         "Energy Storage",
    "solid-state battery":           "Energy Storage",
    "grid software / infrastructure": "Grid & Smart Energy",
    "ai × grid software":            "Grid & Smart Energy",
    "solar energy":                  "Solar PV",
    "hydrogen":                      "Hydrogen & Fuel Cells",
    "geothermal / egs":              "Geothermal",
    "agritech":                      "Precision Farming",
    "bioengineering":                "Alternative Proteins",
    "soil carbon":                   "Precision Farming",
    "co₂-to-fuels / saf":           "CO₂ Utilization",
    "co2-to-fuels":                  "CO₂ Utilization",
    "bio-based chemicals":           "Green Chemicals",
    "datacenter cooling / hvac":     "Smart Buildings",
    "climate-risk saas":             "Climate Analytics & ESG",
    "carbon credits":                "Carbon Markets & Credits",
    "irrigation":                    "Water Infrastructure",
    "ai × water / cooling":          "Water Technology",
    # Kebab-case Tags die direkt in der DB landen könnten
    "carbon-capture":                "Carbon Capture & Storage",
    "direct-air-capture":            "Direct Air Capture",
    "solid-state-battery":           "Energy Storage",
    "battery":                       "Energy Storage",
    "grid":                          "Grid & Smart Energy",
    "solar":                         "Solar PV",
    "geothermal":                    "Geothermal",
    "waste-to-energy":               "Waste-to-Energy",
    "carbon-credits":                "Carbon Markets & Credits",
    "water-tech":                    "Water Technology",
}


def _legacy_sector(value: str | None) -> str | None:
    if not value:
        return None
    return _LEGACY_SECTOR.get(value.strip().lower())


def _legacy_category(value: str | None) -> str | None:
    if not value:
        return None
    return _LEGACY_CATEGORY.get(value.strip().lower())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("migrate_taxonomy")


# ── Supabase Client ───────────────────────────────────────────────────────────

def get_client():
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL und SUPABASE_SERVICE_ROLE_KEY müssen als Env-Variablen gesetzt sein."
        )
    return create_client(url, key)


# ── Result Dataclass ──────────────────────────────────────────────────────────

@dataclass
class MigrationResult:
    company_id:   str
    name:         str
    old_industry: str | None
    new_industry: str | None
    old_category: str | None
    new_category: str | None
    changed:      bool
    industry_matched: bool
    category_matched: bool


# ── Core Logic ────────────────────────────────────────────────────────────────

def normalize_company(company: dict) -> MigrationResult:
    """
    Normalisiert industry + category einer Company gegen die Taxonomy.
    Reihenfolge: Legacy-Map → Auto-Normalize → Originalwert behalten.
    """
    old_industry = company.get("industry") or None
    old_category = company.get("category") or None

    # 1) Legacy-Map (bekannte alte Werte aus enrichment.py v1)
    new_industry = _legacy_sector(old_industry) or normalize_sector(old_industry)
    # Sector-Kontext für Category-Match: normalized sector bevorzugen
    sector_ctx   = new_industry or old_industry
    new_category = _legacy_category(old_category) or normalize_category(old_category, sector_ctx)

    # Fallback: kein Match → Originalwert behalten, nie None schreiben
    final_industry = new_industry if new_industry else old_industry
    final_category = new_category if new_category else old_category

    changed = (final_industry != old_industry) or (final_category != old_category)

    return MigrationResult(
        company_id=company["id"],
        name=company.get("name", "—"),
        old_industry=old_industry,
        new_industry=final_industry,
        old_category=old_category,
        new_category=final_category,
        changed=changed,
        industry_matched=bool(new_industry),
        category_matched=bool(new_category),
    )


def fetch_companies(client, names: list[str] | None) -> list[dict]:
    """Lädt alle Companies (oder gefiltert nach Namen)."""
    q = client.table("companies").select("id, name, industry, category")
    if names:
        q = q.in_("name", names)
    resp = q.limit(9999).execute()
    return resp.data or []


def apply_patch(client, result: MigrationResult) -> bool:
    """Schreibt normalisierte Werte in die DB. Gibt True bei Erfolg zurück."""
    try:
        client.table("companies").update({
            "industry": result.new_industry,
            "category": result.new_category,
        }).eq("id", result.company_id).execute()
        return True
    except Exception as e:
        logger.error("PATCH FAILED %s (%s): %s", result.name, result.company_id, e)
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Taxonomy Migration — Argo Analytics")
    parser.add_argument("--apply",   action="store_true", help="Änderungen in die DB schreiben")
    parser.add_argument("--names",   type=str, default=None, help="Komma-separierte Company-Namen (Filter)")
    parser.add_argument("--verbose", action="store_true", help="Auch unveränderte Companies loggen")
    args = parser.parse_args()

    dry_run = not args.apply
    filter_names = [n.strip() for n in args.names.split(",")] if args.names else None

    if dry_run:
        logger.info("DRY-RUN Modus — keine DB-Schreibvorgänge. --apply zum Anwenden.")
    else:
        logger.info("APPLY Modus — schreibt in die DB.")

    client = get_client()

    # Companies laden
    companies = fetch_companies(client, filter_names)
    logger.info("Companies geladen: %d", len(companies))

    # Normalisieren
    results = [normalize_company(c) for c in companies]

    changed   = [r for r in results if r.changed]
    unchanged = [r for r in results if not r.changed]
    no_match  = [r for r in results if not r.industry_matched or not r.category_matched]

    # ── Report ────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("ÄNDERUNGEN (%d von %d Companies):", len(changed), len(results))
    logger.info("=" * 60)

    for r in sorted(changed, key=lambda x: x.name):
        ind_change = f"{r.old_industry!r} → {r.new_industry!r}" if r.old_industry != r.new_industry else f"{r.old_industry!r} (unverändert)"
        cat_change = f"{r.old_category!r} → {r.new_category!r}" if r.old_category != r.new_category else f"{r.old_category!r} (unverändert)"
        logger.info("  %-40s | industry: %s", r.name, ind_change)
        logger.info("  %-40s | category: %s", "",              cat_change)

    if no_match:
        logger.info("")
        logger.info("KEIN TAXONOMY-MATCH (%d) — Originalwert bleibt erhalten:", len(no_match))
        for r in sorted(no_match, key=lambda x: x.name):
            if not r.industry_matched:
                logger.warning("  %-40s | industry kein Match: %r", r.name, r.old_industry)
            if not r.category_matched:
                logger.warning("  %-40s | category kein Match: %r", r.name, r.old_category)

    if args.verbose and unchanged:
        logger.info("")
        logger.info("UNVERÄNDERT (%d):", len(unchanged))
        for r in sorted(unchanged, key=lambda x: x.name):
            logger.info("  %-40s | %r / %r", r.name, r.industry, r.category)

    logger.info("")
    logger.info("Zusammenfassung: %d geändert · %d unverändert · %d ohne Match",
                len(changed), len(unchanged), len(no_match))

    # ── Apply ─────────────────────────────────────────────────────────────────
    if args.apply and changed:
        logger.info("")
        logger.info("Schreibe %d Änderungen in die DB ...", len(changed))
        ok = fail = 0
        for r in changed:
            if apply_patch(client, r):
                ok += 1
            else:
                fail += 1
        logger.info("DB-Update: %d OK · %d FEHLER", ok, fail)
    elif dry_run and changed:
        logger.info("")
        logger.info("Dry-Run abgeschlossen. --apply zum Anwenden der %d Änderungen.", len(changed))


if __name__ == "__main__":
    main()
