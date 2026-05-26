"""
taxonomy.py — Argo Analytics Sektor & Kategorie Taxonomy v1.0
==============================================================
SSOT für alle Sektor/Kategorie-Klassifikationen im System.

Ersetzt:
  - PATENT_SCORING_SECTORS   (signal_engine.py)
  - TRENDS_RELEVANT_SECTORS  (signal_engine.py)
  - _ETF_COVERED_CATEGORIES  (score_calculator.py)
  - _MATURE_CATEGORIES       (market_data_enrichment.py)

Import:
  from taxonomy import (
      TAXONOMY, SECTOR_NAMES, ALL_CATEGORIES, CATEGORY_TO_SECTOR,
      PATENT_SCORING_SECTORS, TRENDS_RELEVANT_SECTORS,
      ETF_COVERED_CATEGORIES, MATURE_CATEGORIES,
      normalize_sector, normalize_category,
      ALL_CATEGORIES_FOR_PROMPT,
  )
"""

from __future__ import annotations

# ── Core Taxonomy ─────────────────────────────────────────────────────────────
# 14 Sektoren · 68 Kategorien
# Reihenfolge ist bewusst — VC/PE-relevante Sektoren zuerst.

TAXONOMY: dict[str, list[str]] = {
    "Energy & Power": [
        "Solar PV",
        "Wind Energy",
        "Geothermal",
        "Nuclear",
        "Grid & Smart Energy",
        "Energy Storage",
        "Hydrogen & Fuel Cells",
        "Bioenergy",
    ],
    "Mobility & Transport": [
        "Electric Vehicles",
        "Aviation & SAF",
        "Rail Tech",
        "Autonomous Mobility",
        "Logistics & Supply Chain",
    ],
    "Carbon & Climate": [
        "Direct Air Capture",
        "Carbon Capture & Storage",
        "CO₂ Utilization",
        "Maritime Decarbonization",
        "Carbon Markets & Credits",
        "Climate Analytics & ESG",
        "Nature-Based Solutions",
    ],
    "Industrial & Manufacturing": [
        "Industrial Automation",
        "Advanced Manufacturing",
        "Robotics",
        "Heat & Process Decarbonization",
        "Industrial Software",
        "Waste-to-Energy",
    ],
    "Materials & Chemicals": [
        "Advanced Materials",
        "Green Chemicals",
        "Circular Economy",
        "Semiconductors",
        "Battery Materials",
        "Composites & Polymers",
    ],
    "Agriculture & Food": [
        "Precision Farming",
        "FoodTech",
        "Alternative Proteins",
        "AgriTech SaaS",
        "Aquaculture",
        "Fertilizer & Soil Tech",
    ],
    "Built Environment": [
        "Construction Tech",
        "Smart Buildings",
        "Heat Pumps & HVAC",
        "Real Estate Tech",
        "Water Infrastructure",
    ],
    "Life Sciences & Health": [
        "BioTech",
        "MedTech",
        "HealthTech",
        "Pharma",
        "Diagnostics & Genomics",
    ],
    "Digital Infrastructure": [
        "AI & Machine Learning",
        "Cloud & Infrastructure",
        "SaaS & Enterprise Software",
        "Cybersecurity",
        "Developer Tools",
        "Data & Analytics",
        "IoT & Edge Computing",
    ],
    "Financial Services": [
        "FinTech",
        "InsurTech",
        "Green Finance",
        "Wealth & Asset Management",
    ],
    "Consumer & Commerce": [
        "eCommerce",
        "Consumer Tech",
        "EdTech",
        "Travel Tech",
        "Media & Entertainment",
    ],
    "Space & Defense": [
        "Space Tech",
        "Aerospace",
        "Drones & UAV",
        "Defense Tech",
    ],
    "Water & Circular Economy": [
        "Water Technology",
        "Waste Management",
        "Recycling & Upcycling",
        "Plastic Alternatives",
    ],
    "Mining & Resources": [
        "Critical Minerals",
        "Mining Tech",
        "Oil & Gas Tech",
    ],
}

# Convenience
SECTOR_NAMES: list[str] = list(TAXONOMY.keys())
ALL_CATEGORIES: list[str] = [cat for cats in TAXONOMY.values() for cat in cats]

# Reverse lookup: Kategorie → Sektor (O(1))
CATEGORY_TO_SECTOR: dict[str, str] = {
    cat: sector
    for sector, cats in TAXONOMY.items()
    for cat in cats
}

# ── Derived Sets — Signal Engine & Scoring ───────────────────────────────────

# SE-14: Kategorien wo Patent-Aktivität in tech_readiness einfließt.
# Deep Tech, Chemie, Materialien, Life Sciences, Hardware-intensive Sektoren.
PATENT_SCORING_SECTORS: frozenset[str] = frozenset({
    # Energy & Power
    "Energy Storage",
    "Hydrogen & Fuel Cells",
    "Geothermal",
    "Nuclear",
    # Carbon & Climate
    "Direct Air Capture",
    "Carbon Capture & Storage",
    "CO₂ Utilization",
    # Materials & Chemicals
    "Advanced Materials",
    "Green Chemicals",
    "Battery Materials",
    "Composites & Polymers",
    "Semiconductors",
    # Industrial
    "Heat & Process Decarbonization",
    # Life Sciences
    "BioTech",
    "MedTech",
    "Pharma",
    "Diagnostics & Genomics",
    # Mining
    "Critical Minerals",
})

# SE-15: Kategorien wo Google Trends ein sinnvolles Nachfrage-Signal liefert.
# Primär: Consumer-seitige Software/SaaS/Plattformen — Suchvolumen = Marktnachfrage.
# Explizit NICHT: Deep Tech / B2B Industrial (niemand googelt "Elektrolyse-Stack").
TRENDS_RELEVANT_SECTORS: frozenset[str] = frozenset({
    # Digital Infrastructure
    "SaaS & Enterprise Software",
    "AI & Machine Learning",
    "Cloud & Infrastructure",
    "Cybersecurity",
    "Developer Tools",
    "Data & Analytics",
    "IoT & Edge Computing",
    # Financial Services
    "FinTech",
    "InsurTech",
    # Consumer
    "Consumer Tech",
    "eCommerce",
    "EdTech",
    "Travel Tech",
    "Media & Entertainment",
    # Carbon & Climate (consumer-facing)
    "Climate Analytics & ESG",
    "Carbon Markets & Credits",
    # Agriculture (SaaS-only)
    "AgriTech SaaS",
    # Health
    "HealthTech",
})

# SC-03 / SC-09: Kategorien mit aktivem ETF-Coverage.
# Ermittelt ETF-Score-Boost in compute_etf_score().
ETF_COVERED_CATEGORIES: frozenset[str] = frozenset({
    # Energy
    "Solar PV",
    "Wind Energy",
    "Energy Storage",
    "Hydrogen & Fuel Cells",
    "Grid & Smart Energy",
    "Geothermal",
    "Nuclear",
    "Bioenergy",
    # Mobility
    "Electric Vehicles",
    "Autonomous Mobility",
    # Carbon
    "Direct Air Capture",
    "Carbon Capture & Storage",
    # Materials
    "Semiconductors",
    "Battery Materials",
    "Advanced Materials",
    # Life Sciences
    "BioTech",
    "MedTech",
    # Digital
    "AI & Machine Learning",
    "Cloud & Infrastructure",
    "SaaS & Enterprise Software",
    "Cybersecurity",
    # Space & Defense
    "Space Tech",
    "Aerospace",
    "Drones & UAV",
    # Industrial
    "Industrial Automation",
    "Robotics",
    # Resources
    "Critical Minerals",
    # Other
    "FinTech",
    "Precision Farming",
    "Heat Pumps & HVAC",
    "Logistics & Supply Chain",
})

# MD-B06: Kategorien wo listed + kein Peer-Funding-Data → "mature" als Marktzyklus-Fallback.
# Traditionelle, reife Industrien wo "early" als Default falsch wäre.
MATURE_CATEGORIES: frozenset[str] = frozenset({
    "Industrial Automation",
    "Industrial Software",
    "Waste-to-Energy",
    "Green Chemicals",
    "SaaS & Enterprise Software",
    "Oil & Gas Tech",
    "Mining Tech",
    "Logistics & Supply Chain",
    "Real Estate Tech",
    "Fertilizer & Soil Tech",
    "Composites & Polymers",
    "Water Infrastructure",
})

# ── Normalisierung ────────────────────────────────────────────────────────────

def _normalize_str(s: str) -> str:
    """Normalisiert für Vergleich: lowercase + & ↔ and angleichen."""
    return s.lower().replace(" & ", " and ").replace("&", "and").strip()


def normalize_sector(value: str | None) -> str | None:
    """
    Normalisiert einen Sektor-String auf den nächsten Taxonomy-Match.
    Reihenfolge: Exakt → Case-insensitive (inkl. & ↔ and) → Startswith → Partial.
    Gibt None zurück wenn kein Match — nie halluzinierte Werte in die DB schreiben.
    """
    if not value:
        return None
    v = value.strip()
    # 1) Exakter Match
    if v in TAXONOMY:
        return v
    v_norm = _normalize_str(v)
    # 2) Normalisierter Exakt-Match (& ↔ and, case)
    for sector in SECTOR_NAMES:
        if _normalize_str(sector) == v_norm:
            return sector
    # 3) Startswith (z.B. "Energy" → "Energy & Power")
    for sector in SECTOR_NAMES:
        if _normalize_str(sector).startswith(v_norm) or v_norm.startswith(_normalize_str(sector)):
            return sector
    # 4) Substring-Match
    for sector in SECTOR_NAMES:
        if v_norm in _normalize_str(sector) or _normalize_str(sector) in v_norm:
            return sector
    return None


def normalize_category(value: str | None, sector: str | None = None) -> str | None:
    """
    Normalisiert einen Kategorie-String auf den nächsten Taxonomy-Match.
    Optional sector-gefiltert — schnellerer + präziserer Match.
    Priorität: Exakt → Case-insensitive → Startswith → Substring.
    Startswith vor Substring verhindert Fehl-Matches (z.B. "saas" → "SaaS & Enterprise Software",
    nicht "AgriTech SaaS").
    Fallback auf alle Kategorien wenn sector-gefiltert kein Treffer.
    Gibt None zurück wenn kein Match.
    """
    if not value:
        return None
    v = value.strip()
    candidates = TAXONOMY.get(sector, ALL_CATEGORIES) if sector else ALL_CATEGORIES

    # 1) Exakter Match
    if v in candidates:
        return v
    v_norm = _normalize_str(v)
    # 2) Normalisierter Exakt-Match
    for cat in candidates:
        if _normalize_str(cat) == v_norm:
            return cat
    # 3) Startswith — verhindert Substring-Fehlmatches
    for cat in candidates:
        if _normalize_str(cat).startswith(v_norm):
            return cat
    # 4) Substring
    for cat in candidates:
        if v_norm in _normalize_str(cat) or _normalize_str(cat) in v_norm:
            return cat

    # Wenn sector-gefiltert kein Match → alle Kategorien versuchen
    if sector and candidates is not ALL_CATEGORIES:
        return normalize_category(value, sector=None)
    return None


def is_patent_relevant(category: str | None, industry: str | None = None) -> bool:
    """Prüft ob Patent-Scoring für diese Company aktiv ist."""
    return bool(
        (category and category in PATENT_SCORING_SECTORS)
        or (industry and industry in PATENT_SCORING_SECTORS)
    )


def is_trends_relevant(category: str | None, industry: str | None = None) -> bool:
    """Prüft ob Google Trends ein sinnvolles Signal liefert."""
    return bool(
        (category and category in TRENDS_RELEVANT_SECTORS)
        or (industry and industry in TRENDS_RELEVANT_SECTORS)
    )


def is_etf_covered(category: str | None, industry: str | None = None) -> bool:
    """Prüft ob ETF-Coverage für diese Kategorie/Sektor vorhanden."""
    return bool(
        (category and category in ETF_COVERED_CATEGORIES)
        or (industry and industry in ETF_COVERED_CATEGORIES)
    )


def is_mature_market(category: str | None) -> bool:
    """Prüft ob der Markt als reif gilt (MD-B06 Marktzyklus-Fallback)."""
    return bool(category and category in MATURE_CATEGORIES)


# ── Prompt Helpers ────────────────────────────────────────────────────────────

SECTOR_LIST_FOR_PROMPT: str = "\n".join(f"- {s}" for s in SECTOR_NAMES)

ALL_CATEGORIES_FOR_PROMPT: str = "\n".join(
    f"[{sector}]\n" + "\n".join(f"  - {cat}" for cat in cats)
    for sector, cats in TAXONOMY.items()
)

ENRICHMENT_TAXONOMY_BLOCK: str = f"""
SECTOR & CATEGORY CLASSIFICATION (MANDATORY):
You MUST select exactly one sector and one category from the lists below.
Never invent values. Never use synonyms or translations.
If nothing fits well, pick the closest match.

Allowed sectors:
{SECTOR_LIST_FOR_PROMPT}

Allowed categories per sector:
{ALL_CATEGORIES_FOR_PROMPT}

Output the exact string as shown — no abbreviations, no reformatting.
""".strip()
