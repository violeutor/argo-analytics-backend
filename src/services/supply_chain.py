"""
Supply Chain Tag Mapping
========================
Statisches Mapping: Technologie-Tag → Upstream / Downstream Profiteure.

Struktur je Eintrag:
  upstream:   Rohstoff- / Komponenten-Lieferanten (profitieren von Wachstum des Targets)
  downstream: Abnehmer / Integratoren / Endkunden
  etfs:       Thematische ETFs mit Exposure
  tickers:    Direkte börsennotierte Profiteure mit Begründung

Wird in Phase 2 durch Graph-Modell + Web-Scraping angereichert.
"""

SUPPLY_CHAIN_MAP: dict[str, dict] = {

    # ── Carbon Capture / DAC ──────────────────────────────────────────────────
    "carbon-capture": {
        "upstream": [
            {"ticker": "SLB",  "name": "SLB (Schlumberger)", "exchange": "NYSE",  "role": "Drilling & subsurface tech", "relevance": 0.8},
            {"ticker": "BKR",  "name": "Baker Hughes",       "exchange": "Nasdaq","role": "Industrial compressors & CO₂ handling", "relevance": 0.75},
            {"ticker": "HAL",  "name": "Halliburton",        "exchange": "NYSE",  "role": "Well engineering for CO₂ storage", "relevance": 0.65},
        ],
        "downstream": [
            {"ticker": "XOM",  "name": "ExxonMobil",         "exchange": "NYSE",  "role": "Carbon credit buyer / CCS deployer", "relevance": 0.85},
            {"ticker": "CVX",  "name": "Chevron",            "exchange": "NYSE",  "role": "Carbon offset buyer", "relevance": 0.75},
        ],
        "etfs": [
            {"ticker": "ICLN", "name": "iShares Global Clean Energy ETF", "relevance": 0.6},
            {"ticker": "CTEC", "name": "Global X CleanTech ETF",          "relevance": 0.65},
        ],
    },

    # ── Low-Carbon Cement / Green Cement ──────────────────────────────────────
    "low-carbon-cement": {
        "upstream": [
            {"ticker": "VMC",  "name": "Vulcan Materials",   "exchange": "NYSE",  "role": "Aggregates supplier", "relevance": 0.7},
            {"ticker": "MLM",  "name": "Martin Marietta",    "exchange": "NYSE",  "role": "Aggregates & limestone", "relevance": 0.7},
        ],
        "downstream": [
            {"ticker": "CRH",  "name": "CRH",                "exchange": "NYSE",  "role": "Global cement & building materials buyer", "relevance": 0.95},
            {"ticker": "AMRZ", "name": "Amrize",             "exchange": "NYSE",  "role": "Green cement integrator", "relevance": 0.9},
            {"ticker": "URI",  "name": "United Rentals",     "exchange": "NYSE",  "role": "Construction equipment — demand driver", "relevance": 0.5},
        ],
        "etfs": [
            {"ticker": "IFRA", "name": "iShares US Infrastructure ETF", "relevance": 0.55},
        ],
    },

    # ── Battery / Energy Storage ──────────────────────────────────────────────
    "battery": {
        "upstream": [
            {"ticker": "ALB",  "name": "Albemarle",          "exchange": "NYSE",  "role": "Lithium supplier", "relevance": 0.9},
            {"ticker": "SQM",  "name": "SQM",                "exchange": "NYSE",  "role": "Lithium & specialty chemicals", "relevance": 0.85},
            {"ticker": "LTHM", "name": "Livent",             "exchange": "NYSE",  "role": "Lithium compounds", "relevance": 0.8},
            {"ticker": "MP",   "name": "MP Materials",       "exchange": "NYSE",  "role": "Rare earth elements", "relevance": 0.7},
        ],
        "downstream": [
            {"ticker": "TSLA", "name": "Tesla",              "exchange": "Nasdaq","role": "EV & grid storage integrator", "relevance": 0.85},
            {"ticker": "NEE",  "name": "NextEra Energy",     "exchange": "NYSE",  "role": "Grid-scale storage deployer", "relevance": 0.8},
            {"ticker": "ENPH", "name": "Enphase Energy",     "exchange": "Nasdaq","role": "Home battery integration", "relevance": 0.7},
        ],
        "etfs": [
            {"ticker": "BATT", "name": "Amplify Lithium & Battery Technology ETF", "relevance": 0.85},
            {"ticker": "LIT",  "name": "Global X Lithium & Battery Tech ETF",      "relevance": 0.85},
            {"ticker": "DRIV", "name": "Global X Autonomous & Electric Vehicles",  "relevance": 0.6},
        ],
    },

    # ── Solar ─────────────────────────────────────────────────────────────────
    "solar": {
        "upstream": [
            {"ticker": "ENPH", "name": "Enphase Energy",     "exchange": "Nasdaq","role": "Microinverters & solar components", "relevance": 0.85},
            {"ticker": "SEDG", "name": "SolarEdge",          "exchange": "Nasdaq","role": "Power optimizers", "relevance": 0.8},
        ],
        "downstream": [
            {"ticker": "FSLR", "name": "First Solar",        "exchange": "Nasdaq","role": "Utility-scale solar deployment", "relevance": 0.8},
            {"ticker": "NEE",  "name": "NextEra Energy",     "exchange": "NYSE",  "role": "Largest solar operator in US", "relevance": 0.85},
        ],
        "etfs": [
            {"ticker": "TAN",  "name": "Invesco Solar ETF",             "relevance": 0.9},
            {"ticker": "ICLN", "name": "iShares Global Clean Energy ETF","relevance": 0.7},
        ],
    },

    # ── Hydrogen ──────────────────────────────────────────────────────────────
    "hydrogen": {
        "upstream": [
            {"ticker": "LIN",  "name": "Linde",              "exchange": "Nasdaq","role": "Industrial gas & H₂ infrastructure", "relevance": 0.9},
            {"ticker": "APD",  "name": "Air Products",       "exchange": "NYSE",  "role": "Green hydrogen production", "relevance": 0.85},
        ],
        "downstream": [
            {"ticker": "PLUG", "name": "Plug Power",         "exchange": "Nasdaq","role": "Fuel cell systems deployer", "relevance": 0.8},
            {"ticker": "BE",   "name": "Bloom Energy",       "exchange": "NYSE",  "role": "Stationary fuel cells", "relevance": 0.7},
        ],
        "etfs": [
            {"ticker": "HDRO", "name": "Defiance Next Gen H2 ETF",    "relevance": 0.9},
            {"ticker": "HYDR", "name": "Global X Hydrogen ETF",       "relevance": 0.9},
        ],
    },

    # ── Grid / Power Infrastructure ───────────────────────────────────────────
    "grid": {
        "upstream": [
            {"ticker": "GEV",  "name": "GE Vernova",         "exchange": "NYSE",  "role": "Grid hardware & software", "relevance": 0.9},
            {"ticker": "ETN",  "name": "Eaton",              "exchange": "NYSE",  "role": "Power management systems", "relevance": 0.85},
            {"ticker": "ABB",  "name": "ABB",                "exchange": "NYSE",  "role": "Grid automation & HVDC", "relevance": 0.85},
            {"ticker": "PWR",  "name": "Quanta Services",    "exchange": "NYSE",  "role": "Grid construction & EPC", "relevance": 0.8},
        ],
        "downstream": [
            {"ticker": "NEE",  "name": "NextEra Energy",     "exchange": "NYSE",  "role": "Grid operator & renewable developer", "relevance": 0.9},
            {"ticker": "AEE",  "name": "Ameren",             "exchange": "NYSE",  "role": "Utility grid modernisation", "relevance": 0.65},
        ],
        "etfs": [
            {"ticker": "GRID", "name": "First Trust NASDAQ Clean Edge Smart Grid ETF", "relevance": 0.9},
            {"ticker": "ICLN", "name": "iShares Global Clean Energy ETF",              "relevance": 0.6},
        ],
    },

    # ── Geothermal ────────────────────────────────────────────────────────────
    "geothermal": {
        "upstream": [
            {"ticker": "SLB",  "name": "SLB",                "exchange": "NYSE",  "role": "Drilling technology & services", "relevance": 0.9},
            {"ticker": "HAL",  "name": "Halliburton",        "exchange": "NYSE",  "role": "Well completion services", "relevance": 0.8},
            {"ticker": "BKR",  "name": "Baker Hughes",       "exchange": "Nasdaq","role": "Downhole tools & turbines", "relevance": 0.75},
        ],
        "downstream": [
            {"ticker": "NEE",  "name": "NextEra Energy",     "exchange": "NYSE",  "role": "24/7 baseload buyer / PPA offtaker", "relevance": 0.85},
            {"ticker": "GOOGL","name": "Alphabet",           "exchange": "Nasdaq","role": "AI datacenter clean energy PPA", "relevance": 0.8},
            {"ticker": "MSFT", "name": "Microsoft",         "exchange": "Nasdaq","role": "Carbon-free energy PPA buyer", "relevance": 0.75},
        ],
        "etfs": [
            {"ticker": "ICLN", "name": "iShares Global Clean Energy ETF", "relevance": 0.65},
            {"ticker": "QCLN", "name": "First Trust NASDAQ Clean Edge ETF","relevance": 0.6},
        ],
    },

    # ── Agritech / Precision Agriculture ─────────────────────────────────────
    "agritech": {
        "upstream": [
            {"ticker": "CTVA", "name": "Corteva",            "exchange": "NYSE",  "role": "Seed & crop protection", "relevance": 0.85},
            {"ticker": "DE",   "name": "John Deere",         "exchange": "NYSE",  "role": "Smart farming equipment", "relevance": 0.8},
            {"ticker": "AGCO", "name": "AGCO",               "exchange": "NYSE",  "role": "Precision ag machinery", "relevance": 0.75},
        ],
        "downstream": [
            {"ticker": "NTR",  "name": "Nutrien",            "exchange": "NYSE",  "role": "Fertilizer & agri-services", "relevance": 0.85},
            {"ticker": "ADM",  "name": "Archer-Daniels-Midland","exchange":"NYSE","role": "Grain & commodity buyer", "relevance": 0.65},
        ],
        "etfs": [
            {"ticker": "MOO",  "name": "VanEck Agribusiness ETF", "relevance": 0.8},
            {"ticker": "SOIL", "name": "Global X AgTech & Food Innovation ETF", "relevance": 0.85},
        ],
    },

    # ── CO₂-to-Fuels / SAF ───────────────────────────────────────────────────
    "co2-to-fuels": {
        "upstream": [
            {"ticker": "HON",  "name": "Honeywell",          "exchange": "Nasdaq","role": "SAF process technology partner", "relevance": 0.9},
            {"ticker": "LIN",  "name": "Linde",              "exchange": "Nasdaq","role": "Industrial gas supply", "relevance": 0.7},
        ],
        "downstream": [
            {"ticker": "UAL",  "name": "United Airlines",    "exchange": "Nasdaq","role": "SAF offtake buyer", "relevance": 0.8},
            {"ticker": "DAL",  "name": "Delta Air Lines",    "exchange": "NYSE",  "role": "SAF offtake buyer", "relevance": 0.8},
            {"ticker": "BA",   "name": "Boeing",             "exchange": "NYSE",  "role": "SAF-compatible aircraft", "relevance": 0.65},
        ],
        "etfs": [
            {"ticker": "JETS", "name": "US Global Jets ETF", "relevance": 0.6},
            {"ticker": "CTEC", "name": "Global X CleanTech ETF", "relevance": 0.65},
        ],
    },

    # ── Datacenter Cooling ────────────────────────────────────────────────────
    "datacenter-cooling": {
        "upstream": [
            {"ticker": "CARR", "name": "Carrier Global",     "exchange": "NYSE",  "role": "HVAC systems", "relevance": 0.85},
            {"ticker": "TT",   "name": "Trane Technologies", "exchange": "NYSE",  "role": "Industrial cooling", "relevance": 0.85},
            {"ticker": "JCI",  "name": "Johnson Controls",   "exchange": "NYSE",  "role": "Building management & cooling", "relevance": 0.8},
        ],
        "downstream": [
            {"ticker": "EQIX", "name": "Equinix",            "exchange": "Nasdaq","role": "Global datacenter operator", "relevance": 0.9},
            {"ticker": "DLR",  "name": "Digital Realty",     "exchange": "NYSE",  "role": "Datacenter REIT", "relevance": 0.85},
            {"ticker": "MSFT", "name": "Microsoft",         "exchange": "Nasdaq","role": "Hyperscale datacenter operator", "relevance": 0.8},
        ],
        "etfs": [
            {"ticker": "CLOU", "name": "Global X Cloud Computing ETF", "relevance": 0.55},
            {"ticker": "SRVR", "name": "Pacer Benchmark Data & Infrastructure ETF", "relevance": 0.7},
        ],
    },

    # ── Long-Duration Storage ─────────────────────────────────────────────────
    "long-duration-storage": {
        "upstream": [
            {"ticker": "CLF",  "name": "Cleveland-Cliffs",   "exchange": "NYSE",  "role": "Iron ore / steel for iron-air batteries", "relevance": 0.75},
            {"ticker": "NUE",  "name": "Nucor",              "exchange": "NYSE",  "role": "Steel supply chain", "relevance": 0.65},
        ],
        "downstream": [
            {"ticker": "NEE",  "name": "NextEra Energy",     "exchange": "NYSE",  "role": "Long-duration grid storage buyer", "relevance": 0.85},
            {"ticker": "EDF",  "name": "EDF",                "exchange": "Euronext","role": "EU grid operator & pilot partner", "relevance": 0.9},
        ],
        "etfs": [
            {"ticker": "BATT", "name": "Amplify Lithium & Battery Technology ETF", "relevance": 0.65},
            {"ticker": "GRID", "name": "First Trust Smart Grid ETF",               "relevance": 0.7},
        ],
    },

    # ── Bioengineering / CRISPR Seeds ────────────────────────────────────────
    "bioengineering": {
        "upstream": [
            {"ticker": "ILMN", "name": "Illumina",           "exchange": "Nasdaq","role": "Genomic sequencing equipment", "relevance": 0.75},
            {"ticker": "TMO",  "name": "Thermo Fisher",      "exchange": "NYSE",  "role": "Lab equipment & reagents", "relevance": 0.7},
        ],
        "downstream": [
            {"ticker": "CTVA", "name": "Corteva",            "exchange": "NYSE",  "role": "CRISPR seed commercialisation", "relevance": 0.9},
            {"ticker": "NTR",  "name": "Nutrien",            "exchange": "NYSE",  "role": "Agri-biologicals distribution", "relevance": 0.7},
            {"ticker": "SYT",  "name": "Syngenta (via ChemChina)", "exchange": "—","role": "Crop science integrator", "relevance": 0.65},
        ],
        "etfs": [
            {"ticker": "SOIL", "name": "Global X AgTech & Food Innovation ETF", "relevance": 0.75},
            {"ticker": "ARKG", "name": "ARK Genomic Revolution ETF",            "relevance": 0.7},
        ],
    },

    # ── Waste-to-Energy / Biogas ──────────────────────────────────────────────
    "waste-to-energy": {
        "upstream": [
            {"ticker": "RSG",  "name": "Republic Services",  "exchange": "NYSE",  "role": "Waste collection & landfill gas", "relevance": 0.8},
            {"ticker": "WM",   "name": "Waste Management",   "exchange": "NYSE",  "role": "Largest US waste operator", "relevance": 0.8},
        ],
        "downstream": [
            {"ticker": "BEP",  "name": "Brookfield Renewable","exchange":"NYSE",  "role": "Renewable energy offtake", "relevance": 0.7},
            {"ticker": "NEE",  "name": "NextEra Energy",     "exchange": "NYSE",  "role": "Biogas-to-grid buyer", "relevance": 0.65},
        ],
        "etfs": [
            {"ticker": "ICLN", "name": "iShares Global Clean Energy ETF", "relevance": 0.55},
        ],
    },

}


# ── Tag → Supply Chain lookup ─────────────────────────────────────────────────

def get_supply_chain(tags: list[str]) -> dict:
    """
    Gibt aggregierte Upstream/Downstream/ETF-Listen für eine Tag-Liste zurück.
    Dedupliziert nach Ticker, höchste Relevanz gewinnt.
    """
    upstream: dict[str, dict]   = {}
    downstream: dict[str, dict] = {}
    etfs: dict[str, dict]       = {}

    for tag in tags:
        tag_clean = tag.lower().replace(" ", "-")
        mapping = SUPPLY_CHAIN_MAP.get(tag_clean, {})

        for item in mapping.get("upstream", []):
            t = item["ticker"]
            if t not in upstream or upstream[t]["relevance"] < item["relevance"]:
                upstream[t] = item

        for item in mapping.get("downstream", []):
            t = item["ticker"]
            if t not in downstream or downstream[t]["relevance"] < item["relevance"]:
                downstream[t] = item

        for item in mapping.get("etfs", []):
            t = item["ticker"]
            if t not in etfs or etfs[t]["relevance"] < item["relevance"]:
                etfs[t] = item

    return {
        "upstream":   sorted(upstream.values(),   key=lambda x: -x["relevance"]),
        "downstream": sorted(downstream.values(), key=lambda x: -x["relevance"]),
        "etfs":       sorted(etfs.values(),       key=lambda x: -x["relevance"]),
    }


# ── Company → Tag mapping (für Seed-Daten) ───────────────────────────────────

COMPANY_TAGS: dict[str, list[str]] = {
    "Climeworks":         ["carbon-capture"],
    "Charm Industrial":   ["carbon-capture"],
    "Heirloom":           ["carbon-capture"],
    "Twelve":             ["carbon-capture", "co2-to-fuels"],
    "LanzaTech":          ["co2-to-fuels"],
    "CarbonCure":         ["low-carbon-cement", "carbon-capture"],
    "Running Tide":       ["carbon-capture"],
    "Living Carbon":      ["bioengineering"],
    "Verdox":             ["carbon-capture"],
    "Carbon Clean":       ["carbon-capture"],
    "Brimstone":          ["low-carbon-cement"],
    "Sublime Systems":    ["low-carbon-cement"],
    "Solugen":            ["carbon-capture"],
    "CropX":              ["agritech"],
    "Brightmark":         ["waste-to-energy"],
    "Indigo Ag":          ["agritech", "carbon-capture"],
    "Pairwise":           ["bioengineering", "agritech"],
    "Loam Bio":           ["agritech", "carbon-capture"],
    "Enapter":            ["hydrogen"],
    "Emerald AI":         ["grid"],
    "VoltaGrid":          ["grid"],
    "Base Power":         ["battery", "grid"],
    "GRST":               ["battery"],
    "HT Materials Science":["datacenter-cooling"],
    "Relectrify":         ["battery"],
    "WAVR Technologies":  ["datacenter-cooling"],
    "Factorial Energy":   ["battery"],
    "Syzygy Plasmonics":  ["co2-to-fuels"],
    "Ore Energy":         ["long-duration-storage", "battery"],
    "Fervo Energy":       ["geothermal"],
    "Moment Energy":      ["battery"],
    "Beehive":            ["grid"],
    "SunCulture":         ["solar", "agritech"],
    "Netafim":            ["agritech"],
    "ClimateAi":          ["agritech"],
    "Agmatix":            ["agritech"],
    "Micropep":           ["bioengineering", "agritech"],
    "Amini":              ["agritech"],
    "12Tree":             ["carbon-capture", "agritech"],
    "Notpla":             ["carbon-capture"],
    "Noya":               ["carbon-capture"],
    "Remora":             ["carbon-capture"],
}
