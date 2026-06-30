"""
valuation.py — Target-Bewertung & Multiplikator-SSOT
=====================================================
VALUATION-SSOT-01: Die EINE Quelle für Target-Bewertung. Ersetzt die zwei
divergierenden Stage-Tabellen (Frontend page.tsx STAGE_MULT + scoring.py
_STAGE_MULTIPLIERS) durch eine konsolidierte, datengestützte Tabelle.

WICHTIG — Abgrenzung:
  Dieses Modul liefert NUR die Target-Bewertung (funding × stage × vertical)
  und FX-Normalisierung. Es berechnet KEIN MFR/SRR — die leben in
  src/pipelines/scoring.py (die echte Per-Buyer-Deal-Engine). Kein
  konkurrierendes Scoring hier, sonst Drift.

Bewertungsformel:
  target_valuation_usd_mn = funding_total_usd_mn
                            × STAGE_MULT[stage]
                            × VERTICAL_DELTA.get(vertical, 1.0)

  Listed:  eigene market_cap (nativ → USD), kein Stage/Vertical.

Multiplikatoren = Faustregel-Heuristik (keine präzise Einzelbewertung).
Für ein Feasibility-Rating in Tiers reicht die Größenordnung. Datenbasis:
Carta/Pitchbook/Dealroom 2025-Medianwerte (kumuliertes Funding → Bewertung).
Feinjustierung der Dicts ändert NICHTS an der Logik — bewusst entkoppelt.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# ── Stage-Multiplikatoren (SSOT — konsolidiert aus zwei Alt-Tabellen) ─────────
# Hebel funding_total (kumuliert) -> Bewertung. Monoton fallend: frueher Stage =
# hoeherer Hebel pro Dollar (reine Potenzialbewertung), spaeter = naeher am Markt.
# Kalibriert gegen 2025-Mediane (Carta/Pitchbook): pre-seed ~$1M->$10-15M (~8-10x),
# Series A ~$15M->$45M (~3x, AI zieht hoch), Series B ~$40M->$119M (~3x),
# Series C+ ~$90M->$300M+ (~3x, Late-Stage-Recovery), Series D ~$200M->$460M (~2x).
STAGE_MULT: dict[str, float] = {
    "pre_seed":      6.0,
    "seed":          5.0,
    "series_a":      3.5,
    "series_b":      3.0,
    "series_c":      2.5,
    "series_d":      2.0,
    "series_d_plus": 2.0,
    "growth":        1.5,
    "pre_ipo":       1.3,
    "public":        1.0,   # Gate - bei listed eigene market_cap nutzen
}
_STAGE_MULT_DEFAULT = 2.5   # zwischen Series B/C - neutraler Mittelwert fuer Unbekannt

# Stage-Aliase -> kanonischer Key (vor Lookup normalisieren)
_STAGE_ALIASES: dict[str, str] = {
    "series_d+":  "series_d_plus",
    "series_e":   "series_d_plus",
    "late":       "series_d_plus",
    "series_a_1": "series_a",
    "series_a_2": "series_a",
}


# ── Vertical-Delta (optionaler Korrektur-Faktor obendrauf) ────────────────────
# Multiplikativ auf den Stage-Wert. Default 1.0 -> unbekanntes Vertical = neutral
# (faellt sauber auf reinen Stage-Multiplikator zurueck, kein Bruch).
# Keys = 14-Sektor-Taxonomie (user_industry_preferences.sector_key).
#
# Basis (2025-Recherche):
#   AI/Deep-Tech/SaaS mit hohem Wachstum -> Premium-Multiples (Carta: AI +42-100%)
#   Climate/Energy/Industrial -> kapitalintensiv, moderatere Multiples
#   Biotech -> eigene Logik (klinische Meilensteine), hier konservativ
# Feinjustierbar ohne Logik-Eingriff.
VERTICAL_DELTA: dict[str, float] = {
    "deep_tech":            1.30,   # AI/LLM-Infra Premium (Carta: ~2x Median)
    "enterprise_software":  1.20,   # SaaS hohe Multiples bei Wachstum
    "fintech":              1.10,
    "health_tech":          1.05,
    "space_defense":        1.05,   # strategischer Premium, duenner Markt
    "media_entertainment":  1.00,
    "consumer_tech":        1.00,
    "mobility":             0.95,
    "real_estate_proptech": 0.95,
    "climate_tech":         0.90,   # kapitalintensiv, langsamerer Hebel
    "energy_transition":    0.90,
    "industrial_tech":      0.90,
    "food_agritech":        0.88,
    "biotech_pharma":       0.85,   # Meilenstein-getrieben, hoher Burn pro Stage
}
_VERTICAL_DELTA_DEFAULT = 1.0


# ── EXIT_ADJUSTMENT (VALUATION-MULTIPLE-01, S78) ──────────────────────────────
# Dritte, optionale Multiplikator-Schicht oben auf funding_x_stage —
# korrigiert die Funding-Runden-implizite Bewertung (was VCs für einen
# Minderheitsanteil zahlen) in Richtung dessen, was ein M&A-Käufer für die
# GANZE Company tatsächlich gezahlt hat (Kontrollübernahme, oft systematisch
# abweichend — Down-Rounds bei Exits sind keine Ausnahme). Siehe Diskussion
# S78: valuation.py bleibt bewusst der Goldstandard für die Funding-implizite
# Schätzung, EXIT_ADJUSTMENT ist eine zusätzliche, transparente Korrektur
# obendrauf — kein Ersatz, beide Werte koexistieren in der API-Response.
#
# LEER bei Einführung (S78) — befüllt erst durch echte Kalibrierung gegen
# comparable_transactions (s. src/services/valuation_calibration.py),
# Mindeststichprobe n≥5 pro (industry, stage)-Bucket. Bis dahin liefert
# exit_adjustment() für JEDEN Bucket den neutralen Default 1.0 — die
# Funding-Bewertung bleibt unverändert sichtbar, kein erfundener Exit-Wert
# aus zu wenig Daten. Format identisch zu VERTICAL_DELTA (verschachteltes
# Dict aus Übersichtlichkeit: {industry: {stage: factor}}), Schlüssel
# IDENTISCH zu VERTICAL_DELTA/STAGE_MULT — kein drittes Vokabular.
EXIT_ADJUSTMENT: dict[str, dict[str, float]] = {}
_EXIT_ADJUSTMENT_DEFAULT = 1.0
_EXIT_ADJUSTMENT_MIN_SAMPLE = 5   # gleiche Vorsicht wie _MIN_POOL beim Buyer-Pool


def exit_adjustment(industry: str | None, funding_stage: str | None) -> float:
    """
    Lookup mit Default 1.0 (neutral) — fällt sauber zurück, kein Bruch,
    solange EXIT_ADJUSTMENT leer oder der Bucket nicht kalibriert ist.
    """
    industry_key = (industry or "").lower().strip()
    stage_key = (funding_stage or "").lower().strip().replace(" ", "_").replace("-", "_")
    stage_key = _STAGE_ALIASES.get(stage_key, stage_key)
    return EXIT_ADJUSTMENT.get(industry_key, {}).get(stage_key, _EXIT_ADJUSTMENT_DEFAULT)


def compute_exit_adjusted_valuation(company: dict) -> dict:
    """
    VALUATION-MULTIPLE-01: erweitert compute_target_valuation() um die
    EXIT_ADJUSTMENT-Schicht — NUR für method='funding_x_stage' relevant
    (Listed-Companies haben bereits eine echte market_cap, keine Korrektur
    nötig/sinnvoll). Gibt IMMER beide Werte zurück (value_usd_mn = Basis,
    exit_adjusted_value_usd_mn = korrigiert) — Frontend zeigt beide
    nebeneinander (Andreas, S78: "Est. Valuation" + "M&A Multiple" als
    zwei Kacheln, nicht eine ersetzt die andere).

    is_calibrated=False bedeutet: exit_adjustment_factor ist der neutrale
    Default 1.0, exit_adjusted_value_usd_mn == value_usd_mn (keine
    Information verloren, aber auch keine erfundene Korrektur).
    """
    base = compute_target_valuation(company)
    if base["method"] != "funding_x_stage" or base["value_usd_mn"] is None:
        return {**base, "exit_adjustment_factor": None,
                "exit_adjusted_value_usd_mn": base["value_usd_mn"], "is_calibrated": False}

    industry = company.get("industry")
    stage = company.get("funding_stage")
    factor = exit_adjustment(industry, stage)
    is_calibrated = factor != _EXIT_ADJUSTMENT_DEFAULT

    return {
        **base,
        "exit_adjustment_factor": factor,
        "exit_adjusted_value_usd_mn": round(base["value_usd_mn"] * factor, 2),
        "is_calibrated": is_calibrated,
    }


# ── FX -> USD (statische Fallback-Kurse, grob jaehrlich nachziehen) ───────────
# Nur Groessenordnung fuer Tiers - keine Live-Rates (Scheingenauigkeit vermeiden).
_FX_TO_USD: dict[str, float] = {
    "USD": 1.00, "EUR": 1.08, "GBP": 1.27, "GBp": 0.0127,
    "CHF": 1.12, "SEK": 0.095, "NOK": 0.092, "DKK": 0.145,
    "JPY": 0.0064, "CAD": 0.73, "AUD": 0.66,
}


def to_usd(amount: float | None, currency: str | None) -> float | None:
    """Konvertiert Betrag nach USD via statischem Kurs. Unbekannte Waehrung -> None."""
    if amount is None:
        return None
    cur = (currency or "USD").strip()
    rate = _FX_TO_USD.get(cur) or _FX_TO_USD.get(cur.upper())
    if rate is None:
        logger.debug("to_usd: unbekannte Waehrung '%s'", currency)
        return None
    return round(amount * rate, 4)


# ── Multiplikator-Lookups ─────────────────────────────────────────────────────

def stage_multiplier(funding_stage: str | None) -> float:
    """Stage-Multiplikator (mit Alias-Normalisierung, Default 2.5)."""
    key = (funding_stage or "").lower().strip().replace(" ", "_").replace("-", "_")
    key = _STAGE_ALIASES.get(key, key)
    return STAGE_MULT.get(key, _STAGE_MULT_DEFAULT)


def vertical_delta(vertical: str | None) -> float:
    """Vertical-Korrektur-Faktor (Default 1.0 = neutral)."""
    key = (vertical or "").lower().strip()
    return VERTICAL_DELTA.get(key, _VERTICAL_DELTA_DEFAULT)


# ── Target-Bewertung (SSOT) ───────────────────────────────────────────────────

def compute_target_valuation(company: dict) -> dict:
    """
    Einheitliche Target-Bewertung in USD-Mio - EINE Quelle fuer scoring.py + Frontend.

    Listed:  eigene market_cap (nativ -> USD). method='market_cap'.
    Private: funding_total x stage_mult x vertical_delta. method='funding_x_stage'.
    Kein verlaesslicher Input -> value_usd_mn=None, method='none'.

    Returns dict mit value_usd_mn, method, stage_mult, vertical_delta, inputs.
    """
    inputs: dict = {}

    if company.get("is_listed"):
        mcap = company.get("market_cap_usd_mn")
        if mcap is not None:
            inputs["market_cap_usd_mn"] = mcap
            return {"value_usd_mn": round(float(mcap), 2), "method": "market_cap",
                    "stage_mult": None, "vertical_delta": None, "inputs": inputs}
        mcap_native = company.get("market_cap")
        currency    = company.get("market_cap_currency") or company.get("currency")
        if mcap_native is not None:
            mcap_usd = to_usd(float(mcap_native), currency)
            inputs["market_cap_native"] = mcap_native
            inputs["market_cap_currency"] = currency
            if mcap_usd is not None:
                value_mn = round(mcap_usd / 1e6, 2) if mcap_usd > 1e6 else round(mcap_usd, 2)
                return {"value_usd_mn": value_mn, "method": "market_cap",
                        "stage_mult": None, "vertical_delta": None, "inputs": inputs}

    funding = company.get("funding_total_usd_mn")
    if funding is not None:
        s_mult = stage_multiplier(company.get("funding_stage"))
        v_delt = vertical_delta(company.get("industry"))
        inputs.update({"funding_total_usd_mn": funding,
                       "funding_stage": company.get("funding_stage"),
                       "vertical": company.get("industry")})
        return {"value_usd_mn": round(float(funding) * s_mult * v_delt, 2),
                "method": "funding_x_stage", "stage_mult": s_mult,
                "vertical_delta": v_delt, "inputs": inputs}

    return {"value_usd_mn": None, "method": "none",
            "stage_mult": None, "vertical_delta": None, "inputs": inputs}


def estimated_ev_usd_bn(company: dict) -> float | None:
    """Convenience: Target-Bewertung in Mrd USD (fuer scoring.py MFR). None wenn nicht bewertbar."""
    tv = compute_target_valuation(company)
    if tv["value_usd_mn"] is None:
        return None
    return round(tv["value_usd_mn"] / 1000.0, 6)
