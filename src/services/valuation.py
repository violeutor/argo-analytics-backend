"""
valuation.py — Target-Bewertung, FX-Normalisierung & MFR (SSOT)
================================================================
VALUATION-SSOT-01: Zieht die Bewertungslogik aus dem Frontend (page.tsx,
STAGE_MULT + estVal) ins Backend, damit Scoring (SC-02) und Anzeige
DIESELBE Quelle nutzen. Kein Logik-Drift zwischen Frontend und Scoring.

Zwei Bewertungspfade:
  - Private:  funding_total_usd_mn × STAGE_MULT[funding_stage]  (VC-Faustregel)
  - Listed:   eigene market_cap (in nativer Währung) → USD normalisiert

MFR (Merger Feasibility Rating) — BUYER-MFR-01:
  Pro Käufer: Verhältnis Buyer-Marktkapitalisierung ÷ Target-Bewertung.
  Etabliertes M&A-Konzept (anders als das proprietäre SRR).
  Schwellen siehe _MFR_TIERS — abgeleitet aus der "Buyer ≥ 5× Target"-Regel
  der Enrichment-Pipeline, erweitert um Stretch/Unfeasible-Stufen.

FX-Hinweis (bewusste Entscheidung):
  Statische Fallback-Kurse statt Live-FX. MFR denkt in Tiers (Feasible/
  Stretch/Unfeasible), nicht in Nachkommastellen — eine Größenordnung
  reicht. Live-FX wäre Scheingenauigkeit. Kurse jährlich grob nachziehen.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# ── Stage-Multiplikatoren (SSOT — identisch zur bisherigen Frontend-Tabelle) ──
# Quelle: page.tsx STAGE_MULT (VC-Faustregel). Frühe Stages höher (mehr
# Wachstumserwartung pro Funding-Dollar), späte Stages niedriger.
STAGE_MULT: dict[str, float] = {
    "pre_seed":      5.0,
    "seed":          4.0,
    "series_a":      3.5,
    "series_b":      2.5,
    "series_c":      2.0,
    "series_d":      1.5,
    "series_d_plus": 1.3,
    "growth":        1.2,
}
_STAGE_MULT_DEFAULT = 2.0   # identisch Frontend-Fallback (?? 2.0)


# ── FX → USD (statische Fallback-Kurse, grob jährlich nachziehen) ─────────────
# Nur Größenordnung für MFR-Tiers — keine Live-Rates (s. Modul-Docstring).
_FX_TO_USD: dict[str, float] = {
    "USD": 1.00,
    "EUR": 1.08,
    "GBP": 1.27,
    "GBp": 0.0127,   # London notiert teils in Pence (1/100 GBP)
    "CHF": 1.12,
    "SEK": 0.095,
    "NOK": 0.092,
    "DKK": 0.145,
    "JPY": 0.0064,
    "CAD": 0.73,
    "AUD": 0.66,
}


def to_usd(amount: float | None, currency: str | None) -> float | None:
    """
    Konvertiert einen Betrag nach USD via statischem Fallback-Kurs.
    Unbekannte Währung → None (kein stiller 1:1-Fehler, der MFR verfälscht).
    """
    if amount is None:
        return None
    cur = (currency or "USD").strip()
    rate = _FX_TO_USD.get(cur)
    if rate is None:
        # Case-insensitiver Zweitversuch (außer GBp, das ist case-signifikant)
        rate = _FX_TO_USD.get(cur.upper())
    if rate is None:
        logger.debug("to_usd: unbekannte Währung '%s' — kein Kurs", currency)
        return None
    return round(amount * rate, 4)


# ── Target-Bewertung (SSOT) ───────────────────────────────────────────────────

def stage_multiplier(funding_stage: str | None) -> float:
    """Stage-Multiplikator für eine Funding-Stage (Default 2.0)."""
    return STAGE_MULT.get((funding_stage or "").strip(), _STAGE_MULT_DEFAULT)


def compute_target_valuation(company: dict) -> dict:
    """
    Einheitliche Target-Bewertung in USD-Mio — die EINE Quelle für Frontend + Scoring.

    Listed:  eigene market_cap (nativ → USD). method='market_cap'.
    Private: funding_total_usd_mn × stage_multiplier. method='funding_x_stage'.
    Kein verlässlicher Input → value_usd_mn=None, method='none'.

    Returns:
        {
          "value_usd_mn":   float | None,   # Bewertung in Mio USD
          "method":         str,            # 'market_cap' | 'funding_x_stage' | 'none'
          "stage_mult":     float | None,   # nur bei funding_x_stage
          "inputs":         dict,           # Nachvollziehbarkeit (Datentransparenz)
        }
    """
    inputs: dict = {}

    is_listed = bool(company.get("is_listed"))

    # ── Listed: eigene Marktkapitalisierung ──────────────────────────────────
    if is_listed:
        mcap = company.get("market_cap_usd_mn")
        if mcap is not None:
            inputs["market_cap_usd_mn"] = mcap
            return {
                "value_usd_mn": round(float(mcap), 2),
                "method":       "market_cap",
                "stage_mult":   None,
                "inputs":       inputs,
            }

        # Fallback: market_cap nur in nativer Währung vorhanden → normalisieren
        mcap_native = company.get("market_cap")
        currency    = company.get("market_cap_currency") or company.get("currency")
        if mcap_native is not None:
            mcap_usd = to_usd(float(mcap_native), currency)
            inputs["market_cap_native"]   = mcap_native
            inputs["market_cap_currency"] = currency
            if mcap_usd is not None:
                # market_cap ist meist absolut → in Mio USD bringen, falls > 1e6
                value_mn = round(mcap_usd / 1e6, 2) if mcap_usd > 1e6 else round(mcap_usd, 2)
                return {
                    "value_usd_mn": value_mn,
                    "method":       "market_cap",
                    "stage_mult":   None,
                    "inputs":       inputs,
                }
        # Listed, aber keine market_cap → fällt durch auf funding (selten)

    # ── Private: funding_total × Stage-Multiplikator ─────────────────────────
    funding = company.get("funding_total_usd_mn")
    if funding is not None:
        mult = stage_multiplier(company.get("funding_stage"))
        inputs["funding_total_usd_mn"] = funding
        inputs["funding_stage"]        = company.get("funding_stage")
        return {
            "value_usd_mn": round(float(funding) * mult, 2),
            "method":       "funding_x_stage",
            "stage_mult":   mult,
            "inputs":       inputs,
        }

    # ── Kein verlässlicher Input ─────────────────────────────────────────────
    return {
        "value_usd_mn": None,
        "method":       "none",
        "stage_mult":   None,
        "inputs":       inputs,
    }


# ── MFR · Merger Feasibility Rating (BUYER-MFR-01) ────────────────────────────
# Schwellen = Verhältnis Buyer-Marktkapitalisierung ÷ Target-Bewertung.
# Basis: "Buyer ≥ 5× Target" (Enrichment-Regel) als Feasible-Untergrenze.
# Stretch = Käufer könnte stemmen, aber materieller Deal (transformativ).
# Unfeasible = Target zu groß relativ zum Käufer.
#
# Tiers (ratio = buyer_mcap_usd / target_value_usd):
#   ratio >= 5.0   → Feasible    (Käufer ≥ 5× Target — komfortabel)
#   2.0 <= r < 5.0 → Stretch     (machbar, aber großer Deal)
#   r < 2.0        → Unfeasible  (Target zu groß / Merger-of-Equals-Risiko)
_MFR_FEASIBLE_MIN = 5.0
_MFR_STRETCH_MIN  = 2.0


def compute_mfr(buyer_mcap_usd_bn: float | None, target_value_usd_mn: float | None) -> dict:
    """
    MFR für EINEN Käufer gegen ein Target.

    buyer_mcap_usd_bn:    Marktkapitalisierung des Käufers in Mrd USD (potential_buyers)
    target_value_usd_mn:  Target-Bewertung in Mio USD (compute_target_valuation)

    Returns:
        {
          "mfr":   "Feasible" | "Stretch" | "Unfeasible" | "Unknown",
          "ratio": float | None,   # buyer ÷ target (beide USD)
        }
    Unknown wenn ein Input fehlt — kein Raten, kein Default-Tier.
    """
    if buyer_mcap_usd_bn is None or target_value_usd_mn is None or target_value_usd_mn <= 0:
        return {"mfr": "Unknown", "ratio": None}

    buyer_usd_mn = buyer_mcap_usd_bn * 1000.0   # Mrd → Mio
    ratio = buyer_usd_mn / target_value_usd_mn

    if ratio >= _MFR_FEASIBLE_MIN:
        tier = "Feasible"
    elif ratio >= _MFR_STRETCH_MIN:
        tier = "Stretch"
    else:
        tier = "Unfeasible"

    return {"mfr": tier, "ratio": round(ratio, 2)}


def annotate_buyers_with_mfr(buyers: list[dict], company: dict) -> list[dict]:
    """
    Reichert eine Käuferliste um das MFR-Feld an (in-place + return).
    Wird vom Scoring (SC-02) UND vom Frontend-Endpoint genutzt — eine Quelle.

    Erwartet pro Käufer 'market_cap_usd_bn' (aus potential_buyers).
    Setzt 'mfr' (str) + 'mfr_ratio' (float|None) je Käufer.
    """
    target = compute_target_valuation(company)
    tv = target["value_usd_mn"]

    for b in buyers or []:
        res = compute_mfr(b.get("market_cap_usd_bn"), tv)
        b["mfr"]       = res["mfr"]
        b["mfr_ratio"] = res["ratio"]

    return buyers
