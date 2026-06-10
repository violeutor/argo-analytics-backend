"""
Argo Analytics – Scoring Engine
================================
Implements:
  - SRR  (Strategic Relevance Ratio)   = TAM / Buyer MarketCap
  - MFR  (M&A Feasibility Ratio)       = f(cash, debt/EBITDA, target valuation)
  - TechReadiness                       = weighted mean of 7 Amazon/Google-style factors
  - DealSuccessScore                    = SRR_norm × MFR_norm × TechReadiness

v1.1 — Auto TechReadiness:
  - compute_auto_tech_readiness(): berechnet TR-Basiswert aus Stage, Kategorie, Funding-Pace
  - Ersetzt pauschalen 0.5-Fallback für private Companies
  - tech_readiness_override in AnalyzeRequest: wenn gesetzt, überschreibt compute_tech_readiness()

v1.2 — MFR Cash-Wiring + execution_warning (S65):
  - BUYER-FIN-01-WIRE: buyer_cash_usd_bn fließt jetzt in MFR ein (war toter Parameter).
    Feasibility basiert auf Netto-Finanzierungsbedarf (EV − deployable Cash), nicht roher EV.
  - EXEC-WARN-WIRE: execution_warning blockiert das A-Rating (war berechnet, nie genutzt).
"""

import math
from src.models.schemas import (
    AnalyzeRequest,
    SRRResult,
    MFRResult,
    TechReadinessResult,
    ScoreResult,
    TechReadinessInputs,
)
# VALUATION-SSOT-01: Stage/Vertical-Multiplikatoren + Target-Bewertung kommen
# aus valuation.py — die EINE Quelle. Lokale _STAGE_MULTIPLIERS entfernt
# (war divergent: seed 8.0 vs. valuation 5.0 → systematische Überbewertung).
from src.services.valuation import stage_multiplier, vertical_delta

# ── Constants ─────────────────────────────────────────────────────────────────

_TR_WEIGHTS = {
    "tech_stack_fit": 0.20,
    "gtm_fit": 0.15,
    "integration_capacity": 0.20,
    "rd_intensity": 0.10,
    "capital_deployment_velocity": 0.15,
    "regulatory_readiness": 0.10,
    "strategic_coherence": 0.10,
}

_MFR_FEASIBLE_THRESHOLD = 0.15
_MFR_WATCH_THRESHOLD = 0.50

# Anteil der Cash-Reserve, der realistisch für einen Deal einsetzbar ist
# (Working Capital / Liquiditätspuffer bleibt). Tunable — 0.70 = konservativ.
_DEPLOYABLE_CASH_FRACTION = 0.70

_SRR_HIGH_STRATEGIC = 0.15
_SRR_TRANSFORMATIONAL = 0.50
_SRR_TRANSFORMATIONAL_PP = 1.0

_LOW_CAP_THRESHOLD_BN = 10.0
_HIGH_SRR_WARNING = 5.0


# ── Auto TechReadiness ────────────────────────────────────────────────────────

# Stage → TR-Basiswert (dominanter Faktor)
_STAGE_TR: dict[str, float] = {
    "seed":          0.28,
    "series_a":      0.40,
    "series_b":      0.53,
    "series_c":      0.64,
    "series_d_plus": 0.73,
    "pre_ipo":       0.80,
    "public":        1.00,  # Gate — wird im Frontend nicht angezeigt
}

# Kategorie → Branchenspezifischer Boost/Abzug
# Reife Kategorien: höherer Basis-TR / Frühe oder risikoreiche: Abzug
_CATEGORY_TR_DELTA: dict[str, float] = {
    "Agritech SaaS":                     +0.10,
    "AI × Grid Software":                +0.08,
    "CO₂-to-Fuels":                      +0.08,
    "Solar Irrigation":                  +0.07,
    "Low-Carbon Concrete":               +0.06,
    "Electrified Cement":                +0.06,
    "Distributed Power Infrastructure":  +0.06,
    "Climate-Risk SaaS":                 +0.06,
    "Irrigation":                        +0.05,
    "Industrial Capture":                +0.04,
    "Bio-based Chemicals":               +0.04,
    "Distributed Battery / Grid":        +0.04,
    "Carbon Credits":                    +0.03,
    "Geothermal / EGS":                  +0.02,
    "Long-Duration Storage":             +0.02,
    # Frühe / risikoreiche Kategorien
    "Ocean CDR":                         -0.10,
    "Bioengineering":                    -0.08,
    "Mineralization":                    -0.06,
    "Modular Capture":                   -0.06,
    "Solid-State Battery":               -0.05,
    "Mobile Capture":                    -0.05,
    "Electrochemical Capture":           -0.04,
    "Biomass CDR":                       -0.03,
}

# Funding-Pace → Marktsignal-Boost
_FUNDING_PACE_DELTA: dict[str, float] = {
    "fast":   +0.05,   # > $100M Funding-Total
    "medium":  0.00,
    "slow":   -0.04,   # < $10M oder nicht öffentlich
}


def compute_auto_tech_readiness(
    stage: str | None,
    category: str | None,
    funding_total_usd_mn: float | None,
    funding_last_round: str | None,
) -> tuple[float, str]:
    """
    Berechnet automatischen TechReadiness-Basiswert für private Companies.
    Gibt (tr_value: float, confidence: str) zurück.

    confidence-Werte:
      'auto_high'   — Stage + Kategorie beide im Mapping → verlässliche Schätzung
      'auto_medium' — nur Stage bekannt
      'auto_low'    — weder Stage noch Kategorie bekannt → generischer Fallback

    Logik:
      1. Stage  → Basiswert (dominanter Faktor, ~60% Gewicht)
      2. Kategorie → branchenspezifischer Delta
      3. Funding-Pace → Marktsignal-Delta

    Wird nur für is_listed=False aufgerufen.
    Kann durch User-Inputs (CD-F02) überschrieben werden → confidence='user'.
    """
    # 1. Stage normalisieren
    stage_key = (stage or "").lower().replace(" ", "_").replace("-", "_")
    # Aliase abfangen
    if stage_key in ("series_d", "series_d+", "series_e", "late"):
        stage_key = "series_d_plus"
    if stage_key in ("series_a_1", "series_a_2"):
        stage_key = "series_a"

    base = _STAGE_TR.get(stage_key)
    known_stage = base is not None
    if base is None:
        base = 0.50  # generischer Fallback

    # 2. Kategorie-Delta
    cat_delta = _CATEGORY_TR_DELTA.get(category or "", 0.0)
    known_category = (category or "") in _CATEGORY_TR_DELTA

    # 3. Funding-Pace
    if funding_total_usd_mn and funding_total_usd_mn > 100:
        pace = "fast"
    elif not funding_total_usd_mn or funding_total_usd_mn < 10:
        pace = "slow"
    else:
        pace = "medium"
    pace_delta = _FUNDING_PACE_DELTA[pace]

    # Finaler Wert, geclampt auf [0.10, 0.92]
    tr = round(min(max(base + cat_delta + pace_delta, 0.10), 0.92), 3)

    # Konfidenz
    if known_stage and known_category:
        confidence = "auto_high"
    elif known_stage:
        confidence = "auto_medium"
    else:
        confidence = "auto_low"

    return tr, confidence


# ── SRR ──────────────────────────────────────────────────────────────────────

def compute_srr(tam_usd_bn: float, buyer_market_cap_usd_bn: float) -> SRRResult:
    if buyer_market_cap_usd_bn <= 0:
        raise ValueError("Buyer market cap must be > 0")

    srr = tam_usd_bn / buyer_market_cap_usd_bn

    if srr >= _SRR_TRANSFORMATIONAL_PP:
        category = "Transformational++"
    elif srr >= _SRR_TRANSFORMATIONAL:
        category = "Transformational"
    elif srr >= _SRR_HIGH_STRATEGIC:
        category = "High Strategic"
    else:
        category = "Low Strategic"

    if buyer_market_cap_usd_bn < _LOW_CAP_THRESHOLD_BN:
        cap_segment = "low"
    elif buyer_market_cap_usd_bn < 100:
        cap_segment = "mid"
    else:
        cap_segment = "high"

    execution_warning = cap_segment == "low" and srr > _HIGH_SRR_WARNING

    return SRRResult(
        value=round(srr, 4),
        category=category,
        cap_segment=cap_segment,
        execution_warning=execution_warning,
    )


# ── MFR ──────────────────────────────────────────────────────────────────────

def compute_mfr(
    buyer_market_cap_usd_bn: float,
    buyer_cash_usd_bn: float,
    buyer_debt_ebitda: float,
    target_funding_usd_mn: float,
    target_stage: str,
    target_vertical: str | None = None,
) -> MFRResult:
    # VALUATION-SSOT-01: EV = funding × stage_mult × vertical_delta (zentrale Tabelle).
    # Ersetzt die lokale _STAGE_MULTIPLIERS-Berechnung. Vertical justiert branchenspezifisch
    # (z.B. deep_tech 1.30, biotech_pharma 0.85) — default 1.0 wenn unbekannt.
    multiplier = stage_multiplier(target_stage) * vertical_delta(target_vertical)
    estimated_ev_usd_bn = (target_funding_usd_mn * multiplier) / 1000

    # BUYER-FIN-01-WIRE: Cash-Polster reduziert den Finanzierungsbedarf.
    # Vorher war buyer_cash_usd_bn ein toter Parameter — BUYER-FIN-01 holt die
    # echte Cash via yfinance, aber sie floss nie in die Feasibility ein.
    # Was nach Einsatz der liquiden Mittel bleibt (EV − Cash), muss über
    # Debt/Equity gestemmt werden — DAS ist die Last relativ zur Marktkapitalisierung.
    # _DEPLOYABLE_CASH_FRACTION: kein Käufer kippt 100% Treasury in einen Deal
    # (Working Capital bleibt) — konservativer Anteil statt voller Reserve.
    # Cash fehlt (None) → 0.0 = Käufer muss voll finanzieren, nie geschenkt.
    deployable_cash = max(buyer_cash_usd_bn or 0.0, 0.0) * _DEPLOYABLE_CASH_FRACTION
    net_financing_need = max(estimated_ev_usd_bn - deployable_cash, 0.0)

    mfr = net_financing_need / buyer_market_cap_usd_bn if buyer_market_cap_usd_bn > 0 else 999

    # Hohe Verschuldung verteuert die Restfinanzierung (Debt-Kapazität knapper).
    if buyer_debt_ebitda > 3.0:
        mfr = mfr * (1 + (buyer_debt_ebitda - 3.0) * 0.1)

    if mfr < _MFR_FEASIBLE_THRESHOLD:
        signal = "Feasible"
    elif mfr < _MFR_WATCH_THRESHOLD:
        signal = "Watch"
    else:
        signal = "Overstretch"

    return MFRResult(value=round(mfr, 4), signal=signal)


# ── TechReadiness ─────────────────────────────────────────────────────────────

def compute_tech_readiness(
    inputs: TechReadinessInputs | None,
    override: float | None = None,
) -> TechReadinessResult:
    """
    Weighted mean across 7 factors.

    Priorität:
      1. override (float) — von compute_auto_tech_readiness() oder User-DB-Wert
      2. inputs (TechReadinessInputs) — manuelles CD-F02 Formular
      3. Fallback 0.5 — nur noch für listed Companies (wird im Frontend nicht angezeigt)
    """
    if override is not None:
        # Override: Faktor-Scores gleichmäßig auf override-Wert skalieren
        # (proportional zur jeweiligen Gewichtung — kein pauschales 0.5)
        factor_scores = {k: round(override, 4) for k in _TR_WEIGHTS}
        return TechReadinessResult(value=round(override, 4), factor_scores=factor_scores)

    if inputs is None:
        factor_scores = {k: 0.5 for k in _TR_WEIGHTS}
        return TechReadinessResult(value=0.5, factor_scores=factor_scores)

    factor_scores = {
        "tech_stack_fit":            inputs.tech_stack_fit,
        "gtm_fit":                   inputs.gtm_fit,
        "integration_capacity":      inputs.integration_capacity,
        "rd_intensity":              inputs.rd_intensity,
        "capital_deployment_velocity": inputs.capital_deployment_velocity,
        "regulatory_readiness":      inputs.regulatory_readiness,
        "strategic_coherence":       inputs.strategic_coherence,
    }

    score = sum(v * _TR_WEIGHTS[k] for k, v in factor_scores.items())

    return TechReadinessResult(
        value=round(score, 4),
        factor_scores={k: round(v, 4) for k, v in factor_scores.items()},
    )


# ── Normalization helpers ─────────────────────────────────────────────────────

def _normalize_srr(srr_value: float) -> float:
    if srr_value <= 0:
        return 0.0
    ceiling = 10.0
    return min(math.log1p(srr_value) / math.log1p(ceiling), 1.0)


def _normalize_mfr(mfr_value: float) -> float:
    if mfr_value <= 0:
        return 1.0
    if mfr_value < _MFR_FEASIBLE_THRESHOLD:
        return 1.0
    return max(0.0, 1.0 - min(mfr_value / _MFR_WATCH_THRESHOLD, 1.0))


# ── Rating & Quadrant ─────────────────────────────────────────────────────────

def _derive_rating(srr: SRRResult, mfr: MFRResult, tr: TechReadinessResult) -> str:
    # EXEC-WARN-WIRE: execution_warning (low-cap + SRR>5) = Buyer-seitiges
    # Integrationsrisiko. Vorher berechnet, aber nie genutzt — ein kleiner Käufer
    # konnte "A · No-Brainer" UND execution-geflaggt sein (Widerspruch).
    # TechReadiness misst nur Target-Reife, nicht ob der Käufer die Übernahme
    # schultern kann. → A blockiert bei Warnung, fällt in den B-Pfad.
    # Die Flag bleibt auf dem SRRResult → Frontend zeigt Badge unabhängig vom Rating.
    if (srr.category == "Transformational++" and mfr.signal == "Feasible"
            and tr.value >= 0.6 and not srr.execution_warning):
        return "A · No-Brainer"
    if srr.category in ("Transformational++", "Transformational") and mfr.signal in ("Feasible", "Watch") and tr.value >= 0.5:
        return "B · Solide"
    if srr.category in ("High Strategic", "Transformational") and mfr.signal != "Overstretch":
        return "C · Abwägen"
    return "D · Uninteressant"


def _derive_quadrant(deal_success_score: float, mfr: MFRResult) -> str:
    high_potential = deal_success_score >= 0.5
    low_risk = mfr.signal == "Feasible"

    if high_potential and low_risk:
        return "HighPotential_LowRisk"
    if high_potential and not low_risk:
        return "HighPotential_HighRisk"
    if not high_potential and low_risk:
        return "LowPotential_LowRisk"
    return "LowPotential_HighRisk"


# ── Segment-Linse (BUYER-FE-RENDER-01 / SRR-SIZE-BIAS-01) ─────────────────────
# customer_type bestimmt NUR die Anzeige-Reihenfolge der Buyer-Liste — NICHT die
# Scores (SRR/MFR/TR sind betrachter-unabhängig) und NICHT das ma_aggregate
# (intrinsische M&A-Stärke der Company). Zwei Sortier-Modi:
#   transformativity → nach deal_success (höchster Upside-falls-Eintritt zuerst)
#   probability      → Feasible-first, dann deal_success (wer kauft realistisch)
# Mapping aus dem user_profiles-Schema-Kommentar (Z.388ff): ma_agency/pe/corporate
# = Wahrscheinlichkeit; family_office/vc = Transformativität.
_SEGMENT_LENS: dict[str, tuple[str, str]] = {
    "family_office": ("transformativity", "Investitionspfad"),
    "vc":            ("transformativity", "Exit-Landschaft"),
    "other":         ("transformativity", "Investitionspfad"),
    "pe":            ("probability",      "Exit & Buy-and-Build"),
    "ma_agency":     ("probability",      "Käufer-Shortlist"),
    "corporate":     ("probability",      "Build-vs-Buy"),
}
_MFR_RANK: dict[str, int] = {"Feasible": 0, "Watch": 1, "Overstretch": 2}


def segment_lens(customer_type: str | None) -> tuple[str, str]:
    """customer_type → (sort_mode, anzeige_label). Unbekannt → 'other'-Default."""
    return _SEGMENT_LENS.get(customer_type or "other", _SEGMENT_LENS["other"])


def sort_scorings_by_lens(scorings: list, customer_type: str | None) -> tuple[list, dict]:
    """
    Sortiert die Buyer-Scoring-Liste nach der customer_type-Linse (Read-Time, Anzeige).
    Operiert duck-typed auf Objekten mit .mfr_signal + .deal_success_score
    (z.B. ScoringDetail) — kein Import des Response-Modells nötig.
    Returns (sortierte_liste, lens_meta) — lens_meta trägt Tab-Label + Modus fürs FE.
    """
    mode, label = segment_lens(customer_type)
    if mode == "probability":
        ordered = sorted(
            scorings,
            key=lambda s: (_MFR_RANK.get(s.mfr_signal, 3), -s.deal_success_score),
        )
    else:
        ordered = sorted(scorings, key=lambda s: -s.deal_success_score)
    return ordered, {"mode": mode, "label": label, "customer_type": customer_type or "other"}


# ── Master scoring function ───────────────────────────────────────────────────

def compute_scores(request: AnalyzeRequest) -> ScoreResult:
    srr = compute_srr(request.tam_usd_bn, request.buyer_market_cap_usd_bn)
    mfr = compute_mfr(
        buyer_market_cap_usd_bn=request.buyer_market_cap_usd_bn,
        buyer_cash_usd_bn=request.buyer_cash_usd_bn,
        buyer_debt_ebitda=request.buyer_debt_ebitda,
        target_funding_usd_mn=request.target_funding_usd_mn,
        target_stage=request.target_stage,
        target_vertical=request.target_vertical,
    )
    tr = compute_tech_readiness(
        inputs=request.tech_readiness_inputs,
        override=request.tech_readiness_override,
    )

    srr_norm = _normalize_srr(srr.value)
    mfr_norm = _normalize_mfr(mfr.value)

    deal_success_score = round(srr_norm * mfr_norm * tr.value, 4)

    rating = _derive_rating(srr, mfr, tr)
    quadrant = _derive_quadrant(deal_success_score, mfr)

    return ScoreResult(
        srr=srr,
        mfr=mfr,
        tech_readiness=tr,
        deal_success_score=deal_success_score,
        rating=rating,
        quadrant=quadrant,
    )
