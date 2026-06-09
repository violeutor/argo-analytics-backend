"""
SC-01–SC-13 · score_calculator.py
Pfad: argo-analytics-backend/src/services/score_calculator.py

Scoring-Engine — berechnet alle Sub-Scores, Path-Scores und Composite.
Aufgerufen on-demand (company_detail.py) oder täglich via Cron (nach signal_engine).

Sub-Scores (0–10):
  SC-01  Financial Score     — BA-Bridge Fundamentals + Funding-Stage-Proxy
  SC-02  Strategic Score     — SRR × TechReadiness × Buyer Fit
  SC-03  Market Score        — CAGR + Competition + Data Richness
  SC-04  Risk Score          — Beta + Governance + Negative Signals + Stage
  SC-08  Ownership Score     — Investor-Qualität + Diversifikation + Transparenz
  SC-09  Value Driver Score  — Dependency + Netzeffekte + TechReadiness

Path-Scores (0–10):
  IPO Score      — TechReadiness + IPO Signals + Funding Stage + ipo_potential
  M&A Score      — SRR × MFR × TechReadiness × Buyer Fit
  ETF Score      — Listed Status + Kategorie-Coverage + Explizite ETF-Proxies
  Enabler Score  — B2B-Industrie + Dependency Scores + TechReadiness

Composite + Rating:
  SC-05  Composite Score     — Gewichteter Durchschnitt der Sub-Scores
                               Financial 25% + Strategic 20% + Market 20%
                               + Ownership 15% + Value Driver 10% + (10−Risk) 10%
  SC-11  Hero Path           — argmax(path_scores) → dominanter Investitionspfad
  SC-12  Score Inputs        — serialisierte Inputs je Score (für Tooltip)
  SC-13  Rating A–D          — aus hero_score; A ≥7.5 · B ≥5.5 · C ≥3.5 · D <3.5

Architektur-Entscheidungen:
  - Scores sind heuristisch, nicht ML-basiert (interpretierbar, kein Black-Box)
  - Graceful Degradation: fehlende Daten → niedrigere Confidence, kein Crash
  - Ergebnisse werden in company_scores (Supabase) gecacht → täglicher Cron
  - Cached scores ermöglichen Verlaufs-Tracking + Trend-Ableitung
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ── Typen ─────────────────────────────────────────────────────────────────────

@dataclass
class ScoreResult:
    # Sub-Scores (SC-01–SC-04, SC-08–SC-09)
    financial_score:     float | None = None
    strategic_score:     float | None = None
    market_score:        float | None = None
    risk_score:          float | None = None
    ownership_score:     float | None = None
    value_driver_score:  float | None = None

    # Path-Scores
    ipo_score:           float | None = None
    ma_score:            float | None = None
    etf_score:           float | None = None
    enabler_score:       float | None = None

    # SC-10: Compound Risk Score (aggregiert aus 6 Dimensions-Risiken)
    compound_risk_score: float | None = None

    # Composite + Hero (SC-05, SC-11, SC-13)
    composite_score:     float | None = None
    hero_path:           str   | None = None   # 'ipo' | 'm_and_a' | 'etf' | 'enabler'
    hero_score:          float | None = None
    hero_path_label:     str   | None = None   # Display: 'IPO Play' | 'M&A Target' etc.
    rating:              str   | None = None   # 'A' | 'B' | 'C' | 'D'

    # SC-12: Inputs für Tooltip + Debugging
    confidence:          str = "auto"
    score_inputs:        dict = field(default_factory=dict)
    score_source:        str = "algorithmic"   # BUG-41: war nie gesetzt

    def to_dict(self) -> dict:
        return {
            "financial_score":    self.financial_score,
            "strategic_score":    self.strategic_score,
            "market_score":       self.market_score,
            "risk_score":         self.risk_score,
            "ownership_score":    self.ownership_score,
            "value_driver_score": self.value_driver_score,
            "compound_risk_score": self.compound_risk_score,
            "ipo_score":          self.ipo_score,
            "ma_score":           self.ma_score,
            "etf_score":          self.etf_score,
            "enabler_score":      self.enabler_score,
            "composite_score":    self.composite_score,
            "hero_path":          self.hero_path,
            "hero_score":         self.hero_score,
            "hero_path_label":    self.hero_path_label,
            "rating":             self.rating,
            "confidence":         self.confidence,
            "score_source":       self.score_source,   # BUG-41
            "score_inputs":       self.score_inputs,
        }


# ── Lookup-Tabellen ────────────────────────────────────────────────────────────

# ETF_COVERED_CATEGORIES + PATENT_SCORING_SECTORS → SSOT: src/taxonomy.py
from src.taxonomy import (
    ETF_COVERED_CATEGORIES  as _ETF_COVERED_CATEGORIES,
    PATENT_SCORING_SECTORS  as _PATENT_SCORING_SECTORS,
    is_etf_covered          as _is_etf_covered,
    is_patent_relevant      as _is_patent_relevant,
)

# IPO-Attraktivität je Funding Stage (raw base)
_STAGE_IPO_SCORE: dict[str, float] = {
    "s-1 filed":  9.0,
    "pre-ipo":    8.0,
    "series d":   6.5,
    "series c":   5.5,
    "series b":   4.0,
    "series a":   3.0,
    "seed":       2.0,
    "bootstrap":  1.5,
    "listed":     0.0,   # bereits public → IPO-Pfad abgeschlossen
}

# M&A-Attraktivität je Stage (Series A–C = Sweet Spot für Acqui-Hire + Strategic)
_STAGE_MA_SCORE: dict[str, float] = {
    "seed":       6.0,
    "series a":   7.5,
    "series b":   8.5,
    "series c":   7.5,
    "series d":   6.0,
    "pre-ipo":    4.5,   # zu nah am Börsengang → M&A unwahrscheinlicher
    "s-1 filed":  2.5,
    "listed":     2.0,   # möglich aber teuer (Public-to-Private)
    "bootstrap":  7.0,   # Owner-managed = klassisches Akquisitions-Target
}

# SRR → Score-Beitrag
_SRR_SCORE: dict[str | None, float] = {
    "high":   3.5,
    "medium": 2.5,
    "low":    1.5,
    "none":   0.5,
    None:     1.0,
}

# MFR → Score-Beitrag
_MFR_SCORE: dict[str | None, float] = {
    "Feasible":  3.0,
    "Watch":     2.0,
    "Unlikely":  1.0,
    "None":      0.0,
    None:        0.5,
}

# Investor-Tier-Lookup (keyword → 1–3)
_INVESTOR_TIER_KW: list[tuple[str, int]] = [
    # Tier 3 — Top-VC / Strategic Flagship
    ("sequoia", 3), ("a16z", 3), ("andreessen horowitz", 3), ("benchmark", 3),
    ("kleiner", 3), ("accel", 3), ("index ventures", 3), ("tiger global", 3),
    ("softbank", 3), ("general catalyst", 3), ("lightspeed", 3), ("greylock", 3),
    ("khosla", 3), ("founders fund", 3), ("breakthrough energy", 3),
    ("lowercarbon", 3), ("google ventures", 3), ("gv", 3), ("microsoft ventures", 3),
    ("amazon industrial", 3), ("2150", 3), ("systemiq", 3),
    # Tier 2 — Etablierte EU/US VC + Corporate VC
    ("earlybird", 2), ("hv capital", 2), ("atomico", 2), ("northzone", 2),
    ("cherry", 2), ("point nine", 2), ("b capital", 2), ("btov", 2),
    ("nato innovation", 2), ("eib", 2), ("kfw", 2), ("cdpq", 2),
    # Tier 1 — Corporate, Government, unbekannt
]

# Display-Labels je Path
_PATH_LABELS: dict[str, str] = {
    "ipo":     "IPO",
    "m_and_a": "M&A",
    "etf":     "ETF-Proxy",
    "enabler": "Enabler",
}

# Composite-Gewichte (SC-05)
_COMPOSITE_WEIGHTS: dict[str, float] = {
    "financial":    0.25,
    "strategic":    0.20,
    "market":       0.20,
    "ownership":    0.15,
    "value_driver": 0.10,
    "risk_inv":     0.10,   # (10 − risk_score) × 0.10
}


# ── Hilfsfunktionen ────────────────────────────────────────────────────────────

def _clamp(v: float) -> float:
    return min(10.0, max(0.0, v))


def _safe_round(v: float | None, decimals: int = 2) -> float | None:
    return None if v is None else round(_clamp(v), decimals)


def _stage_match(stage: str, mapping: dict[str, float]) -> float:
    """Matcht Funding Stage Case-insensitiv gegen Mapping-Keys (longest-match)."""
    stage_lower = stage.lower()
    result = 0.0
    best_len = -1
    for key, pts in mapping.items():
        if key in stage_lower and len(key) > best_len:
            result = pts
            best_len = len(key)
    return result


def _is_listed(company: dict) -> bool:
    """
    Robuste Listed-Erkennung — prüft ticker UND ipo_status.
    Verhindert false-negatives wenn Ticker noch nicht in DB steht
    (z.B. kurz nach IPO-Eintrag vor erstem Enrichment-Lauf).
    """
    if bool(company.get("ticker")):
        return True
    ipo_status = (company.get("ipo_status") or "").lower()
    return ipo_status in ("listed", "public", "ipo_erfolgt", "ipo erfolgt")


def _resolve_funding_stage(company: dict) -> str:
    """
    BUG-51: Normalisiert funding_stage für alle Score-Funktionen.
    Listed Companies haben keine Venture-Runden in DB → funding_stage wäre NULL.
    Fix: ipo_status == 'listed' → 'public' direkt setzen, unabhängig von funding_rounds.
    """
    if _is_listed(company):
        return "public"
    return company.get("funding_stage") or ""


def _investor_tier(name: str, investor_type: str) -> int:
    name_lower = name.lower()
    for kw, tier in _INVESTOR_TIER_KW:
        if kw in name_lower:
            return tier
    # Typ-basierter Fallback
    if investor_type in ("corporate", "strategic"):
        return 2
    if investor_type in ("government", "development_bank"):
        return 2
    return 1   # default


# ── SC-01 · Financial Score ────────────────────────────────────────────────────

def compute_financial_score(
    company: dict,
    funding_momentum: dict | None = None,
    headcount_snapshots: list[dict] | None = None,
) -> tuple[float, dict]:
    """
    SC-01: Financial Score (0–10).

    Inputs:
      revenue_usd_mn     → Umsatz-Basis (BA-Bridge oder Yahoo)
      ebitda_margin      → Marge (BA-Bridge oder Damodaran-Branche)
      revenue_cagr       → Wachstumsrate (aus kpi_timeseries)
      funding_stage      → Reifegrad-Proxy wenn Fundamentals fehlen

    Für private Companies ohne Financials (US Private):
      funding_momentum   → Ersetzt Stage-Proxy durch Momentum Score (Rundengröße,
                           Frequenz, Wachstum, Stage-Progression)
      headcount_snapshots→ Optionaler CAGR-Bonus wenn ≥2 Snapshots mit ≥90d Abstand

    Gewichtung listed / DE private (mit BA-Daten):
      Revenue    0–3 Pkt
      Marge      0–3 Pkt
      CAGR       0–2 Pkt
      Stage      0–2 Pkt

    Gewichtung US private (kein Revenue/Marge):
      Momentum   0–8 Pkt  (normalisiert aus FundingMomentum.momentum_score)
      Headcount  0–2 Pkt  (CAGR-Bonus, only if → then)
    """
    inputs: dict = {}
    score = 0.0
    data_points = 0

    is_private = not _is_listed(company)
    has_financials = bool(
        company.get("revenue_usd_mn") or
        company.get("ba_revenue_mn") or
        company.get("ebitda_margin")
    )

    # ── US Private ohne Financials → Momentum-Pfad ───────────────────────────
    if is_private and not has_financials and funding_momentum:
        raw_momentum = funding_momentum.get("momentum_score")
        if raw_momentum is not None:
            # Momentum Score 0–10 → auf 8 Pkt skalieren (lässt Raum für HC-Bonus)
            momentum_pts = _clamp(float(raw_momentum)) * 0.8
            score += momentum_pts
            inputs["momentum_score"]            = raw_momentum
            inputs["rounds_count"]              = funding_momentum.get("rounds_count")
            inputs["days_since_last_round"]     = funding_momentum.get("days_since_last_round")
            inputs["avg_months_between_rounds"] = funding_momentum.get("avg_months_between_rounds")
            inputs["round_size_growth_pct"]     = funding_momentum.get("round_size_growth_pct")
            inputs["last_round_amount_usd_mn"]  = funding_momentum.get("last_round_amount_usd_mn")
            data_points += 1

        # Headcount CAGR Bonus (0–2 Pkt) — if → then, nur wenn Verlauf vorhanden
        if headcount_snapshots and len(headcount_snapshots) >= 2:
            try:
                from datetime import date
                snapshots_sorted = sorted(headcount_snapshots, key=lambda s: s["snapshot_date"])
                first = snapshots_sorted[0]
                last  = snapshots_sorted[-1]
                first_date = date.fromisoformat(first["snapshot_date"])
                last_date  = date.fromisoformat(last["snapshot_date"])
                days_span  = (last_date - first_date).days

                if days_span >= 90 and first["headcount"] > 0 and last["headcount"] > 0:
                    years = days_span / 365.25
                    hc_cagr = ((last["headcount"] / first["headcount"]) ** (1 / years) - 1) * 100
                    hc_bonus = (
                        2.0 if hc_cagr >= 50 else
                        1.5 if hc_cagr >= 25 else
                        1.0 if hc_cagr >= 10 else
                        0.5 if hc_cagr >= 0  else
                        0.0
                    )
                    score += hc_bonus
                    inputs["headcount_cagr_pct"]    = round(hc_cagr, 1)
                    inputs["headcount_bonus_pts"]   = hc_bonus
                    inputs["headcount_span_days"]   = days_span
                    inputs["headcount_first"]       = first["headcount"]
                    inputs["headcount_last"]        = last["headcount"]
                    data_points += 1
            except Exception:
                pass  # Snapshot-Fehler → Score bleibt unverändert, kein Crash

        return _safe_round(min(score, 10.0)), inputs

    # ── Standard-Pfad: listed + private DE mit BA-Daten ─────────────────────

    # Revenue (0–3)
    rev = company.get("revenue_usd_mn")
    if rev is not None:
        rev = float(rev)
        inputs["revenue_usd_mn"] = rev
        if rev >= 500:    score += 3.0
        elif rev >= 100:  score += 2.5
        elif rev >= 50:   score += 2.0
        elif rev >= 10:   score += 1.5
        elif rev >= 1:    score += 1.0
        else:             score += 0.5
        data_points += 1

    # EBITDA-Marge (0–3)
    margin = company.get("ebitda_margin")
    if margin is None:
        # Fallback: aus BA-Bridge-Feldern wenn vorhanden
        ebitda = company.get("ba_ebitda_mn")
        rev_mn = company.get("revenue_usd_mn") or company.get("ba_revenue_mn")
        if ebitda and rev_mn and float(rev_mn) > 0:
            margin = float(ebitda) / float(rev_mn) * 100
    if margin is not None:
        margin = float(margin)
        inputs["ebitda_margin"] = round(margin, 1)
        if margin >= 25:   score += 3.0
        elif margin >= 15: score += 2.5
        elif margin >= 5:  score += 2.0
        elif margin >= 0:  score += 1.0
        else:              score += 0.0   # negativ → kein Beitrag (nicht bestrafen, da frühe Stage OK)
        data_points += 1

    # Revenue CAGR (0–2)
    cagr = company.get("revenue_cagr")
    if cagr is not None:
        cagr = float(cagr)
        inputs["revenue_cagr"] = round(cagr, 1)
        if cagr >= 40:    score += 2.0
        elif cagr >= 20:  score += 1.5
        elif cagr >= 10:  score += 1.0
        elif cagr >= 0:   score += 0.5
        # negativ: 0 Punkte
        data_points += 1

    # Funding Stage Proxy (0–2): immer verfügbar als Minimum-Signal
    stage = _resolve_funding_stage(company)
    inputs["funding_stage"] = stage
    stage_pts = (
        2.0 if any(s in stage.lower() for s in ["series d", "series e", "pre-ipo", "growth"]) else
        1.5 if "series c" in stage.lower() else
        1.0 if "series b" in stage.lower() else
        0.5 if "series a" in stage.lower() else
        0.5 if "seed" in stage.lower() else
        1.5 if "bootstrap" in stage.lower() else   # Bootstrapped → profitabel
        1.5 if any(s in stage.lower() for s in ["listed", "public"]) else       # listed/public = hat Kapitalmarkttest bestanden
        1.0
    )
    score += stage_pts
    data_points += 1

    # Bei sehr wenig Datenbasis: Minimum-Baseline setzen (verhindert 0.5-Scores aus Stage allein)
    if data_points <= 1:
        score = max(score, 2.5)

    return _safe_round(score), inputs


# ── SC-02 · Strategic Score ────────────────────────────────────────────────────

def compute_strategic_score(company: dict, buyers: list[dict], ma_aggregate: dict | None = None) -> tuple[float, dict]:
    """
    SC-02: Strategic Score (0–10).

    Breiter als ma_score: strategische Attraktivität für M&A, Partnerschaften
    UND Peer-Positionierung. Behält eigene SRR/TR/MFR-Gewichtung.

    Inputs: srr, mfr_confidence, tech_readiness, feasible_buyer_count (aus Aggregat).

    Gewichtung:
      SRR             0–3.5 Pkt  (Strategische Relevanz)
      TechReadiness   0–3.5 Pkt  (Technologische Reife)
      MFR             0–3.0 Pkt  (Buyer Feasibility)
      Buyer Bonus     0–1.0 Pkt  (Anzahl realistischer Käufer aus Deal-Engine)

    HINWEIS: SRR/MFR werden hier weiter aus Company-Feldern gelesen (Legacy).
    Der feasible-Count kommt jetzt aus dem echten Deal-Aggregat (BUYER-AGG-01)
    statt aus nicht-existentem b["mfr"]. Vollständige SC-02-Neuausrichtung auf
    die Per-Buyer-Engine ist eine offene Produktentscheidung (SC02-REWORK-01).
    """
    inputs: dict = {}

    srr = company.get("srr") or company.get("strategic_relevance_rating")
    mfr = company.get("mfr_confidence")
    tr  = float(company.get("tech_readiness") or 0.5)

    inputs["srr"]            = srr
    inputs["mfr"]            = mfr
    inputs["tech_readiness"] = tr

    srr_pts = _SRR_SCORE.get(srr, 1.0)
    mfr_pts = _MFR_SCORE.get(mfr, 0.5)
    tr_pts  = tr * 3.5

    # Buyer Bonus aus echtem Deal-Aggregat (feasible_count), nicht aus b["mfr"]
    feasible_count = (ma_aggregate or {}).get("feasible_count", 0)
    buyer_bonus = min(1.0, feasible_count * 0.25)
    inputs["feasible_buyers"] = feasible_count

    score = srr_pts + mfr_pts + tr_pts + buyer_bonus
    return _safe_round(score), inputs


# ── SC-03 · Market Score ───────────────────────────────────────────────────────

def compute_market_score(market_data: dict, company: dict) -> tuple[float, dict]:
    """
    SC-03: Market Score (0–10).

    Inputs: cagr_pct, competition_score, segments, growth_drivers.

    Gewichtung:
      CAGR           0–4 Pkt   (Markt-Wachstumsdynamik)
      Competition    0–3 Pkt   (invertiert: low → hoch)
      Data Richness  0–2 Pkt   (Qualitäts-Proxy)
      Baseline       1   Pkt   (immer)
    """
    inputs: dict = {}
    score = 1.0   # Baseline

    # CAGR (0–4): bevorzugt aus market_data, Fallback aus company-Feld
    cagr = market_data.get("cagr_pct") or company.get("market_cagr_pct")
    if cagr is not None:
        cagr = float(cagr)
        inputs["cagr_pct"] = cagr
        if cagr >= 40:    score += 4.0
        elif cagr >= 25:  score += 3.0
        elif cagr >= 15:  score += 2.5
        elif cagr >= 8:   score += 1.5
        elif cagr >= 0:   score += 0.5
        else:             score -= 0.5   # schrumpfender Markt

    # Competition (0–3): low = wenig Wettbewerb = gute Position
    comp = market_data.get("competition_score", "")
    inputs["competition_score"] = comp
    score += {"low": 3.0, "medium": 1.5, "high": 0.0}.get(comp, 1.0)

    # Data Richness: Segmente + Wachstumstreiber erhöhen Vertrauen in Score
    segs    = market_data.get("segments") or []
    drivers = market_data.get("growth_drivers") or []
    if len(segs) >= 3:    score += 0.5
    if len(drivers) >= 2: score += 0.5
    inputs["segments_count"] = len(segs)
    inputs["drivers_count"]  = len(drivers)

    return _safe_round(score), inputs


# ── SC-04 · Risk Score ─────────────────────────────────────────────────────────

def compute_risk_score(
    company: dict,
    signals: list[dict],
    ownership_entries: list[dict],
) -> tuple[float, dict]:
    """
    SC-04: Risk Score (0–10). Höher = mehr Risiko.
    Wird im Composite invertiert: (10 − risk_score) × 0.10.

    Inputs: signals (direction=negative/absence), beta, ownership_entries, funding_stage.

    Gewichtung:
      Signal-Risiko   0–3 Pkt   (negative + absence Signale, offsett durch positive)
      Beta            0–2 Pkt   (Markt-Volatilität, nur listed)
      Governance      0–3 Pkt   (Ownership-Transparenz)
      Stage-Risiko    0–2 Pkt   (frühe Stage = höheres operatives Risiko)
    """
    inputs: dict = {}
    score = 0.0

    sigs = signals or []

    # Signal-Risiko (0–3)
    neg = [s for s in sigs if s.get("direction") == "negative" and s.get("source") != "internal_absence"]
    abs_ = [s for s in sigs if s.get("source") == "internal_absence"]
    pos  = [s for s in sigs if s.get("direction") == "positive"]

    risk_pts = min(3.0, len(neg) * 0.5 + len(abs_) * 0.25)
    pos_offset = min(1.0, len(pos) * 0.2)
    score += max(0.0, risk_pts - pos_offset)
    inputs["negative_signals"]  = len(neg)
    inputs["absence_signals"]   = len(abs_)
    inputs["positive_signals"]  = len(pos)

    # Beta / Volatilität (0–2)
    # Market Beta (listed)    → volle Gewichtung — direkt beobachtbar
    # Damodaran Beta (private) → 60% Gewichtung — Branchen-Proxy, nicht company-spezifisch
    beta = company.get("beta_1y") or company.get("beta")
    beta_source = company.get("beta_source", "")
    if beta is not None:
        beta = float(beta)
        inputs["beta"] = beta
        inputs["beta_source"] = beta_source
        damodaran_factor = 0.6 if beta_source == "damodaran" else 1.0
        raw_pts = (
            2.0 if beta >= 2.0 else
            1.5 if beta >= 1.5 else
            1.0 if beta >= 1.0 else
            0.5 if beta >= 0.5 else 0.0
        )
        score += raw_pts * damodaran_factor

    # Governance (0–3): Ownership-Intransparenz = Risiko
    is_listed = _is_listed(company)
    if is_listed:
        score += 0.3   # listed: öffentliche Rechenschaftspflicht → niedrig
        inputs["governance"] = "listed"
    elif not ownership_entries:
        score += 3.0   # keine Daten = maximale Governance-Unsicherheit
        inputs["governance"] = "opaque"
    elif len(ownership_entries) == 1:
        score += 1.5
        inputs["governance"] = "single_entry"
    else:
        score += 0.5
        inputs["governance"] = "partial"

    # Stage-Risiko (0–2)
    stage = _resolve_funding_stage(company)
    stage_lower = stage.lower()
    stage_risk = (
        2.0 if "seed" in stage_lower else
        1.5 if "series a" in stage_lower else
        1.0 if "series b" in stage_lower else
        0.5 if "series c" in stage_lower else
        0.3 if any(s in stage_lower for s in ["series d", "pre-ipo"]) else
        0.1 if any(s in stage_lower for s in ["listed", "public"]) else
        1.0   # unbekannt
    )
    score += stage_risk
    inputs["funding_stage"] = stage

    return _safe_round(score), inputs


# ── SC-08 · Ownership Score ────────────────────────────────────────────────────

def compute_ownership_score(
    company: dict,
    ownership_entries: list[dict],
) -> tuple[float, dict]:
    """
    SC-08: Ownership Score (0–10). Höher = bessere Investor-Qualität + Transparenz.

    Inputs: ownership_entries (name, investor_type, share_pct, source), is_listed.

    Gewichtung:
      Investor-Qualität  0–5 Pkt   (Tier 1–3 VC, Corporate, Government)
      Diversifikation    0–2 Pkt   (3–7 Investoren ideal)
      Transparenz        0–2.5 Pkt (Datenquelle: Bundesanzeiger > Wikipedia > manual)
      Listed Bonus       0–1 Pkt   (öffentliche Märkte = höchste Transparenz)
    """
    inputs: dict = {}
    is_listed = _is_listed(company)

    if not ownership_entries:
        if is_listed:
            inputs["source"] = "listed_no_local_data"
            return _safe_round(5.5), inputs
        inputs["source"] = "opaque"
        return _safe_round(1.0), inputs

    # Investor-Qualität (0–5)
    tiers = []
    for entry in ownership_entries:
        name = entry.get("name") or ""
        itype = entry.get("investor_type") or entry.get("type") or ""
        tiers.append(_investor_tier(name, itype))

    avg_tier = sum(tiers) / len(tiers) if tiers else 1.0
    quality_pts = (avg_tier / 3.0) * 5.0
    inputs["avg_investor_tier"] = round(avg_tier, 2)
    inputs["investor_count"]    = len(tiers)

    # Diversifikation (0–2)
    n = len(ownership_entries)
    div_pts = (
        2.0 if 3 <= n <= 7 else
        1.5 if n >= 8 else
        1.0 if n == 2 else
        0.5   # n == 1
    )
    inputs["ownership_count"] = n

    # Transparenz (0–2.5)
    sources = {(e.get("source") or "manual").lower() for e in ownership_entries}
    if "ba_bridge" in sources or "bundesanzeiger" in sources:
        trans_pts = 2.5
        inputs["transparency"] = "bundesanzeiger"
    elif "wikipedia" in sources:
        trans_pts = 1.5
        inputs["transparency"] = "wikipedia"
    elif "edgar" in sources or "sec" in sources:
        trans_pts = 2.0
        inputs["transparency"] = "sec_edgar"
    else:
        trans_pts = 0.5
        inputs["transparency"] = "manual"

    # Listed Bonus (0–1)
    listed_bonus = 1.0 if is_listed else 0.0
    inputs["listed_bonus"] = is_listed

    score = quality_pts + div_pts + trans_pts + listed_bonus
    return _safe_round(score), inputs


# ── SC-09 · Value Driver Score ─────────────────────────────────────────────────

def compute_value_driver_score(
    company: dict,
    value_drivers: list[dict],
) -> tuple[float, dict]:
    """
    SC-09: Value Driver Score (0–10). Höher = stärkere strukturelle Vorteile.

    Inputs: value_drivers (dependency_score, market_position, type), tech_readiness.

    Gewichtung:
      Dependency Score   0–4.0 Pkt  (Kernmetrik — tatsächliche Enabler-Qualität)
      Market Position    0–2.0 Pkt  (Leader/Dominant Einträge)
      Driver Count       0–2.0 Pkt  (Anzahl identifizierter Value Drivers)
      TechReadiness      0–1.5 Pkt  (Stage-Proxy — unterstützend, nicht dominant)
      Baseline           0.5  Pkt   (immer)

    Rationale: TechReadiness ist ein Stage-Proxy, keine echte Enabler-Qualität.
    Dependency Score und Market Position reflektieren die tatsächliche Supply-Chain-Stärke.
    Eine Series-A Company mit kritischen Enablerabhängigkeiten schlägt eine
    Series-C Company mit commodity Enablers — wie in der Realität.
    """
    inputs: dict = {}
    score = 0.5   # Baseline

    tr = float(company.get("tech_readiness") or 0.5)
    inputs["tech_readiness"] = tr
    score += tr * 1.5   # Stage-Proxy: unterstützend, nicht dominant (war: 3.0)

    vds = value_drivers or []
    if not vds:
        inputs["driver_count"] = 0
        return _safe_round(score), inputs

    # Driver Count (0–2)
    score += min(2.0, len(vds) * 0.4)
    inputs["driver_count"] = len(vds)

    # Dependency Score Durchschnitt (0–4) — Kernmetrik
    dep_vals = [float(d["dependency_score"]) for d in vds if d.get("dependency_score") is not None]
    if dep_vals:
        avg_dep = sum(dep_vals) / len(dep_vals)
        score += avg_dep * 4.0   # war: 3.0 — Dependency ist die echte Qualitätsmetrik
        inputs["avg_dependency_score"] = round(avg_dep, 2)

    # Market Position Bonus (0–2.0) — erhöht von 1.5 auf 2.0
    strong = sum(
        1 for d in vds
        if (d.get("market_position") or "").lower() in ("leader", "market leader", "dominant", "monopol", "quasi-monopol")
    )
    score += min(2.0, strong * 0.75)   # war: min(1.5, ...)
    inputs["strong_positions"] = strong

    return _safe_round(score), inputs


# ── Path Scores ────────────────────────────────────────────────────────────────

def compute_ipo_score(company: dict, signals: list[dict]) -> tuple[float, dict]:
    """
    IPO Score (0–10): Attraktivität des IPO-Pfads.

    0.0 für bereits gelistete Companies (IPO abgeschlossen).
    Hoch für: S-1 gefilter, viele IPO-Signale, hohes ipo_potential, hohe TechReadiness.

    Gewichtung:
      TechReadiness   0–3.0 Pkt
      IPO Signals     0–3.0 Pkt  (je ipo_progress Signal +1.0)
      Stage Base      0–2.7 Pkt  (×0.3 aus _STAGE_IPO_SCORE)
      ipo_potential   0–2.0 Pkt  (Hoch/Mittel/Niedrig)
    """
    inputs: dict = {}

    if _is_listed(company):
        inputs["note"] = "already_listed"
        return 0.0, inputs

    stage         = _resolve_funding_stage(company)
    tr            = float(company.get("tech_readiness") or 0.5)
    ipo_potential = company.get("ipo_potential") or ""
    ipo_status    = company.get("ipo_status") or ""

    inputs.update({"funding_stage": stage, "tech_readiness": tr,
                   "ipo_potential": ipo_potential, "ipo_status": ipo_status})

    stage_base  = _stage_match(stage, _STAGE_IPO_SCORE)
    tr_pts      = tr * 3.0

    ipo_sigs = [
        s for s in (signals or [])
        if s.get("signal_category") == "ipo_progress"
        or s.get("event_type") == "ipo_status_change"
    ]
    signal_pts = min(3.0, len(ipo_sigs) * 1.0)
    inputs["ipo_signals"] = len(ipo_sigs)

    potential_pts = (
        2.0 if ipo_potential in ("Hoch", "High") else
        1.5 if ipo_potential == "Mittel-hoch" else
        1.0 if ipo_potential in ("Mittel", "Medium") else
        0.0
    )

    # ipo_status boost: S-1 gefilter = konkreter Schritt
    status_boost = 1.5 if "s-1" in ipo_status.lower() else 0.0
    inputs["status_boost"] = status_boost

    score = stage_base * 0.3 + tr_pts + signal_pts + potential_pts + status_boost
    return _safe_round(score), inputs


def compute_ma_score(company: dict, ma_aggregate: dict | None = None) -> tuple[float, dict]:
    """
    M&A Score (0–10): Attraktivität als Akquisitions-Target.

    BUYER-AGG-01 Neuausrichtung: Konsumiert das Deal-Aggregat aus der echten
    Per-Buyer-Engine (src/pipelines/scoring.py, SRR×MFR×TechReadiness), statt
    SRR/MFR aus nicht-existenten Company-Feldern zu raten (alter toter Pfad:
    company.get("srr")/mfr_confidence → immer Default).

    ma_aggregate (aus company_detail._ma_aggregate_meta):
        aggregate_score:  Mittelwert Top-3-feasible deal_success_score [0..1]
        feasible_count:   Anzahl realistischer Käufer
        basis:            'top3_feasible' | 'top3_all_fallback' | 'none'

    Score-Aufbau (0–10):
        deal_success-Aggregat   0–7.0 Pkt  (aggregate_score × 7)
        Feasible-Käufer-Breite  0–1.5 Pkt  (mehrere Heimathäfen = robuster)
        Stage-Attraktivität     0–0.85 Pkt
        Reserve / kein-Aggregat → konservativer Mittelwert
    """
    inputs: dict = {}
    agg = ma_aggregate or {}

    agg_score      = agg.get("aggregate_score")
    feasible_count = agg.get("feasible_count", 0)
    basis          = agg.get("basis", "none")
    stage          = _resolve_funding_stage(company)

    inputs.update({
        "ma_aggregate_score": agg_score,
        "feasible_count":     feasible_count,
        "aggregate_basis":    basis,
        "funding_stage":      stage,
    })

    if agg_score is None:
        # Kein Buyer-Scoring verfügbar (noch nicht angereichert / nicht bewertbar).
        # Konservativer Stage-basierter Mittelwert statt Null — kein hartes 0.
        stage_pts = _stage_match(stage, _STAGE_MA_SCORE) * 0.1
        score = 3.0 + stage_pts
        inputs["fallback"] = "no_buyer_aggregate"
        return _safe_round(score), inputs

    deal_pts  = float(agg_score) * 7.0                       # 0..7
    # Breite: mehrere feasible Käufer = mehrere realistische Exits
    width_pts = min(1.5, feasible_count * 0.5)               # 0..1.5
    stage_pts = _stage_match(stage, _STAGE_MA_SCORE) * 0.1   # 0..0.85
    # Fallback-Basis (keine feasiblen, nur Top-3-all) leicht dämpfen
    penalty   = 0.85 if basis == "top3_all_fallback" else 1.0

    score = (deal_pts + width_pts + stage_pts) * penalty
    inputs["width_pts"] = round(width_pts, 2)
    return _safe_round(score), inputs


def compute_etf_score(company: dict, value_drivers: list[dict]) -> tuple[float, dict]:
    """
    ETF Score (0–10): Wie gut ist das ETF-Proxy-Potenzial?

    Hoch für: listed + in ETF-relevanter Kategorie + explizite ETF-Value-Drivers.
    Mittel für: private Companies in gut abgedeckten Sektoren (indirekter Proxy).

    Gewichtung:
      Listed Status     0 / 2 / 5 Pkt  (listed = direkt trackbar)
      Kategorie-Match   0 / 3.0 Pkt    (ETF-abgedeckte Sektoren)
      Explizite Drivers 0 / 2.0 Pkt    (ETF in value_drivers)
      TechReadiness     0–1.0 Pkt      (kleiner Relevanz-Bonus)
    """
    inputs: dict = {}
    score = 0.0

    # BUG-44: ipo_status verwenden, nicht bool(ticker) — ticker kann auch bei privaten gesetzt sein
    is_listed = company.get("ipo_status") == "listed"
    category  = (company.get("category") or "").lower()
    industry  = (company.get("industry") or "").lower()
    tr        = float(company.get("tech_readiness") or 0.5)

    inputs.update({"is_listed": is_listed, "category": category, "tech_readiness": tr})

    # Listed Status
    if is_listed:
        score += 5.0
    else:
        # Peer-ETF-Proxy: private Company kann in ETF-relevanten Sektoren als Proxy fungieren
        score += 2.0

    # Kategorie in ETF-Universe
    cat_match = _is_etf_covered(category, industry)
    if cat_match:
        score += 3.0
        inputs["etf_category_match"] = True
    else:
        inputs["etf_category_match"] = False

    # Explizite ETF-Value-Drivers
    etf_vds = [d for d in (value_drivers or []) if "etf" in (d.get("type") or "").lower()]
    if etf_vds:
        score += 2.0
        inputs["explicit_etf_drivers"] = len(etf_vds)

    score += tr * 1.0   # kleiner TechReadiness-Bonus
    return _safe_round(score), inputs


def compute_enabler_score(
    company: dict,
    value_drivers: list[dict],
    buyers: list[dict],
) -> tuple[float, dict]:
    """
    Enabler Score (0–10): Stärke der Enabler/Supply-Chain-Rolle.

    Hoch für: B2B-Industrien, viele Enabler-Value-Drivers, hohe Dependency Scores,
              quasi-monopolistische Marktposition.

    Gewichtung:
      TechReadiness          0–3.0 Pkt  (Core-Tech = wichtiger Enabler)
      B2B-Industrie-Signal   0 / 2.0    (Hardware, Materials, Infra usw.)
      Enabler Driver Count   0–3.0 Pkt  (explizite Enabler-Value-Drivers)
      Dependency Score       0–2.0 Pkt  (Abhängigkeit der Käufer)
    """
    inputs: dict = {}
    score = 0.0

    tr       = float(company.get("tech_readiness") or 0.5)
    industry = (company.get("industry") or "").lower()
    category = (company.get("category") or "").lower()

    inputs.update({"tech_readiness": tr, "industry": industry})

    score += tr * 3.0

    # B2B-Industrie (0–2)
    _B2B_KW = (
        "semiconductor", "industrial", "software", "saas", "deep tech",
        "materials", "infrastructure", "chemicals", "components",
        "manufacturing", "automation", "robotics", "sensor", "battery",
        "electrolysis", "hydrogen", "grid", "storage", "carbon",
    )
    if any(kw in industry or kw in category for kw in _B2B_KW):
        score += 2.0
        inputs["b2b_signal"] = True
    else:
        inputs["b2b_signal"] = False

    # Enabler Value Drivers (0–3)
    enabler_vds = [
        d for d in (value_drivers or [])
        if (d.get("type") or d.get("role") or "").lower()
        in ("enabler", "supplier", "component", "technology provider")
    ]
    score += min(3.0, len(enabler_vds) * 1.0)
    inputs["enabler_driver_count"] = len(enabler_vds)

    # Dependency Score (0–2)
    dep_vals = [float(d["dependency_score"]) for d in enabler_vds if d.get("dependency_score") is not None]
    if dep_vals:
        avg_dep = sum(dep_vals) / len(dep_vals)
        score += avg_dep * 2.0
        inputs["avg_dependency"] = round(avg_dep, 2)

    return _safe_round(score), inputs


# ── SC-05 · Composite Score ─────────────────────────────────────────────────────

def compute_composite_score(result: ScoreResult) -> float | None:
    """
    SC-05: Gewichteter Composite aller Sub-Scores.

    Formel: Financial 25% + Strategic 20% + Market 20% + Ownership 15%
            + Value Driver 10% + (10−Risk) 10%

    Gibt None wenn weniger als 3 Sub-Scores berechnet wurden (zu wenig Datenbasis).
    Normalisiert Gewichte automatisch wenn Sub-Scores fehlen.
    """
    components: list[tuple[float, float]] = []   # (score_value, weight)

    def _try_add(score: float | None, key: str) -> None:
        if score is not None:
            components.append((score, _COMPOSITE_WEIGHTS[key]))

    _try_add(result.financial_score,    "financial")
    _try_add(result.strategic_score,    "strategic")
    _try_add(result.market_score,       "market")
    _try_add(result.ownership_score,    "ownership")
    _try_add(result.value_driver_score, "value_driver")

    if result.risk_score is not None:
        components.append((10.0 - result.risk_score, _COMPOSITE_WEIGHTS["risk_inv"]))

    if len(components) < 3:
        return None

    total_weight = sum(w for _, w in components)
    if total_weight == 0:
        return None

    composite = sum(s * w for s, w in components) / total_weight
    return _safe_round(composite)


# ── SC-11 · Hero Path + SC-13 · Rating ─────────────────────────────────────────

def select_hero_path(
    result: ScoreResult,
    is_listed: bool = False,
) -> tuple[str | None, float | None, str | None]:
    """
    SC-11: Wählt den dominanten Investitionspfad (argmax der Path-Scores).

    Paths mit Score < 1.5 gelten als N/A (nicht viable) und werden herausgefiltert.
    IPO wird für bereits gelistete Companies explizit ausgeschlossen — unabhängig
    vom gecachten Score (Fervo-Fix: ipo_status=listed aber Ticker noch nicht in DB).

    Returns: (hero_path, hero_score, hero_path_label)
    """
    candidates: dict[str, float] = {}
    # IPO nie Hero für listed Companies — auch wenn gecachter Score > 0
    if result.ipo_score     is not None and not is_listed:
        candidates["ipo"]     = result.ipo_score
    if result.ma_score      is not None: candidates["m_and_a"] = result.ma_score
    if result.etf_score     is not None: candidates["etf"]     = result.etf_score
    if result.enabler_score is not None: candidates["enabler"] = result.enabler_score

    if not candidates:
        return None, None, None

    # Viable Paths: Score ≥ 1.5
    viable = {k: v for k, v in candidates.items() if v >= 1.5}
    pool   = viable if viable else candidates   # Fallback: alle

    hero_path  = max(pool, key=lambda k: pool[k])
    hero_score = pool[hero_path]
    hero_label = _PATH_LABELS.get(hero_path)

    return hero_path, _safe_round(hero_score), hero_label


def derive_rating(hero_score: float | None, composite_score: float | None) -> str | None:
    """
    SC-13: Rating A–D.

    Primär: hero_score (path-kontextuelles Urteil).
    Fallback: composite_score (wenn kein hero vorhanden).
    Tiebreaker (BUG-44): wenn hero_score > 1.5 unter composite_score,
    composite_score verwenden — verhindert dass ein schwacher Hero-Path
    das Rating nach unten zieht (typisch: listed Companies ohne M&A-/IPO-Relevanz).

    A (No-Brainer)    ≥ 7.5
    B (Interessant)   ≥ 5.5
    C (Beobachten)    ≥ 3.5
    D (Uninteressant) < 3.5
    """
    if hero_score is not None and composite_score is not None:
        # Tiebreaker: composite gewinnt wenn Hero deutlich schlechter
        ref = composite_score if (composite_score - hero_score) > 1.5 else hero_score
    else:
        ref = hero_score if hero_score is not None else composite_score
    if ref is None:
        return None
    if ref >= 7.5: return "A"
    if ref >= 5.5: return "B"
    if ref >= 3.5: return "C"
    return "D"


# ── Haupt-Funktion ─────────────────────────────────────────────────────────────

def compute_dimension_risks(
    company:       dict,
    market_data:   dict | None       = None,
    signals:       list[dict] | None = None,
    value_drivers: list[dict] | None = None,
) -> dict:
    """
    SC-10 Basis: Algorithmische Opportunity/Risk-Scores für 6 Dimensionen.
    Datenmangel → neutral (4.0–5.0), kein halluziniertes Risiko (BUG-31).

    Returns: {dim_id: {opportunity_score, risk_score, data_confidence, opportunity_sources, risk_sources}}
    """
    md   = market_data or {}
    sigs = signals or []
    vd   = value_drivers or []

    dims: dict[str, dict] = {}

    # ─── 1. MARKET ─────────────────────────────────────────────────────────
    cagr        = md.get("cagr_pct")
    competition = md.get("competition_score")   # "low"|"medium"|"high"
    market_cycle= md.get("market_cycle")        # "early"|"growth"|"mature"|"consolidation"

    _CYCLE_OPP  = {"early": 7.5, "growth": 7.0, "mature": 5.0, "consolidation": 4.0}
    _CYCLE_RISK = {"early": 5.0, "growth": 3.5, "mature": 5.0, "consolidation": 7.5}
    _COMP_RISK  = {"low": 2.5, "medium": 5.0, "high": 8.0}

    mkt_opp = (
        min(10.0, max(3.0, float(cagr) * 0.4 + 5.0)) if cagr is not None
        else _CYCLE_OPP.get(market_cycle or "", 5.0)
    )
    comp_risk  = _COMP_RISK.get(competition or "", 4.0)
    cycle_risk = _CYCLE_RISK.get(market_cycle or "", 4.0)
    mkt_risk   = (comp_risk * 0.7 + cycle_risk * 0.3) if competition else 4.0

    mkt_conf = "high" if (cagr and competition) else ("medium" if (cagr or competition) else "low")
    dims["market"] = {
        "opportunity_score": _safe_round(mkt_opp),
        "risk_score":        _safe_round(mkt_risk),
        "data_confidence":   mkt_conf,
        "opportunity_sources": ["market_data.cagr_pct", "market_data.market_cycle"],
        "risk_sources":        ["market_data.competition_score", "market_data.market_cycle"],
    }

    # ─── 2. FINANCIALS ─────────────────────────────────────────────────────
    stage   = (_resolve_funding_stage(company)).lower().replace(" ", "_").replace("-", "_")
    funding = float(company.get("funding_total_usd_mn") or 0)

    _STAGE_OPP  = {
        "pre_seed": 3.0, "seed": 4.5, "series_a": 5.5, "series_b": 6.5,
        "series_c": 7.5, "series_d": 8.0, "series_d_plus": 8.5,
        "growth": 9.0, "pre_ipo": 9.5, "listed": 9.0,
    }
    _STAGE_RISK = {
        "pre_seed": 8.0, "seed": 7.0, "series_a": 6.0, "series_b": 5.0,
        "series_c": 4.0, "series_d": 3.5, "series_d_plus": 3.0,
        "growth": 2.5, "pre_ipo": 2.0, "listed": 2.0,
    }
    fin_opp  = _STAGE_OPP.get(stage, 5.0)
    fin_risk = _STAGE_RISK.get(stage, 5.0)

    if funding >= 1000:
        fin_opp = min(10.0, fin_opp + 0.5)
    elif funding < 50 and stage not in ("pre_seed", "seed"):
        fin_opp = max(0.0, fin_opp - 0.5)

    neg_fin = [s for s in sigs if s.get("signal_category") in ("negative_earnings", "high_burn", "debt_increase") and s.get("direction") == "negative"]
    fin_risk = min(10.0, fin_risk + len(neg_fin) * 0.75)

    fin_conf = "high" if (stage and funding > 0) else ("medium" if stage else "low")
    dims["financials"] = {
        "opportunity_score": _safe_round(fin_opp),
        "risk_score":        _safe_round(fin_risk),
        "data_confidence":   fin_conf,
        "opportunity_sources": ["companies.funding_stage", "companies.funding_total_usd_mn"],
        "risk_sources":        ["companies.funding_stage", "signals[negative_earnings,high_burn]"],
    }

    # ─── 3. STRATEGY (Wettbewerbsposition) ────────────────────────────────
    # Rationale: peer_count war ein inverser Proxy — mehr Peers = mehr Opportunität
    # ist strukturell falsch (mehr Peers = mehr Wettbewerb). Fix: competition_score
    # und market_cycle aus Market Data als primäre Basis, Peers als Risiko-Signal.
    peers_resolved = company.get("peers_resolved") or []
    peer_count     = len(peers_resolved)

    # Opportunität aus Market Cycle + Positive Signals (nicht aus Peer Count)
    _CYCLE_STRAT_OPP = {"early": 7.0, "growth": 6.5, "mature": 5.0, "consolidation": 4.5}
    market_cycle_strat = md.get("market_cycle") or ""
    strat_opp = _CYCLE_STRAT_OPP.get(market_cycle_strat, 5.0)

    # Risiko: Wettbewerbsdichte (competition_score) + Peer Count als Dichte-Proxy
    _COMP_STRAT_RISK = {"low": 2.5, "medium": 4.5, "high": 7.5}
    competition_strat = md.get("competition_score") or ""
    if competition_strat:
        strat_risk = _COMP_STRAT_RISK.get(competition_strat, 4.5)
    else:
        # Fallback: Peer Count als Wettbewerbsdichte-Proxy (Risiko, nicht Opportunität)
        strat_risk = min(7.5, 3.5 + peer_count * 0.4)

    strat_conf = "high" if (competition_strat and market_cycle_strat) else (
        "medium" if (competition_strat or market_cycle_strat) else
        ("medium" if peer_count >= 2 else "low")
    )

    strat_neg = [s for s in sigs if s.get("signal_category") in ("leadership_change", "strategy_pivot")]
    strat_pos = [s for s in sigs if s.get("signal_category") in ("new_partnership", "expansion", "acquisition") and s.get("direction") == "positive"]
    strat_risk = min(10.0, strat_risk + len(strat_neg) * 1.0)
    strat_opp  = min(10.0, strat_opp  + len(strat_pos) * 0.5)

    dims["strategy"] = {
        "opportunity_score": _safe_round(strat_opp),
        "risk_score":        _safe_round(strat_risk),
        "data_confidence":   strat_conf,
        "opportunity_sources": ["market_data.market_cycle", "signals[new_partnership,expansion]"],
        "risk_sources":        ["market_data.competition_score", "peers.competitive_density", "signals[leadership_change]"],
    }

    # ─── 4. POLITICAL ──────────────────────────────────────────────────────
    pol_pos = [s for s in sigs if s.get("signal_category") in ("regulatory_positive", "subsidy", "policy_support") and s.get("direction") == "positive"]
    pol_neg = [s for s in sigs if s.get("signal_category") in ("regulatory_intervention", "policy_risk", "sanctions") and s.get("direction") == "negative"]

    pol_opp  = min(10.0, 5.0 + len(pol_pos) * 1.0)
    pol_risk = min(10.0, 3.5 + len(pol_neg) * 1.5)

    pol_conf = "high" if (pol_pos or pol_neg) else "low"
    dims["political"] = {
        "opportunity_score": _safe_round(pol_opp),
        "risk_score":        _safe_round(pol_risk),
        "data_confidence":   pol_conf,
        "opportunity_sources": ["signals[regulatory_positive,subsidy,policy_support]"],
        "risk_sources":        ["signals[regulatory_intervention,policy_risk,sanctions]"],
    }

    # ─── 5. TECHNOLOGY (Stage-TR-Proxy + Signals + SE-14 Patent Depth) ────
    _STAGE_TR = {
        "pre_seed": 0.15, "seed": 0.20, "series_a": 0.35, "series_b": 0.50,
        "series_c": 0.65, "series_d": 0.75, "series_d_plus": 0.80,
        "growth": 0.85, "pre_ipo": 0.88, "listed": 0.90,
    }
    tr_proxy  = _STAGE_TR.get(stage, 0.50)
    tech_opp  = min(10.0, round(tr_proxy * 9.0 + 0.5, 1))
    tech_risk = min(10.0, round((1.0 - tr_proxy) * 6.5 + 1.0, 1))

    tech_pos = [s for s in sigs if s.get("signal_category") in ("patent", "new_product", "tech_milestone") and s.get("direction") == "positive"]
    tech_neg = [s for s in sigs if s.get("signal_category") in ("ip_risk", "tech_obsolescence") and s.get("direction") == "negative"]
    tech_opp  = min(10.0, tech_opp  + len(tech_pos) * 0.4)
    tech_risk = min(10.0, tech_risk + len(tech_neg) * 0.8)

    # SE-14: Patent-Tiefe aus EPO OPS — nur für PATENT_SCORING_SECTORS
    # Datensammlung universell; Scoring nur wo IP ein echter Moat ist (Deep Tech, Chemie, Pharma …)
    tech_conf    = "medium" if stage else "low"
    tech_sources = ["auto_tech_readiness(stage_proxy)", "signals[patent,tech_milestone]"]
    patent_bonus = 0.0
    category_raw = (company.get("category") or company.get("industry") or "").lower()
    in_patent_sector = _is_patent_relevant(category_raw)
    patent_count   = int(company.get("patent_count") or 0)
    granted_ratio  = float(company.get("patent_granted_ratio") or 0.0)

    if in_patent_sector and patent_count > 0:
        # Stufenbonus: Patente vorhanden (+0.5) → ≥30% granted (+0.5) → ≥60% granted (+0.5)
        patent_bonus = (
            0.5                              # Patente überhaupt vorhanden
            + (0.5 if granted_ratio >= 0.3 else 0.0)   # solides Portfolio
            + (0.5 if granted_ratio >= 0.6 else 0.0)   # starkes Portfolio
        )
        tech_opp  = min(10.0, tech_opp + patent_bonus)
        tech_conf = "high" if granted_ratio >= 0.3 else "medium"
        tech_sources.append(f"epo_ops(patent_count={patent_count}, granted_ratio={round(granted_ratio, 2)})")

    dims["technology"] = {
        "opportunity_score": _safe_round(tech_opp),
        "risk_score":        _safe_round(tech_risk),
        "data_confidence":   tech_conf,
        "opportunity_sources": tech_sources,
        "risk_sources":        ["auto_tech_readiness(stage_proxy,inverted)", "signals[ip_risk,obsolescence]"],
        # Debug-Felder für Tooltip
        "patent_count":        patent_count,
        "patent_granted_ratio": round(granted_ratio, 2),
        "patent_sector_active": in_patent_sector,
        "patent_bonus":         round(patent_bonus, 2),
    }

    # ─── 6. OPERATIONS (Abhängigkeitsrisiko aus Value Drivers) ─────────────
    enablers     = [v for v in vd if v.get("type") == "enabler"]
    contributors = [v for v in vd if v.get("type") == "contributor"]
    critical_e   = [e for e in enablers if e.get("dependency_level") == "critical"]
    high_e       = [e for e in enablers if e.get("dependency_level") == "high"]
    high_c       = [c for c in contributors if c.get("exposure_level") == "high"]

    if enablers or contributors:
        ops_opp  = min(10.0, 4.5 + len(contributors) * 0.4 + len(high_c) * 0.3)
        ops_risk = min(10.0, 3.0 + len(critical_e) * 2.5 + len(high_e) * 0.8)
        ops_conf = "high" if (critical_e or high_e) else "medium"
    else:
        ops_opp, ops_risk, ops_conf = 5.0, 4.0, "low"   # neutral, keine Bestrafung (BUG-31)

    ops_neg = [s for s in sigs if s.get("signal_category") in ("key_person_risk", "customer_concentration", "supply_chain_issue")]
    ops_pos = [s for s in sigs if s.get("signal_category") == "headcount_growth" and s.get("direction") == "positive"]
    ops_risk = min(10.0, ops_risk + len(ops_neg) * 0.5)
    ops_opp  = min(10.0, ops_opp  + len(ops_pos) * 0.3)

    dims["operations"] = {
        "opportunity_score": _safe_round(ops_opp),
        "risk_score":        _safe_round(ops_risk),
        "data_confidence":   ops_conf,
        "opportunity_sources": ["value_drivers.contributors", "value_drivers.exposure_level"],
        "risk_sources":        ["value_drivers.dependency_level(critical/high)", "signals[key_person_risk]"],
    }

    return dims


def compute_compound_risk_score(dimension_risks: dict) -> tuple[float, dict]:
    """
    SC-10: Compound Risk Score aus 6 Dimensions-Risiken (0–10, höher = mehr Risiko).
    Confidence-Dämpfung: low-confidence Dimensionen zählen 50% (kein Aufblasen durch Datenmangel).
    """
    _WEIGHTS: dict[str, float] = {
        "market":     0.20, "financials": 0.20, "strategy":  0.20,
        "operations": 0.15, "technology": 0.15, "political": 0.10,
    }
    _CONF_FACTOR = {"high": 1.0, "medium": 0.75, "low": 0.50}

    total_w = 0.0
    weighted_sum = 0.0
    inputs: dict = {}

    for dim_id, weight in _WEIGHTS.items():
        dim  = dimension_risks.get(dim_id, {})
        risk = dim.get("risk_score")
        if risk is None:
            continue
        conf  = dim.get("data_confidence", "low")
        eff_w = weight * _CONF_FACTOR.get(conf, 0.5)
        weighted_sum += float(risk) * eff_w
        total_w      += eff_w
        inputs[f"{dim_id}_risk"] = risk
        inputs[f"{dim_id}_conf"] = conf

    if total_w == 0:
        return 5.0, inputs

    return _safe_round(weighted_sum / total_w), inputs


def compute_all_scores(
    company: dict,
    market_data:         dict | None       = None,
    signals:             list[dict] | None = None,
    ownership_entries:   list[dict] | None = None,
    buyers:              list[dict] | None = None,
    value_drivers:       list[dict] | None = None,
    funding_momentum:    dict | None       = None,   # SC-01: Momentum-Pfad für US Private
    headcount_snapshots: list[dict] | None = None,   # SC-01: optionaler CAGR-Bonus
    ma_aggregate:        dict | None       = None,   # BUYER-AGG-01: Deal-Aggregat aus scoring.py-Engine
) -> ScoreResult:
    """
    SC-Haupt-Pipeline: Berechnet alle Sub-Scores, Path-Scores und Composite.

    Wird aufgerufen:
      - On-demand in company_detail.py (GET /api/v1/company/{name})
      - Täglich via Cron in main.py nach signal_engine-Lauf
      - Ergebnis wird in company_scores (Supabase) gecacht

    Args:
        company:           Vollständiger Company-Dict aus DB + Enrichment
        market_data:       Marktdaten-Dict (aus market_data_enrichment / /market Route)
        signals:           Signal-Liste (aus signals-Tabelle)
        ownership_entries: Ownership-Einträge (aus ownership_entries-Tabelle)
        buyers:            Buyer-Liste aus company response (competitors/buyers)
        value_drivers:     Value Driver Einträge (aus value_drivers-Tabelle)

    Returns:
        ScoreResult mit allen Scores, hero_path, rating, confidence, score_inputs.
    """
    mkt  = market_data or {}
    sigs = signals or []
    own  = ownership_entries or []
    buy  = buyers or []
    vds  = value_drivers or []

    result     = ScoreResult()
    all_inputs: dict = {}

    # ── Sub-Scores ──────────────────────────────────────────────────────────
    _run(result, "financial_score",    lambda: compute_financial_score(company, funding_momentum, headcount_snapshots), all_inputs, "financial",    "SC-01")
    _run(result, "strategic_score",    lambda: compute_strategic_score(company, buy, ma_aggregate), all_inputs, "strategic",    "SC-02")
    _run(result, "market_score",       lambda: compute_market_score(mkt, company),           all_inputs, "market",       "SC-03")
    _run(result, "risk_score",         lambda: compute_risk_score(company, sigs, own),       all_inputs, "risk",         "SC-04")
    _run(result, "ownership_score",    lambda: compute_ownership_score(company, own),        all_inputs, "ownership",    "SC-08")
    _run(result, "value_driver_score", lambda: compute_value_driver_score(company, vds),    all_inputs, "value_driver", "SC-09")

    # SC-10: Compound Risk Score (algorithmisch aus 6 Dimensionen)
    try:
        _dim_risks = compute_dimension_risks(company=company, market_data=mkt, signals=sigs, value_drivers=vds)
        _crs, _crs_inputs = compute_compound_risk_score(_dim_risks)
        result.compound_risk_score = _crs
        all_inputs["compound_risk"] = _crs_inputs
        logger.debug("SC-10 Compound Risk: %.2f", _crs)
    except Exception as e:
        logger.warning("SC-10 Compound Risk failed for %s: %s", company.get("name", "?"), e, exc_info=True)

    # ── Path-Scores ─────────────────────────────────────────────────────────
    _run(result, "ipo_score",          lambda: compute_ipo_score(company, sigs),             all_inputs, "ipo",          "IPO")
    _run(result, "ma_score",           lambda: compute_ma_score(company, ma_aggregate),      all_inputs, "ma",           "M&A")
    _run(result, "etf_score",          lambda: compute_etf_score(company, vds),              all_inputs, "etf",          "ETF")
    _run(result, "enabler_score",      lambda: compute_enabler_score(company, vds, buy),     all_inputs, "enabler",      "Enabler")

    # ── Composite + Hero ─────────────────────────────────────────────────────
    try:
        result.composite_score = compute_composite_score(result)
    except Exception as e:
        logger.warning("SC-05 Composite Score failed: %s", e)

    try:
        listed = _is_listed(company)
        result.hero_path, result.hero_score, result.hero_path_label = select_hero_path(result, is_listed=listed)
        result.rating = derive_rating(result.hero_score, result.composite_score)
    except Exception as e:
        logger.warning("SC-11/13 Hero Path / Rating failed: %s", e)

    # ── Confidence ───────────────────────────────────────────────────────────
    data_richness = sum(bool(v) for v in [
        company.get("revenue_usd_mn"),
        company.get("ebitda_margin") or company.get("ba_ebitda_mn"),
        mkt.get("cagr_pct"),
        sigs,
        own,
        vds,
        company.get("patent_count"),   # SE-14: EPO-Daten erhöhen Confidence
    ])
    result.confidence   = "high" if data_richness >= 5 else "medium" if data_richness >= 3 else "low"
    result.score_inputs = all_inputs

    logger.info(
        "Scores [%s]: composite=%.1f hero=%s(%.1f) rating=%s conf=%s",
        company.get("name", "?"),
        result.composite_score or 0.0,
        result.hero_path or "?",
        result.hero_score or 0.0,
        result.rating or "?",
        result.confidence,
    )

    return result


def _run(
    result: ScoreResult,
    attr: str,
    fn,
    all_inputs: dict,
    input_key: str,
    label: str,
) -> None:
    """Helper: Score-Funktion aufrufen, Ergebnis + Inputs speichern, Fehler abfangen."""
    try:
        score, inputs = fn()
        setattr(result, attr, score)
        all_inputs[input_key] = inputs
    except Exception as e:
        logger.warning("%s Score failed: %s", label, e)
