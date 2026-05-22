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

    # Composite + Hero (SC-05, SC-11, SC-13)
    composite_score:     float | None = None
    hero_path:           str   | None = None   # 'ipo' | 'm_and_a' | 'etf' | 'enabler'
    hero_score:          float | None = None
    hero_path_label:     str   | None = None   # Display: 'IPO Play' | 'M&A Target' etc.
    rating:              str   | None = None   # 'A' | 'B' | 'C' | 'D'

    # SC-12: Inputs für Tooltip + Debugging
    confidence:          str = "auto"
    score_inputs:        dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "financial_score":    self.financial_score,
            "strategic_score":    self.strategic_score,
            "market_score":       self.market_score,
            "risk_score":         self.risk_score,
            "ownership_score":    self.ownership_score,
            "value_driver_score": self.value_driver_score,
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
            "score_inputs":       self.score_inputs,
        }


# ── Lookup-Tabellen ────────────────────────────────────────────────────────────

# Kategorien mit guter ETF-Abdeckung (ICLN, QCLN, CTEC, BATT, LIT, ARKX usw.)
_ETF_COVERED_CATEGORIES = {
    "solar", "wind", "battery", "ev", "hydrogen", "carbon-capture",
    "energy-storage", "smart-grid", "climate-tech", "cleantech",
    "industrial-tech", "semiconductor", "biotech", "ai", "cloud",
    "space", "aerospace", "nuclear", "geothermal", "agri-tech",
    "heat-pump", "electrolysis", "fuel-cell", "direct-air-capture",
}

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
    "ipo":     "IPO Play",
    "m_and_a": "M&A Target",
    "etf":     "ETF Proxy",
    "enabler": "Enabler Play",
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

def compute_financial_score(company: dict) -> tuple[float, dict]:
    """
    SC-01: Financial Score (0–10).

    Inputs:
      revenue_usd_mn     → Umsatz-Basis (BA-Bridge oder Yahoo)
      ebitda_margin      → Marge (BA-Bridge oder Damodaran-Branche)
      revenue_cagr       → Wachstumsrate (aus kpi_timeseries)
      funding_stage      → Reifegrad-Proxy wenn Fundamentals fehlen

    Gewichtung:
      Revenue    0–3 Pkt  (Größe + Relevanz)
      Marge      0–3 Pkt  (Profitabilität)
      CAGR       0–2 Pkt  (Wachstumsdynamik)
      Stage      0–2 Pkt  (Reifegrad-Proxy)
    """
    inputs: dict = {}
    score = 0.0
    data_points = 0

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
    stage = company.get("funding_stage") or ""
    inputs["funding_stage"] = stage
    stage_pts = (
        2.0 if any(s in stage.lower() for s in ["series d", "series e", "pre-ipo", "growth"]) else
        1.5 if "series c" in stage.lower() else
        1.0 if "series b" in stage.lower() else
        0.5 if "series a" in stage.lower() else
        0.5 if "seed" in stage.lower() else
        1.5 if "bootstrap" in stage.lower() else   # Bootstrapped → profitabel
        1.5 if "listed" in stage.lower() else       # listed = hat Kapitalmarkttest bestanden
        1.0
    )
    score += stage_pts
    data_points += 1

    # Bei sehr wenig Datenbasis: Minimum-Baseline setzen (verhindert 0.5-Scores aus Stage allein)
    if data_points <= 1:
        score = max(score, 2.5)

    return _safe_round(score), inputs


# ── SC-02 · Strategic Score ────────────────────────────────────────────────────

def compute_strategic_score(company: dict, buyers: list[dict]) -> tuple[float, dict]:
    """
    SC-02: Strategic Score (0–10).

    Inputs: srr, mfr_confidence, tech_readiness, buyer_count.

    Gewichtung:
      SRR             0–3.5 Pkt  (Strategische Relevanz)
      TechReadiness   0–3.5 Pkt  (Technologische Reife)
      MFR             0–3.0 Pkt  (Buyer Feasibility)
      Buyer Bonus     0–1.0 Pkt  (Anzahl valider Käufer)
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

    # Feasible Buyer Count Bonus (max +1.0 bei ≥5 Käufern)
    feasible_buyers = [b for b in (buyers or []) if b.get("mfr") == "Feasible"]
    buyer_bonus = min(1.0, len(feasible_buyers) * 0.25)
    inputs["feasible_buyers"] = len(feasible_buyers)

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

    # Beta / Markt-Volatilität (0–2) — nur sinnvoll für listed
    beta = company.get("beta_1y") or company.get("beta")
    if beta is not None:
        beta = float(beta)
        inputs["beta"] = beta
        if beta >= 2.0:    score += 2.0
        elif beta >= 1.5:  score += 1.5
        elif beta >= 1.0:  score += 1.0
        elif beta >= 0.5:  score += 0.5
        # low beta: kein Beitrag

    # Governance (0–3): Ownership-Intransparenz = Risiko
    is_listed = bool(company.get("ticker"))
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
    stage = company.get("funding_stage") or ""
    stage_lower = stage.lower()
    stage_risk = (
        2.0 if "seed" in stage_lower else
        1.5 if "series a" in stage_lower else
        1.0 if "series b" in stage_lower else
        0.5 if "series c" in stage_lower else
        0.3 if any(s in stage_lower for s in ["series d", "pre-ipo"]) else
        0.1 if "listed" in stage_lower else
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
    is_listed = bool(company.get("ticker"))

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
      TechReadiness      0–3.0 Pkt  (Proxy für Innovations- und Skalierungspotenzial)
      Driver Count       0–2.0 Pkt  (Anzahl identifizierter Value Drivers)
      Dependency Score   0–3.0 Pkt  (Durchschnitt; hohe Abhängigkeit = starke Position)
      Market Position    0–1.5 Pkt  (Leader/Dominant Einträge)
      Baseline           0.5  Pkt   (immer)
    """
    inputs: dict = {}
    score = 0.5   # Baseline

    tr = float(company.get("tech_readiness") or 0.5)
    inputs["tech_readiness"] = tr
    score += tr * 3.0

    vds = value_drivers or []
    if not vds:
        inputs["driver_count"] = 0
        return _safe_round(score), inputs

    # Driver Count (0–2)
    score += min(2.0, len(vds) * 0.4)
    inputs["driver_count"] = len(vds)

    # Dependency Score Durchschnitt (0–3)
    dep_vals = [float(d["dependency_score"]) for d in vds if d.get("dependency_score") is not None]
    if dep_vals:
        avg_dep = sum(dep_vals) / len(dep_vals)
        score += avg_dep * 3.0
        inputs["avg_dependency_score"] = round(avg_dep, 2)

    # Market Position Bonus (0–1.5)
    strong = sum(
        1 for d in vds
        if (d.get("market_position") or "").lower() in ("leader", "market leader", "dominant", "monopol", "quasi-monopol")
    )
    score += min(1.5, strong * 0.75)
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

    is_listed = bool(company.get("ticker"))
    if is_listed:
        inputs["note"] = "already_listed"
        return 0.0, inputs

    stage         = company.get("funding_stage") or ""
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


def compute_ma_score(company: dict, buyers: list[dict]) -> tuple[float, dict]:
    """
    M&A Score (0–10): Attraktivität als Akquisitions-Target.

    Basis: SRR × MFR × TechReadiness (bestehende Deal-Composite-Logik).
    Ergänzt durch Stage-Attraktivität + Feasible Buyer Count.

    Hoch für: Series A–C, hoher SRR, Feasible MFR, viele potenzielle Käufer.
    Niedrig für: S-1 gefilter (geht an Markt), bereits listed (zu teuer).
    """
    inputs: dict = {}

    srr = company.get("srr") or company.get("strategic_relevance_rating")
    mfr = company.get("mfr_confidence")
    tr  = float(company.get("tech_readiness") or 0.5)
    stage = company.get("funding_stage") or ""

    inputs.update({"srr": srr, "mfr": mfr, "tech_readiness": tr, "funding_stage": stage})

    srr_pts   = _SRR_SCORE.get(srr, 1.0)
    mfr_pts   = _MFR_SCORE.get(mfr, 0.5)
    tr_pts    = tr * 3.0
    stage_pts = _stage_match(stage, _STAGE_MA_SCORE) * 0.1   # max +0.85 Pkt

    feasible = [b for b in (buyers or []) if b.get("mfr") == "Feasible"]
    buyer_pts = min(1.5, len(feasible) * 0.5)
    inputs["feasible_buyers"] = len(feasible)

    score = srr_pts + mfr_pts + tr_pts + stage_pts + buyer_pts
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

    is_listed = bool(company.get("ticker"))
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
    cat_match = any(c in category or c in industry for c in _ETF_COVERED_CATEGORIES)
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

def select_hero_path(result: ScoreResult) -> tuple[str | None, float | None, str | None]:
    """
    SC-11: Wählt den dominanten Investitionspfad (argmax der Path-Scores).

    Paths mit Score < 1.5 gelten als N/A (nicht viable) und werden herausgefiltert.
    Wenn alle Paths < 1.5: bester Path trotzdem als Hero (kein Null-Hero).

    Returns: (hero_path, hero_score, hero_path_label)
    """
    candidates: dict[str, float] = {}
    if result.ipo_score     is not None: candidates["ipo"]     = result.ipo_score
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

    A (No-Brainer)    ≥ 7.5
    B (Interessant)   ≥ 5.5
    C (Beobachten)    ≥ 3.5
    D (Uninteressant) < 3.5
    """
    ref = hero_score if hero_score is not None else composite_score
    if ref is None:
        return None
    if ref >= 7.5: return "A"
    if ref >= 5.5: return "B"
    if ref >= 3.5: return "C"
    return "D"


# ── Haupt-Funktion ─────────────────────────────────────────────────────────────

def compute_all_scores(
    company: dict,
    market_data:       dict | None       = None,
    signals:           list[dict] | None = None,
    ownership_entries: list[dict] | None = None,
    buyers:            list[dict] | None = None,
    value_drivers:     list[dict] | None = None,
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
    _run(result, "financial_score",    lambda: compute_financial_score(company),             all_inputs, "financial",    "SC-01")
    _run(result, "strategic_score",    lambda: compute_strategic_score(company, buy),        all_inputs, "strategic",    "SC-02")
    _run(result, "market_score",       lambda: compute_market_score(mkt, company),           all_inputs, "market",       "SC-03")
    _run(result, "risk_score",         lambda: compute_risk_score(company, sigs, own),       all_inputs, "risk",         "SC-04")
    _run(result, "ownership_score",    lambda: compute_ownership_score(company, own),        all_inputs, "ownership",    "SC-08")
    _run(result, "value_driver_score", lambda: compute_value_driver_score(company, vds),    all_inputs, "value_driver", "SC-09")

    # ── Path-Scores ─────────────────────────────────────────────────────────
    _run(result, "ipo_score",          lambda: compute_ipo_score(company, sigs),             all_inputs, "ipo",          "IPO")
    _run(result, "ma_score",           lambda: compute_ma_score(company, buy),               all_inputs, "ma",           "M&A")
    _run(result, "etf_score",          lambda: compute_etf_score(company, vds),              all_inputs, "etf",          "ETF")
    _run(result, "enabler_score",      lambda: compute_enabler_score(company, vds, buy),     all_inputs, "enabler",      "Enabler")

    # ── Composite + Hero ─────────────────────────────────────────────────────
    try:
        result.composite_score = compute_composite_score(result)
    except Exception as e:
        logger.warning("SC-05 Composite Score failed: %s", e)

    try:
        result.hero_path, result.hero_score, result.hero_path_label = select_hero_path(result)
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
