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
                             (S88, SUBSCORE-COMPOSITION-AUDIT-01: aus dem
                             Composite entfernt, s. SC-10. Weiterhin berechnet
                             als Frontend-Fallback/Diagnose, kein UI-Tab zeigt
                             SC-04 direkt.)
  SC-08  Ownership Score     — Transparenz-Score (Quelle) ± Investoren-Tier-
                             Modifier (CATEGORY-CEILING-REVIEW-01, S84 —
                             war Investor-Qualität + Diversifikation +
                             Transparenz, s. compute_ownership_score)
  SC-09  Value Driver Score  — Dependency (Enabler, dependency_level seit
                             S88) + Market Position + Driver Count
  SC-10  Compound Risk Score — 7 Dimensionen (Market/Financials/Strategy/
                             Operations/Technology/Political/Governance, S88:
                             +Governance +Beta-in-Market). Seit S88 alleiniger
                             Risk-Lieferant für den Composite (löst SC-04 ab).

Path-Scores (0–10):
  IPO Score      — TechReadiness + IPO Signals + Funding Stage + ipo_potential
  M&A Score      — SRR × MFR × TechReadiness × Buyer Fit
  ETF Score      — Listed Status + Kategorie-Coverage + Explizite ETF-Proxies
  Enabler Score  — B2B-Industrie + Dependency Scores + TechReadiness

Composite + Rating:
  SC-05  Composite Score     — Gewichteter Durchschnitt der Sub-Scores
                             COMPOSITE-DEFINITION-01 (S81): Financial 25% +
                             Market 20% + Ownership 15% + Value Driver 10%
                             + (10−Risk) 10% (Basis 0.80, auto-normalisiert).
                             SUBSCORE-COMPOSITION-AUDIT-01 (S88): Risk-
                             Komponente liest jetzt SC-10 (compound_risk_score)
                             statt SC-04 — derselbe Score, den der Potenziale
                             & Risiken-Tab anzeigt, kein unsichtbarer Parallel-
                             Score mehr. SC-02 Strategic bewusst NICHT Teil des
                             Composite — ist Buyer-Matching-Signal (M&A), keine
                             Company-intrinsische Attraktivität. Bleibt eigen-
                             ständig berechnet, speist weiterhin M&A Score.
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
# STAGE-FORMAT-MISMATCH-01 (S75): Keys auf Underscore-Format umgestellt
# (DB-Konvention companies.funding_stage), vorher Leerzeichen-Keys, die nie
# gegen den echten DB-Wert matchten. series_d_plus hat bewusst keinen eigenen
# Key — _stage_match() (longest-match-Substring) faellt auf "series_d" zurueck,
# identischer Score wie series_d. Eigene Kalibrierung waere TR-STAGE-OVERLAP-01-
# Folgearbeit, nicht Teil dieses Format-Fixes.
_STAGE_IPO_SCORE: dict[str, float] = {
    "s_1_filed":  9.0,
    "pre_ipo":    8.0,
    "series_d":   6.5,
    "series_c":   5.5,
    "series_b":   4.0,
    "series_a":   3.0,
    "seed":       2.0,
    "bootstrap":  1.5,
    "listed":     0.0,   # bereits gelistet → IPO-Pfad abgeschlossen
}

# M&A-Attraktivität je Stage (Series A–C = Sweet Spot für Acqui-Hire + Strategic)
# STAGE-FORMAT-MISMATCH-01 (S75): Keys auf Underscore-Format, siehe Begründung
# bei _STAGE_IPO_SCORE.
_STAGE_MA_SCORE: dict[str, float] = {
    "seed":       6.0,
    "series_a":   7.5,
    "series_b":   8.5,
    "series_c":   7.5,
    "series_d":   6.0,
    "pre_ipo":    4.5,   # zu nah am Börsengang → M&A unwahrscheinlicher
    "s_1_filed":  2.5,
    "listed":     2.0,   # möglich aber teuer (Public-to-Private)
    "bootstrap":  7.0,   # Owner-managed = klassisches Akquisitions-Target
}

# SC02-MA-UNIFY-01 (S76): _SRR_CATEGORY_SCORE + _MFR_SIGNAL_SCORE entfernt.
# SC-02 las bisher die String-Labels (srr_category/mfr_signal) des besten Buyers
# und bog sie in zwei separat kalibrierte Punktetabellen — eine zweite
# Aggregations-Logik neben der echten Per-Buyer-Engine (scoring.py). M&A Score
# nutzte parallel den kontinuierlichen deal_success_score (srr_norm × mfr_norm ×
# tr.value). Zwei Namen, eine Grundgröße, unterschiedlich aggregiert → nicht
# nachvollziehbar. Jetzt lesen BEIDE deal_success_score direkt, nur auf
# unterschiedlicher Aggregationsstufe (SC-02: bester Buyer; M&A: Top-3-Mittel).
# TR ist in deal_success_score bereits enthalten (multiplikativ) — kein
# separates tr_pts×3.5 mehr in SC-02. Siehe compute_strategic_score.

# Investor-Tier-Lookup (keyword → 1–3)
_INVESTOR_TIER_KW: list[tuple[str, int]] = [
    # Tier 3 — Top-VC / Strategic Flagship
    ("sequoia", 3), ("a16z", 3), ("andreessen horowitz", 3), ("benchmark", 3),
    ("kleiner", 3), ("accel", 3), ("index ventures", 3), ("tiger global", 3),
    ("softbank", 3), ("general catalyst", 3), ("lightspeed", 3), ("greylock", 3),
    ("khosla", 3), ("founders fund", 3), ("breakthrough energy", 3),
    ("lowercarbon", 3), ("google ventures", 3), ("gv", 3), ("microsoft ventures", 3),
    ("amazon industrial", 3), ("2150", 3), ("systemiq", 3),
    # Tier 3 — EU-Buyout/Growth (CATEGORY-CEILING-REVIEW-01, S84: fehlten
    # komplett, obwohl reputationsgleich zu den US-Mega-Funds oben —
    # INVESTOR-TIER-LABEL-01 US-Westcoast-Bias-Befund, jetzt teilweise
    # behoben. Vollständigkeitslücke, keine strukturelle Asymmetrie wie das
    # SC-08-Register-Thema — bewusst getrennt gehalten.)
    ("eqt", 3), ("permira", 3), ("cinven", 3), ("nordic capital", 3),
    ("cvc capital", 3), ("apax", 3), ("bridgepoint", 3),
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
# COMPOSITE-DEFINITION-01 (S81, Andreas-Entscheidung): "strategic" (SC-02) entfernt.
# SC-02 ist deal_success_score des besten Buyers — eine Buyer-Matching-Größe
# (M&A-Signal), keine Company-intrinsische Attraktivität. Composite soll laut
# Andreas NUR Marktposition/-potential + Risiko + finanzielle Situation messen.
# Beweis der Kontamination: Mega-Caps/listed ohne Buyer-Aggregat (Acquirability-
# Gate) fielen strukturell auf SC-02≈1.5-1.8/10 zurück (compute_strategic_score,
# "no_buyer_aggregate"-Fallback), was den Composite bei 20% Gewicht unabhängig
# von der tatsächlichen Unternehmensqualität nach unten zog (Apple/Nvidia <5,
# SCORING-TAB-AUDIT-01). SC-02 bleibt vollständig berechnet + im Buyer-Tab
# sichtbar + speist weiterhin M&A Score (SC02-MA-UNIFY-01) — nur der Composite-
# Beitrag entfällt. Restliche Gewichte summieren jetzt auf 0.80 (statt 1.0) —
# compute_composite_score() normalisiert bereits automatisch über total_weight,
# kein manuelles Reskalieren der verbleibenden vier Werte nötig.
_COMPOSITE_WEIGHTS: dict[str, float] = {
    "financial":    0.25,
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
    Robuste Listed-Erkennung — v3 (LISTED-STATUS-REVIEW-01, S75).

    Jetzt die EINZIGE Implementierung im Backend — company_detail.py's
    `_resolve_is_listed()` importiert diese Funktion (Alias), statt eine
    eigene Kopie zu pflegen. Identische Prüfreihenfolge:

      1. companies.is_listed (generated column, COL-ISLISTED-01) — bevorzugt,
         Single Source of Truth, garantiert konsistent.
      2. ipo_status == "listed"            (kanonisch, migration_003)
      3. ipo_potential == "IPO erfolgt"    (Legacy-Fallback. DB-Check S75:
         0 Zeilen mit ipo_potential='IPO erfolgt' UND ipo_status≠'listed' —
         Fallback aktuell nie scharf, bleibt aber für Teilprojektionen ohne
         is_listed-Spalte bzw. künftige Altdaten-Importe stehen.)
      4. ticker gesetzt                    (eigener Ticker = selbst börsennotiert)

    Vorher prüfte diese Funktion drei zusätzliche ipo_status-Werte ("public",
    "ipo_erfolgt", "ipo erfolgt"), die seit IPO-STATUS-ENUM-01 (S69) strukturell
    nie mehr in companies.ipo_status stehen können (Typ-Kommentar in
    company_detail.py: ipo_status ∈ {listed, pre_ipo_high, pre_ipo_medium,
    pre_ipo_low, NULL}) — toter Code, jetzt entfernt. Sie las außerdem nie die
    is_listed-Spalte und nie ipo_potential — echte Divergenz zu
    company_detail.py's bisheriger Eigenimplementierung war dadurch möglich
    (Company mit ipo_potential="IPO erfolgt", aber noch ohne Ticker und ohne
    migriertem ipo_status — aktuell laut DB-Check 0 Fälle, aber strukturell
    nicht ausgeschlossen).
    """
    _col = company.get("is_listed")
    if _col is not None:
        return bool(_col)
    if company.get("ipo_status") == "listed":
        return True
    if company.get("ipo_potential") == "IPO erfolgt":
        return True
    if company.get("ticker"):
        return True
    return False


def _resolve_funding_stage(company: dict) -> str:
    """
    BUG-51: Normalisiert funding_stage für alle Score-Funktionen.
    Listed Companies haben keine Venture-Runden in DB → funding_stage wäre NULL.
    Fix: ipo_status == 'listed' → 'listed' direkt setzen, unabhängig von funding_rounds.

    STAGE-FORMAT-MISMATCH-01 (S75): Zwei Fixes am Single Chokepoint, alle
    Aufrufer profitieren automatisch:
    1) Rückgabewert ist jetzt durchgängig Underscore-normalisiert (DB-Konvention,
       z.B. "series_b") statt den Rohwert unverändert durchzureichen. Vorher
       matchte das zufällig gegen Leerzeichen-Keys in den Konsumenten, nie
       gegen den echten DB-Wert.
    2) "public" → "listed" — sämtliche Lookup-Tabellen (inkl. der Underscore-
       Tabellen in compute_dimension_risks) keyen auf "listed", nie auf
       "public". Nebeneffekt: behebt denselben Mismatch auch für SC-10
       (compute_dimension_risks fiel für gelistete Companies bisher auf den
       5.0/5.0/0.50-Default zurück statt die "listed"-Einträge zu nutzen —
       war in der S74-Diagnose als "NICHT SC-10 betroffen" eingestuft; das
       gilt weiterhin für den Series-A–D-Bug, aber nicht für diesen zweiten,
       public/listed-Teilbug).
    Randfall beim Test gegen das DB-Sample gefunden: 3 Companies haben
    funding_stage="public" als Rohwert (nicht aus _is_listed() abgeleitet).
    Falls bei diesen ticker/ipo_status NICHT gesetzt sind, würde _is_listed()
    False liefern und der Rohwert "public" unverändert durchfallen — selbe
    Lücke wie oben, nur über den anderen Pfad. Defensiv mitgefangen, ohne
    Kenntnis ob der Fall in der Live-DB tatsächlich auftritt (nicht verifiziert,
    nur abgesichert).
    """
    if _is_listed(company):
        return "listed"
    stage = (company.get("funding_stage") or "").lower().replace(" ", "_").replace("-", "_")
    return "listed" if stage == "public" else stage


# SC02-MA-UNIFY-01 + TR-CONSISTENCY-AUDIT-01 (S76): _compute_target_tech_readiness()
# ENTFERNT. War der gemeinsame Company-Level-TR-Helper für SC-02/09/IPO/ETF/
# Enabler. Nach diesem Block hat KEIN Score in dieser Datei mehr eine
# TechReadiness-Komponente:
#   - SC-02 liest deal_success_score direkt (TR multiplikativ enthalten, kommt
#     aus dem Per-Buyer-Loop in company_detail.py / scoring.py)
#   - IPO Score: TR raus (TR-STAGE-OVERLAP-01, S75)
#   - ETF / Enabler / Value Driver: TR raus (TR-CONSISTENCY-AUDIT-01, S76)
# TechReadiness lebt damit AUSSCHLIESSLICH in der Per-Buyer-Engine (scoring.py),
# wo sie als Buyer↔Target-Fit-Signal hingehört. Der User-Override (_tr_override)
# fließt dort über AnalyzeRequest.tech_readiness_override ein, nicht mehr hier.


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


_LOGGED_UNKNOWN_OWNERSHIP_SOURCES: set[str] = set()

# Bewusst im 6.0-Fallback eingeordnete, bekannte Tags — kein Log-Rauschen.
# Nur Tags AUSSERHALB dieser Menge sind eine echte Überraschung.
_KNOWN_FALLBACK_OWNERSHIP_SOURCES: set[str] = {"manual", "curated", "enrichment"}


def _log_unknown_ownership_source_once(sources: set[str]) -> None:
    """
    CATEGORY-CEILING-REVIEW-01 (S84): fängt WIRKLICH neue ownership_entries.
    source-Werte sichtbar ab (z. B. ein zukünftiger North-Data/OpenRegister-
    Tag), statt sie lautlos auf den 6.0-Fallback fallen zu lassen — gleiches
    Prinzip wie _log_unknown_schema_once (ESMA-Fix, S84). manual/curated/
    enrichment sind bewusst im Fallback und lösen KEIN Log aus, sonst Rauschen
    bei jedem regulären Aufruf. Pro Tag nur einmal pro Prozess geloggt.
    """
    surprising = sources - _KNOWN_FALLBACK_OWNERSHIP_SOURCES - _LOGGED_UNKNOWN_OWNERSHIP_SOURCES
    if not surprising:
        return
    _LOGGED_UNKNOWN_OWNERSHIP_SOURCES.update(surprising)
    logger.warning(
        "SC-08: unbekannte(r) ownership_entries.source-Tag(s) %s auf 6.0-Fallback "
        "eingeordnet — Transparenz-Ladder in compute_ownership_score ggf. nachziehen.",
        sorted(surprising),
    )


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
        2.0 if any(s in stage.lower() for s in ["series_d", "series_e", "pre_ipo", "growth"]) else
        1.5 if "series_c" in stage.lower() else
        1.0 if "series_b" in stage.lower() else
        0.5 if "series_a" in stage.lower() else
        0.5 if "seed" in stage.lower() else
        1.5 if "bootstrap" in stage.lower() else   # Bootstrapped → profitabel
        1.5 if any(s in stage.lower() for s in ["listed", "public"]) else       # listed/public = hat Kapitalmarkttest bestanden
        1.0
    )
    score += stage_pts
    data_points += 1

    # Bei sehr wenig Datenbasis: additiver Cushion statt Flat-Override (COMPOSITE-
    # STAGE-MULTICOUNT-01-Audit, S78). Vorher max(score, 2.5) buegelte JEDEN
    # stage_pts-Wert (0.5-2.0) auf eine flache 2.5 — das eigentliche Stage-Signal
    # ging fuer Companies ohne Revenue/Margin/CAGR komplett verloren (Mehrheit
    # des privaten Portfolios laut S78-DB-Sample). Additiv erhaelt die
    # Untergrenze fuer den schlechtesten Fall exakt (stage_pts=0.5 -> 2.5,
    # identisch zum alten Floor) UND laesst bessere Stages nach oben
    # differenzieren (stage_pts=2.0 -> 4.0 statt 2.5). Ob 2.5 der richtige
    # Cushion-Wert ist (vs. SC-10s 4.0-5.0-Neutral-Konvention) ist Kalibrierung,
    # bewusst nicht Teil dieses Fixes.
    if data_points <= 1:
        score += 2.0

    return _safe_round(score), inputs


# ── SC-02 · Strategic Score ────────────────────────────────────────────────────

def compute_strategic_score(company: dict, ma_aggregate: dict | None = None) -> tuple[float, dict]:
    """
    SC-02: Strategic Score (0–10).

    SC02-MA-UNIFY-01 (S76): Liest jetzt die EINE kontinuierliche Grundgröße der
    Per-Buyer-Engine — deal_success_score (= srr_norm × mfr_norm × tr.value,
    scoring.py) — direkt, statt die String-Labels (srr_category/mfr_signal) in
    eine zweite, separat kalibrierte Punktetabelle zu biegen. Dieselbe Rohgröße
    wie compute_ma_score, nur andere Aggregationsstufe:
      SC-02      = deal_success_score des BESTEN Buyers      → "gibt es überhaupt
                   eine strategisch starke Story?"
      M&A Score  = Top-3-Mittel der deal_success_scores       → "wie wahrscheinlich
                   ist ein Exit über die realistischsten Käufer?"
    Damit ist die Zahl für den User nachvollziehbar (eine Quelle, zwei
    Aggregationen) statt zweier zufällig ähnlicher Kategorie-Lookups.

    TechReadiness ist in deal_success_score bereits multiplikativ enthalten —
    KEIN separates tr_pts × 3.5 mehr (das war die SC02-REWORK-01-Logik, jetzt
    abgelöst). TR-STAGE-OVERLAP-01 hat dieselbe Doppelzählungs-Lehre für den
    IPO Score gezogen; hier ist die saubere Lösung die gemeinsame Basisgröße,
    nicht ein additiver TR-Summand.

    Hinweis zum Wertebereich: Auto-TR cappt bei 0.92 (scoring.py), d.h. der
    deal_success-Anteil erreicht im Auto-Modus nominal max 9.2 von 10 — der
    Buyer-Bonus (0–1.0) füllt den Rest. Bei manuellem User-Override (TR bis 1.0)
    sind volle 10 erreichbar. Das ist Absicht, kein Deckelungs-Bug.

    contributors[0] trägt deal_success_score (company_detail.py::_ma_aggregate_meta).
    Kein Buyer-Aggregat (noch nicht angereichert / kein tauglicher Käufer) →
    konservativer Sockel statt 0, damit eine Company ohne Buyer-Scoring nicht
    fälschlich als strategisch wertlos erscheint.

    Signatur-Cleanup (SC02-REWORK-01-Folge): `buyers`-Parameter bereits entfernt.

    Gewichtung:
      Deal-Success (bester Buyer)   0–9.2/10 Pkt  (× 10, TR multiplikativ enthalten)
      Buyer Bonus                   0–1.0 Pkt     (Anzahl realistischer Käufer)
    """
    inputs: dict = {}
    agg = ma_aggregate or {}
    contributors = agg.get("contributors") or []
    top = contributors[0] if contributors else {}

    top_dss = top.get("deal_success_score")
    inputs["top_buyer"]           = top.get("buyer")
    inputs["top_deal_success"]    = top_dss
    inputs["srr_category"]        = top.get("srr_category")   # nur Anzeige/Tooltip
    inputs["mfr_signal"]          = top.get("mfr_signal")     # nur Anzeige/Tooltip

    if top_dss is None:
        # Kein Buyer-Scoring verfügbar (Mega-Cap/listed → kein Aggregat per
        # Acquirability-Gate; oder Buyer-Enrichment noch nicht gelaufen).
        # Stage-sensitiver konservativer Sockel statt platt — analog zum
        # no-buyer-Fallback in compute_ma_score, damit eine private Company
        # ohne fertiges Buyer-Enrichment nicht kurzzeitig auf einen
        # Einheitswert fällt. (Hinweis: SC-02 fließt NICHT in den Composite,
        # s. COMPOSITE-DEFINITION-01/S81 — der Sockel dient allein der
        # Aussagekraft von SC-02 selbst im Buyer-/M&A-Kontext.)
        stage = _resolve_funding_stage(company)
        stage_pts = _stage_match(stage, _STAGE_MA_SCORE) * 0.15   # 0..1.275
        score = 1.5 + stage_pts
        inputs["funding_stage"] = stage
        inputs["fallback"] = "no_buyer_aggregate"
        return _safe_round(score), inputs

    deal_pts = float(top_dss) * 10.0

    # Buyer Bonus aus echtem Deal-Aggregat (feasible_count) — unverändert seit BUYER-AGG-01
    feasible_count = agg.get("feasible_count", 0)
    buyer_bonus = min(1.0, feasible_count * 0.25)
    inputs["feasible_buyers"] = feasible_count

    score = deal_pts + buyer_bonus
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
    # CATEGORY-CEILING-REVIEW-01 (S84, Andreas-Entscheidung): No-Data-Floor
    # war 3.0 (= maximales Risiko, identisch zu aktiv verschleiert). "Keine
    # Daten" darf nicht schlechter stehen als "eine bekannte Beteiligung" —
    # jetzt auf Höhe von single_entry (1.5), SC-10-Neutralkonvention
    # (BUG-31: "Datenmangel → neutral, kein halluziniertes Risiko").
    is_listed = _is_listed(company)
    if is_listed:
        score += 0.3   # listed: öffentliche Rechenschaftspflicht → niedrig
        inputs["governance"] = "listed"
    elif not ownership_entries:
        score += 1.5   # keine Daten = neutral, kein Verschleierungs-Signal
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
        1.5 if "series_a" in stage_lower else
        1.0 if "series_b" in stage_lower else
        0.5 if "series_c" in stage_lower else
        0.3 if any(s in stage_lower for s in ["series_d", "pre_ipo"]) else
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
    SC-08: Ownership Score (0–10) — Transparenz-Score, keine Investoren-Bewertung.

    CATEGORY-CEILING-REVIEW-01 (S84, Andreas-Entscheidung): Misst, wie
    belastbar/vollständig Argos Kenntnis der Eigentümerstruktur ist — NICHT,
    wie "gut" die Investoren sind. Vorher: quality_pts (Investoren-Tier)
    machte 0–5 von ~10.5 Pkt aus (fast 50%) und vermischte ein Werturteil
    über Investoren mit einer Datenbelastbarkeits-Metrik (INVESTOR-TIER-
    LABEL-01-Kopplung). Jetzt: Quelle bestimmt den Basiswert, Investoren-Tier
    ist nur noch ein kleiner, gedeckelter Modifier.

    Tag-Inventar gegen drei echte Dateien verifiziert (ownership_enrichment.py,
    company_detail.py, hai_enrichment.py) — NICHT mehr geraten (die erste
    Fassung dieses Patches hatte "wikipedia"/"edgar"/"sec" verdrahtet, was
    keinem echten Tag entsprach und praktisch alles auf "manual" durchfallen
    ließ — S84, Selbstkorrektur im selben Rutsch).

    Transparenz-Ladder (region-neutral, sourcenklassenbasiert statt listed-
    Flag oder Datentiefe — misst Quellen-AUTORITÄT, nicht wie vollständig
    der aktuelle Parse ist; vollständigeres Filing-Parsing ist ein separates,
    zukünftiges Ticket):
      10.0  handelsregister (hai_enrichment.py), bundesanzeiger/ba_bridge
            (Legacy, Bestands-Rows vor Bridge-Kill S52), bafin_stimmrechte
            (WpHG-Meldepflicht DE-Listed), edgar_sc_13g/edgar_sc_13d
            (US-Listed Pflicht-Offenlegung, EN-07) — amtlich/pflichtoffengelegt.
       8.0  edgar_form_d, edgar_d (Form-D-Investoren/Directors, keine
            Offenlegungspflicht), funding_rounds (Lead/Co-Investor aus
            Funding-Runden), yahoo_institutional (13F-abgeleitet, aber nur
            Top-Holder via Drittanbieter — Andreas, S84: "wenn nur
            eingeschränkte Pflichtdaten, dann eher mit einer 8 ansetzen").
            8.0 ist zugleich der strukturelle Bestwert für US-Private: kein
            öffentliches UBO-Register in den USA seit FinCEN-BOI-Aussetzung
            für Inlandsunternehmen (Interim Final Rule 26.03.2025, weiterhin
            in Kraft, S84-Recherche) — anders als DE (Handelsregister) ist
            die 10 für US-Private strukturell nicht erreichbar, unabhängig
            von Datenqualität. Andreas: "das wäre ein gemeinsamer Anspruch
            an Transparenz, die US-Unternehmen nicht erfüllen können."
       7.0  wikipedia_infobox
       6.0  curated (company_detail.py::_OWNERSHIP_OVERRIDES — statischer
            ~10-Company-Dict ohne Zeitstempel/Quelle, keine laufende
            Verifikation, S84 als Legacy-Seed identifiziert, nicht als
            User-Feature — separate Frage, ob das Konstrukt bestehen bleibt),
            enrichment (Herkunft ungeklärt, s. TODO unten), alles Unbekannte.
       5.0  keine Daten, listed   (Neutralzone — Pipeline-Lücke, kein
            Verschleierungs-Signal: Ownership-Disclosure ist bei Listed
            gesetzlich verpflichtend, "keine Daten" heißt fast immer "Argo
            hat die Quelle noch nicht abgefragt", nicht "Company verschleiert".)
       4.0  keine Daten, privat   (Neutralzone, unteres Ende)

    TODO (S84, nicht blockierend): "enrichment"-Tag-Herkunft (enrichment.
    investors in company_detail.py) nicht abschließend geklärt — liegt auf
    6.0-Fallback, bis geklärt. Unbekannte Tags werden einmalig geloggt
    (_log_unknown_ownership_source_once) statt lautlos auf 6.0 zu fallen —
    fängt zukünftige Quellen (z. B. North Data/OpenRegister, Phase 3 laut
    ownership_enrichment.py-Docstring) auf, bevor sie unbemerkt fehlklassifiziert
    werden.

    Investoren-Tier-Modifier (±1.0, additiv, gedeckelt): kann den Quellen-
    Basiswert NICHT überschreiten (jeder Basiswert ist bereits der Bestwert
    seiner Quellenkategorie) — Tier 3 lässt den Basiswert unverändert, Tier 1/2
    ziehen innerhalb der Kategorie ab. Andreas: "Investoren-Reputation einfach
    nur mit geringerer Gewichtung" (vorher 50% des Scores, jetzt ≤1 Pkt Zug).

    Diversifikation + Listed-Bonus ENTFERNT (S84) — Listed-Status steckt jetzt
    strukturell im No-Data-Floor + in der SEC/BaFin-Disclosure-Tier, nicht
    mehr als separater Bonus obendrauf (Doppel-Zählung vermieden).
    """
    inputs: dict = {}
    is_listed = _is_listed(company)

    if not ownership_entries:
        if is_listed:
            inputs["transparency"] = "no_data_listed"
            return _safe_round(5.0), inputs
        inputs["transparency"] = "no_data_private"
        return _safe_round(4.0), inputs

    sources = {(e.get("source") or "manual").lower() for e in ownership_entries}

    _TIER_10 = {"handelsregister", "bundesanzeiger", "ba_bridge",
                "bafin_stimmrechte", "edgar_sc_13g", "edgar_sc_13d"}
    _TIER_8  = {"edgar_form_d", "edgar_d", "funding_rounds", "yahoo_institutional"}

    if sources & _TIER_10:
        base, inputs["transparency"] = 10.0, "official_disclosure"
    elif sources & _TIER_8:
        base, inputs["transparency"] = 8.0, "known_but_not_mandatory"
    elif "wikipedia_infobox" in sources:
        base, inputs["transparency"] = 7.0, "wikipedia"
    else:
        base, inputs["transparency"] = 6.0, "manual"
        _log_unknown_ownership_source_once(sources)

    tiers = [
        _investor_tier(e.get("name") or "", e.get("investor_type") or e.get("type") or "")
        for e in ownership_entries
    ]
    avg_tier = sum(tiers) / len(tiers) if tiers else 1.0
    inputs["avg_investor_tier"] = round(avg_tier, 2)
    inputs["investor_count"]    = len(tiers)

    # Tier 3 → 0.0 (Basiswert bleibt, da Basiswert schon Bestwert der Quelle
    # ist), Tier 2 → -0.5, Tier 1 → -1.0. Nie positiv — s. Docstring.
    modifier = 0.0 if avg_tier >= 3.0 else -0.5 if avg_tier >= 2.0 else -1.0
    inputs["investor_modifier"] = modifier

    score = _clamp(base + modifier)
    return _safe_round(score), inputs


# ── SC-09 · Value Driver Score ─────────────────────────────────────────────────

def compute_value_driver_score(
    company: dict,
    value_drivers: list[dict],
) -> tuple[float, dict]:
    """
    SC-09: Value Driver Score (0–10). Höher = stärkere strukturelle Vorteile.

    Inputs: value_drivers (dependency_level [nur Enabler], market_position, type).

    Gewichtung (TR-CONSISTENCY-AUDIT-01, S76 — TechReadiness entfernt):
      Dependency Level   0–5.5 Pkt  (Kernmetrik — tatsächliche Enabler-Qualität)
      Market Position    0–2.0 Pkt  (Leader/Dominant Einträge)
      Driver Count       0–2.0 Pkt  (Anzahl identifizierter Value Drivers)
      Baseline           0.5  Pkt   (immer)

    Rationale: Dependency Level und Market Position reflektieren die tatsächliche
    Supply-Chain-Stärke. Eine Series-A Company mit kritischen Enablerabhängig-
    keiten schlägt eine Series-C Company mit commodity Enablers — wie in der
    Realität.

    TR-CONSISTENCY-AUDIT-01 (S76): TechReadiness ENTFERNT (war tr × 1.5,
    bereits S34 von 3.0 runtergewichtet mit der Begründung "Stage-Proxy, keine
    echte Enabler-Qualität" — dieselbe Logik führt jetzt konsequent zur
    Entfernung). Die 1.5 Pkt auf Dependency umverteilt (4.0→5.5), die laut
    eigener Doku die echte Qualitätsmetrik ist. _compute_target_tech_readiness()
    wird hier nicht mehr aufgerufen.

    VALUE-DRIVER-DEPENDENCYSCORE-01 (S88, live-Bug seit S76): Das ursprünglich
    hier gelesene numerische Feld "dependency_score" existiert in keiner
    einzigen DB-Row (SQL-Stichprobe S88 — Keyliste realer Enabler-Einträge
    enthält es nicht). dep_vals war seit der S76-Umgewichtung (4.0→5.5)
    strukturell immer leer — SC-09 lieferte seitdem für jede Company nur
    Baseline+Driver-Count+Market-Position (max. 4.5 von 10), nie mehr, ohne
    Fehler oder Log. Fix: dependency_level (kategorisch: critical/high/medium/
    commodity — dasselbe Feld, das SC-10s Operations-Dimension bereits nutzt,
    s. compute_dimension_risks). Bewusst nur Enabler-Einträge (Docstring
    spricht explizit von "Enabler-Qualität") — Contributors tragen weiterhin
    nur zum Driver-Count bei, unverändert. Reale Nachkalibrierung der
    Punktetabelle bleibt eigenständiges Ticket, hier zunächst plausible
    Erstbelegung (critical > high > medium > commodity).
    """
    inputs: dict = {}
    score = 0.5   # Baseline

    vds = value_drivers or []
    if not vds:
        # CATEGORY-CEILING-REVIEW-01 (S84): dritte Fundstelle desselben
        # Anti-Patterns wie SC-04/SC-08 — "keine Value-Driver-Daten" fiel
        # bisher auf die reine Baseline (0.5/10), identisch zu "keine
        # strukturellen Vorteile vorhanden". SC-10-Neutralkonvention
        # übernommen (BUG-31): fehlende Pipeline-Abdeckung ≠ fehlende Stärke.
        inputs["driver_count"] = 0
        return _safe_round(4.5), inputs

    # Driver Count (0–2)
    score += min(2.0, len(vds) * 0.4)
    inputs["driver_count"] = len(vds)

    # Dependency Level Durchschnitt (0–5.5) — Kernmetrik, nur Enabler
    # VALUE-DRIVER-DEPENDENCYSCORE-01 (S88): dependency_level statt totem
    # dependency_score, s. Docstring oben. Erstbelegung, Nachkalibrierung
    # eigenständiges Ticket.
    _DEP_LEVEL_PTS = {"critical": 1.0, "high": 0.7, "medium": 0.4, "commodity": 0.1}
    enabler_levels = [
        _DEP_LEVEL_PTS[d["dependency_level"]]
        for d in vds
        if d.get("type") == "enabler" and d.get("dependency_level") in _DEP_LEVEL_PTS
    ]
    if enabler_levels:
        avg_dep = sum(enabler_levels) / len(enabler_levels)
        score += avg_dep * 5.5
        inputs["avg_dependency_level_pts"] = round(avg_dep, 2)
        inputs["enabler_count_scored"]     = len(enabler_levels)

    # Market Position Bonus (0–2.0)
    strong = sum(
        1 for d in vds
        if (d.get("market_position") or "").lower() in ("leader", "market leader", "dominant", "monopol", "quasi-monopol")
    )
    score += min(2.0, strong * 0.75)
    inputs["strong_positions"] = strong

    return _safe_round(score), inputs


# ── Path Scores ────────────────────────────────────────────────────────────────

def compute_ipo_score(company: dict, signals: list[dict]) -> tuple[float, dict]:
    """
    IPO Score (0–10): Attraktivität des IPO-Pfads.

    0.0 für bereits gelistete Companies (IPO abgeschlossen).
    Hoch für: belastbare/viele IPO-Signale, fortgeschrittene Funding-Stage.

    TR-STAGE-OVERLAP-01 (S75, nachgeschärft): TechReadiness vollständig raus
    — auch der User-Override-Zweig aus dem ersten Fix-Versuch. Ursprünglich
    nur als Stage-Redundanz-Problem diagnostiziert (Auto-TR ist für private
    Companies selbst stage-/funding-abgeleitet, doppelte Zählung mit
    stage_base). Zweiter, wichtigerer Befund beim Nachschärfen: TR beantwortet
    hier die FALSCHE Frage, unabhängig davon ob der Wert Stage-Proxy oder
    echter User-Override ist. TRs eigentliches, gewichtsmäßig dominantes
    Zuhause ist SC-02 Strategic Score (TR×3.5, Formel `srr_pts + mfr_pts +
    tr_pts + buyer_bonus` — das produktseitige "SRR×MFR×TechReadiness" liegt
    konzeptionell hier, nicht im IPO Score). TR ist auch dort als "target-
    intrinsisch" dokumentiert, aber im selben Atemzug mit SRR/MFR verrechnet
    (beide klar buyer-relativ, "bestplatzierter Buyer-Match") — TR beantwortet
    strukturell "wie attraktiv ist dieses Tech als Akquisitionsziel", nicht
    "ist diese Company börsenreif". Ein Börsengang ist gerade der Pfad, der
    KEINEN Buyer voraussetzt — TechReadiness in diesem buyer-fit-geprägten
    Sinne hat hier keine inhaltliche Entsprechung, auch nicht über einen
    "echten" manuellen Wert. Bleibt für SC-02 (sowie SC-09/ETF/Enabler mit
    geringerem Gewicht, ungeprüft) bestehen — siehe TR-CONSISTENCY-AUDIT-01,
    das jetzt auch die inhaltliche Passungsfrage für diese drei mit klären
    sollte, nicht nur die technische Gewichts-Spreizung.

    ipo_potential-Bucket (0–2.0 Pkt) UND S-1-Status-Boost (0–1.5 Pkt) raus —
    beide strukturell tot in der aktuellen Pipeline: ipo_status kann "s-1"
    nie enthalten (Enum seit migration_003 nur listed/pre_ipo_*/NULL, s.
    LISTED-STATUS-REVIEW-01), ipo_potential wird seit der Migration nirgends
    mehr geschrieben (verifiziert gegen company_detail.py/signal_engine.py/
    main.py — nicht gegen enrichment.py/peers.py, daher "nirgends gesehen",
    nicht "nirgends im System").

    Signal-Gewichtung (vorher pauschal 1.0/Signal, Cap 3.0 — keine
    Differenzierung nach Quelle/Belastbarkeit):
      EDGAR-Quelle (S-1/S-11, harte Filing-Evidenz)     1.5 Pkt / Signal
      Andere Quellen (News/TechCrunch, Keyword-Match)   0.75 Pkt / Signal
      Cap 5.0 (vorher 3.0) — Kalibrierung, kein Hard-Fact, anpassbar.

    Neuer nominaler Maximalwert: 7.7 (vorher 10.0, faktisch nie erreichbar —
    drei der ursprünglich vier Komponenten waren tot oder kategorial fehl am
    Platz).
    """
    inputs: dict = {}

    if _is_listed(company):
        inputs["note"] = "already_listed"
        return 0.0, inputs

    stage = _resolve_funding_stage(company)
    inputs["funding_stage"] = stage

    stage_base = _stage_match(stage, _STAGE_IPO_SCORE)

    ipo_sigs = [
        s for s in (signals or [])
        if s.get("signal_category") == "ipo_progress"
        or s.get("event_type") == "ipo_status_change"
    ]
    edgar_sigs = [s for s in ipo_sigs if s.get("source") == "edgar"]
    other_sigs = [s for s in ipo_sigs if s.get("source") != "edgar"]
    signal_pts = min(5.0, len(edgar_sigs) * 1.5 + len(other_sigs) * 0.75)
    inputs["ipo_signals"]       = len(ipo_sigs)
    inputs["ipo_signals_edgar"] = len(edgar_sigs)

    score = stage_base * 0.3 + signal_pts
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

    TR-CONSISTENCY-AUDIT-01 (S76): TechReadiness ENTFERNT (war tr × 1.0).
    ETF-Trackbarkeit ist eine Frage von Listed-Status + Sektor-Coverage —
    TechReadiness beantwortet hier inhaltlich die falsche Frage (Kategorienfehler,
    dieselbe Lehre wie TR-STAGE-OVERLAP-01 für den IPO Score). Bewusst NICHT
    umverteilt — der freigewordene Punkt wird nicht künstlich auf andere
    Komponenten geschoben, der nominale Max sinkt von 10.0 auf 10.0 (war faktisch
    7+TR; die TR-Entfernung senkt den realen Range leicht, das ist gewollt und
    wird beim Hero-Path-Test sichtbar). _compute_target_tech_readiness() wird hier
    nicht mehr aufgerufen.
    """
    inputs: dict = {}
    score = 0.0

    # BUG-44: ipo_status verwenden, nicht bool(ticker) — ticker kann auch bei privaten gesetzt sein
    is_listed = company.get("ipo_status") == "listed"
    category  = (company.get("category") or "").lower()
    industry  = (company.get("industry") or "").lower()

    inputs.update({
        "is_listed": is_listed, "category": category,
    })

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

    return _safe_round(score), inputs


def compute_enabler_score(
    company: dict,
    value_drivers: list[dict],
) -> tuple[float, dict]:
    """
    Enabler Score (0–10): Stärke der Enabler/Supply-Chain-Rolle.

    Hoch für: B2B-Industrien, viele Enabler-Value-Drivers, hohe Dependency Scores,
              quasi-monopolistische Marktposition.

    Gewichtung (TR-CONSISTENCY-AUDIT-01, S76 — TechReadiness entfernt):
      Baseline               0.5  Pkt   (als Enabler klassifiziert)
      B2B-Industrie-Signal   0 / 3.0    (Hardware, Materials, Infra usw.)
      Enabler Driver Count   0–3.0 Pkt  (explizite Enabler-Value-Drivers)
      Dependency Score       0–3.5 Pkt  (Abhängigkeit der Käufer — Kernmetrik)

    TR-CONSISTENCY-AUDIT-01 (S76): TechReadiness ENTFERNT (war tr × 3.0, das
    höchste TR-Gewicht außerhalb SC-02). Im Auto-Modus war TR ein reiner
    Stage-Proxy; im Manual-Override buyer-fit-geprägt (TR-FIELDNAME-HYGIENE-01) —
    in beiden Fällen beantwortet TR hier nicht die Enabler-Frage ("wie kritisch
    ist diese Tech als Infrastruktur"). Die freigewordenen 3.0 Pkt bewusst auf
    die inhaltlich tragenden Komponenten umverteilt (NICHT auf TR-Ersatz):
    B2B-Signal 2.0→3.0 (verlässlichstes Enabler-Indiz), Dependency 2.0→3.5
    (echte Kernmetrik der Käufer-Abhängigkeit), + 0.5 Baseline. Range bleibt
    0–10. _compute_target_tech_readiness() wird hier nicht mehr aufgerufen.

    Signatur-Cleanup (SC02-REWORK-01-Folge): `buyers`-Parameter bereits entfernt.
    """
    inputs: dict = {}
    score = 0.5   # Baseline: als Enabler-Kandidat überhaupt betrachtet

    industry = (company.get("industry") or "").lower()
    category = (company.get("category") or "").lower()

    inputs.update({
        "industry": industry,
    })

    # B2B-Industrie (0–3) — verlässlichstes strukturelles Enabler-Indiz
    _B2B_KW = (
        "semiconductor", "industrial", "software", "saas", "deep tech",
        "materials", "infrastructure", "chemicals", "components",
        "manufacturing", "automation", "robotics", "sensor", "battery",
        "electrolysis", "hydrogen", "grid", "storage", "carbon",
    )
    if any(kw in industry or kw in category for kw in _B2B_KW):
        score += 3.0
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

    # Dependency Level (0–3.5) — Kernmetrik der Käufer-Abhängigkeit
    # VALUE-DRIVER-DEPENDENCYSCORE-01 (S88): identischer Bug wie in SC-09
    # gefunden, unabhängig davon — "dependency_score" existiert in keiner
    # DB-Row (SQL-Stichprobe S88). dep_vals war seit S76 (TR-CONSISTENCY-
    # AUDIT-01, Umgewichtung 2.0→3.5 auf dieses Feld) strukturell immer leer.
    # Fix analog SC-09 (compute_value_driver_score): dependency_level-Mapping
    # statt totem numerischen Feld.
    _DEP_LEVEL_PTS_ENABLER = {"critical": 1.0, "high": 0.7, "medium": 0.4, "commodity": 0.1}
    dep_levels = [
        _DEP_LEVEL_PTS_ENABLER[d["dependency_level"]]
        for d in enabler_vds
        if d.get("dependency_level") in _DEP_LEVEL_PTS_ENABLER
    ]
    if dep_levels:
        avg_dep = sum(dep_levels) / len(dep_levels)
        score += avg_dep * 3.5
        inputs["avg_dependency_level_pts"] = round(avg_dep, 2)

    return _safe_round(score), inputs


# ── SC-05 · Composite Score ─────────────────────────────────────────────────────

def compute_composite_score(result: ScoreResult) -> float | None:
    """
    SC-05: Gewichteter Composite aller Sub-Scores.

    Formel: Financial 25% + Market 20% + Ownership 15% + Value Driver 10%
            + (10−Risk) 10% — Basis 0.80, automatisch normalisiert über
            total_weight. Strategic (SC-02) bewusst NICHT Teil des Composite
            (COMPOSITE-DEFINITION-01, S81) — s. Begründung bei _COMPOSITE_WEIGHTS.

    Gibt None wenn weniger als 3 Sub-Scores berechnet wurden (zu wenig Datenbasis).
    Normalisiert Gewichte automatisch wenn Sub-Scores fehlen.
    """
    components: list[tuple[float, float]] = []   # (score_value, weight)

    def _try_add(score: float | None, key: str) -> None:
        if score is not None:
            components.append((score, _COMPOSITE_WEIGHTS[key]))

    _try_add(result.financial_score,    "financial")
    _try_add(result.market_score,       "market")
    _try_add(result.ownership_score,    "ownership")
    _try_add(result.value_driver_score, "value_driver")
    # COMPOSITE-DEFINITION-01 (S81): strategic_score (SC-02) bewusst NICHT mehr
    # Teil des Composite — s. Begründung bei _COMPOSITE_WEIGHTS oben. SC-02
    # bleibt auf result.strategic_score unverändert berechnet für Buyer-Tab/M&A.

    if result.compound_risk_score is not None:
        # SUBSCORE-COMPOSITION-AUDIT-01 (S88): SC-04 (risk_score) hier ersetzt
        # durch SC-10 (compound_risk_score) — genau EIN Risk-Score speist jetzt
        # den Composite, und es ist derselbe, den der Potenziale & Risiken-Tab
        # anzeigt (vorher: unsichtbarer Parallel-Score). compute_risk_score()
        # (SC-04) bleibt als Funktion bestehen und wird weiterhin berechnet
        # (result.risk_score) — Frontend-Fallback für den unwahrscheinlichen
        # Fall, dass compute_dimension_risks() komplett fehlschlägt, sowie
        # Diagnose-Referenz während der Migration. Kein Composite-Input mehr.
        components.append((10.0 - result.compound_risk_score, _COMPOSITE_WEIGHTS["risk_inv"]))

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
    company:           dict,
    market_data:       dict | None       = None,
    signals:           list[dict] | None = None,
    value_drivers:     list[dict] | None = None,
    ownership_entries: list[dict] | None = None,
) -> dict:
    """
    SC-10 Basis: Algorithmische Opportunity/Risk-Scores für 7 Dimensionen.
    Datenmangel → neutral (4.0–5.0), kein halluziniertes Risiko (BUG-31).

    SUBSCORE-COMPOSITION-AUDIT-01 (S88): Beta + Governance aus dem
    retirierten SC-04 (compute_risk_score) migriert — Beta in die Market-
    Dimension (identische Kalibrierung, Ziel-Skala 0–10 statt 0–2), Governance
    als eigene 7. Dimension (bewusst keine Fusion mit SC-08 — SC-08 misst
    Quellen-Autorität/Transparenz, diese Dimension misst Governance-RISIKO
    aus Bekanntheit der Struktur; zwei Konzepte auf denselben Rohdaten,
    analog zur Operations-Dimension, die Value Drivers ebenfalls anders liest
    als SC-09 — dort als Stärke, hier als Risiko, bewusste Doppel-Perspektive).

    Returns: {dim_id: {opportunity_score, risk_score, data_confidence, opportunity_sources, risk_sources}}
    """
    md   = market_data or {}
    sigs = signals or []
    vd   = value_drivers or []
    own  = ownership_entries or []

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

    # Beta / Volatilität — Migration aus SC-04 (compute_risk_score, retiriert
    # aus dem Composite, S88). Identische Kalibrierung: Market Beta (listed)
    # volle Gewichtung, Damodaran-Sektor-Beta (privat) 60% — nur Ziel-Skala
    # 0–10 statt 0–2 (×5).
    beta        = company.get("beta_1y") or company.get("beta")
    beta_source = company.get("beta_source", "")
    beta_risk: float | None = None
    if beta is not None:
        beta = float(beta)
        damodaran_factor = 0.6 if beta_source == "damodaran" else 1.0
        raw_beta_risk = (
            10.0 if beta >= 2.0 else
            7.5  if beta >= 1.5 else
            5.0  if beta >= 1.0 else
            2.5  if beta >= 0.5 else 1.0
        )
        beta_risk = raw_beta_risk * damodaran_factor

    comp_risk  = _COMP_RISK.get(competition or "", 4.0)
    cycle_risk = _CYCLE_RISK.get(market_cycle or "", 4.0)
    if competition and beta_risk is not None:
        mkt_risk = comp_risk * 0.5 + cycle_risk * 0.2 + beta_risk * 0.3
    elif competition:
        mkt_risk = comp_risk * 0.7 + cycle_risk * 0.3
    elif beta_risk is not None:
        mkt_risk = beta_risk * 0.5 + 4.0 * 0.5   # Rest neutral, Beta bekannt
    else:
        mkt_risk = 4.0

    mkt_conf = "high" if (cagr and competition) else (
        "medium" if (cagr or competition or beta_risk is not None) else "low"
    )
    dims["market"] = {
        "opportunity_score": _safe_round(mkt_opp),
        "risk_score":        _safe_round(mkt_risk),
        "data_confidence":   mkt_conf,
        "opportunity_sources": ["market_data.cagr_pct", "market_data.market_cycle"],
        "risk_sources":        ["market_data.competition_score", "market_data.market_cycle", "companies.beta_1y"],
        "beta":               beta,
        "beta_source":        beta_source or None,
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

    # ─── 5. TECHNOLOGY (User-Override > kanonische Auto-TR > Signals/Patents) ──
    # SC10-STAGE-TR-PROXY-01 (S81, Andreas-Entscheidung): vorher eine VIERTE,
    # unabhängig kalibrierte lokale _STAGE_TR-Kopie, die sich selbst fälschlich
    # als "auto_tech_readiness(stage_proxy)" auswies, ohne die Funktion je
    # aufzurufen (andere Zahlen: seed 0.20 hier vs. 0.28 im Original). Jetzt:
    # (1) kanonische compute_auto_tech_readiness() aus scoring.py statt eigener
    # Tabelle — ein Ort für "was ist der Stage-basierte TR-Schätzwert", nicht
    # vier. (2) Aktiver User-TR-Override (company["_tr_override"], TR-MODAL-01)
    # wird respektiert, wenn vorhanden — bewusste Revision der S76-Aussage
    # "TechReadiness lebt ausschließlich in der Per-Buyer-Engine": TR-MODAL-01
    # ist company-level (kein Buyer-Parameter), ein User, der sich die Mühe
    # macht, TR manuell einzuschätzen, soll das auch im 6D-Radar sehen statt
    # dass dort weiter aus der Stage geraten wird.
    # SCORING-PATH-01 (S82): Pfad-Korrektur — scoring.py liegt unter src/pipelines/,
    # nicht src/services/. Der S81-Import (SC10-STAGE-TR-PROXY-01) nahm den falschen
    # Pfad an; Datei war korrekt gepusht/deployed, nur der Import zeigte ins Leere —
    # ModuleNotFoundError, compute_dimension_risks komplett ausgefallen (fail-loud
    # abgefangen in assessments.py, aber Risk-Dimension-Assessments plattformweit tot).
    from src.pipelines.scoring import compute_auto_tech_readiness

    _tr_override = company.get("_tr_override")
    if _tr_override and _tr_override.get("value") is not None:
        tr_proxy  = float(_tr_override["value"])
        tr_source_label = f"user_tr_override(mode={_tr_override.get('mode', '?')})"
        tech_conf_base = "high"
    else:
        tr_proxy, _tr_confidence = compute_auto_tech_readiness(
            stage=stage,
            category=company.get("category") or company.get("industry"),
            funding_total_usd_mn=company.get("funding_total_usd_mn"),
            funding_last_round=company.get("funding_last_round"),
        )
        tr_source_label = "auto_tech_readiness(canonical)"
        tech_conf_base = "medium" if _tr_confidence != "auto_low" else "low"

    tech_opp  = min(10.0, round(tr_proxy * 9.0 + 0.5, 1))
    tech_risk = min(10.0, round((1.0 - tr_proxy) * 6.5 + 1.0, 1))

    tech_pos = [s for s in sigs if s.get("signal_category") in ("patent", "new_product", "tech_milestone") and s.get("direction") == "positive"]
    tech_neg = [s for s in sigs if s.get("signal_category") in ("ip_risk", "tech_obsolescence") and s.get("direction") == "negative"]
    tech_opp  = min(10.0, tech_opp  + len(tech_pos) * 0.4)
    tech_risk = min(10.0, tech_risk + len(tech_neg) * 0.8)

    # SE-14: Patent-Tiefe aus EPO OPS — nur für PATENT_SCORING_SECTORS
    # Datensammlung universell; Scoring nur wo IP ein echter Moat ist (Deep Tech, Chemie, Pharma …)
    tech_conf    = tech_conf_base if stage else "low"
    tech_sources = [tr_source_label, "signals[patent,tech_milestone]"]
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

    # ─── 7. GOVERNANCE (Bekanntheit der Eigentümerstruktur) ─────────────────
    # SUBSCORE-COMPOSITION-AUDIT-01 (S88): Migriert aus dem retirierten SC-04
    # (compute_risk_score). Skala 1:1 aus SC-04 übernommen (0–3 → 0–10, ×3.33),
    # keine Neukalibrierung — Ausgangswerte sind identisch zur bisherigen,
    # bereits S84 bestätigten Neutralzone-Logik ("keine Daten ≠ aktiv
    # verschleiert"). Bewusst getrennt von SC-08: SC-08 misst Autorität/
    # Belastbarkeit der Ownership-Quelle (Transparenz-Score), diese Dimension
    # misst Governance-RISIKO aus der schieren Bekanntheit der Struktur —
    # unterschiedliche Fragen an dieselben Rohdaten (ownership_entries).
    is_listed_gov = _is_listed(company)
    if is_listed_gov:
        gov_risk, gov_conf, gov_basis = 1.0, "high", "listed"
    elif not own:
        gov_risk, gov_conf, gov_basis = 5.0, "low", "opaque"
    elif len(own) == 1:
        gov_risk, gov_conf, gov_basis = 5.0, "medium", "single_entry"
    else:
        gov_risk, gov_conf, gov_basis = 1.67, "medium", "partial"

    dims["governance"] = {
        "opportunity_score": _safe_round(10.0 - gov_risk),
        "risk_score":        _safe_round(gov_risk),
        "data_confidence":   gov_conf,
        "opportunity_sources": ["ownership_entries.count", "companies.is_listed"],
        "risk_sources":        ["ownership_entries.count", "companies.is_listed"],
        "governance_basis":   gov_basis,
    }

    return dims


def compute_compound_risk_score(dimension_risks: dict) -> tuple[float, dict]:
    """
    SC-10: Compound Risk Score aus 7 Dimensions-Risiken (0–10, höher = mehr Risiko).
    Confidence-Dämpfung: low-confidence Dimensionen zählen 50% (kein Aufblasen durch Datenmangel).

    SUBSCORE-COMPOSITION-AUDIT-01 (S88): Governance neu aufgenommen (0.08),
    Market/Strategy/Operations/Technology/Political moderat runtergewichtet,
    um auf 1.00 zu summieren. Erstbelegung — bewusst niedrig gewichtet, da
    neue Dimension ohne Kalibrierungs-Historie. Nachkalibrierung erwartet,
    sobald Composite-Feed (statt SC-04) einige Sessions live gelaufen ist.
    """
    _WEIGHTS: dict[str, float] = {
        "market":     0.20, "financials": 0.20, "strategy":   0.18,
        "operations": 0.13, "technology": 0.13, "political":  0.08,
        "governance": 0.08,
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
        value_drivers:     Value Driver Einträge (aus value_drivers-Tabelle)

    SC02-REWORK-01 (vollständig kaskadiert): `buyers`-Parameter entfernt.
    Trug seit BUYER-AGG-01 keine Funktion mehr (compute_strategic_score/
    compute_enabler_score lesen SRR/MFR/TR aus ma_aggregate bzw. berechnen TR
    selbst) — main.py und company_detail.py reichten ihn trotzdem weiter mit
    der Begründung "MFR-Annotation in compute_all_scores", die nie zutraf
    (Phantom-Referenz, gleiches Muster wie BUYER-MFR-01).

    Returns:
        ScoreResult mit allen Scores, hero_path, rating, confidence, score_inputs.
    """
    mkt  = market_data or {}
    sigs = signals or []
    own  = ownership_entries or []
    vds  = value_drivers or []

    result     = ScoreResult()
    all_inputs: dict = {}

    # ── Sub-Scores ──────────────────────────────────────────────────────────
    _run(result, "financial_score",    lambda: compute_financial_score(company, funding_momentum, headcount_snapshots), all_inputs, "financial",    "SC-01")
    _run(result, "strategic_score",    lambda: compute_strategic_score(company, ma_aggregate),  all_inputs, "strategic",    "SC-02")
    _run(result, "market_score",       lambda: compute_market_score(mkt, company),           all_inputs, "market",       "SC-03")
    # SC-04: weiterhin berechnet (Frontend-Fallback, Diagnose) — S88 aus dem
    # Composite entfernt, s. compute_composite_score(). Kein UI-Tab zeigt SC-04.
    _run(result, "risk_score",         lambda: compute_risk_score(company, sigs, own),       all_inputs, "risk",         "SC-04")
    _run(result, "ownership_score",    lambda: compute_ownership_score(company, own),        all_inputs, "ownership",    "SC-08")
    _run(result, "value_driver_score", lambda: compute_value_driver_score(company, vds),    all_inputs, "value_driver", "SC-09")

    # SC-10: Compound Risk Score (algorithmisch aus 7 Dimensionen, S88: +Governance)
    try:
        _dim_risks = compute_dimension_risks(company=company, market_data=mkt, signals=sigs, value_drivers=vds, ownership_entries=own)
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
    _run(result, "enabler_score",      lambda: compute_enabler_score(company, vds),          all_inputs, "enabler",      "Enabler")

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
