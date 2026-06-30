"""
valuation_calibration.py — EXIT_ADJUSTMENT-Recalibration
==========================================================
VALUATION-MULTIPLE-01 (S78): berechnet einen Vorschlag für valuation.py's
EXIT_ADJUSTMENT-Dict aus echten comparable_transactions-Zeilen. Bewusst
GETRENNT von valuation.py (das bleibt DB-frei, s. dortiger Moduldocstring)
und bewusst NICHT in einen Cron verdrahtet — wie bei STAGE_MULT/VERTICAL_
DELTA ist Kalibrierung ein bewusster, manueller Schritt ("feinjustierbar
ohne Logik-Eingriff"), kein automatischer Self-Update-Mechanismus, der
unbeobachtet Bewertungen verschiebt.

Mechanik:
  Pro kalibrierungsfähiger Transaktion wird verglichen, was valuation.py
  zum Verkaufszeitpunkt geschätzt hätte (funding_x_stage, aus dem
  *_at_sale-Snapshot) gegen den tatsächlich gezahlten Preis. Der Median
  dieses Verhältnisses pro (industry, stage)-Bucket ist der vorgeschlagene
  Korrekturfaktor — Median statt Mittelwert, robust gegen einzelne
  Ausreißer-Deals (z.B. ein extremer Distressed-Sale oder Strategic-Premium-
  Ausreißer in einem sonst kleinen Bucket).

  Mindeststichprobe n≥5 pro Bucket (_EXIT_ADJUSTMENT_MIN_SAMPLE in
  valuation.py) — Buckets darunter werden hier zwar berechnet und
  ausgegeben (Transparenz), aber explizit als "noch nicht decision-ready"
  markiert statt automatisch übernommen.

Nutzung (manuell, z.B. via Python-Shell oder ein Ad-hoc-Skript):
    from src.services.valuation_calibration import propose_exit_adjustment
    proposal = propose_exit_adjustment()
    # proposal manuell gegen EXIT_ADJUSTMENT in valuation.py abgleichen,
    # bei Bedarf von Hand übernehmen — kein automatisches Schreiben in
    # den Code, exakt wie bei STAGE_MULT/VERTICAL_DELTA.
"""

from __future__ import annotations

import logging
import statistics

from src.services.valuation import (
    STAGE_MULT,
    VERTICAL_DELTA,
    _EXIT_ADJUSTMENT_MIN_SAMPLE,
    stage_multiplier,
    vertical_delta,
)

logger = logging.getLogger(__name__)


def _predicted_value_usd_mn(
    funding_total_usd_mn: float, funding_stage: str | None, industry: str | None,
) -> float:
    """
    Repliziert compute_target_valuation()'s funding_x_stage-Formel direkt
    (statt die Funktion mit einem synthetischen company-dict aufzurufen) —
    hier zählt nur die reine Formel auf den Snapshot-Werten, kein is_listed/
    market_cap-Pfad relevant (Snapshot-Zeilen sind per Definition private
    Targets zum Verkaufszeitpunkt).
    """
    return funding_total_usd_mn * stage_multiplier(funding_stage) * vertical_delta(industry)


def propose_exit_adjustment() -> dict:
    """
    Gibt {"buckets": {industry: {stage: {...}}}, "ready": {...}} zurück.
    'ready' enthält nur Buckets mit n≥_EXIT_ADJUSTMENT_MIN_SAMPLE — das ist
    der Teil, der tatsächlich in valuation.py::EXIT_ADJUSTMENT übernommen
    werden könnte. 'buckets' zeigt ALLES, auch unterhalb der Schwelle, zur
    Transparenz ("wie nah sind wir an genug Stichprobe").
    """
    from src.integrations.supabase import fetch_calibration_eligible_transactions

    rows = fetch_calibration_eligible_transactions()
    ratios: dict[str, dict[str, list[float]]] = {}

    skipped_zero_or_negative = 0
    for row in rows:
        funding = row.get("target_funding_total_usd_mn_at_sale")
        stage = row.get("target_funding_stage_at_sale")
        industry = row.get("industry")
        price = row.get("deal_price_usd_mn")
        if not funding or not price or funding <= 0 or price <= 0:
            skipped_zero_or_negative += 1
            continue

        predicted = _predicted_value_usd_mn(float(funding), stage, industry)
        if predicted <= 0:
            skipped_zero_or_negative += 1
            continue

        ratio = float(price) / predicted
        industry_key = (industry or "").lower().strip()
        stage_key = (stage or "").lower().strip()
        ratios.setdefault(industry_key, {}).setdefault(stage_key, []).append(ratio)

    buckets: dict = {}
    ready: dict = {}
    for industry_key, stage_dict in ratios.items():
        buckets[industry_key] = {}
        for stage_key, vals in stage_dict.items():
            n = len(vals)
            median_ratio = round(statistics.median(vals), 3)
            buckets[industry_key][stage_key] = {
                "median_ratio": median_ratio, "n": n,
                "ready": n >= _EXIT_ADJUSTMENT_MIN_SAMPLE,
            }
            if n >= _EXIT_ADJUSTMENT_MIN_SAMPLE:
                ready.setdefault(industry_key, {})[stage_key] = median_ratio

    logger.info(
        "EXIT_ADJUSTMENT-Vorschlag: %d Transaktionen verarbeitet (%d übersprungen "
        "wegen Null/Negativ-Werten), %d Buckets gesamt, %d davon ready (n≥%d)",
        len(rows), skipped_zero_or_negative, sum(len(s) for s in buckets.values()),
        sum(len(s) for s in ready.values()), _EXIT_ADJUSTMENT_MIN_SAMPLE,
    )
    return {"buckets": buckets, "ready": ready, "transactions_processed": len(rows)}
