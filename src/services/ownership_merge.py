"""
src/services/ownership_merge.py

SUBSCORE-COMPOSITION-AUDIT-01 (S88): Gemeinsame Ownership-Raw-Merge-Logik,
extrahiert aus company_detail.py — dort stand sie inline und wurde nur von
compute_all_scores() dort gefüttert. assessments.py braucht dieselben
Rohdaten für SC-10s neue Governance-Dimension (compute_dimension_risks) —
Single-Chokepoint statt einer zweiten, divergenten Implementierung. Genau
dieses Muster (zwei Stellen, die "wie viel wissen wir über die Eigentümer-
struktur" unterschiedlich beantworten) war OWNERSHIP-SCORE-SOURCE-GAP-01
(S81) — dort zwischen Tab-Anzeige und Scoring, hier zwischen company_detail
und assessments.
"""

from __future__ import annotations


def build_ownership_raw(
    enrichment_investors: list[dict] | None,
    funding_rounds: list[dict] | None,
    db_ownership_entries: list[dict] | None,
) -> list[dict]:
    """
    Merged drei Ownership-Rohquellen zu einer deduplizierten Liste im Format
    {name, investor_type, share_pct, source} — Input-Format für
    score_calculator.compute_ownership_score() / compute_dimension_risks().
    Dedup nach normalisiertem Namen, erste Quelle gewinnt (Reihenfolge unten
    = Priorität: enrichment > funding_rounds > db_ownership_entries).

    enrichment_investors: Liste von Dicts mit mind. "name", optional "type".
        Aufrufer mit Pydantic-/Objekt-Instanzen (z. B. company_detail.py's
        EnrichmentResult.investors) normalisieren VOR dem Aufruf selbst, z. B.
        [{"name": inv.name, "type": inv.type} for inv in enrichment.investors].
        Darf leer/None sein — s. Docstring in assessments.py zur Konsequenz
        (EN-08 zieht enrichment.investors ohnehin in ownership_entries nach,
        sobald die Enrichment-Pipeline einmal gelaufen ist; eine leere Liste
        hier ist also nur zwischen Erstanlage und erstem Enrichment-Lauf ein
        Blindspot, kein struktureller).
    funding_rounds: rohe funding_rounds-Rows (lead_investor, co_investors).
    db_ownership_entries: rohe ownership_entries-Rows (name, type, source,
        share_pct). Sentinel-Rows (source="enrichment_attempted", reiner
        Loop-Guard gegen Re-Trigger, nie echte Daten) werden hier gefiltert,
        nicht beim Aufrufer — sonst besteht die Gefahr, dass ein Aufrufer den
        Filter vergisst und Loop-Guard-Rows fälschlich als bekannte Investoren
        zählt.
    """
    seen: set[str] = set()
    result: list[dict] = []

    def _add(name, investor_type, source, share_pct=None) -> None:
        nm = (name or "").strip()
        if not nm or nm.lower() in seen:
            return
        seen.add(nm.lower())
        result.append({
            "name": nm, "investor_type": investor_type,
            "share_pct": share_pct, "source": source,
        })

    for inv in (enrichment_investors or []):
        _add(inv.get("name"), inv.get("type"), "enrichment")

    for r in (funding_rounds or []):
        _add(r.get("lead_investor"), "VC/Investor", "funding_rounds")
        for co in (r.get("co_investors") or []):
            _add(co, "VC/Investor", "funding_rounds")

    for e in (db_ownership_entries or []):
        if e.get("source") == "enrichment_attempted":
            continue
        _add(e.get("name"), e.get("type"), e.get("source") or "manual", e.get("share_pct"))

    return result
