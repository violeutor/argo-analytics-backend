import logging
from src.models.schemas import AnalyzeRequest, AnalyzeResponse
from src.pipelines.scoring import compute_scores
from src.integrations.supabase import (
    fetch_company_by_name,
    fetch_buyer_by_name,
    insert_deal,
    insert_score,
)

logger = logging.getLogger(__name__)


def _build_executive_summary(request: AnalyzeRequest, response_scores) -> str:
    srr = response_scores.srr
    mfr = response_scores.mfr
    tr  = response_scores.tech_readiness

    lines = [
        f"**{request.buyer_name} / {request.company_name}** – Argo Analytics Deal Assessment",
        "",
        f"Strategic Relevance (SRR): {srr.value:.2f}x — {srr.category}. "
        f"The target's TAM of ${request.tam_usd_bn:.1f}B represents "
        f"{'a transformative opportunity' if 'Transformational' in srr.category else 'a meaningful but bounded opportunity'} "
        f"relative to {request.buyer_name}'s market cap of ${request.buyer_market_cap_usd_bn:.1f}B.",

        f"M&A Feasibility (MFR): {mfr.value:.3f} — {mfr.signal}. "
        f"{'The deal is comfortably within financial reach.' if mfr.signal == 'Feasible' else 'Financing deserves additional scrutiny before proceeding.' if mfr.signal == 'Watch' else 'The deal would represent a significant financial stretch for the buyer.'}",

        f"Tech Readiness: {tr.value:.2f}/1.00. "
        f"{'Integration capacity and strategic coherence are strong.' if tr.value >= 0.7 else 'Moderate integration readiness — targeted due diligence recommended.' if tr.value >= 0.5 else 'Low tech readiness signals meaningful integration risk.'}",

        f"Deal Success Score: {response_scores.deal_success_score:.3f} → Rating: {response_scores.rating}.",
    ]

    if srr.execution_warning:
        lines.append(
            "Execution Warning: Low buyer cap combined with high SRR — "
            "financial feasibility must be stress-tested before relying on this signal."
        )

    return "\n".join(lines)


def _collect_warnings(request: AnalyzeRequest, scores) -> list[str]:
    warnings = []

    if request.tech_readiness_inputs is None:
        warnings.append(
            "TechReadiness inputs were not provided. Score defaulted to neutral (0.5). "
            "Supply the 7-factor assessment for a reliable DealSuccessScore."
        )
    if scores.srr.execution_warning:
        warnings.append(
            f"Low-cap execution warning: buyer market cap ${request.buyer_market_cap_usd_bn:.1f}B "
            f"with SRR {scores.srr.value:.2f}x. High SRR may be a false positive — "
            "validate financing capacity independently."
        )
    if request.tam_usd_bn > 1000:
        warnings.append(
            f"TAM input of ${request.tam_usd_bn:.0f}B is very large. "
            "Verify source quality (BNEF, IEA, McKinsey). Consider applying a discount factor."
        )

    return warnings


def run_analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    # 1. Scoring (pure, kein DB-Zugriff)
    scores  = compute_scores(request)
    summary = _build_executive_summary(request, scores)
    warnings = _collect_warnings(request, scores)

    # 2. Supabase: Company + Buyer per Name nachschlagen (optional — kein Hard-Fail)
    deal_id = None
    try:
        company = fetch_company_by_name(request.company_name)
        buyer   = fetch_buyer_by_name(request.buyer_name)

        company_id = company["id"] if company else None
        buyer_id   = buyer["id"]   if buyer   else None

        # 3. Deal persistieren
        deal_id = insert_deal(request, company_id, buyer_id)

        # 4. Score persistieren
        insert_score(deal_id, scores, summary, warnings)

        logger.info("Deal %s persisted (company_id=%s, buyer_id=%s)", deal_id, company_id, buyer_id)

    except Exception as exc:
        # DB-Fehler blockieren nie die API-Antwort — Score wird immer zurückgegeben
        logger.warning("Supabase persistence failed: %s", exc)
        warnings.append(
            f"DB persistence skipped: {exc}. Scoring result is still valid."
        )

    return AnalyzeResponse(
        deal_id=deal_id,
        company_name=request.company_name,
        buyer_name=request.buyer_name,
        scores=scores,
        executive_summary=summary,
        warnings=warnings,
    )
