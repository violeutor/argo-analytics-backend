from fastapi import APIRouter, HTTPException
from src.models.schemas import AnalyzeRequest, AnalyzeResponse
from src.controllers.analyze import run_analyze

router = APIRouter(prefix="/api/v1", tags=["analyze"])


@router.post("/analyze", response_model=AnalyzeResponse)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    """
    Run full Argo Analytics deal scoring:
    - SRR  (Strategic Relevance Ratio)
    - MFR  (M&A Feasibility Ratio)
    - TechReadiness (7-factor)
    - DealSuccessScore = SRR_norm × MFR_norm × TechReadiness
    """
    try:
        return run_analyze(request)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
