from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.routes.analyze import router as analyze_router
from src.routes.companies import router as companies_router
from src.routes.search import router as search_router
from src.routes.company_detail import router as detail_router

app = FastAPI(
    title="Argo Analytics API",
    description="M&A Deal Scoring Engine — SRR × MFR × TechReadiness + Company Enrichment",
    version="0.4.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analyze_router)
app.include_router(companies_router)
app.include_router(search_router)
app.include_router(detail_router)


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.4.0"}
