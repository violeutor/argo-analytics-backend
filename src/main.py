from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.routes.analyze import router as analyze_router
from src.routes.companies import router as companies_router

app = FastAPI(
    title="Argo Analytics API",
    description="M&A Deal Scoring Engine — SRR x MFR x TechReadiness",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in production: auf argo-analytics.app einschränken
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analyze_router)
app.include_router(companies_router)


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.2.0"}
