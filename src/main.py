from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from src.routes.analyze import router as analyze_router
from src.routes.companies import router as companies_router
from src.routes.search import router as search_router
from src.routes.company_detail import router as detail_router
import os

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


class APIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Health check braucht keinen Key
        if request.url.path == "/health":
            return await call_next(request)

        api_key = request.headers.get("X-API-Key")
        expected = os.getenv("API_KEY")

        if not expected:
            raise RuntimeError("API_KEY environment variable not set")

        if api_key != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")

        return await call_next(request)


app.add_middleware(APIKeyMiddleware)

app.include_router(analyze_router)
app.include_router(companies_router)
app.include_router(search_router)
app.include_router(detail_router)


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.4.0"}
