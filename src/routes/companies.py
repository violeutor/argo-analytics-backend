"""
GET /api/v1/companies  — Liste aller Companies aus Supabase
GET /api/v1/buyers     — Liste aller Buyers

Dient auch als Verbindungstest: 200 + Daten = Supabase connected.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from src.models.schemas import CompanyListItem, BuyerListItem
from src.integrations.supabase import fetch_companies, fetch_buyers, upsert_ticker_yf

router = APIRouter(prefix="/api/v1", tags=["data"])


@router.get("/companies", response_model=list[CompanyListItem])
def list_companies(
    limit: int = Query(default=100, le=500),
    source: str | None = Query(default=None, description="bestand | woche1 | woche2 | manual"),
):
    """
    Gibt alle Companies aus der Supabase-Datenbank zurück.
    Verbindungstest: curl http://localhost:8000/api/v1/companies
    """
    try:
        return fetch_companies(limit=limit, source=source)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


@router.get("/buyers", response_model=list[BuyerListItem])
def list_buyers(limit: int = Query(default=50, le=100)):
    """
    Gibt alle Buyers / Proxy-Titel aus der Supabase-Datenbank zurück.
    """
    try:
        return fetch_buyers(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


class TickerYfUpdate(BaseModel):
    ticker_yf: str


@router.patch("/companies/{company_id}/ticker-yf", status_code=204)
def patch_ticker_yf(company_id: str, body: TickerYfUpdate):
    """
    Schreibt ticker_yf (yfinance-Ticker) für eine Company.
    Wird vom BA-Bridge price_fetcher Cron aufgerufen.
    Body: {"ticker_yf": "SIE.DE"}
    """
    ticker_yf = body.ticker_yf.strip()
    if not ticker_yf:
        raise HTTPException(status_code=422, detail="ticker_yf darf nicht leer sein.")
    upsert_ticker_yf(company_id, ticker_yf)
