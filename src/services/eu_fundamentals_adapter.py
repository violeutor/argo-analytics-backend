"""
EU Fundamentals Adapter — v1.0
================================
Abstrahiert den Datenprovider für EU-Listed Finanzkennzahlen.

Heute:  EODHD (Flat-fee, Caching-freundlich)
2027:   ESAP API (ESMA, kostenlos, vollständige EU-Coverage inkl. DE-Listings)

Austausch-Mechanik:
  - EUFundamentals: stabiler Dataclass — orientiert an IFRS-XBRL-Tags
  - EUFundamentalsProvider: abstraktes Interface
  - fetch_eu_fundamentals(): einziger Einstiegspunkt für den Rest des Systems
  - Provider wird via EU_FUNDAMENTALS_PROVIDER env var gesteuert:
      "eodhd"  → EODHDProvider  (default)
      "esap"   → ESAPProvider   (ab 2027, wenn API live)

IFRS-Tag-Mapping (Referenz für beide Provider):
  ifrs-full:Revenue                    → revenue_mn
  ifrs-full:GrossProfit                → gross_profit_mn
  ifrs-full:ProfitLossFromOperations   → ebit_mn
  ifrs-full:ProfitLoss                 → net_income_mn
  ifrs-full:Assets                     → total_assets_mn
  ifrs-full:Liabilities                → total_liabilities_mn
  ifrs-full:Equity                     → equity_mn
  ifrs-full:EarningsPerShareBasic      → eps
  ifrs-full:SharesOutstanding          → shares_outstanding (für market_cap = shares × live_price)

Caching-Strategie:
  Fundamentals werden NICHT live abgerufen — sie liegen in kpi_timeseries (quarterly refresh).
  fetch_eu_fundamentals() schreibt via _write_eu_kpi_rows() in kpi_timeseries,
  analog dem EDGAR-Pfad in edgar_kpi.py.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
import httpx
from src.config import settings

logger = logging.getLogger(__name__)


# ─── Dataclass (IFRS-orientiert, provider-agnostisch) ────────────────────────

@dataclass
class EUFundamentals:
    """
    Finanzkennzahlen für EU-Listed Companies.
    Feldnamen orientieren sich an IFRS-XBRL-Tags — stabil über Provider-Wechsel.
    Alle Geldwerte in EUR Mio. fiscal_year = YYYY (aus Reporting-Periodenende).
    """
    ticker: str
    fiscal_year: int

    # Income Statement
    revenue_mn: Optional[float] = None             # ifrs-full:Revenue
    gross_profit_mn: Optional[float] = None        # ifrs-full:GrossProfit
    ebit_mn: Optional[float] = None                # ifrs-full:ProfitLossFromOperations
    ebitda_mn: Optional[float] = None              # abgeleitet (kein eigener IFRS-Tag)
    net_income_mn: Optional[float] = None          # ifrs-full:ProfitLoss

    # Balance Sheet
    total_assets_mn: Optional[float] = None        # ifrs-full:Assets
    total_liabilities_mn: Optional[float] = None   # ifrs-full:Liabilities
    equity_mn: Optional[float] = None              # ifrs-full:Equity
    cash_mn: Optional[float] = None                # ifrs-full:CashAndCashEquivalents
    debt_mn: Optional[float] = None                # ifrs-full:Borrowings

    # Per Share
    eps: Optional[float] = None                    # ifrs-full:EarningsPerShareBasic
    shares_outstanding: Optional[int] = None       # für market_cap = shares × live_price

    # Metadata
    currency: str = "EUR"
    provider: str = "unknown"                      # "eodhd" | "esap"
    source_url: Optional[str] = None               # Audit-Trail
    filing_date: Optional[str] = None              # ISO-Datum des Filings


# ─── Abstract Provider Interface ─────────────────────────────────────────────

class EUFundamentalsProvider(ABC):
    """
    Austauschbares Backend. Beide Provider liefern EUFundamentals.
    2027: ESAPProvider ersetzt EODHDProvider für EU-Listed ohne Code-Änderung
    in fetch_eu_fundamentals() oder dem aufrufenden enrichment-Code.
    """

    @abstractmethod
    async def fetch(self, ticker: str, exchange: str) -> Optional[EUFundamentals]:
        """
        Holt aktuellste Jahreskennzahlen für ticker@exchange.
        Gibt None zurück wenn Company nicht gefunden oder Provider-Fehler.
        """
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """True wenn API-Key / Endpoint konfiguriert und erreichbar."""
        ...


# ─── EODHD Provider (aktiv ab EODHD-INTEGRATION-01) ─────────────────────────

class EODHDProvider(EUFundamentalsProvider):
    """
    EODHD Historical Data API.
    Endpoint: https://eodhd.com/api/fundamentals/{ticker}.{exchange}?api_token=...
    Exchange-Codes: XETR (Xetra/DE), XPAR (Paris), XAMS (Amsterdam), XMIL (Milan) etc.
    Flat-fee → ideal für Caching. Shares outstanding für market_cap-Berechnung verfügbar.

    Aktivierung: EODHD_API_KEY in Render-Env-Vars setzen.
    """

    BASE_URL = "https://eodhd.com/api/fundamentals"

    def is_available(self) -> bool:
        return bool(getattr(settings, "EODHD_API_KEY", None))

    async def fetch(self, ticker: str, exchange: str) -> Optional[EUFundamentals]:
        if not self.is_available():
            logger.warning("EODHDProvider: EODHD_API_KEY nicht gesetzt")
            return None

        # EODHD erwartet Ticker.ExchangeCode — z.B. SIE.XETR, ASML.XAMS
        eodhd_exchange = _map_exchange_to_eodhd(exchange)
        symbol = f"{ticker}.{eodhd_exchange}"
        url = f"{self.BASE_URL}/{symbol}"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params={
                    "api_token": settings.EODHD_API_KEY,
                    "fmt": "json",
                })

            if resp.status_code == 404:
                logger.info("EODHD: %s nicht gefunden", symbol)
                return None
            if not resp.is_success:
                logger.warning("EODHD %s: HTTP %s", symbol, resp.status_code)
                return None

            data = resp.json()
            return _parse_eodhd_response(ticker, data)

        except httpx.TimeoutException:
            logger.warning("EODHD timeout für %s", symbol)
            return None
        except Exception as e:
            logger.warning("EODHD fetch failed für %s: %s", symbol, e)
            return None


def _map_exchange_to_eodhd(exchange: str) -> str:
    """
    Argo-interne Exchange-Codes (aus OpenFIGI) → EODHD Exchange-Codes.
    Ergänzen sobald neue Exchanges getestet werden (Trial-Ergebnis).
    """
    _MAP = {
        "GY": "XETR",   # Xetra (DE)
        "FP": "XPAR",   # Euronext Paris
        "NA": "XAMS",   # Euronext Amsterdam
        "IM": "XMIL",   # Borsa Italiana
        "SM": "XMAD",   # BME Madrid
        "SS": "XSTO",   # Nasdaq Stockholm
        "DC": "XCSE",   # Nasdaq Copenhagen
        "HB": "XHEL",   # Nasdaq Helsinki
        "LN": "LSE",    # London Stock Exchange
    }
    return _MAP.get(exchange.upper(), exchange)


def _parse_eodhd_response(ticker: str, data: dict) -> Optional[EUFundamentals]:
    """
    Parst EODHD /fundamentals Response → EUFundamentals.
    EODHD liefert Financials unter data["Financials"]["Income_Statement"]["annual"]
    und Balance Sheet unter data["Financials"]["Balance_Sheet"]["annual"].
    Neuestes Jahr gewinnt (analog EDGAR-Periode-Regel).
    """
    try:
        financials = data.get("Financials", {})
        income_annual = financials.get("Income_Statement", {}).get("annual", {})
        balance_annual = financials.get("Balance_Sheet", {}).get("annual", {})
        shares_data = data.get("SharesStats", {})

        if not income_annual:
            logger.info("EODHD: keine Annual-Financials für %s", ticker)
            return None

        # Neuestes Jahr aus fiscal_year-Keys (Format: "2023-12-31")
        latest_year_key = max(income_annual.keys())
        fiscal_year = int(latest_year_key[:4])
        inc = income_annual[latest_year_key]
        bal = balance_annual.get(latest_year_key, {})

        def _mn(val) -> Optional[float]:
            """Rohwert (in Einzel-EUR bei EODHD) → EUR Mio."""
            try:
                return round(float(val) / 1_000_000, 2) if val not in (None, "", "None") else None
            except (ValueError, TypeError):
                return None

        # EBITDA ableiten wenn nicht direkt vorhanden
        ebit = _mn(inc.get("ebit"))
        da = _mn(inc.get("depreciationAndAmortization"))
        ebitda = round(ebit + da, 2) if (ebit is not None and da is not None) else None

        shares_raw = shares_data.get("SharesOutstanding")
        shares = int(float(shares_raw)) if shares_raw not in (None, "", "None") else None

        return EUFundamentals(
            ticker=ticker,
            fiscal_year=fiscal_year,
            revenue_mn=_mn(inc.get("totalRevenue")),
            gross_profit_mn=_mn(inc.get("grossProfit")),
            ebit_mn=ebit,
            ebitda_mn=ebitda,
            net_income_mn=_mn(inc.get("netIncome")),
            total_assets_mn=_mn(bal.get("totalAssets")),
            total_liabilities_mn=_mn(bal.get("totalLiab")),
            equity_mn=_mn(bal.get("totalStockholderEquity")),
            cash_mn=_mn(bal.get("cash")),
            debt_mn=_mn(bal.get("longTermDebt")),
            eps=_mn(inc.get("epsActual")),
            shares_outstanding=shares,
            currency="EUR",
            provider="eodhd",
            filing_date=latest_year_key,
        )

    except Exception as e:
        logger.warning("EODHD parse error für %s: %s", ticker, e)
        return None


# ─── ESAP Provider (Stub — ab Juli 2027) ─────────────────────────────────────

class ESAPProvider(EUFundamentalsProvider):
    """
    ESAP API — European Single Access Point.
    Operational ab Juli 2027 (Regulation EU 2023/2859, Art. 7: free public API).
    Primary Key: LEI (Legal Entity Identifier).
    Format: Inline XBRL / XBRL-CSV — IFRS-Tags direkt verfügbar.

    Dieser Stub zeigt die erwartete Struktur. Implementierung erfolgt sobald
    ESAP API-Spec und Endpunkte bekannt sind (erwartet H1 2027).

    ESAP ersetzt EODHD für EU-Listed Companies:
      - DE-Listings (Siemens, Bayer, SAP) — heute OAM-Sonderweg, dann via ESAP
      - Vollständige EU-Coverage via LEI
      - Kostenlos + offiziell (kein Vendor-Lock-in)
    """

    # Placeholder — wird mit offizieller Spec befüllt
    BASE_URL = "https://esap.esma.europa.eu/api/v1"  # spekulativ, noch nicht live

    def is_available(self) -> bool:
        # ESAP ist noch nicht live — immer False bis 2027
        return False

    async def fetch(self, ticker: str, exchange: str) -> Optional[EUFundamentals]:
        """
        TODO (2027): LEI aus companies-Tabelle ziehen (wird über Wikidata/GLEIF befüllt),
        dann ESAP /filings?lei={lei}&type=ESEF anfragen,
        neuestes iXBRL-Filing parsen → EUFundamentals.

        Interface bleibt identisch zu EODHDProvider.fetch() —
        fetch_eu_fundamentals() muss nicht geändert werden.
        """
        logger.warning("ESAPProvider: noch nicht implementiert (erwartet 2027)")
        return None


# ─── Einstiegspunkt ───────────────────────────────────────────────────────────

def _get_provider() -> EUFundamentalsProvider:
    """
    Wählt Provider via EU_FUNDAMENTALS_PROVIDER env var.
    Default: eodhd. Fallback auf nächst-verfügbaren Provider.
    """
    preference = getattr(settings, "EU_FUNDAMENTALS_PROVIDER", "eodhd").lower()
    providers = {
        "eodhd": EODHDProvider(),
        "esap": ESAPProvider(),
    }
    provider = providers.get(preference, EODHDProvider())
    if not provider.is_available():
        logger.warning(
            "EU_FUNDAMENTALS_PROVIDER=%s nicht verfügbar (API-Key fehlt oder nicht live). "
            "EU-Fundamentals werden übersprungen.", preference
        )
        return provider
    return provider


async def fetch_eu_fundamentals(
    ticker: str,
    exchange: str,
) -> Optional[EUFundamentals]:
    """
    Einziger Einstiegspunkt für EU-Fundamentals im gesamten System.
    Aufgerufen aus enrichment.py (Phase B, EU-Listed-Zweig).

    Gibt None zurück wenn:
    - Provider nicht konfiguriert (kein API-Key)
    - Ticker nicht gefunden
    - Provider-Fehler (Timeout, HTTP-Error)

    None = kein Hard-Fail — enrichment.py läuft normal weiter.
    """
    provider = _get_provider()
    if not provider.is_available():
        return None

    result = await provider.fetch(ticker, exchange)
    if result:
        logger.info(
            "EU Fundamentals: %s@%s FY%s via %s — Revenue %s EUR Mio",
            ticker, exchange, result.fiscal_year, result.provider,
            result.revenue_mn,
        )
    return result


# ─── KPI-Write (analog edgar_kpi.py) ─────────────────────────────────────────

def write_eu_kpi_rows(company_id: str, fundamentals: EUFundamentals) -> None:
    """
    Schreibt EUFundamentals in kpi_timeseries — analog _write_kpi_rows() in edgar_kpi.py.
    Aufgerufen nach erfolgreichem fetch_eu_fundamentals().

    Felder → metric_key Mapping:
      revenue_mn       → "revenue"
      gross_profit_mn  → "gross_profit"
      ebit_mn          → "ebit"
      ebitda_mn        → "ebitda"
      net_income_mn    → "net_income"
      total_assets_mn  → "total_assets"
      equity_mn        → "equity"
      shares_outstanding → "shares_outstanding"  (für market_cap = shares × live_price)
    """
    from src.integrations.supabase import get_supabase

    rows = []
    metric_map = {
        "revenue":            fundamentals.revenue_mn,
        "gross_profit":       fundamentals.gross_profit_mn,
        "ebit":               fundamentals.ebit_mn,
        "ebitda":             fundamentals.ebitda_mn,
        "net_income":         fundamentals.net_income_mn,
        "total_assets":       fundamentals.total_assets_mn,
        "total_liabilities":  fundamentals.total_liabilities_mn,
        "equity":             fundamentals.equity_mn,
        "cash":               fundamentals.cash_mn,
        "debt":               fundamentals.debt_mn,
        "eps":                fundamentals.eps,
        "shares_outstanding": fundamentals.shares_outstanding,
    }

    for metric_key, value in metric_map.items():
        if value is None:
            continue
        rows.append({
            "company_id":  company_id,
            "metric_key":  metric_key,
            "value":       float(value),
            "fiscal_year": fundamentals.fiscal_year,
            "period_type": "annual",
            "source":      fundamentals.provider,  # "eodhd" | "esap"
            "currency":    fundamentals.currency,
        })

    if not rows:
        logger.info("write_eu_kpi_rows: keine Werte für %s", company_id)
        return

    try:
        sb = get_supabase()
        sb.table("kpi_timeseries").upsert(
            rows,
            on_conflict="company_id,metric_key,fiscal_year,period_type",
        ).execute()
        logger.info(
            "write_eu_kpi_rows: %d rows für company_id=%s (FY%s via %s)",
            len(rows), company_id, fundamentals.fiscal_year, fundamentals.provider,
        )
    except Exception as e:
        logger.warning("write_eu_kpi_rows failed für %s: %s", company_id, e)
