"""
Route: GET /internal/yftest/{ticker}
=====================================
Pfad: argo-analytics-backend/src/routes/yftest.py

DIAGNOSE-ENDPOINT (temporär) — isoliert die yfinance-Kette auf Render.

Zweck: Klären ob das Siemens-„leer im Frontend"-Symptom ein
EU-Datenproblem (Yahoo hat die Daten nicht) ODER ein Render-Infrastruktur-
problem (.info/quoteSummary kommt auf der Cloud-IP nicht durch) ist.

Trennt die zwei yfinance-Quellen:
  - fast_info  → chart-Endpoint (/v8/chart), KEIN Crumb nötig  → Preis/MarketCap
  - .info      → quoteSummary, Crumb nötig                     → Revenue/EBITDA

Interpretation des JSON-Felds "diagnosis":
  - "info_leer_fastinfo_ok" → genau das Siemens-Symptom: quoteSummary blockiert
    auf Render, chart kommt durch. → Infrastruktur-Problem, NICHT EU.
  - "beides_ok"             → Yahoo + Render-IP liefern alles. Kein Problem.
  - "beides_leer"           → Ticker/Suffix falsch oder yfinance ganz blockiert.

Aufruf im Browser:
  https://<render-backend>/internal/yftest/SIE.DE
  https://<render-backend>/internal/yftest/BAYN.DE
  https://<render-backend>/internal/yftest/ASML.AS

NACH DER DIAGNOSE WIEDER ENTFERNEN (Router-Registrierung in main.py raus).
"""
import asyncio
import logging
from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/internal", tags=["diagnostics"])


@router.get("/yftest/{ticker}")
async def yftest(ticker: str):
    """
    Roher yfinance-Aufruf auf Render — fast_info + .info getrennt ausgewiesen.
    Kein Auth, kein Side-Effect, reine Lese-Diagnose.
    """
    # yfinance-Import absichern — fehlt es, ist DAS die Diagnose.
    # (company_detail._fetch_yf_fundamentals verschluckt denselben ImportError
    #  still und gibt {} zurück → leere Fundamentals, niemand merkt es.)
    try:
        import yfinance as yf
    except Exception as e:
        return {
            "ticker": ticker,
            "diagnosis": "yfinance_fehlt",
            "bedeutung": (
                "yfinance ist auf Render NICHT installiert. Wäre die Ursache für "
                "leere Fundamentals: company_detail fängt diesen ImportError still "
                "ab → {}. Fix: yfinance in requirements.txt. Evtl. mit Bridge-Cleanup "
                "rausgeflogen."
            ),
            "error": f"{type(e).__name__}: {str(e)[:200]}",
        }

    def _sync_probe(sym: str) -> dict:
        result: dict = {"ticker": sym}

        # yfinance-Version (wichtig: veraltete Version bricht .info)
        try:
            result["yfinance_version"] = getattr(yf, "__version__", "unbekannt")
        except Exception:
            result["yfinance_version"] = "unbekannt"

        t = yf.Ticker(sym)

        # ── fast_info (chart-Endpoint, kein Crumb) ──────────────────────────
        try:
            fi = t.fast_info
            result["fast_info"] = {
                "last_price":  getattr(fi, "last_price", None),
                "market_cap":  getattr(fi, "market_cap", None),
                "currency":    getattr(fi, "currency", None),
                "ok":          getattr(fi, "last_price", None) is not None,
            }
        except Exception as e:
            result["fast_info"] = {"ok": False, "error": str(e)[:200]}

        # ── .info (quoteSummary, Crumb nötig) ───────────────────────────────
        try:
            info = t.info or {}
            result["info"] = {
                "feld_anzahl":      len(info),
                "totalRevenue":     info.get("totalRevenue"),
                "ebitda":           info.get("ebitda"),
                "fullTimeEmployees":info.get("fullTimeEmployees"),
                "marketCap":        info.get("marketCap"),
                "regularMarketPrice": info.get("regularMarketPrice"),
                "ok":               len(info) > 10 and info.get("totalRevenue") is not None,
            }
        except Exception as e:
            result["info"] = {"feld_anzahl": 0, "ok": False, "error": str(e)[:200]}

        # ── Diagnose ────────────────────────────────────────────────────────
        fi_ok   = result.get("fast_info", {}).get("ok", False)
        info_ok = result.get("info", {}).get("ok", False)

        if info_ok and fi_ok:
            result["diagnosis"] = "beides_ok"
            result["bedeutung"] = (
                "Yahoo + Render-IP liefern Fundamentals UND Preis. "
                "Kein Datenproblem — EODHD nicht zwingend nötig."
            )
        elif fi_ok and not info_ok:
            result["diagnosis"] = "info_leer_fastinfo_ok"
            result["bedeutung"] = (
                "EXAKT das Siemens-Symptom: Preis (chart) kommt durch, "
                "Revenue/EBITDA (quoteSummary) blockiert auf Render-Cloud-IP. "
                "→ Infrastruktur-Problem, NICHT EU-Datenlücke."
            )
        else:
            result["diagnosis"] = "beides_leer"
            result["bedeutung"] = (
                "Weder Preis noch Fundamentals. Ticker/Suffix prüfen "
                "oder yfinance komplett blockiert/veraltet."
            )

        return result

    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_sync_probe, ticker),
            timeout=20.0,
        )
    except asyncio.TimeoutError:
        return {
            "ticker": ticker,
            "diagnosis": "timeout",
            "bedeutung": "yfinance-Call >20s — auf Render oft = Rate-Limit/Block auf der Cloud-IP.",
        }
    except Exception as e:
        import traceback
        return {
            "ticker": ticker,
            "diagnosis": "endpoint_crash",
            "bedeutung": "Endpoint selbst ist gecrasht — Fehlertext zeigt die Ursache.",
            "error_type": type(e).__name__,
            "error": str(e)[:300],
            "traceback": traceback.format_exc()[-800:],
        }


# ── main.py — Router registrieren (TEMPORÄR) ──────────────────────────────────
# Pfad: argo-analytics-backend/src/main.py
# Bei den festen Pfaden ZUERST registrieren (Router-Shadowing-Regel S61):
#
#   from src.routes.yftest import router as yftest_router
#   app.include_router(yftest_router)
#
# Nach der Diagnose beide Zeilen + diese Datei wieder entfernen.
