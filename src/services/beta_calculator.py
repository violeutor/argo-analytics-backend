"""
src/services/beta_calculator.py
BETA-SOURCE-01 (S50): Backend-native 252-Tage-Beta-Berechnung.

Ersetzt den Bridge-yfinance-Pfad, der von Renders Datacenter-IP chronisch
geblockt war. Zwei Quellen, die sich ergänzen:

  • Yahoo /v8/chart (direkte httpx, kein API-Key):
      - Bestätigt für DE/EU-Listings (BAYN.DE, ^GDAXI) von Render's IP
      - Bestätigt für US-Benchmarks (^GSPC) von Render's IP
      - Kein Rate-Limit-Problem — direkter Chart-Endpoint, kein yfinance-Scraping
  • Twelve Data /time_series (Token-Auth, 8 Credits/min Free Tier):
      - US-Listings als Fallback wenn Yahoo fehlschlägt
      - Nie IP-geblockt (Token-based B2B-API)

Beta = Cov(r_stock, r_bench) / Var(r_bench), trailing 252 Handelstage,
simple Tagesrenditen aus split-/dividendenbereinigten Kursen.

Cache: Supabase `beta_cache`-Tabelle, TTL 24h (Migration: migration_beta_cache.sql).
"""

import logging
from datetime import datetime, timezone

import httpx

from src.config import settings
from src.integrations.supabase import get_supabase

logger = logging.getLogger(__name__)

# ── Benchmark-Mapping ─────────────────────────────────────────────────────────
# Primär nach yf-Ticker-Suffix (BAYN.DE → .de → ^GDAXI).
# Kein Suffix oder US-Suffix → S&P 500.
_BENCHMARK_BY_SUFFIX: dict[str, str] = {
    ".de": "^GDAXI",   # Xetra
    ".l":  "^FTSE",    # London
    ".pa": "^FCHI",    # Euronext Paris
    ".mi": "^FTSEMIB", # Milano
    ".as": "^AEX",     # Amsterdam
    ".sw": "^SSMI",    # Zürich
    ".st": "^OMXSPI",  # Stockholm
    ".vi": "^ATX",     # Wien
}
_US_BENCHMARK = "^GSPC"

# Cache-TTL 24h — Beta ändert sich träge, tägliche Neuberechnung reicht
_BETA_CACHE_TTL_S: int = 86_400

# Beta-Fenster
_BETA_WINDOW: int = 252


# ── Benchmark-Auswahl ─────────────────────────────────────────────────────────

def _select_benchmark(ticker: str) -> str:
    """Wählt Benchmark anhand des yf-Ticker-Suffix."""
    t = (ticker or "").lower()
    for suffix, bench in _BENCHMARK_BY_SUFFIX.items():
        if t.endswith(suffix):
            return bench
    return _US_BENCHMARK


def _is_us_ticker(ticker: str) -> bool:
    t = (ticker or "").lower()
    return not any(t.endswith(s) for s in _BENCHMARK_BY_SUFFIX)


# ── Kursabruf ─────────────────────────────────────────────────────────────────

async def _fetch_yahoo_closes(ticker: str, range_: str = "1y") -> list[float]:
    """
    Holt bereinigte Schlusskurse von Yahoo /v8/finance/chart.
    Kein API-Key, keine IP-Blocking-Probleme mit dem Chart-Endpoint
    (im Gegensatz zu yfinance-Bibliothek, die den RateLimit triggert).
    Confirmed von Renders IP: BAYN.DE, ^GDAXI, ^GSPC — alle 200 OK.
    """
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?interval=1d&range={range_}")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ArgoAnalytics/1.0)"}
    try:
        async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.debug("Yahoo beta /v8/chart %s → HTTP %s", ticker, resp.status_code)
                return []
            data = resp.json()
            result = (data.get("chart") or {}).get("result") or []
            if not result:
                return []
            closes = (result[0]
                      .get("indicators", {})
                      .get("adjclose", [{}])[0]
                      .get("adjclose", []))
            return [float(c) for c in closes if c is not None]
    except Exception as e:
        logger.debug("Yahoo beta fetch %s failed: %s", ticker, e)
        return []


async def _fetch_twelvedata_closes(ticker: str) -> list[float]:
    """
    Holt Schlusskurse von Twelve Data /time_series.
    US-Fallback wenn Yahoo fehlschlägt. 1 Credit pro Call, 8 Credits/min Free Tier.
    Token-basiert → kein IP-Blocking. Neueste Daten zuerst → reversed für Chron.
    """
    apikey = getattr(settings, "twelve_data_api_key", None)
    if not apikey:
        return []
    url = (f"https://api.twelvedata.com/time_series"
           f"?symbol={ticker}&interval=1day&outputsize=300"
           f"&adjusted=true&apikey={apikey}")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.debug("Twelve Data %s → HTTP %s", ticker, resp.status_code)
                return []
            data = resp.json()
            if "code" in data:  # Twelve Data Error-Response
                logger.debug("Twelve Data %s error: %s", ticker, data.get("message"))
                return []
            values = data.get("values") or []
            closes = []
            for row in reversed(values):  # älteste zuerst
                try:
                    closes.append(float(row["close"]))
                except (KeyError, ValueError, TypeError):
                    pass
            return closes
    except Exception as e:
        logger.debug("Twelve Data fetch %s failed: %s", ticker, e)
        return []


# ── Beta-Berechnung (Pure Python, keine Abhängigkeiten) ───────────────────────

def _compute_beta(stock_closes: list[float],
                  bench_closes: list[float],
                  window: int = _BETA_WINDOW) -> dict:
    """
    Berechnet Beta aus bereinigten Schlusskurs-Reihen.
    Beta = Cov(r_stock, r_bench) / Var(r_bench), simple Tagesrenditen.
    Zusätzlich: 30-Tage-Volatilität (annualisiert).
    Gibt {} zurück wenn Daten unzureichend.
    """
    if len(stock_closes) < 30 or len(bench_closes) < 30:
        return {}

    n = min(len(stock_closes), len(bench_closes), window + 1)
    s = stock_closes[-n:]
    b = bench_closes[-n:]

    r_s = [(s[i] / s[i - 1]) - 1 for i in range(1, len(s))]
    r_b = [(b[i] / b[i - 1]) - 1 for i in range(1, len(b))]

    n_ret = len(r_s)
    if n_ret < 20:
        return {}

    mean_s = sum(r_s) / n_ret
    mean_b = sum(r_b) / n_ret

    cov = sum((r_s[i] - mean_s) * (r_b[i] - mean_b)
              for i in range(n_ret)) / (n_ret - 1)
    var_b = sum((r_b[i] - mean_b) ** 2
                for i in range(n_ret)) / (n_ret - 1)

    if var_b == 0:
        return {}

    beta = cov / var_b

    # 30d annualisierte Volatilität
    vol_w = min(30, n_ret)
    r_vol = r_s[-vol_w:]
    mean_v = sum(r_vol) / len(r_vol)
    var_v = sum((r - mean_v) ** 2 for r in r_vol) / max(len(r_vol) - 1, 1)
    vol_30d = (var_v ** 0.5) * (252 ** 0.5)

    return {
        "beta_1y":           round(beta, 4),
        "volatility_30d":    round(vol_30d, 4),
        "trading_days_used": n_ret,
    }


# ── Supabase-Cache ────────────────────────────────────────────────────────────

def _cache_read(ticker: str) -> dict | None:
    """Liest Beta aus Supabase beta_cache. None wenn fehlt oder veraltet (>24h)."""
    try:
        rows = (get_supabase()
                .table("beta_cache")
                .select("beta_1y, volatility_30d, benchmark_ticker, calculated_at, data_quality")
                .eq("ticker", ticker)
                .limit(1)
                .execute()
                .data or [])
        if not rows:
            return None
        row = rows[0]
        calc_at = row.get("calculated_at")
        if calc_at:
            try:
                dt = datetime.fromisoformat(str(calc_at).replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - dt).total_seconds() > _BETA_CACHE_TTL_S:
                    return None  # veraltet
            except Exception:
                pass
        return row if row.get("beta_1y") is not None else None
    except Exception as e:
        logger.debug("beta_cache read %s failed: %s", ticker, e)
        return None


def _cache_write(ticker: str, result: dict, benchmark: str) -> None:
    """Schreibt Beta in Supabase beta_cache (upsert auf ticker)."""
    try:
        get_supabase().table("beta_cache").upsert({
            "ticker":           ticker,
            "beta_1y":          result.get("beta_1y"),
            "volatility_30d":   result.get("volatility_30d"),
            "benchmark_ticker": benchmark,
            "calculated_at":    datetime.now(timezone.utc).isoformat(),
            "data_quality":     "full" if (result.get("trading_days_used") or 0) >= 200
                                else "partial",
        }, on_conflict="ticker").execute()
    except Exception as e:
        logger.debug("beta_cache write %s failed: %s", ticker, e)


# ── Haupt-Einstiegspunkt ──────────────────────────────────────────────────────

async def fetch_beta(ticker: str) -> dict:
    """
    Liefert 252-Tage-Markt-Beta für einen gelisteten Ticker.

    Reihenfolge:
      1. Supabase beta_cache (24h TTL) — kein externer Call
      2. Yahoo /v8/chart (kein Key, DE + US + Benchmarks, von Render bestätigt)
         Twelve Data /time_series als US-Fallback (Token, 1 Credit/Call)
      3. {} bei totalem Fehlschlag — Damodaran sector-beta füllt die Lücke upstream

    Gibt dict mit beta_1y, beta_source='market', beta_benchmark, volatility_30d,
    beta_calculated_at, beta_data_quality zurück. Oder {} bei Fehlschlag.
    """
    if not ticker:
        return {}

    # 1. Cache
    cached = _cache_read(ticker)
    if cached:
        logger.debug("beta_cache hit %s: β=%.2f", ticker, cached["beta_1y"])
        return {
            "beta_1y":                    cached["beta_1y"],
            "volatility_30d":             cached.get("volatility_30d"),
            "beta_source":                "market",
            "beta_benchmark":             cached.get("benchmark_ticker"),
            "beta_benchmark_is_fallback": False,
            "beta_calculated_at":         cached.get("calculated_at"),
            "beta_data_quality":          cached.get("data_quality", "full"),
        }

    # 2. Berechnen
    benchmark = _select_benchmark(ticker)

    # Stock-Kurse: Yahoo primär, Twelve Data als US-Fallback
    stock_closes = await _fetch_yahoo_closes(ticker)
    if len(stock_closes) < 50 and _is_us_ticker(ticker):
        logger.debug("Yahoo miss für %s — Twelve Data Fallback", ticker)
        stock_closes = await _fetch_twelvedata_closes(ticker)

    # Benchmark-Kurse: immer Yahoo (^GDAXI, ^GSPC beide bestätigt)
    bench_closes = await _fetch_yahoo_closes(benchmark)

    if len(stock_closes) < 50 or len(bench_closes) < 50:
        logger.debug(
            "Beta-Daten unzureichend %s: stock=%d bench=%d",
            ticker, len(stock_closes), len(bench_closes),
        )
        return {}

    result = _compute_beta(stock_closes, bench_closes)
    if not result:
        return {}

    logger.info(
        "Beta computed %s: β=%.2f vol30d=%.1f%% bench=%s (%d Tage)",
        ticker,
        result["beta_1y"],
        (result.get("volatility_30d") or 0) * 100,
        benchmark,
        result.get("trading_days_used", 0),
    )

    _cache_write(ticker, result, benchmark)

    return {
        "beta_1y":                    result["beta_1y"],
        "volatility_30d":             result.get("volatility_30d"),
        "beta_source":                "market",
        "beta_benchmark":             benchmark,
        "beta_benchmark_is_fallback": False,
        "beta_calculated_at":         datetime.now(timezone.utc).isoformat(),
        "beta_data_quality":          ("full" if result.get("trading_days_used", 0) >= 200
                                       else "partial"),
    }
