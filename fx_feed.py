# fx_feed.py
from __future__ import annotations
import os
from typing import Optional, Dict, Any
import httpx
import pandas as pd
import logging

log = logging.getLogger("fx_feed")

TD_KEY = os.getenv("TWELVEDATA_API_KEY", "").strip()
AV_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
USE_AV = os.getenv("USE_ALPHA_VANTAGE", "0").strip() == "1"  # по умолчанию выключен

# ---------- helpers ----------

def _pair_from_symbol(sym: str) -> str:
    """EUR/GBP/AUD/JPY -> полная пара. Остальные возвращаем как есть."""
    s = sym.upper().replace("/", "")
    m = {"EUR": "EURUSD", "GBP": "GBPUSD", "AUD": "AUDUSD", "JPY": "USDJPY"}
    return m.get(s, s)

def _td_symbol(pair: str) -> str:
    """TwelveData ждёт формат EUR/USD."""
    p = pair.upper().replace("/", "")
    return f"{p[:3]}/{p[3:]}"

AV_INTERVAL = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "1h": "60min"}
TD_INTERVAL = {
    "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1h", "4h": "4h", "1d": "1day", "1day": "1day"
}

def _norm_df(rows: list[Dict[str, Any]], ts_key: str = "datetime") -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df[ts_key] = pd.to_datetime(df[ts_key], utc=True)
    df = df.set_index(ts_key).sort_index()
    for k in ("open", "high", "low", "close"):
        df[k] = pd.to_numeric(df[k], errors="coerce")
    if "volume" not in df.columns:
        df["volume"] = 0.0
    return df[["open", "high", "low", "close", "volume"]].dropna()

# ---------- Twelve Data ----------

def _fetch_td(pair: str, interval: str = "1min", bars: int = 1500) -> Optional[pd.DataFrame]:
    if not TD_KEY:
        log.error("[TD] TWELVEDATA_API_KEY is empty — TwelveData disabled")
        return None
    td_int = TD_INTERVAL.get(interval, interval)
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": _td_symbol(pair),
        "interval": td_int,
        "outputsize": int(bars),
        "apikey": TD_KEY,
    }
    try:
        with httpx.Client(timeout=20) as c:
            r = c.get(url, params=params)
        j = r.json()
    except Exception as e:
        log.error(f"[TD] HTTP error: {e}")
        return None
    if "values" not in j:
        msg = j.get("message") or str(j)[:200]
        log.error(f"[TD] No values: {msg}")
        return None
    df = _norm_df(j["values"])
    if df.empty:
        log.error("[TD] Empty dataframe")
        return None
    return df

# ---------- Alpha Vantage (опционально) ----------

def _fetch_av(pair: str, interval: str = "1min", full: bool = True) -> Optional[pd.DataFrame]:
    if not USE_AV:
        return None
    if not AV_KEY:
        log.error("[AV] ALPHAVANTAGE_API_KEY is empty — Alpha Vantage disabled")
        return None

    av_int = AV_INTERVAL.get(interval, "60min" if interval.endswith("h") else "5min")
    base, quote = pair[:3], pair[3:]
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "FX_INTRADAY",  # внимание: у AV это премиальный эндпоинт
        "from_symbol": base,
        "to_symbol": quote,
        "interval": av_int,
        "outputsize": "full" if full else "compact",
        "apikey": AV_KEY,
    }
    try:
        with httpx.Client(timeout=25) as c:
            r = c.get(url, params=params)
        j = r.json()
    except Exception as e:
        log.error(f"[AV] HTTP error: {e}")
        return None

    note = (j.get("Note") or j.get("Information") or "").lower()
    if "premium" in note or "thank you for using alpha vantage" in note:
        log.error(f"[AV] Premium endpoint / rate limit: {j.get('Note') or j.get('Information')}")
        return None

    key = f"Time Series FX ({av_int})"
    ts = j.get(key)
    if not ts:
        log.error(f"[AV] Missing key '{key}' in response")
        return None

    rows = []
    for t, ohlc in ts.items():
        rows.append({
            "datetime": pd.to_datetime(t, utc=True),
            "open": float(ohlc["1. open"]),
            "high": float(ohlc["2. high"]),
            "low":  float(ohlc["3. low"]),
            "close":float(ohlc["4. close"]),
            "volume": 0.0,
        })
    rows.sort(key=lambda x: x["datetime"])
    df = _norm_df(rows)
    if df.empty:
        log.error("[AV] Empty dataframe")
        return None
    return df

# ---------- Public ----------

def fetch_ohlcv(sym_or_pair: str, interval: str = "1min",
                bars: int | None = None, limit: int | None = None) -> pd.DataFrame:
    """
    Возвращает DataFrame (index=UTC datetime) с колонками [open, high, low, close, volume].
    sym_or_pair: 'EUR' -> EURUSD, 'USDJPY' -> USDJPY.
    interval: '1m','5m','15m','30m','1h','4h','1d' (TD); для AV конвертируется в *min.
    bars/limit: желаемое количество баров (любой параметр).
    """
    pair = _pair_from_symbol(sym_or_pair)
    want = int(limit or bars or 1000)

    # 1) TwelveData
    df = _fetch_td(pair, interval=interval, bars=want)
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df.tail(want)

    # 2) Alpha Vantage (только если включён)
    df = _fetch_av(pair, interval=interval, full=True)
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df.tail(want)

    raise RuntimeError(f"FX feed failed for {pair} (providers exhausted)")
