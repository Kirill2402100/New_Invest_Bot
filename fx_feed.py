# fx_feed.py
from __future__ import annotations
import os, time, math
from typing import Optional, Tuple
import httpx
import pandas as pd

TD_KEY = os.getenv("TWELVEDATA_API_KEY", "").strip()
AV_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()

# ---- ВСПОМОГАТЕЛЬНОЕ ----
def _pair_from_symbol(sym: str) -> str:
    """Ваши символы из CHAT_SYMBOLS: EUR, GBP, AUD, JPY -> полноценные пары."""
    s = sym.upper()
    m = {
        "EUR": "EURUSD",
        "GBP": "GBPUSD",
        "AUD": "AUDUSD",
        "JPY": "USDJPY",  # важный особый случай
    }
    return m.get(s, s)

def _td_symbol(pair: str) -> str:
    """TwelveData ждёт формат USD/JPY."""
    p = pair.upper()
    return f"{p[:3]}/{p[3:]}"

def _norm_df(rows, ts_key="datetime"):
    df = pd.DataFrame(rows)
    df[ts_key] = pd.to_datetime(df[ts_key], utc=True)
    df = df.set_index(ts_key).sort_index()
    # привести имена столбцов к привычным
    rename = {"open":"open","high":"high","low":"low","close":"close","volume":"volume"}
    for k in ("open","high","low","close"):
        df[k] = pd.to_numeric(df[k], errors="coerce")
    if "volume" not in df.columns:
        df["volume"] = 0.0
    return df[["open","high","low","close","volume"]]

# ---- TWELVE DATA ----
def _fetch_td(pair: str, interval: str="1min", bars: int=2000) -> Optional[pd.DataFrame]:
    if not TD_KEY: return None
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": _td_symbol(pair),
        "interval": interval,
        "outputsize": bars,
        "apikey": TD_KEY,
    }
    with httpx.Client(timeout=15) as c:
        r = c.get(url, params=params)
    data = r.json()
    if "values" not in data:
        # ошибки TD возвращаются в {"code":..., "message": "..."}
        return None
    return _norm_df(data["values"])

# ---- ALPHA VANTAGE ----
def _fetch_av(pair: str, interval: str="1min", full: bool=True) -> Optional[pd.DataFrame]:
    if not AV_KEY: return None
    base, quote = pair[:3], pair[3:]
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "FX_INTRADAY",
        "from_symbol": base,
        "to_symbol": quote,
        "interval": interval,
        "outputsize": "full" if full else "compact",
        "apikey": AV_KEY,
    }
    with httpx.Client(timeout=20) as c:
        r = c.get(url, params=params)
    j = r.json()
    # признак лимита у AV — ключ "Note"
    if "Note" in j or "Information" in j:
        return None
    key = f"Time Series FX ({interval})"
    ts = j.get(key)
    if not ts: return None
    # ts = { "2025-09-07 14:22:00": { "1. open": "...", ...}, ...}
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
    return _norm_df(rows)

# ---- ПУБЛИЧНАЯ ФУНКЦИЯ ----
def fetch_fx_ohlcv(sym_or_pair: str, interval: str="1min", bars: int=2000) -> pd.DataFrame:
    """
    Возвращает DataFrame с индексом-временем и колонками [open,high,low,close,volume].
    sym_or_pair: 'EUR' -> EURUSD, 'USDJPY' -> USDJPY и т.п.
    Интервалы:
      TwelveData: 1min,5min,15min,30min,1h,4h,1day
      AlphaVantage: 1min,5min,15min,30min,60min
    """
    pair = sym_or_pair.upper()
    if len(pair) == 3:  # EUR/GBP/AUD/JPY короткий вид из CHAT_SYMBOLS
        pair = _pair_from_symbol(pair)

    # 1) TwelveData
    df = _fetch_td(pair, interval=interval, bars=bars)
    if isinstance(df, pd.DataFrame) and len(df):
        return df

    # 2) Alpha Vantage (интервалы 60min вместо 1h и т.п.)
    av_interval = interval.replace("h", "0min") if interval.endswith("h") else interval
    df = _fetch_av(pair, interval=av_interval, full=True)
    if isinstance(df, pd.DataFrame) and len(df):
        return df

    raise RuntimeError(f"FX feed failed for {pair} (providers exhausted)")
