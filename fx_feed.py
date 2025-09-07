# fx_feed.py
from __future__ import annotations
import os
from typing import Optional, Dict, Any

import httpx
import pandas as pd

# ===== API KEYS =====
TD_KEY = os.getenv("TWELVEDATA_API_KEY", "").strip()
AV_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()

# ===== Helpers =====
def _pair_from_symbol(sym: str) -> str:
    """
    Преобразует короткие обозначения (EUR, GBP, AUD, JPY) в пары к USD.
    Иначе возвращает исходное (например, EURUSD / USDJPY).
    """
    s = sym.upper()
    m = {
        "EUR": "EURUSD",
        "GBP": "GBPUSD",
        "AUD": "AUDUSD",
        "JPY": "USDJPY",  # особый случай для иены
    }
    return m.get(s, s)

def _td_symbol(pair: str) -> str:
    """TwelveData ожидает формат 'EUR/USD'."""
    p = pair.upper()
    base, quote = p[:3], p[3:]
    return f"{base}/{quote}"

def _normalize_df(rows: list[Dict[str, Any]], ts_key: str = "datetime") -> pd.DataFrame:
    """
    Преобразует список словарей TwelveData/AV в DataFrame c индексом-временем (UTC)
    и столбцами [open, high, low, close, volume].
    """
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    df[ts_key] = pd.to_datetime(df[ts_key], utc=True)
    df = df.set_index(ts_key).sort_index()

    for k in ("open", "high", "low", "close", "volume"):
        if k not in df.columns:
            df[k] = 0.0
    for k in ("open", "high", "low", "close", "volume"):
        df[k] = pd.to_numeric(df[k], errors="coerce")

    return df[["open", "high", "low", "close", "volume"]]

def _resample_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Ресэмплинг минутных/часовых данных в OHLC.
    Используется, например, для 4H из 60min у Alpha Vantage.
    """
    if df.empty:
        return df
    o = df["open"].resample(rule).first()
    h = df["high"].resample(rule).max()
    l = df["low"].resample(rule).min()
    c = df["close"].resample(rule).last()
    v = df["volume"].resample(rule).sum(min_count=1)
    out = pd.concat([o, h, l, c, v], axis=1)
    out.columns = ["open", "high", "low", "close", "volume"]
    out = out.dropna(subset=["open", "high", "low", "close"])
    return out

# ===== Twelve Data =====
def _fetch_td(pair: str, interval: str = "1min", bars: int = 2000) -> Optional[pd.DataFrame]:
    if not TD_KEY:
        return None
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": _td_symbol(pair),
        "interval": interval,           # 1min,5min,15min,30min,1h,4h,1day
        "outputsize": bars,             # сколько баров вернуть
        "timezone": "UTC",
        "apikey": TD_KEY,
    }
    try:
        with httpx.Client(timeout=20) as c:
            r = c.get(url, params=params)
            r.raise_for_status()
        data = r.json()
    except Exception:
        return None

    # Ошибка у TD приходит как {"code": ..., "message": "..."}
    if not isinstance(data, dict) or "values" not in data:
        return None

    values = data["values"]
    # Приведение формата к единому виду
    rows = []
    for row in values:
        rows.append({
            "datetime": row.get("datetime"),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume", 0.0),
        })
    rows.reverse()  # TD отдаёт от нового к старому — перевернём
    return _normalize_df(rows)

# ===== Alpha Vantage =====
def _fetch_av_intraday(pair: str, interval: str = "60min", full: bool = True) -> Optional[pd.DataFrame]:
    """
    FX_INTRADAY: интервалы 1min,5min,15min,30min,60min.
    """
    if not AV_KEY:
        return None
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
    try:
        with httpx.Client(timeout=25) as c:
            r = c.get(url, params=params)
            r.raise_for_status()
        j = r.json()
    except Exception:
        return None

    # Признаки лимита/ошибок
    if not isinstance(j, dict) or "Note" in j or "Information" in j:
        return None

    key = f"Time Series FX ({interval})"
    ts = j.get(key)
    if not isinstance(ts, dict):
        return None

    rows = []
    # AV отдаёт "YYYY-mm-dd HH:MM:SS" -> {"1. open": "...", ...}
    for t, ohlc in ts.items():
        try:
            rows.append({
                "datetime": pd.to_datetime(t, utc=True),
                "open": float(ohlc["1. open"]),
                "high": float(ohlc["2. high"]),
                "low":  float(ohlc["3. low"]),
                "close":float(ohlc["4. close"]),
                "volume": 0.0,
            })
        except Exception:
            continue
    rows.sort(key=lambda x: x["datetime"])
    return _normalize_df(rows)

def _fetch_av_daily(pair: str, full: bool = True) -> Optional[pd.DataFrame]:
    """
    FX_DAILY: дневные бары.
    """
    if not AV_KEY:
        return None
    base, quote = pair[:3], pair[3:]
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "FX_DAILY",
        "from_symbol": base,
        "to_symbol": quote,
        "outputsize": "full" if full else "compact",
        "apikey": AV_KEY,
    }
    try:
        with httpx.Client(timeout=25) as c:
            r = c.get(url, params=params)
            r.raise_for_status()
        j = r.json()
    except Exception:
        return None

    if not isinstance(j, dict) or "Note" in j or "Information" in j:
        return None

    ts = j.get("Time Series FX (Daily)")
    if not isinstance(ts, dict):
        return None

    rows = []
    for t, ohlc in ts.items():
        try:
            rows.append({
                "datetime": pd.to_datetime(t, utc=True),
                "open": float(ohlc["1. open"]),
                "high": float(ohlc["2. high"]),
                "low":  float(ohlc["3. low"]),
                "close":float(ohlc["4. close"]),
                "volume": 0.0,
            })
        except Exception:
            continue
    rows.sort(key=lambda x: x["datetime"])
    return _normalize_df(rows)

# ===== Public API =====
def fetch_fx_ohlcv(sym_or_pair: str, interval: str = "1min", bars: int = 2000) -> pd.DataFrame:
    """
    Возвращает DataFrame с индексом времени (UTC) и колонками:
    [open, high, low, close, volume].

    interval:
      TwelveData: 1min,5min,15min,30min,1h,4h,1day
      Alpha Vantage: 1min,5min,15min,30min,60min (+ daily отдельно)

    bars: желаемое количество баров (если провайдер умеет ограничивать).
    """
    pair = sym_or_pair.upper()
    if len(pair) == 3:  # короткие имена из CHAT_SYMBOLS
        pair = _pair_from_symbol(pair)

    # --- Попытка №1: Twelve Data (напрямую нужный интервал) ---
    df = _fetch_td(pair, interval=interval, bars=bars)
    if isinstance(df, pd.DataFrame) and not df.empty:
        return df

    # --- Попытка №2: Alpha Vantage ---
    # Дневки — отдельная функция
    if interval in ("1day", "1d", "D"):
        df = _fetch_av_daily(pair, full=True)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
        raise RuntimeError(f"FX feed failed for {pair} (providers exhausted for daily)")

    # Для 4h у AV прямого интервала нет — берём 60min и ресемплим
    if interval == "4h":
        base_df = _fetch_av_intraday(pair, interval="60min", full=True)
        if isinstance(base_df, pd.DataFrame) and not base_df.empty:
            return _resample_ohlc(base_df, "4H")

    # Для 1h — берём 60min
    if interval in ("1h", "60min"):
        df = _fetch_av_intraday(pair, interval="60min", full=True)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df

    # Прочие интервалы: 1min/5min/15min/30min
    if interval in ("1min", "5min", "15min", "30min"):
        df = _fetch_av_intraday(pair, interval=interval, full=True)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df

    raise RuntimeError(f"FX feed failed for {pair} (providers exhausted)")

# Обёртка под старую сигнатуру сканера:
# fetch_ohlcv(symbol, timeframe, limit)
_TF_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1day",
    "D": "1day",
}

def fetch_ohlcv(symbol: str, timeframe: str, limit: int | None = 2000) -> pd.DataFrame:
    """
    Совместимо со старым вызовом (как было у yfinance-обёртки в проекте):
      fetch_ohlcv(symbol='USDJPY', timeframe='5m', limit=1200)
    """
    interval = _TF_MAP.get(timeframe, timeframe)
    bars = int(limit or 2000)
    return fetch_fx_ohlcv(symbol, interval=interval, bars=bars)


__all__ = [
    "fetch_fx_ohlcv",
    "fetch_ohlcv",
]
