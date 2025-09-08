# fx_feed.py
from __future__ import annotations

import os
import math
import logging
from typing import Optional, Tuple, Dict

import numpy as np
import pandas as pd

log = logging.getLogger("fx_feed")

__all__ = ["fetch_ohlcv"]

# ------------------------------- Symbols ---------------------------------
def _normalize_symbol(sym: str) -> Dict[str, str]:
    s = (sym or "").upper().replace("/", "")
    base, quote = (s[:3], s[3:]) if len(s) >= 6 else (s[:3], s[3:])
    return {
        "mt5": f"{base}{quote}",
        "yahoo": f"{base}{quote}=X",
        "stooq": f"{base}{quote}".lower(),
        "twelve": f"{base}/{quote}",
    }

# ------------------------- DataFrame normalization ------------------------
_NEED_COLS = ["open", "high", "low", "close", "volume"]

def _as_dataframe(obj) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, pd.Series):
        return obj.to_frame().T
    try:
        return pd.DataFrame(obj)
    except Exception:
        raise RuntimeError("provider returned non-tabular data")

def _flatten_multiindex(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        # Берём 1-й уровень названий ('Open','High',...) — игнорируем тикер
        flat = []
        for c in df.columns:
            if isinstance(c, tuple) and len(c) > 0:
                flat.append(str(c[0]))
            else:
                flat.append(str(c))
        df = df.copy()
        df.columns = flat
    return df

def _lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _to_utc_index(df: pd.DataFrame, time_col: Optional[str] = None) -> pd.DataFrame:
    df = df.copy()

    if time_col is None:
        for cand in ("time", "datetime", "timestamp", "date"):
            if cand in df.columns:
                time_col = cand
                break

    if time_col:
        # Принудительно приводим ко времени c UTC
        df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
        df = df.dropna(subset=[time_col]).sort_values(time_col).set_index(time_col)
    else:
        # Пытаемся привести существующий индекс
        if isinstance(df.index, pd.DatetimeIndex):
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
        else:
            try:
                idx = pd.to_datetime(df.index, utc=True, errors="coerce")
                if idx.notna().any():
                    df.index = idx
                    df = df[~df.index.isna()]
                else:
                    # fallback: переносим индекс в колонку и пробуем снова
                    df = df.reset_index(drop=False).rename(columns={"index": "time"})
                    return _to_utc_index(df, time_col="time")
            except Exception:
                df = df.reset_index(drop=False).rename(columns={"index": "time"})
                return _to_utc_index(df, time_col="time")
    return df

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Иногда Yahoo отдаёт только 'adj close'
    if "close" not in df.columns and "adj close" in df.columns:
        df["close"] = df["adj close"]

    # Если нет volume — нули
    if "volume" not in df.columns:
        df["volume"] = 0.0

    # Гарантируем набор колонок
    for c in _NEED_COLS:
        if c not in df.columns:
            df[c] = 0.0

    df = df[_NEED_COLS]

    # Числовизация + очистка
    for c in _NEED_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.replace([np.inf, -np.inf], np.nan).dropna(how="any")

    if df.empty:
        raise RuntimeError("empty after column sanitize")

    # Сортировка и дедуп индекса
    df = df.sort_index()
    if not isinstance(df.index, pd.DatetimeIndex):
        raise RuntimeError("index is not DatetimeIndex after normalization")
    df = df[~df.index.duplicated(keep="last")]
    return df

def _normalize_ohlcv_df(raw_df) -> pd.DataFrame:
    df = _as_dataframe(raw_df)

    # Если после download()/history() пришёл MultiIndex — расплющим
    df = _flatten_multiindex(df)

    # Приводим имена
    df = _lower_cols(df)

    # Если у нас ещё нет явной колонки времени — reset_index -> time
    if "time" not in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        tmp = df.reset_index(drop=False)
        # разные версии называют Datetime/Date/Index по-разному
        for cand in ("Datetime", "Date", "date", "datetime", "index"):
            if cand in tmp.columns:
                tmp = tmp.rename(columns={cand: "time"})
                break
        df = tmp

    # Привязываем ко времени UTC
    df = _to_utc_index(df)

    # Выровнять OHLCV
    df = _ensure_cols(df)
    return df

# ------------------------------- Providers --------------------------------
def _fetch_mt5(symbol: str, tf: str, limit: int) -> pd.DataFrame:
    try:
        from fx_mt5_adapter import fetch_ohlcv as mt5_fetch  # type: ignore
    except Exception as e:
        raise RuntimeError(f"MT5 unavailable: {e}")
    df = mt5_fetch(symbol, tf, limit)
    return _normalize_ohlcv_df(df)

_YF_INTERVALS = {
    "1m": "1m", "2m": "2m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "60m", "60m": "60m", "1d": "1d",
}

def _yf_period_for(limit: int, tf: str) -> str:
    tf = tf.lower()
    mins = {"1m":1, "2m":2, "5m":5, "15m":15, "30m":30, "60m":60, "1h":60, "1d":1440}.get(tf, 60)
    days_needed = max(1, math.ceil(limit * mins / 1440))
    if tf in ("1m","2m","5m","15m","30m"):
        return "60d" if days_needed > 30 else ("30d" if days_needed > 7 else "7d")
    if tf in ("60m","1h"):
        if days_needed <= 7: return "7d"
        if days_needed <= 30: return "30d"
        if days_needed <= 60: return "60d"
        return "730d"
    if days_needed <= 30: return "1mo"
    if days_needed <= 90: return "3mo"
    if days_needed <= 365: return "1y"
    if days_needed <= 365*2: return "2y"
    return "5y"

def _fetch_yahoo(symbol_y: str, tf: str, limit: int) -> pd.DataFrame:
    try:
        import yfinance as yf  # type: ignore
    except Exception as e:
        raise RuntimeError(f"yfinance not available: {e}")

    interval = _YF_INTERVALS.get(tf.lower())
    if not interval:
        raise RuntimeError(f"Yahoo unsupported timeframe: {tf}")

    period = _yf_period_for(limit, tf)

    # -- PASS A: Ticker().history() (чаще отдаёт ровный фрейм)
    last_err = None
    try:
        t = yf.Ticker(symbol_y)
        df_raw = t.history(period=period, interval=interval, auto_adjust=False, actions=False)
        log.debug("Yahoo(history) raw: shape=%s index=%s cols=%s", getattr(df_raw, "shape", None), type(getattr(df_raw, "index", None)), list(getattr(df_raw, "columns", [])))
        if isinstance(df_raw, (pd.Series, pd.DataFrame)) and not df_raw.empty:
            # Явно сбросим индекс -> time, затем нормализация
            df_norm = _normalize_ohlcv_df(df_raw.reset_index())
            df_norm = df_norm.tail(int(limit))
            if df_norm.empty:
                raise RuntimeError("Yahoo(history) empty after tail()")
            return df_norm
    except Exception as e:
        last_err = e

    # -- PASS B: download() + жёсткая нормализация
    try:
        df_raw = yf.download(
            tickers=symbol_y,
            interval=interval,
            period=period,
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
            group_by="column",
        )
        log.debug("Yahoo(download) raw: shape=%s index=%s cols=%s", getattr(df_raw, "shape", None), type(getattr(df_raw, "index", None)), list(getattr(df_raw, "columns", [])))

        # Универсальный привод к плоскому виду
        if isinstance(df_raw, pd.Series):
            df_raw = df_raw.to_frame().T

        if isinstance(df_raw.columns, pd.MultiIndex):
            # Построим новый фрейм, выбирая по первому уровню колонок
            parts = {}
            for want in ("Open","High","Low","Close","Adj Close","Volume"):
                cols = [c for c in df_raw.columns if (isinstance(c, tuple) and str(c[0]).lower() == want.lower()) or (str(c).lower() == want.lower())]
                if cols:
                    parts[want] = df_raw[cols[0]]
            if parts:
                df_raw = pd.concat(parts, axis=1)

        # Теперь обычное приведение имён + время
        df_raw = df_raw.reset_index(drop=False)
        rename = {}
        for cand in ("Datetime", "Date", "date", "datetime", "index"):
            if cand in df_raw.columns:
                rename[cand] = "time"
                break
        for col in list(df_raw.columns):
            low = col.lower()
            if low in ("open","high","low","close","adj close","adjclose","adj_close","volume"):
                rename[col] = low
        if rename:
            df_raw = df_raw.rename(columns=rename)
        if "close" not in df_raw.columns and "adj close" in df_raw.columns:
            df_raw["close"] = df_raw["adj close"]

        df = _normalize_ohlcv_df(df_raw)
        df = df.tail(int(limit))
        if df.empty:
            raise RuntimeError("Yahoo(download) empty after tail()")
        return df
    except Exception as e2:
        msg = str(e2) if e2 else str(last_err) if last_err else "unknown"
        raise RuntimeError(msg)

def _fetch_stooq(symbol_s: str, tf: str, limit: int) -> pd.DataFrame:
    if tf.lower() != "1d":
        raise RuntimeError("Stooq supports only '1d'")
    url = f"https://stooq.com/q/d/l/?s={symbol_s}&i=d"
    df_raw = pd.read_csv(url)
    df_raw = df_raw.rename(columns={
        "Date": "time", "date": "time",
        "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume",
    })
    df = _normalize_ohlcv_df(df_raw)
    df = df.tail(int(limit))
    if df.empty:
        raise RuntimeError("Stooq returned empty")
    return df

_TD_INTERVALS = {
    "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1h", "60m": "1h", "1d": "1day",
}

def _fetch_twelvedata(symbol_t: str, tf: str, limit: int) -> pd.DataFrame:
    import requests

    api_key = os.getenv("TWELVEDATA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("TwelveData key is missing (TWELVEDATA_API_KEY)")

    interval = _TD_INTERVALS.get(tf.lower())
    if not interval:
        raise RuntimeError(f"TwelveData unsupported timeframe: {tf}")

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol_t,
        "interval": interval,
        "outputsize": int(limit) * 2,
        "format": "JSON",
        "apikey": api_key,
        "timezone": "UTC",
        "order": "ASC",
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    ct = r.headers.get("content-type", "").lower()

    if "json" in ct:
        data = r.json()
        if isinstance(data, dict) and data.get("status") == "error":
            raise RuntimeError(f"TwelveData error: {data.get('message')}")
        rows = (data or {}).get("values") if isinstance(data, dict) else data
        df_raw = pd.DataFrame(rows)
    else:
        from io import StringIO
        df_raw = pd.read_csv(StringIO(r.text))

    if "datetime" in df_raw.columns and "time" not in df_raw.columns:
        df_raw = df_raw.rename(columns={"datetime": "time"})
    if "timestamp" in df_raw.columns and "time" not in df_raw.columns:
        df_raw = df_raw.rename(columns={"timestamp": "time"})

    df = _normalize_ohlcv_df(df_raw)
    df = df.tail(int(limit))
    if df.empty:
        raise RuntimeError("TwelveData returned empty after tail()")
    return df

# ------------------------------ Router ------------------------------------
def _fetch_with_provider_chain(symbol: str, tf: str, limit: int) -> Tuple[pd.DataFrame, str]:
    sym = _normalize_symbol(symbol)
    errors = []

    # 1) MT5
    try:
        df = _fetch_mt5(sym["mt5"], tf, limit)
        return df, "MT5"
    except Exception as e:
        errors.append(f"MT5: {e}")

    # 2) Yahoo
    try:
        df = _fetch_yahoo(sym["yahoo"], tf, limit)
        return df, "Yahoo"
    except Exception as e:
        log.warning("Yahoo fetch failed for %s %s: %s", sym["yahoo"], tf, e)
        errors.append(f"Yahoo: {e}")

    # 3) Stooq (только дневки)
    try:
        if tf.lower() == "1d":
            df = _fetch_stooq(sym["stooq"], tf, limit)
            return df, "Stooq"
        else:
            errors.append("Stooq: unsupported timeframe")
    except Exception as e:
        log.warning("Stooq fetch failed for %s %s: %s", sym["stooq"], tf, e)
        errors.append(f"Stooq: {e}")

    # 4) TwelveData
    try:
        df = _fetch_twelvedata(sym["twelve"], tf, limit)
        return df, "TwelveData"
    except Exception as e:
        log.warning("TwelveData fetch failed for %s %s: %s", sym["twelve"], tf, e)
        errors.append(f"TwelveData: {e}")

    raise RuntimeError(
        f"FX feed failed for {symbol} (providers exhausted: MT5, Yahoo, Stooq, TwelveData). Errors: {errors}"
    )

# ------------------------------ Public API --------------------------------
def fetch_ohlcv(symbol: str, tf: str, limit: int) -> pd.DataFrame:
    df, provider = _fetch_with_provider_chain(symbol, tf, int(limit))
    log.info("FX feed: %s %s via %s -> %d bars", symbol, tf, provider, len(df))
    return df
