# fx_feed.py
from __future__ import annotations

import os
import math
import logging
from typing import Optional, Tuple, Dict

import numpy as np
import pandas as pd

log = logging.getLogger("fx_feed")

# --- Публичный API этого файла ---
__all__ = ["fetch_ohlcv"]

# ----------------------------------------------------------------------
# Символьные маппинги под разные источники
# ----------------------------------------------------------------------
def _normalize_symbol(sym: str) -> Dict[str, str]:
    """Принимает 'EURUSD'/'EUR/USD' и т.п. Возвращает обозначения под провайдеры."""
    s = (sym or "").upper().replace("/", "")
    if len(s) != 6:
        # Фолбэк для экзотики — попробуем как есть
        s = (sym or "").upper().replace("/", "")
    base, quote = s[:3], s[3:]
    return {
        "mt5": f"{base}{quote}",
        "yahoo": f"{base}{quote}=X",            # EURUSD=X
        "stooq": f"{base}{quote}".lower(),      # eurusd
        "twelve": f"{base}/{quote}",            # EUR/USD
    }

# ----------------------------------------------------------------------
# Нормализация датафреймов: столбцы/индекс/типы
# ----------------------------------------------------------------------
_NEED_COLS = ["open", "high", "low", "close", "volume"]

def _lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    return df

def _flatten_multiindex(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [tuple(c)[0] if isinstance(c, tuple) else c for c in df.columns]
    return df

def _to_utc_index(df: pd.DataFrame, time_col: Optional[str] = None) -> pd.DataFrame:
    df = df.copy()
    if time_col and time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
        df = df.dropna(subset=[time_col]).sort_values(time_col).set_index(time_col)
    elif not isinstance(df.index, pd.DatetimeIndex):
        # Пробуем привести индекс
        try:
            idx = pd.to_datetime(df.index, utc=True, errors="coerce")
            df.index = idx
            df = df[~df.index.isna()]
        except Exception:
            pass
    else:
        # уже DatetimeIndex → убеждаемся, что UTC
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
    return df

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # у Yahoo встречается 'adj close'
    if "close" not in df.columns and "adj close" in df.columns:
        df["close"] = df["adj close"]
    for c in _NEED_COLS:
        if c not in df.columns:
            df[c] = 0.0
    df = df[_NEED_COLS]
    # типы
    for c in _NEED_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(how="any")
    return df

def _normalize_ohlcv_df(df: pd.DataFrame, time_col: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        raise RuntimeError("empty dataframe")
    df = _flatten_multiindex(df)
    df = _lower_cols(df)
    # мэппинги разных провайдеров
    rename_map = {
        "date": "time", "datetime": "time", "timestamp": "time",
        "open*": "open", "high*": "high", "low*": "low", "close*": "close",
    }
    # .rename с паттернами нам не нужен, просто базовое приведение
    if "datetime" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"datetime": "time"})
    if "timestamp" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"timestamp": "time"})
    if "date" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"date": "time"})
    df = _to_utc_index(df, time_col="time" if "time" in df.columns else None)
    df = _ensure_cols(df)
    # защитимся от индекса без сортировки
    df = df.sort_index()
    return df

# ----------------------------------------------------------------------
# Провайдеры
# ----------------------------------------------------------------------
def _fetch_mt5(symbol: str, tf: str, limit: int) -> pd.DataFrame:
    """Локальный адаптер MetaTrader 5. Работает только если есть модуль/коннект."""
    try:
        from fx_mt5_adapter import fetch_ohlcv as mt5_fetch  # type: ignore
    except Exception as e:
        raise RuntimeError(f"MT5 unavailable: {e}")
    df = mt5_fetch(symbol, tf, limit)
    return _normalize_ohlcv_df(df)

# ---- Yahoo (yfinance) ------------------------------------------------
_YF_INTERVALS = {
    "1m": "1m", "2m": "2m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "60m", "60m": "60m",
    "1d": "1d",
}

def _yf_period_for(limit: int, tf: str) -> str:
    """Примерная длина периода для yfinance. Для 5m/60m лимитируют 60/730 дней."""
    tf = tf.lower()
    # минут в баре
    mins = {"1m":1, "2m":2, "5m":5, "15m":15, "30m":30, "60m":60, "1h":60, "1d":60*24}.get(tf, 60)
    days_needed = max(1, math.ceil(limit * mins / (60*24)))
    # допустимые периоды Yahoo
    # для внутридня: до 60д на 1–30m, и до 730д на 60m/1h
    if tf in ("1m","2m","5m","15m","30m"):
        return "60d" if days_needed > 30 else ("30d" if days_needed > 7 else "7d")
    if tf in ("60m","1h"):
        if days_needed <= 7: return "7d"
        if days_needed <= 30: return "30d"
        if days_needed <= 60: return "60d"
        return "730d"
    # дневка
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
    df = yf.download(
        tickers=symbol_y,
        interval=interval,
        period=period,
        auto_adjust=False,
        actions=False,
        progress=False,
        threads=False,
    )
    try:
        df = _normalize_ohlcv_df(df)
    except Exception as e:
        log.warning("Yahoo normalize failed for %s %s: %s", symbol_y, tf, e)
        raise
    # ограничим по limit
    df = df.tail(int(limit))
    if df.empty:
        raise RuntimeError("Yahoo returned empty after tail()")
    return df

# ---- Stooq (только дневные данные) -----------------------------------
def _fetch_stooq(symbol_s: str, tf: str, limit: int) -> pd.DataFrame:
    if tf.lower() != "1d":
        raise RuntimeError("Stooq supports only '1d'")
    url = f"https://stooq.com/q/d/l/?s={symbol_s}&i=d"
    df = pd.read_csv(url)
    # Возможные варианты заголовков
    rename = {
        "Date": "time", "date": "time",
        "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume",
        "open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume",
    }
    df = df.rename(columns=rename)
    df = _normalize_ohlcv_df(df, time_col="time")
    df = df.tail(int(limit))
    if df.empty:
        raise RuntimeError("Stooq returned empty")
    return df

# ---- TwelveData -------------------------------------------------------
_TD_INTERVALS = {
    "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1h", "60m": "1h",
    "1d": "1day",
}

def _fetch_twelvedata(symbol_t: str, tf: str, limit: int) -> pd.DataFrame:
    import requests  # stdlib-compatible, есть в окружении

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
        "outputsize": int(limit) * 2,  # небольшой запас
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
        # Ошибка API?
        if isinstance(data, dict) and data.get("status") == "error":
            raise RuntimeError(f"TwelveData error: {data.get('message')}")
        rows = (data or {}).get("values") if isinstance(data, dict) else data
        df = pd.DataFrame(rows)
    else:
        # внезапно CSV
        from io import StringIO
        df = pd.read_csv(StringIO(r.text))

    if df is None or df.empty:
        raise RuntimeError("TwelveData empty dataframe")

    # у TD колонка времени называется 'datetime'
    if "datetime" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"datetime": "time"})
    if "timestamp" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"timestamp": "time"})

    df = _normalize_ohlcv_df(df, time_col="time" if "time" in df.columns else None)
    df = df.tail(int(limit))
    if df.empty:
        raise RuntimeError("TwelveData returned empty after tail()")
    return df

# ----------------------------------------------------------------------
# Роутер: MT5 -> Yahoo -> Stooq -> TwelveData
# ----------------------------------------------------------------------
def _fetch_with_provider_chain(symbol: str, tf: str, limit: int) -> Tuple[pd.DataFrame, str]:
    """Возвращает (df, provider_name) либо кидает исключение после исчерпания провайдеров."""
    names = []
    sym = _normalize_symbol(symbol)
    errors = []

    # 1) MT5
    try:
        df = _fetch_mt5(sym["mt5"], tf, limit)
        return df, "MT5"
    except Exception as e:
        errors.append(f"MT5: {e}")
        names.append("MT5")

    # 2) Yahoo
    try:
        df = _fetch_yahoo(sym["yahoo"], tf, limit)
        return df, "Yahoo"
    except Exception as e:
        log.warning("Yahoo fetch failed for %s %s: %s", sym["yahoo"], tf, e)
        errors.append(f"Yahoo: {e}")
        names.append("Yahoo")

    # 3) Stooq (только дневные)
    try:
        if tf.lower() == "1d":
            df = _fetch_stooq(sym["stooq"], tf, limit)
            return df, "Stooq"
        else:
            errors.append("Stooq: unsupported timeframe")
            names.append("Stooq")
    except Exception as e:
        log.warning("Stooq fetch failed for %s %s: %s", sym["stooq"], tf, e)
        errors.append(f"Stooq: {e}")
        names.append("Stooq")

    # 4) TwelveData
    try:
        df = _fetch_twelvedata(sym["twelve"], tf, limit)
        return df, "TwelveData"
    except Exception as e:
        log.warning("TwelveData fetch failed for %s %s: %s", sym["twelve"], tf, e)
        errors.append(f"TwelveData: {e}")
        names.append("TwelveData")

    # Исчерпали
    provs = ", ".join(names) if names else "no providers"
    raise RuntimeError(f"FX feed failed for {symbol} (providers exhausted: {provs}). Errors: {errors}")

# ----------------------------------------------------------------------
# Публичная функция
# ----------------------------------------------------------------------
def fetch_ohlcv(symbol: str, tf: str, limit: int) -> pd.DataFrame:
    """
    Унифицированный доступ к OHLCV.
    Возвращает DataFrame с индексом DatetimeIndex(UTC) и колонками:
    ['open','high','low','close','volume'].

    :param symbol: 'EURUSD' / 'EUR/USD' / 'USDJPY' и т.п.
    :param tf: '5m' | '1h' | '1d' (и др. см. маппинги провайдеров)
    :param limit: желаемое количество баров (хвост)
    """
    df, provider = _fetch_with_provider_chain(symbol, tf, int(limit))
    log.info("FX feed: %s %s via %s -> %d bars", symbol, tf, provider, len(df))
    return df
