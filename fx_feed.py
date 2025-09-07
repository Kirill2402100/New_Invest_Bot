# fx_feed.py
from __future__ import annotations

import os, time, logging, math
from typing import Optional, Tuple
import pandas as pd

log = logging.getLogger("fx_feed")

# -------------------------------
# КЭШ (на символ+TF)
# -------------------------------
_CACHE: dict[Tuple[str, str], dict] = {}

def _ttl_for_tf(tf: str) -> int:
    tf = tf.lower()
    if tf in ("1m", "2m", "5m", "15m"):        return 20     # сек
    if tf in ("30m", "45m", "1h", "2h", "4h"): return 120
    if tf in ("1d", "1w", "1wk"):              return 600
    return 60

def _from_cache(symbol: str, tf: str, need_rows: int) -> Optional[pd.DataFrame]:
    k = (symbol.upper(), tf)
    it = _CACHE.get(k)
    if not it: return None
    if (time.time() - it["ts"]) > it["ttl"]: return None
    df: pd.DataFrame = it["df"]
    if len(df) >= need_rows:
        return df.tail(need_rows).copy()
    return None

def _to_cache(symbol: str, tf: str, df: pd.DataFrame):
    _CACHE[(symbol.upper(), tf)] = {"ts": time.time(), "ttl": _ttl_for_tf(tf), "df": df.copy()}

# -------------------------------
# НОРМАЛИЗАЦИЯ
# -------------------------------
def _norm_symbol(sym: str) -> str:
    return str(sym or "").replace("/", "").replace("\\", "").replace("-", "").upper()

def _yahoo_symbol(sym: str) -> str:
    s = _norm_symbol(sym)
    return f"{s}=X" if not s.endswith("=X") else s

def _td_symbol(sym: str) -> str:
    s = str(sym or "").upper().replace(" ", "")
    if "/" in s: return s
    # EURUSD -> EUR/USD
    return f"{s[:3]}/{s[3:]}" if len(s) == 6 else s

def _ensure_ohlcv(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    """Приводим к [open, high, low, close, volume] + UTC DatetimeIndex"""
    if df is None or df.empty:
        return pd.DataFrame(columns=["open","high","low","close","volume"])
    df = df.rename(columns={
        "Open":"open","High":"high","Low":"low","Close":"close","Adj Close":"close",
        "Volume":"volume","tick_volume":"volume","real_volume":"volume"
    })
    # гарантируем нужные колонки
    cols = ["open","high","low","close","volume"]
    df = df[[c for c in cols if c in df.columns]]
    for c in cols:
        if c not in df.columns:
            df[c] = 0.0
    # индекс во времени
    if "time" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df.set_index("time")
    if not isinstance(df.index, pd.DatetimeIndex):
        # попробуем привести
        try: df.index = pd.to_datetime(df.index, utc=True)
        except Exception: pass
    df = df.sort_index()
    df = df.dropna(subset=["open","high","low","close"], how="any")
    return df.tail(limit)

# -------------------------------
# MT5
# -------------------------------
_MT5_INITED = False
def _fetch_mt5(symbol: str, tf: str, limit: int) -> Optional[pd.DataFrame]:
    """MetaTrader5: реальные/тик-volume. Возвращает None если нет MT5 или не удалось."""
    global _MT5_INITED
    try:
        import MetaTrader5 as mt5
    except Exception:
        return None

    # Инициализация один раз
    if not _MT5_INITED:
        try:
            # если логин/сервер/пароль нужны — держи их в ENV (опционально)
            login = os.getenv("MT5_LOGIN"); password = os.getenv("MT5_PASSWORD"); server = os.getenv("MT5_SERVER")
            if login and password and server:
                ok = mt5.initialize(login=int(login), password=password, server=server)
            else:
                ok = mt5.initialize()
            if not ok:
                log.warning("MT5 init failed: %s", mt5.last_error())
                return None
            _MT5_INITED = True
        except Exception as e:
            log.warning("MT5 init error: %s", e)
            return None

    tf_map = {
        "1m": mt5.TIMEFRAME_M1, "2m": mt5.TIMEFRAME_M2, "3m": mt5.TIMEFRAME_M3, "5m": mt5.TIMEFRAME_M5,
        "15m": mt5.TIMEFRAME_M15, "30m": mt5.TIMEFRAME_M30, "1h": mt5.TIMEFRAME_H1, "4h": mt5.TIMEFRAME_H4,
        "1d": mt5.TIMEFRAME_D1
    }
    tfi = tf_map.get(tf.lower())
    if tfi is None:
        return None

    sym = _norm_symbol(symbol)
    try:
        rates = mt5.copy_rates_from_pos(sym, tfi, 0, max(50, int(limit)))
        if rates is None or len(rates) == 0:
            return None
        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.rename(columns={"real_volume":"volume", "tick_volume":"volume"})
        df = df[["time","open","high","low","close","volume"]]
        return _ensure_ohlcv(df, limit)
    except Exception as e:
        log.warning("MT5 fetch failed for %s %s: %s", sym, tf, e)
        return None

# -------------------------------
# Yahoo Finance (yfinance)
# -------------------------------
def _yahoo_period_for(tf: str, limit: int) -> str:
    # Выбираем минимально достаточный period
    tf = tf.lower()
    minutes = {"1m":1, "2m":2, "5m":5, "15m":15, "30m":30, "60m":60, "1h":60}
    if tf in minutes:
        need_min = limit * minutes[tf]
        need_days = need_min / (60*24)
        # Допустимые периоды
        candidates = ["1d","5d","7d","14d","30d","60d","90d","6mo","1y","2y"]
        values = {
            "1d":1, "5d":5, "7d":7, "14d":14, "30d":30, "60d":60, "90d":90, "6mo":180, "1y":365, "2y":730
        }
        for p in candidates:
            if values[p] >= need_days - 1e-9:
                return p
        return "2y"
    # час/день
    if tf in ("1h","90m","60m"):
        # для часов хватает 30d для сотен баров
        return "60d"
    return "2y"

def _fetch_yahoo(symbol: str, tf: str, limit: int) -> Optional[pd.DataFrame]:
    try:
        import yfinance as yf
    except Exception:
        return None

    interval_map = {"1m":"1m","2m":"2m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","60m":"60m","1d":"1d"}
    interval = interval_map.get(tf.lower())
    if interval is None:
        return None

    ticker = _yahoo_symbol(symbol)
    try:
        period = _yahoo_period_for(interval, limit)
        df = yf.download(tickers=ticker, interval=interval, period=period, auto_adjust=False, progress=False, prepost=False, threads=False)
        if df is None or df.empty:
            return None
        # у FX volume часто весь ноль — это ок
        df = df.rename_axis("time").reset_index()
        df = _ensure_ohlcv(df, limit)
        return df
    except Exception as e:
        log.warning("Yahoo fetch failed for %s %s: %s", ticker, tf, e)
        return None

# -------------------------------
# Stooq (daily only)
# -------------------------------
def _fetch_stooq(symbol: str, tf: str, limit: int) -> Optional[pd.DataFrame]:
    if tf.lower() not in ("1d", "1day", "d", "day"):
        return None
    try:
        from pandas_datareader import data as pdr
    except Exception:
        return None
    # Stooq использует EURUSD, USDJPY (без суффиксов)
    ticker = _norm_symbol(symbol)
    try:
        df = pdr.DataReader(ticker, "stooq")
        if df is None or df.empty:
            return None
        df = df.sort_index()
        df = df.rename_axis("time").reset_index()
        df = _ensure_ohlcv(df, limit)
        return df
    except Exception as e:
        log.warning("Stooq fetch failed for %s: %s", ticker, e)
        return None

# -------------------------------
# Twelve Data (последний резерв)
# -------------------------------
def _fetch_twelvedata(symbol: str, tf: str, limit: int) -> Optional[pd.DataFrame]:
    api_key = os.getenv("TWELVE_DATA_KEY") or os.getenv("TWELVEDATA_API_KEY")
    if not api_key:
        return None
    import requests
    tf_map = {"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1h","1d":"1day"}
    interval = tf_map.get(tf.lower())
    if interval is None:
        return None
    sym = _td_symbol(symbol)
    params = {
        "symbol": sym,
        "interval": interval,
        "outputsize": max(50, int(limit)),
        "apikey": api_key,
        "format": "CSV",
        "order": "ASC"
    }
    url = "https://api.twelvedata.com/time_series"
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            log.error("TwelveData HTTP %s: %s", r.status_code, r.text[:200])
            return None
        # бывает, что в теле приходит сообщение об исчерпании кредитов — оставим проверку наверху
        from io import StringIO
        df = pd.read_csv(StringIO(r.text))
        # ожидаемые колонки: datetime, open, high, low, close, volume
        df = df.rename(columns={"datetime":"time"})
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df = _ensure_ohlcv(df, limit)
        # Проверка на ошибку-сообщение (в CSV может быть одна колонка "code"/"message")
        if df.empty and "code" in r.text.lower():
            log.error("TwelveData error payload: %s", r.text[:200])
            return None
        return df
    except Exception as e:
        log.warning("TwelveData fetch failed for %s %s: %s", sym, tf, e)
        return None

# -------------------------------
# ПУБЛИЧНЫЙ API
# -------------------------------
def fetch_ohlcv(symbol: str, tf: str, limit: int) -> pd.DataFrame:
    """
    Возвращает DataFrame с колонками [open, high, low, close, volume] и DatetimeIndex (UTC).
    Провайдеры по приоритету: MT5 -> Yahoo -> Stooq -> TwelveData.
    Встроенный кэш на tf.
    """
    if not symbol or not tf or limit <= 0:
        return pd.DataFrame(columns=["open","high","low","close","volume"])

    symbol_u = _norm_symbol(symbol)
    tf_l = tf.lower()
    limit = int(limit)

    # кэш
    cached = _from_cache(symbol_u, tf_l, limit)
    if cached is not None and not cached.empty:
        return cached

    providers = [
        ("MT5", _fetch_mt5),
        ("Yahoo", _fetch_yahoo),
        ("Stooq", _fetch_stooq),
        ("TwelveData", _fetch_twelvedata),
    ]

    last_df = None
    errors = []
    for name, fn in providers:
        df = fn(symbol_u, tf_l, limit)
        if df is not None and not df.empty:
            _to_cache(symbol_u, tf_l, df)
            if name != "MT5":
                log.debug("[fx_feed] %s served %s %s (%d rows)", name, symbol_u, tf_l, len(df))
            else:
                log.debug("[fx_feed] MT5 served %s %s (%d rows)", symbol_u, tf_l, len(df))
            return df.tail(limit)
        errors.append(name)

    # если добрались сюда — все провайдеры пустые
    msg = f"FX feed failed for {symbol_u} (providers exhausted: {', '.join(errors)})"
    log.error(msg)
    raise RuntimeError(msg)
