# fx_mt5_adapter.py
from __future__ import annotations
import math
import pandas as pd
import yfinance as yf

# Каноничные пары и соответствия
FX = {
    # base/quote   yfinance ticker           tick (минимальный шаг цены)   contract size (1 лот)
    "USDJPY": {"yf": "USDJPY=X", "tick": 0.001,  "contract": 100_000},  # pip=0.01, брокеры дают 3й знак
    "EURUSD": {"yf": "EURUSD=X", "tick": 0.00001, "contract": 100_000},
    "GBPUSD": {"yf": "GBPUSD=X", "tick": 0.00001, "contract": 100_000},
    "AUDUSD": {"yf": "AUDUSD=X", "tick": 0.00001, "contract": 100_000},
}

INTERVAL_MAP = {"5m": "5m", "15m": "15m", "30m": "30m", "1h": "60m", "4h": "240m"}

def fetch_ohlcv_yf(symbol: str, tf: str, lookback_days: int) -> pd.DataFrame:
    """Возвращает DataFrame с колонками ts, open, high, low, close, volume (как у ccxt)."""
    t = FX[symbol]["yf"]
    interval = INTERVAL_MAP[tf]
    # ограничение Yahoo: для 5m максимум ~60 дней; для 60m — год и больше
    period = (f"{min(lookback_days, 59)}d" if tf in ("1m", "2m", "5m", "15m") else f"{lookback_days}d")
    df = yf.download(t, interval=interval, period=period, progress=False, auto_adjust=False)
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
    df = df.reset_index()
    # в ts положим миллисекунды (как ccxt)
    df["ts"] = (pd.to_datetime(df["Datetime" if "Datetime" in df.columns else "Date"]).astype("int64") // 1_000_000)
    return df[["ts","open","high","low","close","volume"]].dropna()

def quantize_price(symbol: str, price: float) -> float:
    step = FX[symbol]["tick"]
    return round(round(price / step) * step, 10)

def margin_to_lots(symbol: str, margin_usd: float, price: float, leverage: float) -> float:
    """
    Пересчёт маржи в ЛОТЫ.
    Формула (акк. валюта USD):
      - если USD базовая валюта (USDJPY) → margin = (contract * lots) / leverage
      - если USD котируемая (EURUSD/GBPUSD/AUDUSD) → margin = (contract * lots * price) / leverage
    """
    c = FX[symbol]["contract"]
    base_is_usd = symbol.startswith("USD")
    denom = (c / leverage) if base_is_usd else (c * price / leverage)
    return 0.0 if denom <= 0 else margin_usd / denom

def lots_to_margin(symbol: str, lots: float, price: float, leverage: float) -> float:
    c = FX[symbol]["contract"]
    base_is_usd = symbol.startswith("USD")
    return (c * lots / leverage) if base_is_usd else (c * lots * price / leverage)

def default_tick(symbol: str) -> float:
    return FX[symbol]["tick"]
