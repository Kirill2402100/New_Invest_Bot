# fx_mt5_adapter.py
from __future__ import annotations
import os

# Минимальный адаптер под Forex (без внешних фидов).
# Держим размеры тика и конвертацию маржа↔︎лоты в USD.

# 1 стандартный лот = 100_000 базовой валюты
FX = {
    "USDJPY": {"tick": 0.001,   "contract": 100_000},  # pip=0.01, часто котируют 3 знака
    "EURUSD": {"tick": 0.00001, "contract": 100_000},  # 5 знаков после запятой
    "GBPUSD": {"tick": 0.00001, "contract": 100_000},
    "EURCHF": {"tick": 0.00001, "contract": 100_000},  # заменили AUDUSD -> EURCHF
}

def _norm(sym: str) -> str:
    """USD/JPY -> USDJPY, eurusd -> EURUSD."""
    return (sym or "").replace("/", "").upper()

def default_tick(symbol: str) -> float:
    s = _norm(symbol)
    return FX.get(s, {}).get("tick", 0.0001)

def quantize_price(symbol: str, price: float) -> float:
    """Округление цены к шагу тика пары."""
    step = default_tick(symbol)
    return round(round(float(price) / step) * step, 10)

def _lot_value_usd(symbol: str, price: float) -> float:
    """
    USD-стоимость 1 стандартного лота (100k base).
    - base == USD (напр. USDJPY): ≈ 100_000 USD
    - quote == USD (напр. EURUSD): ≈ 100_000 * price USD
    - кроссы без USD (напр. EURCHF): ≈ 100_000 * (price * CHFUSD)
      где CHFUSD = 1 / USDCHF. Берём USDCHF из ENV (USDCHF_SPOT/ USDCHF) или 0.90 по умолчанию.
    """
    s = _norm(symbol)
    base, quote = s[:3], s[3:]
    contract = float(FX.get(s, {}).get("contract", 100_000))

    if base == "USD":
        return contract
    if quote == "USD":
        return contract * float(price)

    # --- Кросс без USD: используем прокси-конвертацию quote->USD ---
    # сейчас нужен только CHF; добавим при необходимости другие.
    if quote == "CHF":
        usdchf = float(os.getenv("USDCHF_SPOT") or os.getenv("USDCHF") or 0.90)  # USD/CHF
        chfusd = 1.0 / max(usdchf, 1e-12)  # CHFUSD
        # EURCHF = EUR/CHF, значит USD/лот ≈ 100k * (EUR/CHF) * (CHF/USD)^{-1} = 100k * EURUSD
        return contract * float(price) * chfusd

    # Фолбэк: грубая оценка (предположим quote≈USD)
    return contract * float(price)

def margin_to_lots(symbol: str, margin_usd: float, price: float, leverage: float | int) -> float:
    """
    Сколько лотов можно открыть на данную маржу:
      notional = margin * leverage
      lots = notional / lot_value_usd
    """
    lot_usd = _lot_value_usd(symbol, price)
    lev = max(float(leverage), 1.0)
    notional = float(margin_usd) * lev
    return notional / max(lot_usd, 1e-9)

def lots_to_margin(symbol: str, lots: float, price: float, leverage: float | int) -> float:
    """
    Сколько маржи требуется под указанное количество лотов:
      notional = lots * lot_value_usd
      margin = notional / leverage
    """
    lot_usd = _lot_value_usd(symbol, price)
    lev = max(float(leverage), 1.0)
    notional = float(lots) * lot_usd
    return notional / lev
