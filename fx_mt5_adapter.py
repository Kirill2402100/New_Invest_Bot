# fx_mt5_adapter.py
from __future__ import annotations

# Минимальный адаптер под Forex (без yfinance).
# Держим только размеры тика и конвертацию маржа↔︎лоты.

# Каноничные пары и параметры
# 1 стандартный лот = 100_000 базовой валюты
FX = {
    "USDJPY": {"tick": 0.001,   "contract": 100_000},  # pip=0.01, часто котируют 3 знака
    "EURUSD": {"tick": 0.00001, "contract": 100_000},  # 5 знаков после запятой
    "GBPUSD": {"tick": 0.00001, "contract": 100_000},
    "AUDUSD": {"tick": 0.00001, "contract": 100_000},
}

def _norm(sym: str) -> str:
    """USD/JPY -> USDJPY, eurusd -> EURUSD."""
    return sym.replace("/", "").upper()

def default_tick(symbol: str) -> float:
    s = _norm(symbol)
    return FX.get(s, {"tick": 0.0001})["tick"]

def quantize_price(symbol: str, price: float) -> float:
    """Округление цены к шагу тика пары."""
    step = default_tick(symbol)
    return round(round(float(price) / step) * step, 10)

def _lot_value_usd(symbol: str, price: float) -> float:
    """
    USD-стоимость 1 стандартного лота (100k base).
    - base == USD (напр. USDJPY): ~100_000 USD
    - quote == USD (напр. EURUSD): ~100_000 * price USD
    """
    s = _norm(symbol)
    base, quote = s[:3], s[3:]
    contract = FX.get(s, {"contract": 100_000})["contract"]
    if base == "USD":
        return float(contract)
    elif quote == "USD":
        return float(contract) * float(price)
    # На случай экзотических пар: грубая оценка
    return float(contract)

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
