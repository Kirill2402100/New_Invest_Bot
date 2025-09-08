# scanner_bmr_dca.py — patched, full
# - безопасная нормализация символа (str|dict|list)
# - автосоздание листов BMR_DCA_<SYMBOL> и дублирование логов туда
# - интеграция ФА-бота (risk/bias/ttl/updated_at) + периодическое обновление
# - совместимость с fetch_ohlcv(symbol, tf, limit) из fx_feed

from __future__ import annotations

import asyncio, time, logging, json, os, inspect, numbers
from typing import Optional
from datetime import datetime, timezone
from enum import IntEnum

import numpy as np
import pandas as pd
from telegram.ext import Application
import gspread

# === Forex адаптеры и фид ===
from fx_mt5_adapter import FX, margin_to_lots, default_tick
from fx_feed import fetch_ohlcv as fetch_ohlcv_yf

import trade_executor

log = logging.getLogger("bmr_dca_engine")
logging.getLogger("fx_feed").setLevel(logging.WARNING)

# === Мини-TA без внешних зависимостей (EMA/ATR/RSI/ADX/Supertrend) ===
def _ema(s: pd.Series, length: int) -> pd.Series:
    return s.ewm(span=length, adjust=False, min_periods=length).mean()

def _rma(s: pd.Series, length: int) -> pd.Series:
    alpha = 1.0 / max(length, 1)
    return s.ewm(alpha=alpha, adjust=False, min_periods=length).mean()

def ta_atr(h: pd.Series, l: pd.Series, c: pd.Series, length: int = 14) -> pd.Series:
    cp = c.shift(1)
    tr = pd.concat([(h - l), (h - cp).abs(), (l - cp).abs()], axis=1).max(axis=1)
    return _rma(tr, length)

def ta_rsi(c: pd.Series, length: int = 14) -> pd.Series:
    d = c.diff()
    up = d.clip(lower=0.0)
    dn = (-d).clip(lower=0.0)
    rs = _rma(up, length) / _rma(dn, length).replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))

def ta_adx(h: pd.Series, l: pd.Series, c: pd.Series, length: int = 14) -> pd.Series:
    up = h.diff()
    dn = -l.diff()
    plus_dm  = pd.Series(np.where((up > dn) & (up > 0),  up, 0.0), index=h.index)
    minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=h.index)
    cp = c.shift(1)
    tr_raw = pd.concat([(h - l), (h - cp).abs(), (l - cp).abs()], axis=1).max(axis=1)
    tr = _rma(tr_raw, length).replace(0, np.nan)
    plus_di  = 100.0 * _rma(plus_dm,  length) / tr
    minus_di = 100.0 * _rma(minus_dm, length) / tr
    dx  = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100.0
    return _rma(dx, length)

def ta_supertrend(h: pd.Series, l: pd.Series, c: pd.Series, length: int = 10, multiplier: float = 3.0) -> pd.DataFrame:
    atr = ta_atr(h, l, c, length)
    hl2 = (h + l) / 2.0
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr

    f_upper = upper.copy()
    f_lower = lower.copy()
    for i in range(1, len(c)):
        f_upper.iloc[i] = min(upper.iloc[i], f_upper.iloc[i-1]) if c.iloc[i-1] > f_upper.iloc[i-1] else upper.iloc[i]
        f_lower.iloc[i] = max(lower.iloc[i], f_lower.iloc[i-1]) if c.iloc[i-1] < f_lower.iloc[i-1] else lower.iloc[i]

    direction = pd.Series(index=c.index, dtype=int)
    direction.iloc[0] = 1
    for i in range(1, len(c)):
        if c.iloc[i] > f_upper.iloc[i-1]:
            direction.iloc[i] = 1
        elif c.iloc[i] < f_lower.iloc[i-1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i-1]
            if direction.iloc[i] == 1 and f_lower.iloc[i] < f_lower.iloc[i-1]:
                f_lower.iloc[i] = f_lower.iloc[i-1]
            if direction.iloc[i] == -1 and f_upper.iloc[i] > f_upper.iloc[i-1]:
                f_upper.iloc[i] = f_upper.iloc[i-1]
    return pd.DataFrame({"direction": direction, "upper": f_upper, "lower": f_lower})

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
class CONFIG:
    # Пара по умолчанию (можно переопределить через ENV FX_SYMBOL)
    SYMBOL = "USDJPY"

    # Комиссии в долях (0.0004 = 0.04%)
    FEE_MAKER = 0.0
    FEE_TAKER = 0.0

    # Плечо по умолчанию
    LEVERAGE = 200
    MIN_LEVERAGE = 2
    MAX_LEVERAGE = 500

    # Таймфреймы
    TF_ENTRY = "5m"
    TF_RANGE = "1h"

    # Сколько истории собирать под диапазоны
    STRATEGIC_LOOKBACK_DAYS = 60    # для TF_RANGE
    TACTICAL_LOOKBACK_DAYS  = 3     # для TF_RANGE

    # Таймауты/интервалы
    FETCH_TIMEOUT = 25
    SCAN_INTERVAL_SEC = 3
    REBUILD_RANGE_EVERY_MIN    = 15
    REBUILD_TACTICAL_EVERY_MIN = 5

    # Диапазон/квантили/индикаторы
    Q_LOWER = 0.025
    Q_UPPER = 0.975
    RANGE_MIN_ATR_MULT = 1.5
    RSI_LEN = 14
    ADX_LEN = 14
    VOL_WIN = 50

    # Весовая модель
    WEIGHTS = {
        "border": 0.45, "rsi": 0.15, "ema_dev": 0.20,
        "supertrend": 0.10, "vol": 0.10
    }
    SCORE_THR = 0.55

    # DCA
    DCA_LEVELS = 7
    DCA_GROWTH = 2.0
    CUM_DEPOSIT_FRAC_AT_FULL = 0.70
    ADD_COOLDOWN_SEC = 25

    # Тейк/Трейл
    TP_PCT = 0.010
    TRAILING_STAGES = [(0.35, 0.25), (0.60, 0.50), (0.85, 0.75)]

    # Безопасность/ликвидация
    BREAK_EPS = 0.0025
    REENTRY_BAND = 0.003
    MAINT_MMR = 0.004
    LIQ_FEE_BUFFER = 1.0

    # Банк и авто-аллоцирование
    SAFETY_BANK_USDT = 1500.0
    AUTO_LEVERAGE = False
    AUTO_ALLOC = {
        "thin_tac_vs_strat": 0.35,
        "low_vol_z": 0.5,
        "growth_A": 1.6,
        "growth_B": 2.2,
    }

    # «Шипы» / ретест
    SPIKE = {
        "WICK_RATIO": 2.0,
        "ATR_MULT": 1.6,
        "VOLZ_THR": 1.5,
        "RETRACE_FRAC": 0.35,
        "RETRACE_WINDOW_SEC": 120,
    }

    # ФА-политика
    FA_REFRESH_SEC = 300  # перечитывать раз в 5 минут

    # Анти-слипание ценовых уровней
    DCA_MIN_GAP_TICKS = 2       # минимум 2 тика между целями

    # Показываем ML-цену при целевом Margin Level
    ML_TARGET_PCT = 20.0        # "ML цена (20%)"

# ENV-переопределения
CONFIG.SYMBOL    = os.getenv("FX_SYMBOL", CONFIG.SYMBOL)
CONFIG.TF_ENTRY = os.getenv("TF_ENTRY", CONFIG.TF_ENTRY)
CONFIG.TF_RANGE = os.getenv("TF_RANGE", os.getenv("TF_TREND", CONFIG.TF_RANGE))

# ---------------------------------------------------------------------------
# Helpers & Sheets
# ---------------------------------------------------------------------------
async def maybe_await(func, *args, **kwargs):
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

SAFE_LOG_KEYS = {
    "Event_ID","Signal_ID","Leverage","Timestamp_UTC","Pair","Side","Event",
    "Step_No","Step_Margin_USDT","Cum_Margin_USDT","Entry_Price","Avg_Price",
    "TP_Pct","TP_Price","SL_Price","Liq_Est_Price","Next_DCA_Price",
    "Fee_Rate_Maker","Fee_Rate_Taker","Fee_Est_USDT",
    "ATR_5m","ATR_1h","RSI_5m","ADX_5m","Supertrend","Vol_z",
    "Range_Lower","Range_Upper","Range_Width",
    "PNL_Realized_USDT","PNL_Realized_Pct","Time_In_Trade_min","Trail_Stage",
    "Next_DCA_Label","Triggered_Label"
}
SAFE_LOG_KEYS |= {
    "Chat_ID", "Owner_Key",      # для трассировки по чату/владельцу
    "FA_Risk", "FA_Bias"         # подготовка под фунд-политику
}

BMR_HEADERS_FALLBACK = [
    "Event","Event_ID","Timestamp_UTC","Pair","Side","Signal_ID","Leverage",
    "Step_No","Step_Margin_USDT","Cum_Margin_USDT","Entry_Price","Avg_Price",
    "TP_Pct","TP_Price","SL_Price","Liq_Est_Price","Next_DCA_Price","Next_DCA_Label",
    "ATR_5m","ATR_1h","RSI_5m","ADX_5m","Supertrend","Vol_z",
    "Range_Lower","Range_Upper","Range_Width",
    "Fee_Rate_Maker","Fee_Rate_Taker","Fee_Est_USDT",
    "PNL_Realized_USDT","PNL_Realized_Pct","Time_In_Trade_min","Trail_Stage",
    "Triggered_Label","Chat_ID","Owner_Key","FA_Risk","FA_Bias"
]

def _get_master_headers(sh) -> list[str]:
    try:
        ws = sh.worksheet("BMR_DCA_Log")
        row1 = ws.row_values(1)
        return row1 if row1 else BMR_HEADERS_FALLBACK
    except Exception:
        return BMR_HEADERS_FALLBACK

def _clean(v):
    if v is None: return ""
    if isinstance(v, np.generic): v = v.item()
    if isinstance(v, numbers.Real):
        if not np.isfinite(v): return ""
        return float(v)
    return v

def _clean_payload(d: dict) -> dict:
    return {k: _clean(v) for k, v in d.items() if k in SAFE_LOG_KEYS}

# ---- NORMALIZER ----

def _norm_symbol(x) -> str:
    # Принимает str | dict | list/tuple и возвращает строку тикера UPPERCASE
    if isinstance(x, dict):
        for k in ("symbol", "pair", "name"):
            if x.get(k):
                return str(x[k]).upper()
        if len(x) == 1:
            return str(next(iter(x.values()))).upper()
        return str(next(iter(x.keys()), "")).upper()
    if isinstance(x, (list, tuple, set)):
        for it in x:
            if it:
                return _norm_symbol(it)
        return ""
    return str(x or "").upper()

# ---- SHEETS HELPERS ----

def _ensure_ws(sh, name: str, headers: list[str]):
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=2000, cols=max(20, len(headers)))
        if headers:
            ws.append_row(headers)
    return ws

async def log_event_safely(payload: dict):
    # 1) пишем через trade_executor в общий лог (как раньше)
    try:
        await maybe_await(trade_executor.bmr_log_event, _clean_payload(payload))
    except Exception:
        log.exception("[SHEETS] bmr_log_event failed")
    # 2) дублируем в лист по символу
    try:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
        sheet_key  = os.environ.get("SHEET_ID")
        if not (creds_json and sheet_key):
            return
        sym = str(payload.get("Pair") or payload.get("pair") or "").upper()
        if not sym:
            return
        gc = gspread.service_account_from_dict(json.loads(creds_json))
        sh = gc.open_by_key(sheet_key)
        headers = _get_master_headers(sh)
        ws2 = _ensure_ws(sh, f"BMR_DCA_{sym}", headers)
        row = [_clean(payload.get(k)) for k in headers]
        ws2.append_row(row)
    except Exception:
        log.exception("[SHEETS] per-symbol log failed")

# ---- FA POLICY ----

def read_fa_policy(symbol: str) -> dict:
    """Читает политику из листа FA_Signals: pair, risk(Green/Amber/Red), bias(neutral/long-only/short-only),
    ttl (мин), updated_at (ISO). При просрочке TTL возвращает {}.
    """
    try:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
        sheet_key  = os.environ.get("SHEET_ID")
        if not (creds_json and sheet_key):
            return {}
        gc = gspread.service_account_from_dict(json.loads(creds_json))
        sh = gc.open_by_key(sheet_key)
        try:
            ws = sh.worksheet("FA_Signals")
        except gspread.WorksheetNotFound:
            return {}
        rows = ws.get_all_records()
        for r in rows:
            if str(r.get("pair", "")).upper() == symbol.upper():
                risk = (str(r.get("risk", "") or "Green").capitalize())
                bias = (str(r.get("bias", "") or "neutral").lower())
                ttl  = int(r.get("ttl") or 0)
                updated_at = str(r.get("updated_at") or "").strip()
                if ttl and updated_at:
                    try:
                        ts = pd.to_datetime(updated_at, utc=True)
                        if pd.Timestamp.now(tz="UTC") > ts + pd.Timedelta(minutes=ttl):
                            return {}
                    except Exception:
                        pass
                
                scan_lock_until = str(r.get("scan_lock_until") or "").strip()
                reserve_off = str(r.get("reserve_off") or "").strip().lower() in ("1","true","yes","on")
                try: dca_scale = float(r.get("dca_scale") or 1.0)
                except: dca_scale = 1.0
                return {
                   "risk": risk, "bias": bias, "ttl": ttl, "updated_at": updated_at,
                   "scan_lock_until": scan_lock_until, "reserve_off": reserve_off, "dca_scale": dca_scale
                }
        return {}
    except Exception:
        log.exception("read_fa_policy failed")
        return {}

# ---------------------------------------------------------------------------
# Formatting & maths
# ---------------------------------------------------------------------------

def fmt(p: float) -> str:
    if p is None or pd.isna(p): return "N/A"
    if p < 0.01: return f"{p:.6f}"
    if p < 1.0:  return f"{p:.5f}"
    return f"{p:.4f}"

def margin_line(pos, bank: float, px: float | None = None, fees_est: float = 0.0) -> str:
    """
    Показывает used/free и ML% (по equity, с учётом буфера).
    Если px не передан — ML% приблизим как bank/used.
    """
    used = _pos_total_margin(pos)
    if used <= 0:
        return f"Маржа: used 0.00 | свободная {bank:.2f} | ML≈ ∞"

    if px is None:
        ml_pct = (bank / used) * 100.0
        free = max(bank - used, 0.0)
    else:
        eq = equity_at_price(pos, px, bank, fees_est)
        ml_pct = (eq / used) * 100.0
        free = max(eq - used, 0.0)

    ml_txt = "∞" if not np.isfinite(ml_pct) or ml_pct > 9999 else f"{ml_pct:.0f}%"
    return f"Маржа: used {used:.2f} | свободная {free:.2f} | ML≈ {ml_txt}"

def plan_margins_bank_first(bank: float, levels: int, growth: float) -> list[float]:
    if levels <= 0 or bank <= 0: return []
    if abs(growth - 1.0) < 1e-9:
        per = bank / levels
        return [per] * levels
    base = bank * (growth - 1.0) / (growth**levels - 1.0)
    return [base * (growth**i) for i in range(levels)]

def _pos_total_margin(pos):
    used_ord_count = max(0, pos.steps_filled - (1 if getattr(pos, "reserve_used", False) else 0))
    ord_used = sum(pos.step_margins[:min(used_ord_count, len(pos.step_margins))])
    res = pos.reserve_margin_usdt if getattr(pos, 'reserve_used', False) else 0.0
    return ord_used + res

def compute_net_pnl(pos, exit_p: float, fee_entry: float, fee_exit: float) -> tuple[float, float]:
    sum_margin = _pos_total_margin(pos) or 1e-9
    raw_pnl = (exit_p / pos.avg - 1.0) * (1 if pos.side == "LONG" else -1)
    gross_usd = sum_margin * (raw_pnl * pos.leverage)
    entry_notional = sum_margin * pos.leverage
    exit_notional  = exit_p * pos.qty
    fee_entry_usd = entry_notional * fee_entry
    fee_exit_usd  = exit_notional  * fee_exit
    net_usd = gross_usd - fee_entry_usd - fee_exit_usd
    net_pct = (net_usd / sum_margin) * 100.0
    return net_usd, net_pct

def _pnl_at_price(pos, price: float, used_margin: float) -> float:
    """PnL в USD при цене price (без комиссий)."""
    if used_margin <= 0 or price is None or pos.avg <= 0:
        return 0.0
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    if pos.side == "LONG":
        return used_margin * L * (price / pos.avg - 1.0)
    else:  # SHORT
        return used_margin * L * (pos.avg / max(price, 1e-12) - 1.0)

def equity_at_price(pos, price: float, bank: float, fees_est: float) -> float:
    """Equity = банк (включая 30% буфер) + PnL - комиссии."""
    used = _pos_total_margin(pos)
    pnl  = _pnl_at_price(pos, price, used)
    return bank + pnl - max(fees_est, 0.0)

def ml_percent_now(pos, price: float, bank: float, fees_est: float) -> float:
    used = _pos_total_margin(pos)
    if used <= 0: 
        return float('inf')
    return (equity_at_price(pos, price, bank, fees_est) / used) * 100.0

def ml_price_at(pos, target_ml_pct: float, bank: float, fees_est: float) -> float:
    """
    Цена, при которой ML = target_ml_pct.
    ML = Equity / UsedMargin, Equity = bank + PnL - fees_est.
    Решаем относительно цены.
    """
    UM = _pos_total_margin(pos)
    if UM <= 0 or pos.avg <= 0:
        return float('nan')
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    target_equity = (target_ml_pct / 100.0) * UM
    base = (target_equity - (bank - max(fees_est, 0.0))) / (UM * L)

    if pos.side == "LONG":
        # price/avg - 1 = base  ->  price = avg * (1 + base)
        return pos.avg * (1.0 + base)
    else:
        # avg/price - 1 = base  ->  price = avg / (1 + base)
        denom = 1.0 + base
        if denom <= 0:
            return float('nan')
        return pos.avg / denom

def ml_distance_pct(side: str, px: float, ml_price: float) -> float:
    if px is None or ml_price is None or px <= 0 or np.isnan(ml_price):
        return float('nan')
    return (1.0 - ml_price / px) * 100.0 if side == "LONG" else (ml_price / px - 1.0) * 100.0

def chandelier_stop(side: str, price: float, atr: float, mult: float = 3.0):
    return price - mult*atr if side == "LONG" else price + mult*atr

def break_levels(rng: dict) -> tuple[float, float]:
    up = rng["upper"] * (1.0 + CONFIG.BREAK_EPS)
    dn = rng["lower"] * (1.0 - CONFIG.BREAK_EPS)
    return up, dn

def break_distance_pcts(px: float, up: float, dn: float) -> tuple[float, float]:
    if px is None or px <= 0 or any(v is None or np.isnan(v) for v in (up, dn)):
        return float('nan'), float('nan')
    up_pct = max(0.0, (up / px - 1.0) * 100.0)
    dn_pct = max(0.0, (1.0 - dn / px) * 100.0)
    return up_pct, dn_pct

def quantize_to_tick(x: float | None, tick: float) -> float | None:
    if x is None or (isinstance(x, float) and np.isnan(x)): return x
    return round(round(x / tick) * tick, 10)

def _place_segment(start: float, end: float, count: int, tick: float, include_end_last: bool) -> list[float]:
    """Равномерно распределяем точки между start и end. Анти-слипание: >= DCA_MIN_GAP_TICKS."""
    if count <= 0:
        return []
    path = end - start
    if abs(path) < tick * CONFIG.DCA_MIN_GAP_TICKS:
        return []
    
    if include_end_last and count >= 1:
        # фракции: 1/n, 2/n, ..., 1.0  (нет 0.0)
        fracs = [(i + 1) / count for i in range(count)]
    else:
        # по-прежнему исключаем оба конца
        fracs = [(i + 1) / (count + 1) for i in range(count)]
        
    raw = [start + path * f for f in fracs]
    # Квантование и анти-слипание
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    out: list[float] = []
    for x in raw:
        q = quantize_to_tick(x, tick)
        if q is None:
            continue
        if not out or (abs(q - out[-1]) >= min_gap):
            out.append(q)
    return out

def compute_corridor_targets(entry: float, side: str, rng_strat: dict, rng_tac: dict, tick: float) -> list[dict]:
    """
    2 точки в сторону TAC-границы (без включения самой границы),
    3 точки между TAC- и STRAT-границами (последняя может совпасть со STRAT-границей).
    Если «места мало» — количество автоматически сокращается.
    """
    if side == "LONG":
        tac_b  = rng_tac["lower"]
        strat_b = rng_strat["lower"]
        seg1 = _place_segment(entry, tac_b, 2, tick, include_end_last=False)
        seg2 = _place_segment(tac_b, strat_b, 3, tick, include_end_last=True)
    else:
        tac_b  = rng_tac["upper"]
        strat_b = rng_strat["upper"]
        seg1 = _place_segment(entry, tac_b, 2, tick, include_end_last=False)
        seg2 = _place_segment(tac_b, strat_b, 3, tick, include_end_last=True)

    # Метки
    def _labels(prefix: str, n: int, include_end: bool) -> list[str]:
        if n <= 0: return []
        if include_end and n >= 1:
            fr = [(i + 1) / n for i in range(n)]  # 33..100%
        else:
            fr = [(i + 1) / (n + 1) for i in range(n)]  # 33..66% для сегмента entry→TAC
        return [f"{prefix} {int(round(f * 100))}%" for f in fr]

    out: list[dict] = []
    for p, lab in zip(seg1, _labels("TAC", len(seg1), include_end=False)):
        out.append({"price": p, "label": lab})
    for p, lab in zip(seg2, _labels("STRAT", len(seg2), include_end=True)):
        out.append({"price": p, "label": lab})

    # Упорядочить по стороне и удалить возможные дубликаты
    out = merge_targets_sorted(side, tick, out)
    return out

def merge_targets_sorted(side: str, tick: float, targets: list[dict]) -> list[dict]:
    if side == "SHORT":
        targets = sorted(targets, key=lambda t: t["price"])
    else:
        targets = sorted(targets, key=lambda t: t["price"], reverse=True)
    dedup = []
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    for t in targets:
        if not dedup: dedup.append(t)
        else:
            if (side=="SHORT" and t["price"] >= dedup[-1]["price"] + min_gap) or \
               (side=="LONG"  and t["price"] <= dedup[-1]["price"] - min_gap):
                dedup.append(t)
    return dedup

def next_pct_target(pos):
    if not getattr(pos, "ordinary_targets", None):
        return None
    # шагов обычных, включая OPEN
    used_ord_incl_open = pos.steps_filled - (1 if pos.reserve_used else 0)
    # следующий уровень в ordinary_targets имеет индекс на 1 меньше
    idx = used_ord_incl_open - 1
    return pos.ordinary_targets[idx] if 0 <= idx < len(pos.ordinary_targets) else None

def choose_growth(ind: dict, rng_strat: dict, rng_tac: dict) -> float:
    try:
        width_ratio = (rng_tac["width"] / max(rng_strat["width"], 1e-9))
    except Exception:
        width_ratio = 1.0
    thin = width_ratio <= CONFIG.AUTO_ALLOC["thin_tac_vs_strat"]
    low_vol = abs(ind.get("vol_z", 0.0)) <= CONFIG.AUTO_ALLOC["low_vol_z"]
    return CONFIG.AUTO_ALLOC["growth_A"] if (thin and low_vol) else CONFIG.AUTO_ALLOC["growth_B"]

def _is_df_fresh(df: pd.DataFrame, max_age_min: int = 15) -> bool:
    try:
        idx = df.index
        # допускаем DatetimeIndex или колонку с временем
        last_ts = idx[-1].to_pydatetime() if hasattr(idx[-1], "to_pydatetime") else None
        if last_ts is None and "time" in df.columns:
            last_ts = pd.to_datetime(df["time"].iloc[-1]).to_pydatetime()
        if last_ts is None:
            return True  # нет явного времени — не стопорим
        age_min = (datetime.utcnow() - last_ts.replace(tzinfo=None)).total_seconds() / 60.0
        return age_min <= max_age_min
    except Exception:
        return True

# ---------------------------------------------------------------------------
# Диапазоны/индикаторы
# ---------------------------------------------------------------------------
async def build_range_from_df(df: Optional[pd.DataFrame], min_atr_mult: float):
    if df is None or df.empty: return None
    cols = list(df.columns)[-5:]
    df = df[cols].copy()
    df.columns = ["open","high","low","close","volume"]
    ema = _ema(df["close"], length=50)
    atr = ta_atr(df["high"], df["low"], df["close"], length=14)
    lower = float(np.quantile(df["close"].dropna(), CONFIG.Q_LOWER))
    upper = float(np.quantile(df["close"].dropna(), CONFIG.Q_UPPER))
    if pd.notna(ema.iloc[-1]) and pd.notna(atr.iloc[-1]):
        mid = float(ema.iloc[-1])
        atr1h = float(atr.iloc[-1])
        lower = min(lower, mid - min_atr_mult * atr1h)
        upper = max(upper, mid + min_atr_mult * atr1h)
    else:
        atr1h = 0.0
        mid = float(df["close"].iloc[-1])
    return {"lower": lower, "upper": upper, "mid": mid, "atr1h": atr1h, "width": upper-lower}

async def build_ranges(symbol: str):
    s_limit = CONFIG.STRATEGIC_LOOKBACK_DAYS * 24
    t_limit = CONFIG.TACTICAL_LOOKBACK_DAYS  * 24
    s_df_task = asyncio.create_task(maybe_await(fetch_ohlcv_yf, symbol, CONFIG.TF_RANGE, s_limit))
    t_df_task = asyncio.create_task(maybe_await(fetch_ohlcv_yf, symbol, CONFIG.TF_RANGE, t_limit))
    s_df, t_df = await asyncio.gather(s_df_task, t_df_task)
    # STRAT ≥ 3×ATR, TAC ≥ 1.5×ATR
    s_task = asyncio.create_task(build_range_from_df(s_df, min_atr_mult=3.0))
    t_task = asyncio.create_task(build_range_from_df(t_df, min_atr_mult=1.5))
    strat, tac = await asyncio.gather(s_task, t_task)
    return strat, tac

def compute_indicators_5m(df: pd.DataFrame) -> dict:
    atr5m = ta_atr(df["high"], df["low"], df["close"], length=14).iloc[-1]
    rsi   = ta_rsi(df["close"], length=CONFIG.RSI_LEN).iloc[-1]
    adx   = ta_adx(df["high"], df["low"], df["close"], length=CONFIG.ADX_LEN).iloc[-1]
    ema20 = _ema(df["close"], length=20).iloc[-1]
    vol_z = (df["volume"].iloc[-1] - df["volume"].rolling(CONFIG.VOL_WIN).mean().iloc[-1]) / \
            max(df["volume"].rolling(CONFIG.VOL_WIN).std().iloc[-1], 1e-9)
    st = ta_supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3.0)
    dir_now  = int(st["direction"].iloc[-1])
    dir_prev = int(st["direction"].iloc[-2])
    st_state = (
        "down_to_up_near" if (dir_prev == -1 and dir_now == 1) else
        "up_to_down_near" if (dir_prev == 1 and dir_now == -1) else
        ("up" if dir_now == 1 else "down")
    )

    ema_dev_atr = abs(df["close"].iloc[-1] - ema20) / max(float(atr5m), 1e-9)
    for v in (atr5m, rsi, adx, ema20, vol_z, ema_dev_atr):
        if pd.isna(v) or np.isinf(v):
            raise ValueError("Indicators contain NaN/Inf")
    return {
        "atr5m": float(atr5m), "rsi": float(rsi), "adx": float(adx),
        "ema20": float(ema20), "vol_z": float(vol_z),
        "ema_dev_atr": float(ema_dev_atr), "supertrend": st_state
    }

class FSM(IntEnum):
    IDLE = 0      # нет позиции
    OPENED = 1    # открыт 1-й шаг, идёт первичное оповещение
    MANAGING = 2  # можно ADD/RETEST/TRAIL/EXIT

# ---------------------------------------------------------------------------
# Состояние позиции
# ---------------------------------------------------------------------------
class Position:
    def __init__(self, side: str, signal_id: str, leverage: int | None = None, owner_key: str | None = None):
        self.side = side
        self.signal_id = signal_id
        self.owner_key = owner_key
        self.steps_filled = 0
        self.step_margins = []
        self.qty = 0.0
        self.avg = 0.0
        self.tp_pct = CONFIG.TP_PCT
        self.tp_price = 0.0
        self.sl_price = None
        self.open_ts = time.time()
        self.leverage = leverage or CONFIG.LEVERAGE
        self.max_steps = CONFIG.DCA_LEVELS
        self.last_sl_notified_price = None
        self.ordinary_targets: list[dict] = []
        self.trail_stage: int = -1
        self.growth = CONFIG.DCA_GROWTH
        self.ord_levels: int = 0
        self.reserve_margin_usdt: float = 0.0
        self.reserve_available: bool = False
        self.reserve_used: bool = False
        self.freeze_ordinary: bool = False
        self.last_add_ts: float | None = None
        self.spike_flag: bool = False
        self.spike_direction: str | None = None
        self.spike_deadline_ts: float | None = None
        self.spike_ref_ohlc: Optional[tuple[float, float, float, float]] = None
        self.alloc_bank_planned: float = 0.0

    def plan_with_reserve(self, bank: float, growth: float, ord_levels: int):
        self.growth = growth
        self.ord_levels = max(1, int(ord_levels))
        total_target = bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
        margins_full = plan_margins_bank_first(total_target, self.ord_levels + 1, growth)
        self.step_margins = margins_full[:self.ord_levels]
        self.reserve_margin_usdt = margins_full[-1]
        self.reserve_available = True
        self.reserve_used = False
        self.freeze_ordinary = False
        self.max_steps = self.ord_levels + 1

    def rebalance_tail_margins_excluding_reserve(self, bank: float):
        total_target = bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
        used_ord_count = max(0, self.steps_filled - (1 if self.reserve_used else 0))
        used_ord_sum = sum(self.step_margins[:min(used_ord_count, len(self.step_margins))]) if self.step_margins else 0.0
        remaining_budget_for_ord = max(0.0, total_target - used_ord_sum - self.reserve_margin_usdt)
        remaining_levels = max(0, self.ord_levels - used_ord_count)
        if remaining_levels <= 0:
            return
        tail = plan_margins_bank_first(remaining_budget_for_ord, remaining_levels, self.growth)
        self.step_margins = (self.step_margins[:used_ord_count] or []) + tail
        self.max_steps = used_ord_count + remaining_levels + (1 if (self.reserve_available and not self.reserve_used) else 0)

    def add_step(self, price: float):
        used_ord_count = self.steps_filled - (1 if self.reserve_used else 0)
        used_ord_count = min(used_ord_count, max(0, len(self.step_margins) - 1))
        margin = self.step_margins[used_ord_count]
        notional = margin * self.leverage
        new_qty = notional / max(price, 1e-9)
        self.avg = (self.avg * self.qty + price * new_qty) / max(self.qty + new_qty, 1e-9) if self.qty > 0 else price
        self.qty += new_qty
        self.steps_filled += 1
        self.tp_price = self.avg * (1 + self.tp_pct) if self.side == "LONG" else self.avg * (1 - self.tp_pct)
        self.last_add_ts = time.time()
        return margin, notional

    def add_reserve_step(self, price: float):
        if not self.reserve_available or self.reserve_used:
            return 0.0, 0.0
        margin = self.reserve_margin_usdt
        notional = margin * self.leverage
        new_qty = notional / max(price, 1e-9)
        self.avg = (self.avg * self.qty + price * new_qty) / max(self.qty + new_qty, 1e-9) if self.qty > 0 else price
        self.qty += new_qty
        self.steps_filled += 1
        self.reserve_available = False
        self.reserve_used = True
        self.tp_price = self.avg * (1 + self.tp_pct) if self.side == "LONG" else self.avg * (1 - self.tp_pct)
        self.last_add_ts = time.time()
        self.max_steps = self.steps_filled
        return margin, notional

# ---------------------------------------------------------------------------
# Broadcasting helpers
# ---------------------------------------------------------------------------

def _make_bcaster(default_chat_id: int | None):
    async def _bc(app, text, target_chat_id=None):
        cid = target_chat_id or default_chat_id
        if cid is None:
            log.warning("[broadcast-fallback] No chat id, message dropped")
            return
        try:
            await app.bot.send_message(chat_id=cid, text=text, parse_mode="HTML")
        except Exception as e:
            log.error(f"[broadcast-fallback] send failed: {e}")
    return _bc


def _wrap_broadcast(bc, default_chat_id: int | None):
    """Делает переданный broadcast совместимым с нашей сигнатурой."""
    async def _wb(app, text, target_chat_id=None):
        try:
            # нормальный путь (наш main)
            return await bc(app, text, target_chat_id=target_chat_id or default_chat_id)
        except TypeError:
            # если старый bc без kwargs
            return await bc(app, text)
        except Exception as e:
            log.error(f"[broadcast wrapper] falling back: {e}")
            fb = _make_bcaster(default_chat_id)
            return await fb(app, text, target_chat_id=target_chat_id)
    return _wb

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
async def scanner_main_loop(
    app: Application,
    broadcast,
    symbol_override: Optional[str] = None,
    target_chat_id: Optional[int] = None,
    botbox: Optional[dict] = None,
    *args, **kwargs
):
    """
    Основной цикл BMR-DCA. Совместим как с kwargs (symbol_override/target_chat_id/botbox),
    так и с позиционным запуском (app, broadcast, botbox).
    """
    # Совместимость с вызовом через 3 позиционных аргумента: (app, broadcast, box)
    if botbox is None and args:
        botbox = args[0] if len(args) >= 1 and isinstance(args[0], dict) else None

    # Заворачиваем broadcast, чтобы он понимал target_chat_id (и имел фолбэк)
    broadcast = _wrap_broadcast(broadcast, target_chat_id)

    log.info("BMR-DCA loop starting…")
    root = botbox if botbox is not None else app.bot_data
    
    symbol = _norm_symbol(symbol_override or CONFIG.SYMBOL)
    ns_key = f"{symbol}|{target_chat_id or 'default'}"
    
    b = root.setdefault(ns_key, {})       # <- у каждого символа/чата свой карман
    b.setdefault("position", None)
    b.setdefault("fsm_state", int(FSM.IDLE))
    b.setdefault("intro_done", False)
    b["owner_key"] = ns_key               # полезно видеть в логах
    b["chat_id"]   = target_chat_id

    # Google Sheets (необязательно)
    try:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
        sheet_key  = os.environ.get("SHEET_ID")
        if creds_json and sheet_key:
            gc = gspread.service_account_from_dict(json.loads(creds_json))
            sheet = gc.open_by_key(sheet_key)
            await maybe_await(trade_executor.ensure_bmr_log_sheet, sheet, title="BMR_DCA_Log")
    except Exception as e:
        log.error(f"Sheets init error: {e}", exc_info=True)

    # ---- SYMBOL ----
    if symbol not in FX:
        log.critical(f"Unsupported FX symbol: {symbol}. Supported: {list(FX.keys())}")
        return

    tick = default_tick(symbol)
    b["price_tick"] = tick
    log.info(f"Successfully initialized for Forex symbol {symbol} with tick size {tick}")

    # Подготовим листы для символа (если есть доступ)
    try:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
        sheet_key  = os.environ.get("SHEET_ID")
        if creds_json and sheet_key:
            gc = gspread.service_account_from_dict(json.loads(creds_json))
            sh = gc.open_by_key(sheet_key)
            headers = _get_master_headers(sh)
            _ensure_ws(sh, f"BMR_DCA_{symbol}", headers)
    except Exception:
        log.exception("ensure symbol sheet failed")

    # Локальный хэлпер на рассылку
    async def say(txt: str):
        now_ts = time.time()
        # не дублируем точь-в-точь одинаковое сообщение чаще, чем раз в 20с
        if txt == b.get("last_msg_text") and (now_ts - b.get("last_msg_ts", 0.0) < 20):
            return
        b["last_msg_text"] = txt
        b["last_msg_ts"] = now_ts
        await broadcast(app, txt, target_chat_id=target_chat_id)

    # ФА-политика
    fa = read_fa_policy(symbol)
    last_fa_read = 0.0

    rng_strat, rng_tac = None, None
    last_flush = 0
    last_build_strat = 0.0
    last_build_tac = 0.0

    while b.get("bot_on", True):
        try:
            bank = float(b.get("safety_bank_usdt", CONFIG.SAFETY_BANK_USDT))
            fee_maker = float(b.get("fee_maker", CONFIG.FEE_MAKER))
            fee_taker = float(b.get("fee_taker", CONFIG.FEE_TAKER))

            now = time.time()

            # периодически перечитываем ФА
            if now - last_fa_read > CONFIG.FA_REFRESH_SEC:
                fa = read_fa_policy(symbol)
                last_fa_read = now

                # тихое окно от фунд-бота
                scan_until = pd.to_datetime(fa.get("scan_lock_until"), utc=True) if fa.get("scan_lock_until") else None
                b["fa_scan_lock"] = bool(scan_until and pd.Timestamp.now(tz="UTC") < scan_until)
                b["fa_scan_until_ts"] = float(scan_until.timestamp()) if scan_until is not None else None

            # применяем FA: risk Red -> управляем, но не открываем; Amber -> консервативней
            fa_risk = (fa.get("risk") or "Green").capitalize()
            fa_bias = (fa.get("bias") or "neutral").lower()
            b["fa_risk"] = fa_risk # hook
            b["fa_bias"] = fa_bias # hook
            
            # масштаб доборов/резерв
            dca_scale = max(0.0, min(1.0, float(fa.get("dca_scale") or 1.0)))
            reserve_off = bool(fa.get("reserve_off"))

            need_build_strat = (rng_strat is None) or ((now - last_build_strat > CONFIG.REBUILD_RANGE_EVERY_MIN*60) and (b.get("position") is None))
            need_build_tac   = (rng_tac   is None) or ((now - last_build_tac   > CONFIG.REBUILD_TACTICAL_EVERY_MIN*60) and (b.get("position") is None))
            if need_build_strat or need_build_tac:
                s, t = await build_ranges(symbol)
                if need_build_strat and s:
                    rng_strat = s; last_build_strat = now; b["intro_done"] = False
                    log.info(f"[RANGE-STRAT] lower={fmt(rng_strat['lower'])} upper={fmt(rng_strat['upper'])} width={fmt(rng_strat['width'])}")
                if need_build_tac and t:
                    rng_tac = t; last_build_tac = now; b["intro_done"] = False
                    log.info(f"[RANGE-TAC]   lower={fmt(rng_tac['lower'])} upper={fmt(rng_tac['upper'])} width={fmt(rng_tac['width'])}")

            if not (rng_strat and rng_tac):
                log.error("Range is not available. Cannot proceed.")
                await asyncio.sleep(10); continue

            # 5m данные — ограничим кол-во баров
            bars_needed = max(60, CONFIG.VOL_WIN + CONFIG.ADX_LEN + 20)
            ohlc5_df = await maybe_await(fetch_ohlcv_yf, symbol, CONFIG.TF_ENTRY, bars_needed)
            if ohlc5_df is None or ohlc5_df.empty:
                log.warning("Could not fetch 5m OHLCV data. Skipping this cycle.")
                await asyncio.sleep(2); continue

            fa_ts = b.get("fa_scan_until_ts")
            if fa_ts is not None and time.time() < fa_ts:
                b["scan_paused"] = True
            elif not _is_df_fresh(ohlc5_df, max_age_min=15):
                b["scan_paused"] = True
            else:
                b["scan_paused"] = bool(b.get("fa_scan_lock", False))
            
            manage_only_flag = b.get("scan_paused", False) or (fa_risk == "Red")
            
            pos: Position | None = b.get("position")
            if pos and getattr(pos, "owner_key", None) not in (None, b["owner_key"]):
                await asyncio.sleep(1); continue # На всякий случай не трогаем чужую позицию

            if pos and b.get("fsm_state") == int(FSM.MANAGING) and pos.steps_filled > 0:
                if bool(fa.get("reserve_off")) and pos.reserve_available and not pos.reserve_used:
                    pos.reserve_available = False
                    pos.reserve_margin_usdt = 0.0
                    pos.max_steps = max(pos.steps_filled, pos.ord_levels)

                new_alloc_bank = bank * float(fa.get("dca_scale") or 1.0)
                if new_alloc_bank > 0 and pos.steps_filled < pos.ord_levels:
                    if pos.alloc_bank_planned <= 0 or abs(new_alloc_bank - pos.alloc_bank_planned) / max(pos.alloc_bank_planned, 1e-9) > 0.05:
                        pos.rebalance_tail_margins_excluding_reserve(new_alloc_bank)
                        pos.alloc_bank_planned = new_alloc_bank

            df5 = ohlc5_df.copy()
            df5.columns = ["open","high","low","close","volume"]
            try:
                ind = compute_indicators_5m(df5)
            except (ValueError, IndexError) as e:
                log.warning(f"Indicator calculation failed: {e}. Skipping cycle.")
                await asyncio.sleep(2); continue

            px = float(df5["close"].iloc[-1])

            # Интро-сообщение
            if (not b.get("intro_done")) and (pos is None):
                p30_t = rng_tac["lower"] + 0.30 * rng_tac["width"]
                p70_t = rng_tac["lower"] + 0.70 * rng_tac["width"]
                d_to_long  = max(0.0, px - p30_t)
                d_to_short = max(0.0, p70_t - px)
                pct_to_long  = (d_to_long  / max(px, 1e-9)) * 100
                pct_to_short = (d_to_short / max(px, 1e-9)) * 100
                brk_up, brk_dn = break_levels(rng_strat)
                width_ratio = (rng_tac["width"] / max(rng_strat["width"], 1e-9)) * 100.0
                fa_line = ""
                if fa_risk != "Green" or fa_bias != "neutral":
                    fa_line = f"\nFA: risk=<b>{fa_risk}</b>, bias=<b>{fa_bias}</b>"
                await say(
                    "🎯 Пороги входа (<b>TAC 30/70</b>): LONG ≤ <code>{}</code>, SHORT ≥ <code>{}</code>\n"
                    "📏 Диапазоны:\n"
                    "• STRAT: [{} … {}] w={}\n"
                    "• TAC (3d): [{} … {}] w={} (≈{:.0f}% от STRAT)\n"
                    "🔓 Пробой STRAT: ↑{} | ↓{}\n"
                    "Текущая: {}. До LONG: {} ({:.2f}%), до SHORT: {} ({:.2f}%).{}".format(
                        fmt(p30_t), fmt(p70_t),
                        fmt(rng_strat['lower']), fmt(rng_strat['upper']), fmt(rng_strat['width']),
                        fmt(rng_tac['lower']),   fmt(rng_tac['upper']),   fmt(rng_tac['width']), width_ratio,
                        fmt(brk_up), fmt(brk_dn),
                        fmt(px), fmt(d_to_long), pct_to_long, fmt(d_to_short), pct_to_short,
                        fa_line
                    )
                )
                b["intro_done"] = True

            # Ручное закрытие
            if pos and b.get("force_close"):
                if not pos or b.get("fsm_state") != int(FSM.MANAGING) or pos.steps_filled <= 0:
                    await asyncio.sleep(1); continue
                exit_p = px
                time_min = (time.time()-pos.open_ts)/60.0
                net_usd, net_pct = compute_net_pnl(pos, exit_p, fee_taker, fee_taker)
                await say(
                    "🧰 <b>MANUAL_CLOSE</b>\n"
                    f"Цена выхода: <code>{fmt(exit_p)}</code>\n"
                    f"P&L (net)≈ {net_usd:+.2f} USD ({net_pct:+.2f}%)\n"
                    f"Время в сделке: {time_min:.1f} мин"
                )
                await log_event_safely({
                    "Event_ID": f"MANUAL_CLOSE_{pos.signal_id}", "Signal_ID": pos.signal_id,
                    "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "Pair": symbol, "Side": pos.side, "Event": "MANUAL_CLOSE",
                    "PNL_Realized_USDT": net_usd, "PNL_Realized_Pct": net_pct,
                    "Time_In_Trade_min": time_min,
                    "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                    "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                })
                b["force_close"] = False
                pos.last_sl_notified_price = None
                b["position"] = None
                b["fsm_state"] = int(FSM.IDLE)
                continue

            # Пробой диапазона — заморозка обычных усреднений, включаем режим ретеста
            if pos:
                if not pos or b.get("fsm_state") != int(FSM.MANAGING) or pos.steps_filled <= 0:
                    await asyncio.sleep(1); continue
                brk_up, brk_dn = break_levels(rng_strat)
                on_break = (px >= brk_up) or (px <= brk_dn)
                if on_break and not pos.freeze_ordinary:
                    pos.freeze_ordinary = True
                    o, h, l, c = (float(df5[k].iloc[-1]) for k in ("open","high","low","close"))
                    # «шип»?
                    body = abs(c - o); upper_wick = max(0.0, h - max(o, c)); lower_wick = max(0.0, min(o, c) - l)
                    rng_len = max(1e-12, h - l)
                    pos.spike_flag = False; pos.spike_direction = None; pos.spike_deadline_ts = None; pos.spike_ref_ohlc = None
                    if abs(ind["vol_z"]) >= CONFIG.SPIKE["VOLZ_THR"] and rng_len >= CONFIG.SPIKE["ATR_MULT"] * max(ind["atr5m"],1e-12):
                        if lower_wick >= CONFIG.SPIKE["WICK_RATIO"] * body:
                            pos.spike_flag = True; pos.spike_direction = "down"
                        elif upper_wick >= CONFIG.SPIKE["WICK_RATIO"] * body:
                            pos.spike_flag = True; pos.spike_direction = "up"
                        if pos.spike_flag:
                            pos.spike_deadline_ts = time.time() + CONFIG.SPIKE["RETRACE_WINDOW_SEC"]
                            pos.spike_ref_ohlc = (o, h, l, c)
                    await say("📌 Пробой STRAT — обычные усреднения заморожены. Резерв держим на ретест.")

            # Открытие
            if not pos and (b.get("manual_open") is not None or not manage_only_flag):
                manual = b.pop("manual_open", None)
                if manual:
                    side_cand = manual.get("side")
                else:
                    pos_in = max(0.0, min(1.0, (px - rng_tac["lower"]) / max(rng_tac["width"], 1e-9)))
                    side_cand = "LONG" if pos_in <= 0.30 else ("SHORT" if pos_in >= 0.70 else None)

                # Учитываем FA bias
                if side_cand:
                    if fa_bias == "long-only" and side_cand != "LONG":
                        side_cand = None
                    if fa_bias == "short-only" and side_cand != "SHORT":
                        side_cand = None

                if side_cand:
                    pos = Position(side_cand, signal_id=f"{symbol.replace('/','')} {int(now)}", leverage=CONFIG.LEVERAGE, owner_key=b["owner_key"])
                    
                    ord_levels = max(1, min(5, CONFIG.DCA_LEVELS - 1)) # Default
                    if manual and manual.get("max_steps") is not None:
                        try:
                            # Handle both string and number inputs safely
                            steps_val = int(float(manual["max_steps"]))
                            ord_levels = max(1, min(CONFIG.DCA_LEVELS - 1, steps_val))
                        except (ValueError, TypeError):
                            log.warning(f"Could not parse manual max_steps: {manual['max_steps']}. Using default.")
                    
                    growth = choose_growth(ind, rng_strat, rng_tac)

                    # Amber → консервативнее: меньше уровней и рост ближе к A
                    if fa_risk == "Amber":
                        ord_levels = max(1, ord_levels - 1)
                        growth = min(growth, CONFIG.AUTO_ALLOC["growth_A"])

                    alloc_bank = bank * dca_scale
                    pos.plan_with_reserve(alloc_bank, growth, ord_levels)
                    pos.alloc_bank_planned = alloc_bank
                    if reserve_off:
                        pos.reserve_available = False
                        pos.reserve_margin_usdt = 0.0
                        pos.max_steps = pos.ord_levels
                    
                    pos.ordinary_targets = compute_corridor_targets(entry=px, side=pos.side, rng_strat=rng_strat, rng_tac=rng_tac, tick=tick)

                    margin, _ = pos.add_step(px)
                    pos.rebalance_tail_margins_excluding_reserve(alloc_bank)
                    b["position"] = pos
                    b["fsm_state"] = int(FSM.OPENED)

            if pos:
                # Резервное усреднение при ретесте
                if not pos or b.get("fsm_state") != int(FSM.MANAGING) or pos.steps_filled <= 0:
                    pass
                else:
                    now_ts = time.time()
                    need_retest_spike = (pos.spike_flag) and (pos.spike_deadline_ts and now_ts <= pos.spike_deadline_ts)
                    need_retest_plain = (not pos.spike_flag) and (pos.freeze_ordinary)
                    
                    retrace_hit = False
                    if need_retest_spike and pos.spike_ref_ohlc:
                        o, h, l, c = pos.spike_ref_ohlc
                        if pos.spike_direction == "down":
                            ceiling = max(o, c, h)
                            if ceiling > l + 1e-12:
                                progress = (px - l) / (ceiling - l)
                                retrace_hit = (progress >= CONFIG.SPIKE["RETRACE_FRAC"]) and (pos.side == "LONG")
                        elif pos.spike_direction == "up":
                            floor_ = min(o, c, l)
                            if h > floor_ + 1e-12:
                                progress = (h - px) / (h - floor_)
                                retrace_hit = (progress >= CONFIG.SPIKE["RETRACE_FRAC"]) and (pos.side == "SHORT")

                    need_retest_plain_hit = False
                    if need_retest_plain:
                        band = CONFIG.REENTRY_BAND
                        brk_up, brk_dn = break_levels(rng_strat)
                        reentry_up = brk_up * (1.0 - band)
                        reentry_dn = brk_dn * (1.0 + band)
                        if px <= reentry_up and px >= reentry_dn:
                            need_retest_plain_hit = True

                    if (retrace_hit or need_retest_plain_hit) and pos.reserve_available and (not pos.reserve_used):
                        if pos.last_add_ts is None or (now_ts - pos.last_add_ts) >= CONFIG.ADD_COOLDOWN_SEC:
                            margin, _ = pos.add_reserve_step(px)
                            cum_margin = _pos_total_margin(pos)
                            lots = margin_to_lots(symbol, margin, price=px, leverage=pos.leverage)
                            cum_notional = cum_margin * pos.leverage
                            fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER
                            
                            ml_price = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_paid_est)
                            dist_ml_pct = ml_distance_pct(pos.side, px, ml_price)
                            dist_txt = "N/A" if np.isnan(dist_ml_pct) else f"{dist_ml_pct:.2f}%"
                            ml_arrow = "↓" if pos.side == "LONG" else "↑"
                            
                            brk_up, brk_dn = break_levels(rng_strat)
                            brk_up_pct, brk_dn_pct = break_distance_pcts(px, brk_up, brk_dn)
                            brk_line = f"Пробой: ↑<code>{fmt(brk_up)}</code> ({brk_up_pct:.2f}%) | ↓<code>{fmt(brk_dn)}</code> ({brk_dn_pct:.2f}%)"

                            await say(
                                f"↩️ Ретест — резервный добор ({'шип' if need_retest_spike else 'плавный'})\n"
                                f"Цена: <code>{fmt(px)}</code> | <b>{lots:.2f} lot</b>\n"
                                f"Добор (резерв): <b>{margin:.2f} USD</b> | Депозит (тек): <b>{cum_margin:.2f} USD</b>\n"
                                f"Средняя: <code>{fmt(pos.avg)}</code> | TP: <code>{fmt(pos.tp_price)}</code>\n"
                                f"ML цена (20%): {ml_arrow}<code>{fmt(ml_price) or 'N/A'}</code> ({dist_txt})\n"
                                f"{brk_line}\n"
                                f"{margin_line(pos, bank, px, fees_paid_est)}"
                            )
                            await log_event_safely({
                                "Event_ID": f"RETEST_ADD_{pos.signal_id}_{pos.steps_filled}", "Signal_ID": pos.signal_id,
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol, "Side": pos.side, "Event": "RETEST_ADD",
                                "Step_No": pos.steps_filled, "Step_Margin_USDT": margin,
                                "Entry_Price": px, "Avg_Price": pos.avg,
                                "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                                "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                            })

                # Обычные усреднения (включая OPEN)
                used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
                nxt = next_pct_target(pos)
                is_open_event = b.get("fsm_state") == int(FSM.OPENED) and pos.steps_filled == 1
                is_add_event = b.get("fsm_state") == int(FSM.MANAGING) and (nxt is not None) and (
                    (pos.side == "LONG" and px <= nxt["price"]) or (pos.side == "SHORT" and px >= nxt["price"]) )

                now_ts = time.time()
                allowed_now = is_open_event or (pos.last_add_ts is None) or ((now_ts - pos.last_add_ts) >= CONFIG.ADD_COOLDOWN_SEC)
                if (is_open_event or is_add_event) and (not pos.freeze_ordinary) and (used_ord < pos.ord_levels) and allowed_now:
                    if is_add_event:
                        alloc_bank = bank * dca_scale
                        margin, _ = pos.add_step(px)
                        pos.rebalance_tail_margins_excluding_reserve(alloc_bank)
                    else:
                        margin = pos.step_margins[0]

                    used_ord_after = pos.steps_filled - (1 if pos.reserve_used else 0)
                    used_dca = max(0, used_ord_after - 1)
                    nxt2 = next_pct_target(pos)
                    total_ord = max(0, min(pos.ord_levels, len(pos.ordinary_targets)))
                    remaining = max(0, total_ord - used_dca)
                    next_idx = used_ord_after
                    nxt2_margin = pos.step_margins[next_idx] if next_idx < len(pos.step_margins) else None
                    
                    lots = margin_to_lots(symbol, margin, price=px, leverage=pos.leverage)
                    cum_margin = _pos_total_margin(pos)
                    cum_notional = cum_margin * pos.leverage
                    fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER

                    ml_price = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_paid_est)
                    dist_ml_pct = ml_distance_pct(pos.side, px, ml_price)
                    dist_txt = "N/A" if np.isnan(dist_ml_pct) else f"{dist_ml_pct:.2f}%"
                    ml_arrow = "↓" if pos.side == "LONG" else "↑"
                    
                    nxt2_txt = "N/A" if nxt2 is None else f"{fmt(nxt2['price'])} ({nxt2['label']})"
                    
                    dir_tag = "LONG 🟢" if pos.side == "LONG" else "SHORT 🛑"
                    if is_open_event:
                        header_tag = dir_tag
                    else:
                        idx = max(0, min(len(pos.ordinary_targets)-1, used_ord_after - 1))
                        curr_label = (pos.ordinary_targets[idx]["label"] if 0 <= idx < len(pos.ordinary_targets) else "")
                        header_tag = curr_label

                    nxt2_dep_txt = "N/A"
                    if nxt2_margin and nxt2:
                        nxt_lots = margin_to_lots(symbol, nxt2_margin, price=nxt2['price'], leverage=pos.leverage)
                        nxt2_dep_txt = f"{nxt2_margin:.2f} USD ≈ {nxt_lots:.2f} lot"

                    brk_up, brk_dn = break_levels(rng_strat)
                    brk_up_pct, brk_dn_pct = break_distance_pcts(px, brk_up, brk_dn)
                    brk_line = f"Пробой: ↑<code>{fmt(brk_up)}</code> ({brk_up_pct:.2f}%) | ↓<code>{fmt(brk_dn)}</code> ({brk_dn_pct:.2f}%)"

                    event_type_str = "▶️ OPEN" if is_open_event else f"➕ Усреднение #{used_dca}"
                    await say(
                        f"{event_type_str} [{header_tag}]\n"
                        f"Цена: <code>{fmt(px)}</code> | <b>{lots:.2f} lot</b>\n"
                        f"Добор: <b>{margin:.2f} USD</b> | Депозит (тек): <b>{cum_margin:.2f} USD</b>\n"
                        f"Средняя: <code>{fmt(pos.avg)}</code> | TP: <code>{fmt(pos.tp_price)}</code>\n"
                        f"ML цена (20%): {ml_arrow}<code>{fmt(ml_price) or 'N/A'}</code> ({dist_txt})\n"
                        f"{brk_line}\n"
                        f"След. усреднение: <code>{nxt2_txt}</code>\n"
                        f"Плановый добор: <b>{nxt2_dep_txt}</b> (осталось: {remaining} из {total_ord})\n"
                        f"{margin_line(pos, bank, px, fees_paid_est)}"
                    )
                    await log_event_safely({
                        "Event_ID": f"{'OPEN' if is_open_event else 'ADD'}_{pos.signal_id}_{pos.steps_filled}", "Signal_ID": pos.signal_id,
                        "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "Pair": symbol, "Side": pos.side, "Event": "OPEN" if is_open_event else "ADD",
                        "Step_No": pos.steps_filled, "Step_Margin_USDT": margin,
                        "Cum_Margin_USDT": cum_margin, "Entry_Price": px, "Avg_Price": pos.avg,
                        "TP_Price": pos.tp_price, "SL_Price": pos.sl_price or "",
                        "Liq_Est_Price": ml_price, "Next_DCA_Price": (nxt2 and nxt2["price"]) or "",
                        "Next_DCA_Label": (nxt2 and nxt2["label"]) or "",
                        "Triggered_Label": header_tag if not is_open_event else "OPEN",
                        "Fee_Rate_Maker": fee_maker, "Fee_Rate_Taker": fee_taker,
                        "Fee_Est_USDT": (cum_notional * fee_taker), "ATR_5m": ind["atr5m"], "ATR_1h": rng_strat["atr1h"],
                        "RSI_5m": ind["rsi"], "ADX_5m": ind["adx"], "Supertrend": ind["supertrend"], "Vol_z": ind["vol_z"],
                        "Range_Lower": rng_strat["lower"], "Range_Upper": rng_strat["upper"], "Range_Width": rng_strat["width"],
                        "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                        "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                    })
                    if is_open_event:
                        b["fsm_state"] = int(FSM.MANAGING) # Переключаем после первого сообщения

                # Трейл-стоп
                if not pos or b.get("fsm_state") != int(FSM.MANAGING) or pos.steps_filled <= 0:
                    pass
                else:
                    if pos.side == "LONG": gain_to_tp = max(0.0, (px / max(pos.avg, 1e-9) - 1.0) / CONFIG.TP_PCT)
                    else: gain_to_tp = max(0.0, (pos.avg / max(px, 1e-9) - 1.0) / CONFIG.TP_PCT)
                    for stage_idx, (arm, lock) in enumerate(CONFIG.TRAILING_STAGES):
                        if pos.trail_stage >= stage_idx: continue
                        if gain_to_tp < arm: break
                        lock_pct = lock * CONFIG.TP_PCT
                        locked = pos.avg * (1 + lock_pct) if pos.side == "LONG" else pos.avg * (1 - lock_pct)
                        chand = chandelier_stop(pos.side, px, ind["atr5m"])
                        new_sl = max(locked, chand) if pos.side == "LONG" else min(locked, chand)

                        t = b.get("price_tick", 1e-4)
                        new_sl_q  = quantize_to_tick(new_sl, t)
                        curr_sl_q = quantize_to_tick(pos.sl_price, t)
                        last_notif_q = quantize_to_tick(pos.last_sl_notified_price, t)

                        improves = (curr_sl_q is None) or \
                                   (pos.side == "LONG" and new_sl_q > curr_sl_q) or \
                                   (pos.side == "SHORT" and new_sl_q < curr_sl_q)
                        if improves:
                            pos.sl_price = new_sl_q
                            pos.trail_stage = stage_idx
                            if (last_notif_q is None) or \
                               (pos.side == "LONG" and new_sl_q > (last_notif_q + t)) or \
                               (pos.side == "SHORT" and new_sl_q < (last_notif_q - t)):
                                await say(f"🛡️ Трейлинг-SL (стадия {stage_idx+1}) → <code>{fmt(pos.sl_price)}</code>")
                                pos.last_sl_notified_price = pos.sl_price
                                await log_event_safely({
                                    "Event_ID": f"TRAIL_SET_{pos.signal_id}_{int(now)}", "Signal_ID": pos.signal_id,
                                    "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                    "Pair": symbol, "Side": pos.side, "Event": "TRAIL_SET",
                                    "SL_Price": pos.sl_price, "Avg_Price": pos.avg, "Trail_Stage": stage_idx + 1,
                                    "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                                    "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                                })

                # TP / SL выход
                if not pos or b.get("fsm_state") == int(FSM.IDLE) or pos.steps_filled <= 0:
                    pass
                else:
                    tp_hit = (pos.side == "LONG" and px >= pos.tp_price) or (pos.side == "SHORT" and px <= pos.tp_price)
                    sl_hit = pos.sl_price and (
                        (pos.side == "LONG" and px <= pos.sl_price) or (pos.side == "SHORT" and px >= pos.sl_price)
                    )
                    if tp_hit or sl_hit:
                        reason = "TP_HIT" if tp_hit else "SL_HIT"
                        exit_p = pos.tp_price if tp_hit else pos.sl_price
                        time_min = (time.time() - pos.open_ts) / 60.0
                        net_usd, net_pct = compute_net_pnl(pos, exit_p, fee_taker, fee_taker)
                        atr_now = ind["atr5m"]
                        await say(
                            f"{'✅' if net_usd > 0 else '❌'} <b>{reason}</b>\n"
                            f"Цена выхода: <code>{fmt(exit_p)}</code>\n"
                            f"P&L (net)≈ {net_usd:+.2f} USD ({net_pct:+.2f}%)\n"
                            f"ATR(5m): {atr_now:.6f}\n"
                            f"Время в сделке: {time_min:.1f} мин"
                        )
                        await log_event_safely({
                            "Event_ID": f"{reason}_{pos.signal_id}", "Signal_ID": pos.signal_id,
                            "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            "Pair": symbol, "Side": pos.side, "Event": reason,
                            "PNL_Realized_USDT": net_usd, "PNL_Realized_Pct": net_pct,
                            "Time_In_Trade_min": time_min, "ATR_5m": atr_now,
                            "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                            "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                        })
                        pos.last_sl_notified_price = None
                        b["position"] = None
                        b["fsm_state"] = int(FSM.IDLE)

            # Периодический флэш логов в Sheets
            if (time.time() - last_flush) >= 10:
                try:
                    await maybe_await(trade_executor.flush_log_buffers)
                except Exception:
                    log.exception("flush_log_buffers failed")
                last_flush = time.time()

            await asyncio.sleep(CONFIG.SCAN_INTERVAL_SEC)
        except Exception:
            log.exception("BMR-DCA loop error")
            await asyncio.sleep(5)

    log.info("BMR-DCA loop gracefully stopped.")
