# scanner_bmr_dca.py — patched, full
# - безопасная нормализация символа (str|dict|list)
# - автосоздание листов BMR_DCA_<SYMBOL> и дублирование логов туда
# - интеграция ФА-бота (risk/bias/ttl/updated_at) + периодическое обновление
# - совместимость с fetch_ohlcv(symbol, tf, limit) из fx_feed

from __future__ import annotations

import asyncio, time, logging, json, os, inspect, numbers
from typing import Optional
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pandas_ta as ta
from telegram.ext import Application
import gspread

# === Forex адаптеры и фид ===
from fx_mt5_adapter import FX, margin_to_lots, default_tick
from fx_feed import fetch_ohlcv as fetch_ohlcv_yf

import trade_executor

log = logging.getLogger("bmr_dca_engine")

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
    STRATEGIC_LOOKBACK_DAYS = 60   # для TF_RANGE
    TACTICAL_LOOKBACK_DAYS  = 3    # для TF_RANGE

    # Таймауты/интервалы
    FETCH_TIMEOUT = 25
    SCAN_INTERVAL_SEC = 3
    REBUILD_RANGE_EVERY_MIN   = 15
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
    CUM_DEPOSIT_FRAC_AT_FULL = 2/3
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

    # Для расчёта целевых цен усреднений
    TACTICAL_PCTS = [0.25, 0.50, 0.75]
    STRATEGIC_PCTS = [0.33, 0.66, 1.00]

# ENV-переопределения
CONFIG.SYMBOL   = os.getenv("FX_SYMBOL", CONFIG.SYMBOL)
CONFIG.TF_ENTRY = os.getenv("TF_ENTRY", CONFIG.TF_ENTRY)
CONFIG.TF_RANGE = os.getenv("TF_TREND", CONFIG.TF_RANGE)

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
        ws2 = _ensure_ws(sh, f"BMR_DCA_{sym}", list(SAFE_LOG_KEYS))
        row = [_clean(payload.get(k)) for k in SAFE_LOG_KEYS]
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
                        if pd.Timestamp.utcnow() > ts + pd.Timedelta(minutes=ttl):
                            return {}
                    except Exception:
                        pass
                return {"risk": risk, "bias": bias, "ttl": ttl, "updated_at": updated_at}
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

def plan_margins_bank_first(bank: float, levels: int, growth: float) -> list[float]:
    if levels <= 0 or bank <= 0: return []
    if abs(growth - 1.0) < 1e-9:
        per = bank / levels
        return [per] * levels
    base = bank * (growth - 1.0) / (growth**levels - 1.0)
    return [base * (growth**i) for i in range(levels)]

def _pos_total_margin(pos):
    ord_used = sum(pos.step_margins[:min(pos.steps_filled, getattr(pos, 'ord_levels', pos.steps_filled))])
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

def approx_liq_price_cross(avg: float, side: str, qty: float, equity: float, mmr: float, fees_paid: float = 0.0) -> float:
    if qty <= 0 or equity <= 0: return float('nan')
    eq = max(0.0, equity - fees_paid)
    if side == "LONG":
        denom = max(qty * (1.0 - mmr), 1e-12)
        return (avg * qty - eq) / denom
    else:
        denom = max(qty * (1.0 + mmr), 1e-12)
        return (avg * qty + eq) / denom

def liq_distance_pct(side: str, px: float, liq: float) -> float:
    if px is None or liq is None or px <= 0 or np.isnan(liq): return float('nan')
    return (liq / px - 1.0) * 100 if side == "SHORT" else (1.0 - liq / px) * 100

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

def compute_pct_targets(entry: float, side: str, rng: dict, tick: float, pcts: list[float]) -> list[float]:
    if side == "SHORT":
        cap = rng["upper"]
        path = max(0.0, cap - entry)
        raw = [entry + path * p for p in pcts]
        brk_up, _ = break_levels(rng)
        buf = max(tick, 0.05 * max(rng.get("atr1h", 0.0), 1e-9))
        capped = [min(x, brk_up - buf) for x in raw]
    else:
        cap = rng["lower"]
        path = max(0.0, entry - cap)
        raw = [entry - path * p for p in pcts]
        _, brk_dn = break_levels(rng)
        buf = max(tick, 0.05 * max(rng.get("atr1h", 0.0), 1e-9))
        capped = [max(x, brk_dn + buf) for x in raw]
    out = []
    for x in capped:
        q = quantize_to_tick(x, tick)
        if q is not None and (not out or (side=="SHORT" and q > out[-1] + tick) or (side=="LONG" and q < out[-1] - tick)):
            out.append(q)
    return out

def compute_pct_targets_labeled(entry, side, rng, tick, pcts, label):
    prices = compute_pct_targets(entry, side, rng, tick, pcts)
    out = []
    for i, pr in enumerate(prices):
        pct = int(round(pcts[min(i, len(pcts)-1)] * 100))
        out.append({"price": pr, "label": f"{label} {pct}%"})
    return out

def merge_targets_sorted(side: str, tick: float, targets: list[dict]) -> list[dict]:
    if side == "SHORT":
        targets = sorted(targets, key=lambda t: t["price"])
    else:
        targets = sorted(targets, key=lambda t: t["price"], reverse=True)
    dedup = []
    for t in targets:
        if not dedup: dedup.append(t)
        else:
            if (side=="SHORT" and t["price"] > dedup[-1]["price"] + tick) or \
               (side=="LONG"  and t["price"] < dedup[-1]["price"] - tick):
                dedup.append(t)
    return dedup

def compute_mixed_targets(entry: float, side: str, rng_strat: dict, rng_tac: dict, tick: float) -> list[dict]:
    tacs = compute_pct_targets_labeled(entry, side, rng_tac,   tick, CONFIG.TACTICAL_PCTS,  "TAC")
    strs = compute_pct_targets_labeled(entry, side, rng_strat, tick, CONFIG.STRATEGIC_PCTS, "STRAT")
    return merge_targets_sorted(side, tick, tacs + strs)

def next_pct_target(pos):
    idx = pos.steps_filled - 1
    if not getattr(pos, "ordinary_targets", None): return None
    return pos.ordinary_targets[idx] if 0 <= idx < len(pos.ordinary_targets) else None

def choose_growth(ind: dict, rng_strat: dict, rng_tac: dict) -> float:
    try:
        width_ratio = (rng_tac["width"] / max(rng_strat["width"], 1e-9))
    except Exception:
        width_ratio = 1.0
    thin = width_ratio <= CONFIG.AUTO_ALLOC["thin_tac_vs_strat"]
    low_vol = abs(ind.get("vol_z", 0.0)) <= CONFIG.AUTO_ALLOC["low_vol_z"]
    return CONFIG.AUTO_ALLOC["growth_A"] if (thin and low_vol) else CONFIG.AUTO_ALLOC["growth_B"]

# ---------------------------------------------------------------------------
# Диапазоны/индикаторы
# ---------------------------------------------------------------------------
async def build_range_from_df(df: Optional[pd.DataFrame]):
    if df is None or df.empty: return None
    df.columns = ["open","high","low","close","volume"]
    ema = ta.ema(df["close"], length=50)
    atr = ta.atr(df["high"], df["low"], df["close"], length=14)
    lower = float(np.quantile(df["close"].dropna(), CONFIG.Q_LOWER))
    upper = float(np.quantile(df["close"].dropna(), CONFIG.Q_UPPER))
    if pd.notna(ema.iloc[-1]) and pd.notna(atr.iloc[-1]):
        mid = float(ema.iloc[-1])
        atr1h = float(atr.iloc[-1])
        lower = min(lower, mid - CONFIG.RANGE_MIN_ATR_MULT*atr1h)
        upper = max(upper, mid + CONFIG.RANGE_MIN_ATR_MULT*atr1h)
    else:
        atr1h = 0.0
        mid = float(df["close"].iloc[-1])
    return {"lower": lower, "upper": upper, "mid": mid, "atr1h": atr1h, "width": upper-lower}

async def build_ranges(symbol: str):
    # В нашем fx_feed: fetch_ohlcv(symbol, tf, limit)
    s_limit = CONFIG.STRATEGIC_LOOKBACK_DAYS * 24  # бары 1h
    t_limit = CONFIG.TACTICAL_LOOKBACK_DAYS  * 24
    s_df_task = asyncio.create_task(maybe_await(fetch_ohlcv_yf, symbol, CONFIG.TF_RANGE, s_limit))
    t_df_task = asyncio.create_task(maybe_await(fetch_ohlcv_yf, symbol, CONFIG.TF_RANGE, t_limit))
    s_df, t_df = await asyncio.gather(s_df_task, t_df_task)
    s_task = asyncio.create_task(build_range_from_df(s_df))
    t_task = asyncio.create_task(build_range_from_df(t_df))
    strat, tac = await asyncio.gather(s_task, t_task)
    return strat, tac

def compute_indicators_5m(df: pd.DataFrame) -> dict:
    atr5m = ta.atr(df["high"], df["low"], df["close"], length=14).iloc[-1]
    rsi = ta.rsi(df["close"], length=CONFIG.RSI_LEN).iloc[-1]
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=CONFIG.ADX_LEN)
    adx_cols = adx_df.filter(like=f"ADX_{CONFIG.ADX_LEN}").columns
    if len(adx_cols) > 0:
        adx = adx_df[adx_cols[0]].iloc[-1]
    else:
        raise ValueError(f"Could not find ADX column for length {CONFIG.ADX_LEN}")

    ema20 = ta.ema(df["close"], length=20).iloc[-1]
    vol_z = (df["volume"].iloc[-1] - df["volume"].rolling(CONFIG.VOL_WIN).mean().iloc[-1]) / \
            max(df["volume"].rolling(CONFIG.VOL_WIN).std().iloc[-1], 1e-9)
    st = ta.supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3.0)
    d_col = next((c for c in st.columns if c.startswith("SUPERTd_")), None)
    if d_col is None:
        raise ValueError("Supertrend direction column not found")

    dir_now  = int(st[d_col].iloc[-1])
    dir_prev = int(st[d_col].iloc[-2])
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

# ---------------------------------------------------------------------------
# Состояние позиции
# ---------------------------------------------------------------------------
class Position:
    def __init__(self, side: str, signal_id: str, leverage: int | None = None):
        self.side = side
        self.signal_id = signal_id
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
        used_ord = sum(self.step_margins[:self.steps_filled]) if self.step_margins else 0.0
        remaining_budget_for_ord = max(0.0, total_target - used_ord - self.reserve_margin_usdt)
        remaining_levels = max(0, self.ord_levels - self.steps_filled)
        if remaining_levels <= 0:
            return
        tail = plan_margins_bank_first(remaining_budget_for_ord, remaining_levels, self.growth)
        self.step_margins = (self.step_margins[:self.steps_filled] or []) + tail
        self.max_steps = self.steps_filled + remaining_levels + (1 if (self.reserve_available and not self.reserve_used) else 0)

    def add_step(self, price: float):
        margin = self.step_margins[self.steps_filled]
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
    b = botbox if botbox is not None else app.bot_data
    b.setdefault("position", None)

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
    symbol = _norm_symbol(symbol_override or CONFIG.SYMBOL)
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
            _ensure_ws(sh, f"BMR_DCA_{symbol}", list(SAFE_LOG_KEYS))
    except Exception:
        log.exception("ensure symbol sheet failed")

    # Локальный хэлпер на рассылку
    async def say(txt: str):
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

            # применяем FA: risk Red -> управляем, но не открываем; Amber -> консервативней
            fa_risk = (fa.get("risk") or "Green").capitalize()
            fa_bias = (fa.get("bias") or "neutral").lower()

            manage_only_flag = b.get("scan_paused", False) or (fa_risk == "Red")

            pos: Position | None = b.get("position")

            need_build_strat = (rng_strat is None) or ((now - last_build_strat > CONFIG.REBUILD_RANGE_EVERY_MIN*60) and (pos is None))
            need_build_tac   = (rng_tac   is None) or ((now - last_build_tac   > CONFIG.REBUILD_TACTICAL_EVERY_MIN*60) and (pos is None))
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

            pos = b.get("position")

            # Ручное закрытие
            if pos and b.get("force_close"):
                exit_p = px
                time_min = (time.time()-pos.open_ts)/60.0
                net_usd, net_pct = compute_net_pnl(pos, exit_p, fee_taker, fee_maker)
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
                    "Time_In_Trade_min": time_min
                })
                b["force_close"] = False
                pos.last_sl_notified_price = None
                b["position"] = None
                continue

            # Пробой диапазона — заморозка обычных усреднений, включаем режим ретеста
            if pos:
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
                    pos = Position(side_cand, signal_id=f"{symbol.replace('/','')} {int(now)}")
                    pos.leverage = max(CONFIG.MIN_LEVERAGE, min(CONFIG.MAX_LEVERAGE, int(manual["leverage"]))) if (manual and manual.get("leverage") is not None) else CONFIG.LEVERAGE
                    ord_levels  = max(1, min(CONFIG.DCA_LEVELS - 1, int(manual["max_steps"]))) if (manual and manual.get("max_steps") is not None) else max(1, min(5, CONFIG.DCA_LEVELS - 1))
                    growth = choose_growth(ind, rng_strat, rng_tac)

                    # Amber → консервативнее: меньше уровней и рост ближе к A
                    if fa_risk == "Amber":
                        ord_levels = max(1, ord_levels - 1)
                        growth = min(growth, CONFIG.AUTO_ALLOC["growth_A"])

                    pos.plan_with_reserve(bank, growth, ord_levels)
                    pos.ordinary_targets = compute_mixed_targets(entry=px, side=pos.side, rng_strat=rng_strat, rng_tac=rng_tac, tick=tick)

                    margin, _ = pos.add_step(px)
                    pos.rebalance_tail_margins_excluding_reserve(bank)
                    b["position"] = pos

                    cum_margin = _pos_total_margin(pos)
                    lots = margin_to_lots(symbol, margin, price=px, leverage=pos.leverage)

                    cum_notional = cum_margin * pos.leverage
                    fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER
                    liq = approx_liq_price_cross(avg=pos.avg, side=pos.side, qty=pos.qty,
                                                 equity=bank, mmr=CONFIG.MAINT_MMR, fees_paid=fees_paid_est)
                    if not np.isfinite(liq) or liq <= 0: liq = None
                    dist_to_liq_pct = liq_distance_pct(pos.side, px, liq)
                    dist_txt = "N/A" if np.isnan(dist_to_liq_pct) else f"{dist_to_liq_pct:.2f}%"
                    liq_arrow = "↓" if pos.side == "LONG" else "↑"

                    nxt = next_pct_target(pos)
                    nxt_txt = "N/A" if nxt is None else f"{fmt(nxt['price'])} ({nxt['label']})"

                    total_ord = max(0, min(pos.ord_levels - 1, len(pos.ordinary_targets)))
                    used_ord = max(0, min(total_ord, pos.steps_filled - 1 - (1 if pos.reserve_used else 0)))
                    remaining = max(0, total_ord - used_ord)

                    nxt_margin = pos.step_margins[pos.steps_filled] if pos.steps_filled < pos.ord_levels else None
                    if nxt_margin:
                        nxt_lots = margin_to_lots(symbol, nxt_margin, price=px, leverage=pos.leverage)
                        nxt_dep_txt = f"{nxt_margin:.2f} USD ≈ {nxt_lots:.2f} lot"
                    else:
                        nxt_dep_txt = "N/A"

                    brk_up, brk_dn = break_levels(rng_strat)
                    brk_up_pct, brk_dn_pct = break_distance_pcts(px, brk_up, brk_dn)
                    brk_line = (f"Пробой: ↑<code>{fmt(brk_up)}</code> ({brk_up_pct:.2f}%) | "
                                f"↓<code>{fmt(brk_dn)}</code> ({brk_dn_pct:.2f}%)")

                    hdr = f"BMR-DCA {pos.side} ({symbol})" + (" [MANUAL]" if manual else "")
                    fa_tag = ""
                    if fa_risk == "Amber": fa_tag = "\n<i>FA: Amber — используем консервативный план.</i>"
                    await say(
                        f"⚡ <b>{hdr}</b>\n"
                        f"Вход: <code>{fmt(px)}</code> | <b>{lots:.2f} lot</b>\n"
                        f"Депозит (старт): <b>{cum_margin:.2f} USD</b> | Плечо: <b>{pos.leverage}x</b>\n"
                        f"TP: <code>{fmt(pos.tp_price)}</code> (+{CONFIG.TP_PCT*100:.2f}%)\n"
                        f"Ликвидация (est): {liq_arrow}<code>{fmt(liq)}</code> ({dist_txt})\n"
                        f"{brk_line}\n"
                        f"След. усреднение: <code>{nxt_txt}</code>\n"
                        f"Плановый добор: <b>{nxt_dep_txt}</b> (осталось: {remaining} из {total_ord})\n"
                        f"<i>Контролируй Margin level ≥ 20%</i>" + fa_tag
                    )
                    await log_event_safely({
                        "Event_ID": f"OPEN_{pos.signal_id}", "Signal_ID": pos.signal_id, "Leverage": pos.leverage,
                        "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "Pair": symbol, "Side": pos.side, "Event": "OPEN",
                        "Step_No": pos.steps_filled, "Step_Margin_USDT": margin,
                        "Cum_Margin_USDT": cum_margin, "Entry_Price": px, "Avg_Price": pos.avg,
                        "TP_Pct": CONFIG.TP_PCT, "TP_Price": pos.tp_price, "Liq_Est_Price": liq,
                        "Next_DCA_Price": (nxt and nxt["price"]) or "", "Next_DCA_Label": (nxt and nxt["label"]) or "",
                        "Triggered_Label": ("MANUAL" if manual else ""),
                        "Fee_Rate_Maker": fee_maker, "Fee_Rate_Taker": fee_taker,
                        "Fee_Est_USDT": (cum_notional * fee_taker), "ATR_5m": ind["atr5m"], "ATR_1h": rng_strat["atr1h"],
                        "RSI_5m": ind["rsi"], "ADX_5m": ind["adx"], "Supertrend": ind["supertrend"], "Vol_z": ind["vol_z"],
                        "Range_Lower": rng_strat["lower"], "Range_Upper": rng_strat["upper"], "Range_Width": rng_strat["width"]
                    })

            # Доборы
            pos = b.get("position")
            if pos:
                # Ретест по пробою
                if pos.freeze_ordinary and pos.reserve_available and not manage_only_flag:
                    need_retest_slow = (
                        ((pos.side == "SHORT" and px <= rng_strat["upper"] * (1 - CONFIG.REENTRY_BAND)) or
                         (pos.side == "LONG"  and px >= rng_strat["lower"] * (1 + CONFIG.REENTRY_BAND)))
                    )
                    need_retest_spike = False
                    if pos.spike_flag and (pos.spike_deadline_ts is None or time.time() <= pos.spike_deadline_ts):
                        if pos.spike_ref_ohlc is not None:
                            o, h, l, c = pos.spike_ref_ohlc
                            if pos.spike_direction == "down":
                                ceiling = max(o, c, h)
                                if ceiling > l + 1e-12:
                                    progress = (px - l) / (ceiling - l)
                                    need_retest_spike = (progress >= CONFIG.SPIKE["RETRACE_FRAC"]) and (pos.side == "LONG")
                            elif pos.spike_direction == "up":
                                floor_ = min(o, c, l)
                                if h > floor_ + 1e-12:
                                    progress = (h - px) / (h - floor_)
                                    need_retest_spike = (progress >= CONFIG.SPIKE["RETRACE_FRAC"]) and (pos.side == "SHORT")
                    if need_retest_slow or need_retest_spike:
                        now_ts = time.time()
                        if pos.last_add_ts is None or (now_ts - pos.last_add_ts) >= CONFIG.ADD_COOLDOWN_SEC:
                            margin, _ = pos.add_reserve_step(px)
                            pos.spike_flag = False; pos.spike_deadline_ts = None; pos.spike_ref_ohlc = None

                            lots = margin_to_lots(symbol, margin, price=px, leverage=pos.leverage)
                            cum_margin = _pos_total_margin(pos)
                            cum_notional = cum_margin * pos.leverage
                            fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER
                            liq = approx_liq_price_cross(
                                avg=pos.avg, side=pos.side, qty=pos.qty,
                                equity=bank, mmr=CONFIG.MAINT_MMR, fees_paid=fees_paid_est
                            )
                            if not np.isfinite(liq) or liq <= 0:
                                liq = None
                            dist_to_liq_pct = liq_distance_pct(pos.side, px, liq)
                            dist_txt = "N/A" if np.isnan(dist_to_liq_pct) else f"{dist_to_liq_pct:.2f}%"
                            liq_arrow = "↓" if pos.side == "LONG" else "↑"

                            brk_up, brk_dn = break_levels(rng_strat)
                            brk_up_pct, brk_dn_pct = break_distance_pcts(px, brk_up, brk_dn)
                            brk_line = (f"Пробой: ↑<code>{fmt(brk_up)}</code> ({brk_up_pct:.2f}%) | "
                                        f"↓<code>{fmt(brk_dn)}</code> ({brk_dn_pct:.2f}%)")

                            await say(
                                "↩️ Ретест — резервный добор{}\n"
                                "Цена: <code>{}</code> | <b>{:.2f} lot</b>\n"
                                "Добор (резерв): <b>{:.2f} USD</b> | Депозит (тек): <b>{:.2f} USD</b>\n"
                                "Средняя: <code>{}</code> | TP: <code>{}</code>\n"
                                "Ликвидация (est): {}<code>{}</code> ({})\n"
                                "{}\n"
                                "<i>Контролируй Margin level ≥ 20%</i>".format(
                                    " (шип)" if need_retest_spike else "",
                                    fmt(px), lots, margin, cum_margin,
                                    fmt(pos.avg), fmt(pos.tp_price),
                                    liq_arrow, fmt(liq), dist_txt,
                                    brk_line
                                )
                            )
                            await log_event_safely({
                                "Event_ID": f"RETEST_ADD_{pos.signal_id}_{pos.steps_filled}",
                                "Signal_ID": pos.signal_id,
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol, "Side": pos.side, "Event": "RETEST_ADD",
                                "Step_No": pos.steps_filled, "Step_Margin_USDT": margin,
                                "Entry_Price": px, "Avg_Price": pos.avg
                            })

                # Обычные усреднения
                nxt = next_pct_target(pos)
                trigger = (nxt is not None) and ((pos.side=="LONG" and px <= nxt["price"]) or (pos.side=="SHORT" and px >= nxt["price"]))
                if (not pos.freeze_ordinary) and trigger and (pos.steps_filled < pos.ord_levels):
                    now_ts = time.time()
                    if pos.last_add_ts is None or (now_ts - pos.last_add_ts) >= CONFIG.ADD_COOLDOWN_SEC:
                        margin, _ = pos.add_step(px)
                        pos.rebalance_tail_margins_excluding_reserve(bank)

                        lots = margin_to_lots(symbol, margin, price=px, leverage=pos.leverage)
                        cum_margin = _pos_total_margin(pos)
                        cum_notional = cum_margin * pos.leverage
                        fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER
                        liq = approx_liq_price_cross(avg=pos.avg, side=pos.side, qty=pos.qty,
                                                     equity=bank, mmr=CONFIG.MAINT_MMR, fees_paid=fees_paid_est)
                        if not np.isfinite(liq) or liq <= 0: liq = None
                        dist_to_liq_pct = liq_distance_pct(pos.side, px, liq)
                        dist_txt = "N/A" if np.isnan(dist_to_liq_pct) else f"{dist_to_liq_pct:.2f}%"
                        liq_arrow = "↓" if pos.side == "LONG" else "↑"

                        nxt2 = next_pct_target(pos)
                        nxt2_txt = "N/A" if nxt2 is None else f"{fmt(nxt2['price'])} ({nxt2['label']})"

                        total_ord = max(0, min(pos.ord_levels - 1, len(pos.ordinary_targets)))
                        used_ord = max(0, min(total_ord, pos.steps_filled - 1 - (1 if pos.reserve_used else 0)))
                        remaining = max(0, total_ord - used_ord)
                        curr_label = pos.ordinary_targets[used_ord - 1]["label"] if used_ord >= 1 else ""

                        next_idx = used_ord + 1
                        nxt2_margin = pos.step_margins[next_idx] if next_idx < pos.ord_levels else None
                        if nxt2_margin:
                            nxt_lots = margin_to_lots(symbol, nxt2_margin, price=px, leverage=pos.leverage)
                            nxt2_dep_txt = f"{nxt2_margin:.2f} USD ≈ {nxt_lots:.2f} lot"
                        else:
                            nxt2_dep_txt = "N/A"

                        brk_up, brk_dn = break_levels(rng_strat)
                        brk_up_pct, brk_dn_pct = break_distance_pcts(px, brk_up, brk_dn)
                        brk_line = (f"Пробой: ↑<code>{fmt(brk_up)}</code> ({brk_up_pct:.2f}%) | "
                                    f"↓<code>{fmt(brk_dn)}</code> ({brk_dn_pct:.2f}%)")

                        await say(
                            f"➕ Усреднение #{pos.steps_filled} [{curr_label}]\n"
                            f"Цена: <code>{fmt(px)}</code> | <b>{lots:.2f} lot</b>\n"
                            f"Добор: <b>{margin:.2f} USD</b> | Депозит (тек): <b>{cum_margin:.2f} USD</b>\n"
                            f"Средняя: <code>{fmt(pos.avg)}</code> | TP: <code>{fmt(pos.tp_price)}</code>\n"
                            f"Ликвидация (est): {liq_arrow}<code>{fmt(liq)}</code> ({dist_txt})\n"
                            f"{brk_line}\n"
                            f"След. усреднение: <code>{nxt2_txt}</code>\n"
                            f"Плановый добор: <b>{nxt2_dep_txt}</b> (осталось: {remaining} из {total_ord})\n"
                            f"<i>Контролируй Margin level ≥ 20%</i>"
                        )
                        await log_event_safely({
                            "Event_ID": f"ADD_{pos.signal_id}_{pos.steps_filled}", "Signal_ID": pos.signal_id,
                            "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            "Pair": symbol, "Side": pos.side, "Event": "ADD",
                            "Step_No": pos.steps_filled, "Step_Margin_USDT": margin,
                            "Cum_Margin_USDT": cum_margin, "Entry_Price": px, "Avg_Price": pos.avg,
                            "TP_Price": pos.tp_price, "SL_Price": pos.sl_price or "",
                            "Liq_Est_Price": liq, "Next_DCA_Price": (nxt2 and nxt2["price"]) or "", "Next_DCA_Label": (nxt2 and nxt2["label"]) or "", "Triggered_Label": curr_label,
                            "Fee_Rate_Maker": fee_maker, "Fee_Rate_Taker": fee_taker,
                            "Fee_Est_USDT": (cum_notional * fee_taker), "ATR_5m": ind["atr5m"], "ATR_1h": rng_strat["atr1h"],
                            "RSI_5m": ind["rsi"], "ADX_5m": ind["adx"], "Supertrend": ind["supertrend"], "Vol_z": ind["vol_z"],
                            "Range_Lower": rng_strat["lower"], "Range_Upper": rng_strat["upper"], "Range_Width": rng_strat["width"]
                        })

                # Трейл
                if pos.side == "LONG": gain_to_tp = max(0.0, (px / max(pos.avg,1e-9) - 1.0) / CONFIG.TP_PCT)
                else:                   gain_to_tp = max(0.0, (pos.avg / max(px,1e-9) - 1.0) / CONFIG.TP_PCT)

                for stage_idx, (arm, lock) in enumerate(CONFIG.TRAILING_STAGES):
                    if pos.trail_stage >= stage_idx: continue
                    if gain_to_tp < arm: break
                    lock_pct = lock * CONFIG.TP_PCT
                    locked = pos.avg*(1+lock_pct) if pos.side=="LONG" else pos.avg*(1-lock_pct)
                    chand = chandelier_stop(pos.side, px, ind["atr5m"])
                    new_sl = max(locked, chand) if pos.side=="LONG" else min(locked, chand)

                    t = b.get("price_tick", 1e-4)
                    new_sl_q  = quantize_to_tick(new_sl, t)
                    curr_sl_q = quantize_to_tick(pos.sl_price, t)
                    last_notif_q = quantize_to_tick(pos.last_sl_notified_price, t)
                    improves = (curr_sl_q is None) or \
                               (pos.side == "LONG"  and new_sl_q > curr_sl_q) or \
                               (pos.side == "SHORT" and new_sl_q < curr_sl_q)
                    if improves:
                        pos.sl_price = new_sl_q
                        pos.trail_stage = stage_idx
                        if (last_notif_q is None) or (pos.side=="LONG" and new_sl_q > last_notif_q + t) or (pos.side=="SHORT" and new_sl_q < last_notif_q - t):
                            await say(f"🛡️ Трейлинг-SL (стадия {stage_idx+1}) → <code>{fmt(pos.sl_price)}</code>")
                            pos.last_sl_notified_price = pos.sl_price
                            await log_event_safely({
                                "Event_ID": f"TRAIL_SET_{pos.signal_id}_{int(now)}", "Signal_ID": pos.signal_id,
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol, "Side": pos.side, "Event": "TRAIL_SET",
                                "SL_Price": pos.sl_price, "Avg_Price": pos.avg, "Trail_Stage": stage_idx+1
                            })

                # TP/SL выход
                tp_hit = (pos.side=="LONG" and px>=pos.tp_price) or (pos.side=="SHORT" and px<=pos.tp_price)
                sl_hit = pos.sl_price and ((pos.side=="LONG" and px<=pos.sl_price) or (pos.side=="SHORT" and px>=pos.sl_price))
                if tp_hit or sl_hit:
                    reason = "TP_HIT" if tp_hit else "SL_HIT"
                    exit_p = pos.tp_price if tp_hit else pos.sl_price
                    time_min = (time.time()-pos.open_ts)/60.0
                    net_usd, net_pct = compute_net_pnl(pos, exit_p, fee_taker, fee_maker)
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
                        "Time_In_Trade_min": time_min, "ATR_5m": atr_now
                    })
                    pos.last_sl_notified_price = None
                    b["position"] = None

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
