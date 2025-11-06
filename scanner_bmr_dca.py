from __future__ import annotations
import asyncio
import time
import logging
import os
import inspect
from typing import Optional
from datetime import datetime
from enum import IntEnum

import numpy as np
import pandas as pd

# === Forex адаптеры и фид ===
from fx_mt5_adapter import FX, margin_to_lots, default_tick
from fx_feed import fetch_ohlcv

log = logging.getLogger("bmr_dca_engine")
logging.getLogger("fx_feed").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Регистр/ключи — берём из globals(), если их определили извне; иначе дефолт
# ---------------------------------------------------------------------------
TASKS_KEY = globals().get("TASKS_KEY", "scan_tasks")  # app.bot_data[TASKS_KEY] -> dict[ns_key] = asyncio.Task
BOXES_KEY = globals().get("BOXES_KEY", "scan_boxes")  # app.bot_data[BOXES_KEY] -> dict[ns_key] = dict(...)
BANKS_KEY = globals().get("BANKS_KEY", "scan_banks")  # app.bot_data[BANKS_KEY] -> dict["chat:symbol"] = float


# ---------------------------------------------------------------------------
# Namespace helpers для ключей и банка
# ---------------------------------------------------------------------------
def _ns(chat_id: int, symbol: str) -> str:
    return f"{chat_id}:{symbol.upper()}"


def _ns_key(chat_id: int | None, symbol: str) -> str:
    return _ns(int(chat_id), str(symbol))


def _resolve_bank_usd(app, chat_id: int | None, symbol: str) -> float:
    """
    Банк читается из app.bot_data[BANKS_KEY] по ключу chat_id:SYMBOL.
    Если не задан — берём CONFIG.SAFETY_BANK_USDT.
    """
    try:
        reg = app.bot_data.get(BANKS_KEY, {})
        return float(reg.get(_ns_key(int(chat_id), symbol), CONFIG.SAFETY_BANK_USDT))
    except Exception:
        return float(CONFIG.SAFETY_BANK_USDT)


# --- Status snapshot helper (для /status)
def _update_status_snapshot(
    box: dict,
    *,
    symbol: str,
    bank_fact: float,
    bank_target: float,
    pos,
    scan_paused: bool,
    rng_strat,
    rng_tac,
):
    state = "ПАУЗА" if scan_paused else "РАБОТАЕТ"
    pos_line = "Нет активной позиции."
    if pos and getattr(pos, "steps_filled", 0) > 0:
        pos_line = f"{pos.side} steps {pos.steps_filled}/{pos.max_steps} | avg={fmt(pos.avg)} | tp={fmt(pos.tp_price)}"
    box["status_snapshot"] = {
        "symbol": symbol,
        "state": state,
        "bank_fact_usdt": float(bank_fact),
        "bank_target_usdt": float(bank_target),
        "has_ranges": bool(rng_strat and rng_tac),
        "ts": time.time(),
    }
    box["status_line"] = (
        f"Состояние ({symbol})\n"
        f"Сканер: {state}\n"
        f"Банк (факт/план): {bank_fact:.2f} / {bank_target:.2f} USD\n"
        f"Позиция: {pos_line}"
    )


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
    plus_dm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=h.index)
    minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=h.index)
    cp = c.shift(1)
    tr_raw = pd.concat([(h - l), (h - cp).abs(), (l - cp).abs()], axis=1).max(axis=1)
    tr = _rma(tr_raw, length).replace(0, np.nan)
    plus_di = 100.0 * _rma(plus_dm, length) / tr
    minus_di = 100.0 * _rma(minus_dm, length) / tr
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100.0
    return _rma(dx, length)


def ta_supertrend(
    h: pd.Series, l: pd.Series, c: pd.Series, length: int = 10, multiplier: float = 3.0
) -> pd.DataFrame:
    atr = ta_atr(h, l, c, length)
    hl2 = (h + l) / 2.0
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr

    f_upper = upper.copy()
    f_lower = lower.copy()
    for i in range(1, len(c)):
        f_upper.iloc[i] = min(upper.iloc[i], f_upper.iloc[i - 1]) if c.iloc[i - 1] > f_upper.iloc[i - 1] else upper.iloc[i]
        f_lower.iloc[i] = max(lower.iloc[i], f_lower.iloc[i - 1]) if c.iloc[i - 1] < f_lower.iloc[i - 1] else lower.iloc[i]

    direction = pd.Series(index=c.index, dtype=int)
    direction.iloc[0] = 1
    for i in range(1, len(c)):
        if c.iloc[i] > f_upper.iloc[i - 1]:
            direction.iloc[i] = 1
        elif c.iloc[i] < f_lower.iloc[i - 1]:
            direction.iloc[i] = -1
        else:
            direction.iloc[i] = direction.iloc[i - 1]
            if direction.iloc[i] == 1 and f_lower.iloc[i] < f_lower.iloc[i - 1]:
                f_lower.iloc[i] = f_lower.iloc[i - 1]
            if direction.iloc[i] == -1 and f_upper.iloc[i] > f_upper.iloc[i - 1]:
                f_upper.iloc[i] = f_upper.iloc[i - 1]
    return pd.DataFrame({"direction": direction, "upper": f_upper, "lower": f_lower})


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
class CONFIG:
    # тип логики хеджа
    HEDGE_MODE = "trend"  # "revert" или "trend"

    # --- ИСПРАВЛЕНИЕ 2 ---
    # насколько внутрь TAC закрываем хедж (в долях ширины TAC)
    HEDGE_CLOSE_FRAC = 0.15  # 15%

    # Пара по умолчанию
    SYMBOL = "USDJPY"

    # Комиссии
    FEE_MAKER = 0.0
    FEE_TAKER = 0.0

    # Плечи
    LEVERAGE = 200
    MIN_LEVERAGE = 2
    MAX_LEVERAGE = 500

    # Таймфреймы
    TF_ENTRY = "5m"
    TF_RANGE = "1h"
    TF_TRIGGER = "1m"

    # История
    STRATEGIC_LOOKBACK_DAYS = 60
    TACTICAL_LOOKBACK_DAYS = 3

    # Интервалы
    FETCH_TIMEOUT = 25
    SCAN_INTERVAL_SEC = 3
    REBUILD_RANGE_EVERY_MIN = 15
    REBUILD_TACTICAL_EVERY_MIN = 5

    # Диапазоны
    Q_LOWER = 0.025
    Q_UPPER = 0.975
    RANGE_MIN_ATR_MULT = 1.5

    RSI_LEN = 14
    ADX_LEN = 14
    VOL_WIN = 50

    WEIGHTS = {
        "border": 0.45,
        "rsi": 0.15,
        "ema_dev": 0.20,
        "supertrend": 0.10,
        "vol": 0.10,
    }
    SCORE_THR = 0.55

    # DCA
    DCA_LEVELS = 7
    DCA_GROWTH = 2.0
    CUM_DEPOSIT_FRAC_AT_FULL = 0.70
    ADD_COOLDOWN_SEC = 25
    WICK_HYST_TICKS = 1

    # ТП/трейл
    TP_PCT = 0.010
    TRAILING_STAGES = [(0.35, 0.25), (0.60, 0.50), (0.85, 0.75)]

    # Безопасность
    BREAK_EPS = 0.0025
    REENTRY_BAND = 0.003
    MAINT_MMR = 0.004
    LIQ_FEE_BUFFER = 1.0

    SAFETY_BANK_USDT = 1500.0
    AUTO_LEVERAGE = False
    AUTO_ALLOC = {
        "thin_tac_vs_strat": 0.35,
        "low_vol_z": 0.5,
        "growth_A": 1.6,
        "growth_B": 2.2,
    }

    SPIKE = {
        "WICK_RATIO": 2.0,
        "ATR_MULT": 1.6,
        "VOLZ_THR": 1.5,
        "RETRACE_FRAC": 0.35,
        "RETRACE_WINDOW_SEC": 120,
    }

    M15_RECO = {
        "ATR_LEN": 14,
        "TRIGGER_MULT": 1.5,
        "CHECK_EVERY_SEC": 60,
        "MSG_COOLDOWN_SEC": 60,
    }

    ORDINARY_ADDS = 5
    DCA_MIN_GAP_TICKS = 2
    ML_TARGET_PCT = 20.0
    ML_BREAK_BUFFER_PCT = 3.0
    ML_BREAK_BUFFER_PCT2 = 2.0
    ML_MIN_AFTER_LAST_PCT = 200.0
    MIN_SPACING_PCT = 0.0035
    ML_REQ_GAP_MODE = "strat_spacing"

    EXT_AFTER_BREAK = {
        "CONFIRM_BARS_5M": 6,
        "EXTRA_LOOKBACK_DAYS": 10,
        "ATR_MULT_MIN": 2.0,
        "PRICE_EPS": 0.0015,
    }

    BOOST_MAX_STEPS = 3
    BREAK_MSG_COOLDOWN_SEC = 45

    REQUIRE_MANUAL_REOPEN_ON = {
        "manual_close": True,
        "sl_hit": True,
        "tp_hit": True,
    }
    REMIND_MANUAL_MSG_COOLDOWN_SEC = 120

    BREAK_PROBE = {
        "SAMPLES": 3,
        "INTERVAL_SEC": 5,
        "TIMEOUT_SEC": 20,
    }

    # --- ПРАВКА 1: Изменяем кол-во уровней добора (1 нога + 3 добора = 4) ---
    STRAT_LEVELS_AFTER_HEDGE = 3 # Было 6. Теперь у нас 3 добора (TAC1, TAC2, STRAT1)
    
    # --- ПРАВКА 2: Изменяем рост маржи на 1.0 (равномерно) ---
    GROWTH_AFTER_HEDGE = 1.0 # Было 1.40

    AFTER_HEDGE_TAIL = {
        "levels": 5,
        "mode": "relative_vector",
        "coeffs_rel_to_leg": [
            0.345,
            0.4983333333,
            0.690,
            0.9583333333,
        ],
        "first_rel_to_leg": 0.345,
        "growth": 1.40,
        "round_usd": 1.0,
    }

    EQUALIZING_TOL_TICKS = 2
    LEG_MIN_FRACTION = 0.30

    # Эта настройка больше не используется, т.к. маржа ноги = 1/4 бюджета
    INITIAL_HEDGE_FRACTION = 0.042  # 4.2%


# ENV-переопределения
CONFIG.SYMBOL = os.getenv("FX_SYMBOL", CONFIG.SYMBOL)
CONFIG.TF_ENTRY = os.getenv("TF_ENTRY", CONFIG.TF_ENTRY)
CONFIG.TF_RANGE = os.getenv("TF_RANGE", os.getenv("TF_TREND", CONFIG.TF_RANGE))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def maybe_await(func, *args, **kwargs):
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


def _norm_symbol(x) -> str:
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


# ---------------------------------------------------------------------------
# Formatting & maths
# ---------------------------------------------------------------------------
def fmt(p: float) -> str:
    if p is None or pd.isna(p):
        return "N/A"
    if p < 0.01:
        return f"{p:.6f}"
    if p < 1.0:
        return f"{p:.5f}"
    return f"{p:.4f}"


def fmt_money(x: float) -> str:
    try:
        return f"{x:,.2f} $".replace(",", " ")
    except Exception:
        return f"{x:.2f} $"


def fmt_pct2(x: float) -> str:
    if x is None or (isinstance(x, float) and (pd.isna(x) or np.isinf(x))):
        return "∞"
    return f"{x:.2f}%"


def _display_label_ru_from_target(t: dict) -> str:
    raw = str(t.get("label", "")).upper()
    is_reserve = bool(t.get("reserve3"))
    if raw.startswith("TAC"):
        return t.get("label", "")
        
    # --- ПРАВКА 3: Отображение нового STRAT 1 ---
    if raw.startswith("STRAT 1"):
        return "STRAT 1"
        
    if "33%" in raw:
        return "STRAT #1 (33%)"
    if "66%" in raw:
        return "STRAT #2 (66%)"
    if "100%" in raw:
        return "STRAT #3 (100%, резерв)" if is_reserve else "STRAT #3 (100%)"
    return t.get("label", "")


def _preview_label_ru(idx: int, t: dict) -> str:
    raw = str(t.get("label", "")).upper()
    if raw.startswith("TAC"):
        if "TAC #" in raw:
            return f"Тактическое усреднение {raw.split('#')[-1]}"
        return "Тактическое усреднение"
    if raw.startswith("STRAT"):
        # --- ПРАВКА 4: Отображение нового STRAT 1 ---
        if "STRAT 1" in raw:
             return "1-й STRAT"
        
        pr = "100%" if "100" in raw else ("66%" if "66" in raw else ("33%" if "33" in raw else ""))
        strat_num = 1
        if "66" in raw:
            strat_num = 2
        if "100" in raw:
            strat_num = 3
        base = f"{strat_num}-е STRAT ({pr})"
        if t.get("reserve3"):
            return base.replace(")", ", резерв)")
        return base
    return t.get("label", "")


def render_remaining_levels_block(
    symbol: str, pos: "Position", bank: float, fee_taker: float, tick: float
) -> str:
    if not getattr(pos, "ordinary_targets", None):
        return ""
    lines = []

    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    used0 = _pos_total_margin(pos)
    avg0, qty0 = float(pos.avg), float(pos.qty)
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    base_off = getattr(pos, "ordinary_offset", 0)

    strat_counter = 0

    for i, t in enumerate(pos.ordinary_targets[base_off:], start=0):
        step_idx = used_ord + i
        if step_idx >= len(pos.step_margins):
            break
        price = float(t["price"])
        m = float(pos.step_margins[step_idx])
        dq = (m * L) / max(price, 1e-12)

        qty_new = qty0 + dq
        avg_new = (avg0 * qty0 + price * dq) / max(qty_new, 1e-9)
        used_new = used0 + m

        class _Tmp:
            pass

        tmp = _Tmp()
        tmp.side = pos.side
        tmp.avg = avg_new
        tmp.qty = qty_new
        tmp.leverage = L
        tmp.steps_filled = 1
        tmp.step_margins = [used_new]
        tmp.reserve_used = False
        tmp.reserve_margin_usdt = 0.0

        ml20 = ml_price_at(tmp, CONFIG.ML_TARGET_PCT, bank, fees_est)

        eq_at_price = equity_at_price(tmp, price, bank, fees_est)
        free_margin_after = max(eq_at_price - used_new, 0.0)
        margin_level_after = (eq_at_price / used_new) * 100.0 if used_new > 0 else float("inf")

        add_lots = margin_to_lots(symbol, m, price=price, leverage=L)
        total_lots_after = margin_to_lots(symbol, used_new, price=avg_new, leverage=L)

        label = _display_label_ru_from_target(t)
        # --- ПРАВКА 5: Обновление лейблов в рендере ---
        if label.startswith("STRAT"):
            strat_counter += 1
            if "STRAT 1" in label:
                label = "1-й STRAT"
            elif "33%" in label:
                label = "1-е STRAT (33%)"
            elif "66%" in label:
                label = "2-е STRAT (66%)"
            elif "100%" in label:
                label = "3-е STRAT (100%)" + (" (резерв)" if t.get("reserve3") else "")
            else:
                label = f"{strat_counter}-е STRAT"

        lines.append(
            f"• {label}: {fmt(price)} "
            f"Добор: {m:.0f} $ ≈ {add_lots:.2f} лота → всего {total_lots_after:.2f} лота; "
            f"средняя {fmt(avg_new)} (P/L 0).\n"
            f"ML 20%: {fmt(ml20)}\n"
            f"Свободная маржа: {fmt_money(free_margin_after)}, "
            f"Уровень маржи: {fmt_pct2(margin_level_after)}"
        )
        qty0, avg0, used0 = qty_new, avg_new, used_new
    return "\n".join(lines)


def render_hedge_preview_block(
    symbol: str,
    pos: "Position",
    bank: float,
    fees_est: float,
    tick: float,
    hc_price: float,
    lots_leg: float,
    leg_usd: float,
) -> str:
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    used0 = _pos_total_margin(pos)
    eq0 = equity_at_price(pos, pos.avg, bank, fees_est)
    free0 = max(eq0 - used0, 0.0)
    mlp0 = (eq0 / used0) * 100.0 if used0 > 0 else float("inf")
    ml20_0 = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_est)

    lines = [
        f"Закрытие хеджа (останется {pos.side}): <code>{fmt(hc_price)}</code> "
        f"Средняя <code>{fmt(pos.avg)}</code> "
        f"Свободная маржа: {fmt_money(free0)}, Уровень маржи: {fmt_pct2(mlp0)}, ML 20%: <code>{fmt(ml20_0)}</code>"
    ]
    lines.append(f"\nОставлена нога: ≈{lots_leg:.2f} лота (маржа {leg_usd:.0f} $).")

    lots_total = float(lots_leg)
    avg0, qty0 = float(pos.avg), float(pos.qty)
    used = float(used0)

    base_off = getattr(pos, "ordinary_offset", 0)
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)

    strat_counter = 0

    for i, t in enumerate(pos.ordinary_targets[base_off:], start=1):
        step_idx = used_ord + (i - 1)
        if step_idx >= len(pos.step_margins):
            break
        price = float(t["price"])
        m = float(pos.step_margins[step_idx])
        notional = m * L
        dq = notional / max(price, 1e-12)
        qty_new = qty0 + dq
        avg_new = (avg0 * qty0 + price * dq) / max(qty_new, 1e-9)
        used_new = used + m

        class _Tmp:
            pass

        tmp = _Tmp()
        tmp.side = pos.side
        tmp.avg = avg_new
        tmp.qty = qty_new
        tmp.leverage = L
        tmp.steps_filled = 1
        tmp.step_margins = [used_new]
        tmp.reserve_used = False
        tmp.reserve_margin_usdt = 0.0

        ml20 = ml_price_at(tmp, CONFIG.ML_TARGET_PCT, bank, fees_est)
        eq = equity_at_price(tmp, price, bank, fees_est)
        free_after = max(eq - used_new, 0.0)
        ml_after = (eq / used_new) * 100.0 if used_new > 0 else float("inf")

        lots_add = margin_to_lots(symbol, m, price=price, leverage=L)
        lots_total = lots_total + lots_add

        raw_label = str(t.get("label", "")).upper()
        if raw_label.startswith("STRAT"):
            strat_counter += 1
            label = _preview_label_ru(strat_counter, t)
        else:
            # TAC
            label = _preview_label_ru(i, t)

        lines.append(
            f"\n• {label}: <code>{fmt(price)}</code>\n"
            f"Добор: {m:.0f} $ ≈ {lots_add:.2f} лота → всего {lots_total:.2f} лота; "
            f"средняя <code>{fmt(avg_new)}</code> (P/L 0).\n"
            f"ML 20%: <code>{fmt(ml20)}</code>\n"
            f"Свободная маржа: {fmt_money(free_after)}, Уровень маржи: {fmt_pct2(ml_after)}"
        )
        qty0, avg0, used = qty_new, avg_new, used_new
    return "\n".join(lines)


def margin_line(pos, bank: float, px: float | None = None, fees_est: float = 0.0) -> str:
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
    if levels <= 0 or bank <= 0:
        return []
    if abs(growth - 1.0) < 1e-9:
        per = bank / levels
        return [per] * levels
    base = bank * (growth - 1.0) / (growth ** levels - 1.0)
    return [base * (growth ** i) for i in range(levels)]


def _pos_total_margin(pos):
    used_ord_count = max(0, pos.steps_filled - (1 if getattr(pos, "reserve_used", False) else 0))
    ord_used = sum(pos.step_margins[: min(used_ord_count, len(pos.step_margins))])
    res = pos.reserve_margin_usdt if getattr(pos, "reserve_used", False) else 0.0
    return ord_used + res


def compute_net_pnl(pos, exit_p: float, fee_entry: float, fee_exit: float) -> tuple[float, float]:
    used = _pos_total_margin(pos) or 1e-9
    L = max(1, int(getattr(pos, "leverage", 1) or 1))

    if pos.side == "LONG":
        gross_usd = used * L * (exit_p / pos.avg - 1.0)
    else:
        gross_usd = used * L * (pos.avg / max(exit_p, 1e-12) - 1.0)

    entry_notional = used * L
    exit_notional = exit_p * pos.qty
    fee_entry_usd = entry_notional * fee_entry
    fee_exit_usd = exit_notional * fee_exit

    net_usd = gross_usd - fee_entry_usd - fee_exit_usd
    net_pct = (net_usd / used) * 100.0
    return net_usd, net_pct


def _pnl_at_price(pos, price: float, used_margin: float) -> float:
    if used_margin <= 0 or price is None or pos.avg <= 0:
        return 0.0
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    if pos.side == "LONG":
        return used_margin * L * (price / pos.avg - 1.0)
    else:
        return used_margin * L * (pos.avg / max(price, 1e-12) - 1.0)


def equity_at_price(pos, price: float, bank: float, fees_est: float) -> float:
    used = _pos_total_margin(pos)
    pnl = _pnl_at_price(pos, price, used)
    return bank + pnl - max(fees_est, 0.0)


def ml_percent_now(pos, price: float, bank: float, fees_est: float) -> float:
    used = _pos_total_margin(pos)
    if used <= 0:
        return float("inf")
    return (equity_at_price(pos, price, bank, fees_est) / used) * 100.0


def ml_reserve_pct_to_ml20(pos, price: float, bank: float, fees_est: float) -> float:
    mlp = ml_percent_now(pos, price, bank, fees_est)
    return float("inf") if (not np.isfinite(mlp)) else (mlp - CONFIG.ML_TARGET_PCT)


def ml_price_at(pos, target_ml_pct: float, bank: float, fees_est: float) -> float:
    UM = _pos_total_margin(pos)
    if UM <= 0 or pos.avg <= 0:
        return float("nan")
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    target_equity = (target_ml_pct / 100.0) * UM
    base = (target_equity - (bank - max(fees_est, 0.0))) / (UM * L)
    denom = 1.0 + base
    if denom <= 0:
        return float("nan")
    return pos.avg * denom if pos.side == "LONG" else pos.avg / denom


def ml_distance_pct(side: str, px: float, ml_price: float) -> float:
    if px is None or ml_price is None or px <= 0 or np.isnan(ml_price):
        return float("nan")
    return (1.0 - ml_price / px) * 100.0 if side == "LONG" else (ml_price / px - 1.0) * 100.0


def chandelier_stop(side: str, price: float, atr: float, mult: float = 3.0):
    return price - mult * atr if side == "LONG" else price + mult * atr


def break_levels(rng: dict) -> tuple[float, float]:
    up = rng["upper"] * (1.0 + CONFIG.BREAK_EPS)
    dn = rng["lower"] * (1.0 - CONFIG.BREAK_EPS)
    return up, dn


def compute_fractals_15m(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    (P5 FIX) Обновленный расчет фракталов (non-strict),
    допускающий 'плато' (равные значения <= и >=).
    """
    h, l = df["high"], df["low"]
    n = len(df)
    up = pd.Series([np.nan] * n, index=df.index)
    dn = pd.Series([np.nan] * n, index=df.index)
    for i in range(2, n - 2):
        # Фрактал ВВЕРХ (non-strict) — центральный high >= 2 слева и 2 справа
        is_up = (
            h.iloc[i - 2] <= h.iloc[i] and
            h.iloc[i - 1] <= h.iloc[i] and
            h.iloc[i + 1] <= h.iloc[i] and
            h.iloc[i + 2] <= h.iloc[i]
        )
        # Фрактал ВНИЗ (non-strict) — центральный low <= 2 слева и 2 справа
        is_dn = (
            l.iloc[i - 2] >= l.iloc[i] and
            l.iloc[i - 1] >= l.iloc[i] and
            l.iloc[i + 1] >= l.iloc[i] and
            l.iloc[i + 2] >= l.iloc[i]
        )
        # Хотя бы один из 4 соседей должен быть СТРОГО < (для up) или > (для dn).
        if is_up:
            is_not_flat_plateau = (
                h.iloc[i - 2] < h.iloc[i] or
                h.iloc[i - 1] < h.iloc[i] or
                h.iloc[i + 1] < h.iloc[i] or
                h.iloc[i + 2] < h.iloc[i]
            )
            if is_not_flat_plateau:
                up.iloc[i] = h.iloc[i]
        if is_dn:
            is_not_flat_plateau_low = (
                l.iloc[i - 2] > l.iloc[i] or
                l.iloc[i - 1] > l.iloc[i] or
                l.iloc[i + 1] > l.iloc[i] or
                l.iloc[i + 2] > l.iloc[i]
            )
            if is_not_flat_plateau_low:
                dn.iloc[i] = l.iloc[i]
    return up, dn


def _last_confirmed_fractals(fr_up: pd.Series, fr_dn: pd.Series) -> tuple[float | None, float | None]:
    up_val = pd.to_numeric(fr_up.dropna(), errors="coerce")
    dn_val = pd.to_numeric(fr_dn.dropna(), errors="coerce")
    last_up = float(up_val.iloc[-1]) if len(up_val) else None
    last_dn = float(dn_val.iloc[-1]) if len(dn_val) else None
    return last_up, last_dn


def _two_tacticals_between(hc_px: float, strat1: float, side: str, tick: float) -> list[float]:
    """
    (P1 FIX) Две тактики на 1/3 и 2/3 пути HC↔STRAT#1 с квантованием по тику и антислипанием.
    """
    dist_total = strat1 - hc_px
    p1_raw = hc_px + dist_total / 3.0
    p2_raw = hc_px + 2.0 * dist_total / 3.0
    t1 = quantize_to_tick(p1_raw, tick)
    t2 = quantize_to_tick(p2_raw, tick)

    min_gap_ticks = max(1, CONFIG.DCA_MIN_GAP_TICKS)

    # Разводим минимум на 1 тик, если слиплись
    if abs(_ticks_between(t1, t2, tick)) < min_gap_ticks:
        if side == "LONG":
            t2 = quantize_to_tick(t1 - min_gap_ticks * tick, tick)
        else:
            t2 = quantize_to_tick(t1 + min_gap_ticks * tick, tick)

    if side == "LONG":
        return [max(t1, t2), min(t1, t2)]
    else:
        return [min(t1, t2), max(t1, t2)]


def _tactical_between(hc_px: float, strat1: float, side: str, tick: float) -> float:
    if side == "LONG":
        mid = hc_px + 0.5 * (strat1 - hc_px)
        min_gap = max(tick * CONFIG.DCA_MIN_GAP_TICKS, hc_px * CONFIG.MIN_SPACING_PCT)
        lo = hc_px - min_gap
        hi = strat1 + min_gap
        p = max(min(mid, lo), hi)
    else:
        mid = hc_px + 0.5 * (strat1 - hc_px)
        min_gap = max(tick * CONFIG.DCA_MIN_GAP_TICKS, hc_px * CONFIG.MIN_SPACING_PCT)
        lo = strat1 - min_gap
        hi = hc_px + min_gap
        p = min(max(mid, hi), lo)
    return quantize_to_tick(p, tick)


# ===========================================================================
# === НАЧАЛО ЗАМЕНЫ build_targets_with_tactical (ИСПРАВЛЕНИЕ 2) ============
# ===========================================================================
def build_targets_with_tactical(
    pos: "Position",
    rng_strat: dict,
    close_px: float,
    tick: float,
    bank: float,
    fees_est: float,
) -> list[dict]:
    base_for_strat = float(close_px)
    strat = auto_strat_targets_with_ml_buffer(
        pos, rng_strat, entry=base_for_strat, tick=tick, bank=bank, fees_est=fees_est
    )
    if not strat:
        return []

    strat1 = strat[0]["price"]
    tac_candidates = []

    if pos.manual_tac_price is not None and pos.manual_tac2_price is not None:
        tac_candidates = [
            quantize_to_tick(pos.manual_tac_price, tick),
            quantize_to_tick(pos.manual_tac2_price, tick),
        ]
        if pos.side == "LONG":
            tac_candidates.sort(reverse=True)
        else:
            tac_candidates.sort()
    elif pos.manual_tac_price is not None:
        tac1 = quantize_to_tick(float(pos.manual_tac_price), tick)
        tac2 = _tactical_between(tac1, strat1, pos.side, tick)
        tac_candidates = [tac1, tac2]
    else:
        tac_candidates = _two_tacticals_between(close_px, strat1, pos.side, tick)

    # --- клиппинг TAC относительно HC и STRAT#1 с буферами
    gap_hc = max(tick * CONFIG.DCA_MIN_GAP_TICKS, close_px * CONFIG.MIN_SPACING_PCT)
    gap_s1 = max(tick * CONFIG.DCA_MIN_GAP_TICKS, strat1 * CONFIG.MIN_SPACING_PCT)
    dist = abs(close_px - strat1)
    min_total = gap_hc + gap_s1

    final_tacs = []

    if len(tac_candidates) == 1:
        tac_px = tac_candidates[0]
        # Узкий коридор — пропорционально сжимаем буферы
        if dist <= min_total:
            w_hc = gap_hc / max(min_total, 1e-12)
            w_s1 = 1.0 - w_hc
            gap_hc = max(tick * CONFIG.DCA_MIN_GAP_TICKS, dist * w_hc)
            gap_s1 = max(tick * CONFIG.DCA_MIN_GAP_TICKS, dist * w_s1)

        if pos.side == "LONG":
            upper_bound = close_px - gap_hc
            lower_bound = strat1 + gap_s1
            tac_px = max(min(tac_px, upper_bound), lower_bound)
        else:
            lower_bound = close_px + gap_hc
            upper_bound = strat1 - gap_s1
            tac_px = min(max(tac_px, lower_bound), upper_bound)
        final_tacs.append(quantize_to_tick(tac_px, tick))

    elif len(tac_candidates) >= 2:
        t1, t2 = tac_candidates[0], tac_candidates[1]

        # --- НАЧАЛО ФИКСА "СЛИПАНИЯ" v2 ---
        # Применяем буферы (клиппинг) ТОЛЬКО если коридор шире,
        # чем сумма самих буферов.
        if dist > min_total:
            if pos.side == "LONG":
                hi = close_px - gap_hc
                lo = strat1 + gap_s1
                t1 = min(max(t1, lo), hi)
                t2 = min(max(t2, lo), hi)
                final_tacs = [max(t1, t2), min(t1, t2)]
            else:
                lo = close_px + gap_hc
                hi = strat1 - gap_s1
                t1 = min(max(t1, lo), hi)
                t2 = min(max(t2, lo), hi)
                final_tacs = [min(t1, t2), max(t1, t2)]
        else:
            # Коридор слишком узкий.
            # Игнорируем буферы и берём "сырые" точки 1/3 и 2/3,
            # которые уже вернула _two_tacticals_between.
            final_tacs = [t1, t2]
        # --- КОНЕЦ ФИКСА "СЛИПАНИЯ" v2 ---

    # Маркировка
    tacs_out: list[dict] = []
    if len(final_tacs) == 1:
        tacs_out.append({"price": final_tacs[0], "label": "TAC"})
    elif len(final_tacs) >= 2:
        # Убедимся, что T1 и T2 не слиплись после квантования (на всякий случай)
        if abs(_ticks_between(final_tacs[0], final_tacs[1], tick)) < 1:
             tacs_out.append({"price": final_tacs[0], "label": "TAC"})
        else:
            tacs_out.append({"price": final_tacs[0], "label": "TAC #1"})
            tacs_out.append({"price": final_tacs[1], "label": "TAC #2"})

    if not tacs_out:
        # Если final_tacs пуст, но место есть — вставляем один TAC по центру
        gap_hc = max(tick * CONFIG.DCA_MIN_GAP_TICKS, close_px * CONFIG.MIN_SPACING_PCT)
        gap_s1 = max(tick * CONFIG.DCA_MIN_GAP_TICKS, strat1 * CONFIG.MIN_SPACING_PCT)
        min_total = gap_hc + gap_s1
        if abs(close_px - strat1) > min_total:
            tac_mid = _tactical_between(close_px, strat1, pos.side, tick)
            tacs_out.append({"price": tac_mid, "label": "TAC"})

    out = tacs_out + strat
    return out
# ===========================================================================
# === КОНЕЦ ЗАМЕНЫ build_targets_with_tactical =============================
# ===========================================================================


def break_distance_pcts(px: float, up: float, dn: float) -> tuple[float, float]:
    if px is None or px <= 0 or any(v is None or np.isnan(v) for v in (up, dn)):
        return float("nan"), float("nan")
    up_pct = max(0.0, (up / px - 1.0) * 100.0)
    dn_pct = max(0.0, (1.0 - dn / px) * 100.0)
    return up_pct, dn_pct


def quantize_to_tick(x: float | None, tick: float) -> float | None:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return x
    return round(round(x / tick) * tick, 10)


def _place_segment(
    start: float,
    end: float,
    count: int,
    tick: float,
    include_end_last: bool,
    side: str,
) -> list[float]:
    if count <= 0:
        return []
    path = end - start
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    if abs(path) < min_gap:
        return []

    if include_end_last and count >= 1:
        fracs = [(i + 1) / count for i in range(count)]
    else:
        fracs = [(i + 1) / (count + 1) for i in range(count)]

    raw = [start + path * f for f in fracs]
    out: list[float] = []
    lo, hi = (min(start, end), max(start, end))
    for i, x in enumerate(raw):
        q = quantize_to_tick(x, tick)
        q = min(max(q, lo), hi)
        if i == 0:
            q = (start - min_gap) if side == "LONG" else (start + min_gap)
            q = min(max(quantize_to_tick(q, tick), lo), hi)
        if (not out) or (side == "LONG" and q <= out[-1] - min_gap) or (side == "SHORT" and q >= out[-1] + min_gap):
            out.append(q)
    return out


def compute_corridor_targets(
    entry: float, side: str, rng_strat: dict, rng_tac: dict, tick: float
) -> list[dict]:
    DESIRED_TAC = 2
    DESIRED_STRAT = 3

    if side == "LONG":
        tac_b = min(entry, rng_tac["lower"])
        strat_b = min(entry, rng_strat["lower"])
        seg1 = _place_segment(entry, tac_b, DESIRED_TAC, tick, include_end_last=False, side=side)
        seg2 = _place_segment(tac_b, strat_b, DESIRED_STRAT, tick, include_end_last=True, side=side)
    else:
        tac_b = max(entry, rng_tac["upper"])
        strat_b = max(entry, rng_strat["upper"])
        seg1 = _place_segment(entry, tac_b, DESIRED_TAC, tick, include_end_last=False, side=side)
        seg2 = _place_segment(tac_b, strat_b, DESIRED_STRAT, tick, include_end_last=True, side=side)

    missing_tac = max(0, DESIRED_TAC - len(seg1))
    if missing_tac > 0 and len(seg2) > 0:
        can_take = max(0, len(seg2) - 1)
        take = min(missing_tac, can_take)
        if take > 0:
            seg1 = seg1 + seg2[:take]
            seg2 = seg2[take:]

    def _labels(prefix: str, n: int, include_end: bool) -> list[str]:
        if n <= 0:
            return []
        if include_end:
            fr = [(i + 1) / n for i in range(n)]
        else:
            fr = [(i + 1) / (n + 1) for i in range(n)]
        return [f"{prefix} {int(round(f * 100))}%" for f in fr]

    labs_tac = _labels("TAC", max(len(seg1), 0), include_end=False)
    labs_strat = _labels("STRAT", max(len(seg2), 0), include_end=True)

    targets: list[dict] = []
    for p, lab in zip(seg1, labs_tac):
        targets.append({"price": p, "label": lab})
    for p, lab in zip(seg2, labs_strat):
        targets.append({"price": p, "label": lab})

    return merge_targets_sorted(side, tick, entry, targets)


def compute_strategic_targets_only(
    entry: float, side: str, rng_strat: dict, tick: float, levels: int = 3
) -> list[dict]:
    assert tick is not None, "tick must be provided"
    if levels <= 0:
        return []
    if side == "LONG":
        strat_b = min(entry, rng_strat["lower"])
        seg = _place_segment(entry, strat_b, levels, tick, include_end_last=True, side=side)
    else:
        strat_b = max(entry, rng_strat["upper"])
        seg = _place_segment(entry, strat_b, levels, tick, include_end_last=True, side=side)
    labs = [f"STRAT {int(round((i + 1) / max(len(seg), 1) * 100))}%" for i in range(len(seg))]
    out = [{"price": p, "label": lab} for p, lab in zip(seg, labs)]
    return merge_targets_sorted(side, tick, entry, out)


def merge_targets_sorted(side: str, tick: float, entry: float, targets: list[dict]) -> list[dict]:
    dedup = []
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    for t in targets:
        if not dedup or (side == "SHORT" and t["price"] >= dedup[-1]["price"] + min_gap) or (
            side == "LONG" and t["price"] <= dedup[-1]["price"] - min_gap
        ):
            dedup.append(t)
    return dedup


def _advance_pointer(pos, tick):
    if pos.last_filled_q is None:
        return
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    base = getattr(pos, "ordinary_offset", 0)
    while base < len(pos.ordinary_targets):
        p = pos.ordinary_targets[base]["price"]
        ok = (pos.side == "SHORT" and p >= pos.last_filled_q + min_gap) or (
            pos.side == "LONG" and p <= pos.last_filled_q - min_gap
        )
        if ok:
            break
        base += 1
    pos.ordinary_offset = base


def next_pct_target(pos):
    if not getattr(pos, "ordinary_targets", None):
        return None
    used_dca = max(0, (pos.steps_filled - (1 if pos.reserve_used else 0)) - 1)
    base = getattr(pos, "ordinary_offset", 0)
    abs_idx = max(base, used_dca)
    if 0 <= abs_idx < len(pos.ordinary_targets):
        t = pos.ordinary_targets[abs_idx]
        if (
            getattr(pos, "reserve3_price", None) is not None
            and abs(float(t["price"]) - float(pos.reserve3_price)) < 1e-12
            and (not getattr(pos, "reserve3_ready", False))
            and (not getattr(pos, "reserve3_done", False))
        ):
            return None
        return t
    return None


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
        last_ts = idx[-1].to_pydatetime() if hasattr(idx[-1], "to_pydatetime") else None
        if last_ts is None and "time" in df.columns:
            last_ts = pd.to_datetime(df["time"].iloc[-1]).to_pydatetime()
        if last_ts is None:
            return True
        age_min = (datetime.utcnow() - last_ts.replace(tzinfo=None)).total_seconds() / 60.0
        return age_min <= max_age_min
    except Exception:
        return True


async def plan_extension_after_break(
    symbol: str,
    pos: "Position",
    rng_strat: dict,
    rng_tac: dict,
    px: float,
    tick: float,
) -> list[dict]:
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    remaining = max(0, pos.ord_levels - used_ord)
    if remaining <= 0:
        return pos.ordinary_targets

    ext_hours = max(
        (CONFIG.TACTICAL_LOOKBACK_DAYS + CONFIG.EXT_AFTER_BREAK["EXTRA_LOOKBACK_DAYS"]) * 24,
        CONFIG.TACTICAL_LOOKBACK_DAYS * 24,
    )
    try:
        ext_df = await maybe_await(fetch_ohlcv, symbol, CONFIG.TF_RANGE, ext_hours)
    except Exception:
        ext_df = None

    ext_rng = (
        await build_range_from_df(ext_df, min_atr_mult=1.5)
        if (ext_df is not None and not ext_df.empty)
        else None
    )

    atr1h = float(rng_strat.get("atr1h", 0.0))
    atr_guard = CONFIG.EXT_AFTER_BREAK["ATR_MULT_MIN"] * max(atr1h, 1e-12)

    if pos.side == "SHORT":
        candidates = [
            px + atr_guard,
            rng_strat["upper"],
            (ext_rng and ext_rng["upper"]) or rng_strat["upper"],
        ]
        end = max(v for v in candidates if np.isfinite(v))
        start = px
    else:
        candidates = [
            px - atr_guard,
            rng_strat["lower"],
            (ext_rng and ext_rng["lower"]) or rng_strat["lower"],
        ]
        end = min(v for v in candidates if np.isfinite(v))
        start = px

    seg = _place_segment(start, end, remaining, tick, include_end_last=True, side=pos.side)
    if not seg:
        return pos.ordinary_targets

    keep_idx = max(getattr(pos, "ordinary_offset", 0), max(0, used_ord - 1))
    already = pos.ordinary_targets[: min(keep_idx, len(pos.ordinary_targets))]

    new_labels = [f"EXT {int(round((i + 1) / len(seg) * 100))}%" for i in range(len(seg))]
    ext_targets = [{"price": p, "label": lab} for p, lab in zip(seg, new_labels)]

    return already + ext_targets


async def build_range_from_df(df: Optional[pd.DataFrame], min_atr_mult: float):
    if df is None or df.empty:
        return None
    want = ["open", "high", "low", "close", "volume"]
    cols_named = [c for c in want if c in df.columns]
    if len(cols_named) == 5:
        df = df[cols_named].copy()
    else:
        df = df.iloc[:, -5:].copy()
        df.columns = want
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
    return {"lower": lower, "upper": upper, "mid": mid, "atr1h": atr1h, "width": upper - lower}


async def build_ranges(symbol: str):
    s_limit = CONFIG.STRATEGIC_LOOKBACK_DAYS * 24
    t_limit = CONFIG.TACTICAL_LOOKBACK_DAYS * 24
    s_df_task = asyncio.create_task(maybe_await(fetch_ohlcv, symbol, CONFIG.TF_RANGE, s_limit))
    t_df_task = asyncio.create_task(maybe_await(fetch_ohlcv, symbol, CONFIG.TF_RANGE, t_limit))
    s_df, t_df = await asyncio.gather(s_df_task, t_df_task)
    s_task = asyncio.create_task(build_range_from_df(s_df, min_atr_mult=3.0))
    t_task = asyncio.create_task(build_range_from_df(t_df, min_atr_mult=1.5))
    strat, tac = await asyncio.gather(s_task, t_task)
    return strat, tac


def compute_indicators_5m(df: pd.DataFrame) -> dict:
    atr5m = ta_atr(df["high"], df["low"], df["close"], length=14).iloc[-1]
    rsi = ta_rsi(df["close"], length=CONFIG.RSI_LEN).iloc[-1]
    adx = ta_adx(df["high"], df["low"], df["close"], length=CONFIG.ADX_LEN).iloc[-1]
    ema20 = _ema(df["close"], length=20).iloc[-1]
    vol = df["volume"]
    rm = vol.rolling(CONFIG.VOL_WIN, min_periods=1).mean().iloc[-1]
    rs = vol.rolling(CONFIG.VOL_WIN, min_periods=2).std().iloc[-1]
    vol_z = (vol.iloc[-1] - rm) / max(rs, 1e-9)
    st = ta_supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3.0)
    dir_now = int(st["direction"].iloc[-1])
    dir_prev = int(st["direction"].iloc[-2]) if len(st) > 1 else dir_now
    st_state = (
        "down_to_up_near"
        if (dir_prev == -1 and dir_now == 1)
        else "up_to_down_near"
        if (dir_prev == 1 and dir_now == -1)
        else ("up" if dir_now == 1 else "down")
    )

    ema_dev_atr = abs(df["close"].iloc[-1] - ema20) / max(float(atr5m), 1e-9)
    for v in (atr5m, rsi, adx, ema20, vol_z, ema_dev_atr):
        if pd.isna(v) or np.isinf(v):
            raise ValueError("Indicators contain NaN/Inf")
    return {
        "atr5m": float(atr5m),
        "rsi": float(rsi),
        "adx": float(adx),
        "ema20": float(ema20),
        "vol_z": float(vol_z),
        "ema_dev_atr": float(ema_dev_atr),
        "supertrend": st_state,
    }


def _break_price_for_side(rng_strat: dict, side: str) -> float:
    up, dn = break_levels(rng_strat)
    return dn if side == "LONG" else up


def _simulate_after_k(pos: "Position", prices: list[float], k: int) -> tuple[float, float, float]:
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    avg = float(pos.avg)
    qty = float(pos.qty)
    used = float(_pos_total_margin(pos))
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    for i in range(k):
        if used_ord + i >= len(pos.step_margins) or i >= len(prices):
            break
        m = float(pos.step_margins[used_ord + i])
        p = float(prices[i])
        dq = (m * L) / max(p, 1e-12)
        qty_new = qty + dq
        avg = (avg * qty + p * dq) / max(qty_new, 1e-9)
        qty = qty_new
        used += m
    return avg, qty, used


def _ml_after_k(pos: "Position", bank: float, fees_est: float, targets: list[float], k: int) -> float:
    class _Tmp:
        pass

    avg, qty, used = _simulate_after_k(pos, targets, k)
    t = _Tmp()
    t.side = pos.side
    t.avg = avg
    t.qty = qty
    t.leverage = pos.leverage
    t.steps_filled = 1
    t.step_margins = [used]
    t.reserve_used = False
    t.reserve_margin_usdt = 0.0
    return ml_price_at(t, CONFIG.ML_TARGET_PCT, bank, fees_est)


def _ml_pct_after_k_at_p_k(
    pos: "Position",
    bank: float,
    fees_est: float,
    targets: list[float],
    k: int,
) -> float:
    avg, qty, used = _simulate_after_k(pos, targets, k)
    class _Tmp:
        pass

    t = _Tmp()
    t.side = pos.side
    t.avg = avg
    t.qty = qty
    t.leverage = pos.leverage
    t.steps_filled = 1
    t.step_margins = [used]
    t.reserve_used = False
    t.reserve_margin_usdt = 0.0
    if k <= 0 or k > len(targets):
        return float("nan")
    p_k = float(targets[k - 1])
    eq = equity_at_price(t, p_k, bank, fees_est)
    return (eq / used) * 100.0 if used > 0 else float("inf")


def _extract_strat_prices(targets: list[dict]) -> list[float]:
    out = []
    for t in targets:
        lab = str(t.get("label", "")).upper()
        if lab.startswith("STRAT"):
            out.append(float(t["price"]))
    return out


def _extract_first_three_strats(targets: list[dict]) -> list[float]:
    out: list[float] = []
    for t in targets or []:
        lab = str(t.get("label", "")).upper()
        if lab.startswith("STRAT"):
            try:
                out.append(float(t["price"]))
            except Exception:
                continue
            if len(out) == 3:
                break
    return out


def _ticks_between(a: float, b: float, tick: float) -> float:
    try:
        return abs((float(a) - float(b)) / max(float(tick), 1e-12))
    except Exception:
        return float("nan")


def _gap_ticks_to_ml_after_k(
    pos: "Position", bank: float, fees_est: float, strat_prices: list[float], k: int, tick: float
) -> float:
    if k <= 0 or k > len(strat_prices):
        return float("nan")
    mlk = _ml_after_k(pos, bank, fees_est, [p for p in strat_prices[:k]], k)
    if np.isnan(mlk):
        return float("nan")
    p_k = strat_prices[k - 1]
    dpx = (p_k - mlk) if pos.side == "LONG" else (mlk - p_k)
    return abs(dpx / max(tick, 1e-12))


def _strat_spacings_in_ticks(side: str, strat_prices: list[float], tick: float) -> list[float]:
    gaps = []
    for i in range(len(strat_prices) - 1):
        gaps.append(abs((strat_prices[i] - strat_prices[i + 1]) / max(tick, 1e-12)))
    return gaps


def _ml_buffer_after_3(
    pos: "Position",
    bank: float,
    fees_est: float,
    rng_strat: dict,
    t1: float,
    t2: float,
    t3: float,
) -> float:
    ml3 = _ml_after_k(pos, bank, fees_est, [t1, t2, t3], 3)
    brk = _break_price_for_side(rng_strat, pos.side)
    return ml_distance_pct(pos.side, brk, ml3)


def _linspace_exclusive(
    a: float,
    b: float,
    n: int,
    include_end: bool,
    tick: float,
    side: str,
) -> list[float]:
    if n <= 0:
        return []
    fr = [(i + 1) / n for i in range(n)] if include_end else [(i + 1) / (n + 1) for i in range(n)]
    raw = [a + (b - a) * f for f in fr]
    out = []
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    lo, hi = (min(a, b), max(a, b))
    for i, x in enumerate(raw):
        q = quantize_to_tick(x, tick)
        q = min(max(q, lo), hi)
        if i == 0:
            q = (a - min_gap) if side == "LONG" else (a + min_gap)
            q = min(max(quantize_to_tick(q, tick), lo), hi)
        if (not out) or (side == "LONG" and q <= out[-1] - min_gap) or (side == "SHORT" and q >= out[-1] + min_gap):
            out.append(q)
    return out

def auto_strat_targets_with_ml_buffer(
    pos: "Position",
    rng_strat: dict,
    entry: float,
    tick: float,
    bank: float,
    fees_est: float,
) -> list[dict]:
    """
    --- ПРАВКА 6: Упрощение.
    Ищем ТОЛЬКО ОДИН уровень (старый STRAT 2) и возвращаем его.
    ML буферы (thr2, thr3) теперь проверяются для ОДНОГО уровня.
    """
    side = pos.side
    hc = float(entry)
    atr = float(rng_strat.get("atr1h", 0.0)) or 0.0

    g_step = max(tick * max(2, CONFIG.DCA_MIN_GAP_TICKS), atr * 0.01)
    # --- Изменяем стартовый g: ищем старый STRAT 2, т.е. шаг 2*g ---
    g = max(g_step, hc * CONFIG.MIN_SPACING_PCT + tick * max(2, CONFIG.DCA_MIN_GAP_TICKS))

    def _target_at_g(gv: float) -> float:
        # --- Ищем старый STRAT 2, т.е. 2 * g ---
        if side == "LONG":
            return quantize_to_tick(hc - 2 * gv, tick)
        else:
            return quantize_to_tick(hc + 2 * gv, tick)

    thr3 = CONFIG.ML_BREAK_BUFFER_PCT # (Оставляем старые названия, но это теперь для p1)
    thr2 = CONFIG.ML_BREAK_BUFFER_PCT2
    max_depth = max(atr * 4.0, g_step * 60)
    moved = 0.0

    # подберём шаг g так, чтобы выдержать и «коридор», и ML-буферы
    while moved <= max_depth:
        p1 = _target_at_g(g)

        # Требование минимального зазора между HC и STRAT#1
        min_total = (
            max(tick * CONFIG.DCA_MIN_GAP_TICKS, hc * CONFIG.MIN_SPACING_PCT)
            + max(tick * CONFIG.DCA_MIN_GAP_TICKS, p1 * CONFIG.MIN_SPACING_PCT)
        )
        ok_corridor = (abs(p1 - hc) >= min_total)

        # Буфер ML (проверяем для p1 как для старого p2)
        buf2 = ml_distance_pct(
            side, _break_price_for_side(rng_strat, side), _ml_after_k(pos, bank, fees_est, [p1], 1)
        )
        ok_ml = (pd.notna(buf2) and buf2 >= thr2)

        if ok_corridor and ok_ml:
            break

        g += g_step
        moved += g_step
    else:
        # Если не нашли «идеальный» g, берём последний рассчитанный
        p1 = _target_at_g(g)

    labels = ["STRAT 1"]
    return [
        {"price": p1, "label": labels[0]},
    ]

def _equalize_p3_to_gap_and_ml(
    pos: "Position",
    bank: float,
    fees_est: float,
    tick: float,
) -> tuple[list[dict], bool]:
    # --- ПРАВКА 7: Эта функция больше не нужна для 1 STRAT ---
    # Оставляем ее, чтобы не сломать _fit_leg... (который мы тоже удалим)
    tol = CONFIG.EQUALIZING_TOL_TICKS
    strat = _extract_first_three_strats(pos.ordinary_targets)
    if not strat:
        return pos.ordinary_targets, True # OK
    if len(strat) < 3:
        return pos.ordinary_targets, True # OK

    p1, p2, p3 = strat
    gap12 = _ticks_between(p1, p2, tick)
    if gap12 < CONFIG.DCA_MIN_GAP_TICKS:
        return pos.ordinary_targets, False
    p3_target = quantize_to_tick(p2 - gap12 * tick, tick) if pos.side == "LONG" else quantize_to_tick(p2 + gap12 * tick, tick)
    ml3 = _ml_after_k(pos, bank, fees_est, [p1, p2, p3_target], 3)
    if np.isnan(ml3):
        return pos.ordinary_targets, False
    gap23 = _ticks_between(p2, p3_target, tick)
    gapML = _ticks_between(p3_target, ml3, tick)
    ok = (abs(gap23 - gap12) <= tol) and (abs(gapML - gap12) <= tol)
    idxs = [
        i
        for i, t in enumerate(pos.ordinary_targets)
        if str(t.get("label", "")).upper().startswith("STRAT")
    ][:3]
    labels = [pos.ordinary_targets[i]["label"] for i in idxs]
    new_vals = [p1, p2, p3_target]
    for j, i in enumerate(idxs):
        pos.ordinary_targets[i]["price"] = quantize_to_tick(new_vals[j], tick)
        pos.ordinary_targets[i]["label"] = labels[j]
        pos.ordinary_targets[i]["reserve3"] = True if j == 2 else pos.ordinary_targets[i].pop("reserve3", None)
    _sync_reserve3_flags(pos)
    return pos.ordinary_targets, ok


def _strat_report_text(
    pos: "Position",
    px: float,
    tick: float,
    bank: float,
    fees_est: float,
    rng_strat: dict,
    hdr: str,
) -> str:
    lines = [hdr]
    base_off = getattr(pos, "ordinary_offset", 0)
    # --- ПРАВКА 8: Показываем 3 уровня (HC, TAC1, TAC2, STRAT1) ---
    num_targets_to_show = 3
    tgts = pos.ordinary_targets[base_off : base_off + num_targets_to_show]
    if getattr(pos, "hedge_close_px", None) is not None:
        dt = abs((pos.hedge_close_px - px) / max(tick, 1e-12))
        dp = abs((pos.hedge_close_px / max(px, 1e-12) - 1.0) * 100.0)
        lines.append(f"HC) <code>{fmt(pos.hedge_close_px)}</code> — Δ≈ {dt:.0f} тик. ({dp:.2f}%)")
    for i, t in enumerate(tgts, start=1):
        dt = abs((t["price"] - px) / max(tick, 1e-12))
        dp = abs((t["price"] / max(px, 1e-12) - 1.0) * 100.0)
        lines.append(f"{i}) <code>{fmt(t['price'])}</code> ({t['label']}) — Δ≈ {dt:.0f} тик. ({dp:.2f}%)")
    ml_now = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_est)

    max_k = 3
    avail = min(
        max_k,
        len(pos.step_margins)
        - (pos.steps_filled - (1 if pos.reserve_used else 0)),
        len(tgts),
    )
    ks = tuple(range(1, avail + 1))
    scen = _ml_multi_scenarios(pos, bank, fees_est, k_list=ks) if avail > 0 else {}

    arrow = "↓" if pos.side == "LONG" else "↑"
    dist_now = ml_distance_pct(pos.side, px, ml_now)
    dist_txt = "N/A" if np.isnan(dist_now) else f"{dist_now:.2f}%"
    lines.append(f"ML(20%): {arrow}<code>{fmt(ml_now)}</code> ({dist_txt} от текущей)")

    ml_lines = []
    if 1 in scen:
        ml_lines.append(f"+1: {('N/A' if scen.get(1) is None or np.isnan(scen.get(1)) else fmt(scen.get(1)))}")
    if 2 in scen:
        ml_lines.append(f"+2: {('N/A' if scen.get(2) is None or np.isnan(scen.get(2)) else fmt(scen.get(2)))}")
    if 3 in scen:
        ml_lines.append(f"+3: {('N/A' if scen.get(3) is None or np.isnan(scen.get(3)) else fmt(scen.get(3)))}")
    
    lines.append(f"ML после: {' | '.join(ml_lines)}")

    # --- ПРАВКА 9: Убираем проверку буфера p1,p2,p3 ---
    # (т.к. у нас остался только 1 STRAT)
    
    return "\n".join(lines)


class FSM(IntEnum):
    IDLE = 0
    OPENED = 1
    MANAGING = 2


def _sync_reserve3_flags(pos: "Position"):
    # --- ПРАВКА 10: Убираем логику reserve3 ---
    pos.reserve3_price = None
    pos.reserve3_armed = False
    pos.reserve3_ready = False
    pos.reserve3_done = False
    # for t in pos.ordinary_targets:
    #     if t.get("reserve3"):
    #         pos.reserve3_price = float(t["price"])
    #         break


class Position:
    def __init__(
        self,
        side: str,
        signal_id: str,
        leverage: int | None = None,
        owner_key: str | None = None,
    ):
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
        self.break_dir: str | None = None
        self.break_confirm_bars: int = 0
        self.extension_planned: bool = False
        self.ordinary_offset: int = 0
        self.last_filled_q: float | None = None
        self.last_px: float | None = None
        self.hedge_entry_px: float | None = None
        self.from_hedge: bool = False
        self.hedge_close_px: float | None = None
        self.reserve3_price: float | None = None
        self.reserve3_armed: bool = False
        self.reserve3_ready: bool = False
        self.reserve3_done: bool = False
        self.manual_tac_price: float | None = None
        self.manual_tac2_price: float | None = None

    def plan_with_reserve(self, bank: float, growth: float, ord_levels: int):
        # --- ПРАВКА 11: Убираем логику резерва, планируем ВСЕ 4 уровня ---
        self.growth = growth # growth теперь 1.0
        self.ord_levels = max(1, int(ord_levels)) # ord_levels = 3 (доборы)
        total_target = bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
        
        # Общее кол-во уровней = 1 (нога) + 3 (доборы) = 4
        total_levels = 1 + self.ord_levels
        
        # Делим бюджет равномерно на 4 части
        margins_full = plan_margins_bank_first(total_target, total_levels, growth)
        
        # Все 4 части идут в step_margins
        self.step_margins = margins_full
        
        # Резерва нет
        self.reserve_margin_usdt = 0.0
        self.reserve_available = False
        self.reserve_used = False
        self.freeze_ordinary = False
        self.max_steps = total_levels # Макс шагов = 4

    def rebalance_tail_margins_excluding_reserve(self, bank: float):
        # --- ПРАВКА 12: Ребаланс хвоста теперь тоже равномерный ---
        total_target = bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
        used_ord_count = max(0, self.steps_filled - (1 if self.reserve_used else 0))
        used_ord_sum = (
            sum(self.step_margins[: min(used_ord_count, len(self.step_margins))])
            if self.step_margins
            else 0.0
        )
        
        # Бюджет на оставшиеся уровни (включая ногу, если она не заполнена)
        remaining_budget_for_ord = max(0.0, total_target - used_ord_sum)
        
        # Общее кол-во уровней = 4 (1 нога + 3 добора)
        total_levels = 1 + self.ord_levels
        remaining_levels = max(0, total_levels - used_ord_count)
        
        if remaining_levels <= 0:
            return
            
        # Делим остаток бюджета РАВНОМЕРНО (growth=1.0)
        tail = plan_margins_bank_first(remaining_budget_for_ord, remaining_levels, 1.0)
        
        self.step_margins = (self.step_margins[:used_ord_count] or []) + tail
        self.max_steps = total_levels
        
        # Резерва нет
        self.reserve_margin_usdt = 0.0
        self.reserve_available = False


    def add_step(self, price: float):
        used_ord_count = self.steps_filled - (1 if self.reserve_used else 0)
        if used_ord_count >= len(self.step_margins):
            raise RuntimeError("No ordinary steps left to add")
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
        # --- ПРАВКА 13: Эта функция больше не должна вызываться ---
        log.warning("add_reserve_step() called, but reserve logic is disabled!")
        return 0.0, 0.0


def _ml_multi_scenarios(pos: "Position", bank: float, fees_est: float, k_list=(1, 2, 3)) -> dict[int, float]:
    out: dict[int, float] = {}

    avg = float(pos.avg)
    qty = float(pos.qty)
    used = float(_pos_total_margin(pos))
    L = max(1, int(getattr(pos, "leverage", 1) or 1))

    base = getattr(pos, "ordinary_offset", 0)
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    next_idx = used_ord

    max_k = max(k_list) if k_list else 0

    for k in range(1, max_k + 1):
        if next_idx + (k - 1) >= len(pos.step_margins):
            break
        if base + (k - 1) >= len(pos.ordinary_targets):
            break

        m = float(pos.step_margins[next_idx + (k - 1)])
        p = float(pos.ordinary_targets[base + (k - 1)]["price"])

        dq = (m * L) / max(p, 1e-12)
        qty_new = qty + dq
        avg_new = (avg * qty + p * dq) / max(qty_new, 1e-9)
        used_new = used + m

        if k in k_list:
            class _Tmp:
                pass

            tmp = _Tmp()
            tmp.side = pos.side
            tmp.avg = avg_new
            tmp.leverage = L
            tmp.qty = qty_new
            tmp.steps_filled = 1
            tmp.step_margins = [used_new]
            tmp.reserve_used = False
            tmp.reserve_margin_usdt = 0.0

            out[k] = ml_price_at(tmp, CONFIG.ML_TARGET_PCT, bank, fees_est)

        avg, qty, used = avg_new, qty_new, used_new

    return out


def _count_tacs(targets: list[dict]) -> int:
    return sum(1 for t in targets if str(t.get("label", "")).upper().startswith("TAC"))


# ---------------------------------------------------------------------------
# Broadcasting helpers
# ---------------------------------------------------------------------------
def _make_say(app, default_chat_id: int | None):
    """
    Возвращает функцию, которую можно вызывать просто так:
        await say("текст")
    или
        await say("текст", target_chat_id=...)
    """
    async def say(text: str, target_chat_id: int | None = None):
        cid = target_chat_id or default_chat_id
        if cid is None:
            log.warning("[broadcast] no chat_id, msg=%r", text)
            return
        try:
            await app.bot.send_message(chat_id=cid, text=text, parse_mode="HTML")
        except Exception as e:
            log.error("[broadcast] send failed: %s", e)

    return say


# ---------------------------------------------------------------------------
# HEDGE helpers
# ---------------------------------------------------------------------------

# --- ИСПРАВЛЕНИЕ 2 ---
# Логика закрытия хеджа теперь "внутрь" диапазона
def planned_hc_price(entry: float, tac_lo: float, tac_hi: float,
                     bias: str, mode: str, tick: float) -> float:
    span = float(tac_hi) - float(tac_lo)
    if bias == "LONG":
        px = entry - span * CONFIG.HEDGE_CLOSE_FRAC
    else:
        px = entry + span * CONFIG.HEDGE_CLOSE_FRAC
    return quantize_to_tick(px, tick)
# ---------------------


def _sum_first_n(lst: list[float], n: int) -> float:
    return sum(lst[: max(0, min(n, len(lst)))]) if lst else 0.0


def _wick_reached(side: str, tgt: float, lo: float, hi: float, tick: float, hyst_ticks: int = 1) -> bool:
    if any(v is None or np.isnan(v) for v in (tgt, lo, hi)):
        return False
    buf = max(1, int(hyst_ticks)) * tick
    if side == "LONG":
        return lo <= (tgt - buf)
    else:
        return hi >= (tgt + buf)


def _tp_sl_hit(
    side: str,
    tp: float | None,
    sl: float | None,
    lo: float,
    hi: float,
    tick: float,
    hyst_ticks: int = 1,
) -> tuple[bool, bool]:
    buf = max(1, int(hyst_ticks)) * tick
    tp_hit = False
    sl_hit = False
    if tp is not None and not np.isnan(tp):
        if side == "LONG":
            tp_hit = hi >= (tp + buf)
        else:
            tp_hit = lo <= (tp - buf)
    if sl is not None and not np.isnan(sl):
        if side == "LONG":
            sl_hit = lo <= (sl - buf)
        else:
            sl_hit = hi >= (sl + buf)
    return tp_hit, sl_hit


def _count_strats(targets: list[dict]) -> int:
    return sum(1 for t in targets if str(t.get("label", "")).upper().startswith("STRAT"))


def _plan_with_leg(
    symbol: str,
    leg_margin: float,
    remain_side: str,
    entry_px: float,
    close_px: float,
    bank: float,
    rng_strat: dict,
    tick: float,
    growth: float,
) -> tuple["Position", list[dict], float]:
    pos = Position(remain_side, signal_id="SIZER", leverage=CONFIG.LEVERAGE, owner_key="SIZER")
    
    # --- ПРАВКА 14: Используем growth (1.0) и levels (3) из CONFIG ---
    pos.plan_with_reserve(bank, CONFIG.GROWTH_AFTER_HEDGE, CONFIG.STRAT_LEVELS_AFTER_HEDGE)
    
    # Устанавливаем маржу ноги (1-й уровень из 4)
    pos.step_margins[0] = float(leg_margin)
    _ = pos.add_step(entry_px)
    
    # Пересчитываем хвост (3 добора) равномерно
    pos.rebalance_tail_margins_excluding_reserve(bank)
    
    # --- ПРАВКА 15: _shape_tail_from_leg() больше не нужна ---
    # _shape_tail_from_leg(pos, bank) 
    
    pos.from_hedge = True
    pos.hedge_entry_px = entry_px
    pos.hedge_close_px = close_px
    cum_margin = _pos_total_margin(pos)
    fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
    pos.ordinary_targets = build_targets_with_tactical(pos, rng_strat, close_px, tick, bank, fees_est)
    pos.ordinary_offset = 0
    _sync_reserve3_flags(pos)
    return pos, pos.ordinary_targets, fees_est


# --- ПРАВКА 16: Эта функция больше не нужна, т.к. мы не оптимизируем сетку ---
def _fit_leg_with_equalization(
    symbol: str,
    leg_margin_init: float,
    remain_side: str,
    entry_px: float,
    close_px: float,
    bank: float,
    rng_strat: dict,
    tick: float,
    growth: float,
) -> tuple[float, "Position", list[dict], float]:
    
    log.warning("Вызвана _fit_leg_with_equalization, которая устарела. Используется прямой расчет.")
    # Возвращаем прямой расчет
    return leg_margin_init, *_plan_with_leg(
        symbol, leg_margin_init, remain_side, entry_px, close_px, bank, rng_strat, tick, growth
    )


def _shape_tail_from_leg(pos: "Position", bank: float):
    # --- ПРАВКА 17: Эта функция больше не используется, но оставляем ее ---
    cfg = getattr(CONFIG, "AFTER_HEDGE_TAIL", None)
    if not cfg:
        return

    total_target = bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL

    used_ord_count = max(0, pos.steps_filled - (1 if getattr(pos, "reserve_used", False) else 0))
    if not pos.step_margins or used_ord_count <= 0:
        return

    leg = float(pos.step_margins[0])
    levels = int(cfg.get("levels", 4))
    mode = str(cfg.get("mode", "relative_vector")).lower()
    rnd = float(cfg.get("round_usd", 1.0))

    if mode == "geom":
        k1 = float(cfg.get("first_rel_to_leg", 0.345))
        g = float(cfg.get("growth", CONFIG.GROWTH_AFTER_HEDGE))
        a0 = max(0.0, leg * k1)
        tail_raw = [a0 * (g ** i) for i in range(max(0, levels))]
    else:
        coeffs = list(cfg.get("coeffs_rel_to_leg", [0.345, 0.4983333333, 0.690, 0.9583333333]))
        if levels > len(coeffs) and len(coeffs) >= 2:
            g_implied = coeffs[-1] / max(coeffs[-2], 1e-9)
            next_c = coeffs[-1] * g_implied
            coeffs.append(next_c)
        tail_raw = [max(0.0, leg * c) for c in coeffs[: max(0, levels)]]

    used_ord_sum = sum(pos.step_margins[:used_ord_count])
    max_tail_budget = max(0.0, total_target - used_ord_sum)

    raw_sum = sum(tail_raw)
    if raw_sum > max_tail_budget and raw_sum > 0:
        s = max_tail_budget / raw_sum
        tail = [t * s for t in tail_raw]
    else:
        tail = tail_raw

    if rnd > 0:
        tail = [round(t / rnd) * rnd for t in tail]

    tail_sum = sum(tail)
    if tail_sum > max_tail_budget and tail_sum > 0:
        s = max_tail_budget / tail_sum
        tail = [t * s for t in tail]

    remaining_levels = max(0, pos.ord_levels - used_ord_count)
    tail = tail[:remaining_levels]
    pos.step_margins = (pos.step_margins[:used_ord_count] or []) + tail

    used_now = sum(pos.step_margins[: pos.ord_levels])
    pos.reserve_margin_usdt = max(0.0, total_target - used_now)
    pos.reserve_available = pos.reserve_margin_usdt > 0
    pos.max_steps = pos.ord_levels + (1 if (pos.reserve_available and not pos.reserve_used) else 0)


def clip_targets_by_ml(
    pos: "Position",
    bank: float,
    fees_est: float,
    targets: list[dict],
    tick: float,
    safety_ticks: int = 2,
) -> list[dict]:
    safety_ticks_eff = safety_ticks
    if CONFIG.ML_REQ_GAP_MODE == "strat_spacing":
        strat_only = _extract_strat_prices(targets)
        if len(strat_only) >= 2:
            gap_ticks = _ticks_between(strat_only[0], strat_only[1], tick)
            if np.isfinite(gap_ticks) and gap_ticks > 0:
                safety_ticks_eff = max(safety_ticks_eff, int(round(gap_ticks)))

    if not targets:
        return targets
    used = _pos_total_margin(pos)
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    avg = pos.avg
    qty = pos.qty
    base = getattr(pos, "ordinary_offset", 0)
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    out = []
    for i, t in enumerate(targets, start=0):
        step_idx = used_ord + i
        if step_idx >= len(pos.step_margins):
            break
        price = float(t["price"])
        m = float(pos.step_margins[step_idx])
        dq = (m * L) / max(price, 1e-12)
        qty_new = qty + dq
        avg_new = (avg * qty + price * dq) / max(qty_new, 1e-9)
        used_new = used + m
        
        # --- ПРАВКА 18: Убираем логику reserve3 ---
        # if t.get("reserve3"):
        #     out.append(t)
        #     qty, avg, used = qty_new, avg_new, used_new
        #     continue
            
        class _Tmp:
            pass

        tmp = _Tmp()
        tmp.side = pos.side
        tmp.avg = avg_new
        tmp.leverage = L
        tmp.qty = qty_new
        tmp.steps_filled = 1
        tmp.step_margins = [used_new]
        tmp.reserve_used = False
        tmp.reserve_margin_usdt = 0.0
        
        ml_guard = ml_price_at(tmp, getattr(CONFIG, "ML_TARGET_PCT", 0.20), bank, fees_est)
        if np.isnan(ml_guard):
            break

        is_tac = str(t.get("label", "")).upper().startswith("TAC")
        if is_tac:
            safety = tick * max(1, safety_ticks_eff // 2)
        else:
            safety = tick * max(1, safety_ticks_eff)

        ok = (price > ml_guard + safety) if pos.side == "LONG" else (price < ml_guard - safety)
        if not ok:
            break
        out.append(t)
        qty, avg, used = qty_new, avg_new, used_new
    return out


# ---------------------------------------------------------------------------
# Служебные штуки цикла: m15, ручные команды
# ---------------------------------------------------------------------------
async def _m15_state_step(b: dict, symbol: str, say):
    try:
        now = time.time()
        if now - b["m15_state"].get("last_fetch", 0.0) >= CONFIG.M15_RECO["CHECK_EVERY_SEC"]:
            m15_df = await maybe_await(fetch_ohlcv, symbol, "15m", max(50, CONFIG.M15_RECO["ATR_LEN"] + 10))
            b["m15_state"]["last_fetch"] = now
            if m15_df is not None and not m15_df.empty:
                d = m15_df.iloc[:, -5:].copy()
                d.columns = ["open", "high", "low", "close", "volume"]
                atr15 = ta_atr(d["high"], d["low"], d["close"], length=CONFIG.M15_RECO["ATR_LEN"]).iloc[-1]
                hi = float(d["high"].iloc[-1])
                lo = float(d["low"].iloc[-1])
                rng = hi - lo

                bar_id = d.index[-1]
                if b["m15_sig"].get("bar_id") != bar_id:
                    b["m15_sig"].update({"bar_id": bar_id, "atr_sent": False})

                if atr15 > 0 and rng >= CONFIG.M15_RECO["TRIGGER_MULT"] * float(atr15):
                    if not b["m15_sig"]["atr_sent"]:
                        dir_txt = "вверх" if (d["close"].iloc[-1] >= d["open"].iloc[-1]) else "вниз"
                        await say(f"📢 Бар выше 1.5 ATR {dir_txt}")
                        b["m15_sig"]["atr_sent"] = True

                fr_up, fr_dn = compute_fractals_15m(d)
                last_up, last_dn = _last_confirmed_fractals(fr_up, fr_dn)

                st = b["м15_sig"] if "м15_sig" in b else b["m15_sig"]

                if last_up is not None:
                    if st.get("last_fr_up") is None or abs(st["last_fr_up"] - last_up) > 1e-9:
                        st["last_fr_up"] = float(last_up)
                        st["up_side"] = "above" if hi >= last_up else "below"
                    else:
                        now_side = "above" if hi >= last_up else "below"
                        prev_side = st.get("up_side")
                        if prev_side == "below" and now_side == "above":
                            await say(f"📐 Фрактал вверх пройден: {fmt(last_up)}")
                        st["up_side"] = now_side

                if last_dn is not None:
                    if st.get("last_fr_dn") is None or abs(st["last_fr_dn"] - last_dn) > 1e-9:
                        st["last_fr_dn"] = float(last_dn)
                        st["dn_side"] = "below" if lo <= last_dn else "above"
                    else:
                        now_side = "below" if lo <= last_dn else "above"
                        prev_side = st.get("dn_side")
                        if prev_side == "above" and now_side == "below":
                            await say(f"📐 Фрактал вниз пройден: {fmt(last_dn)}")
                        st["dn_side"] = now_side
    except Exception:
        log.exception("m15 recommendations failed")


async def _handle_manual_commands(
    b: dict,
    symbol: str,
    say,
    px: float,
    tick: float,
    bank: float,
    rng_strat: dict,
    rng_tac: dict,  # <— добавили
    _alloc_bank,
):
    _ = _alloc_bank
    pos: Position | None = b.get("position")

    # --- Обработка /hedge_flip и /hedge_close с пересчётом HC внутрь TAC ---
    pending_bias = b.pop("pending_hedge_bias", None)
    pending_hc = b.pop("pending_hc_price", None)

    if pos and pos.from_hedge and (pending_bias or pending_hc):
        entry_px = pos.hedge_entry_px or pos.avg or px
        
        # --- ПРАВКА 19: Маржа ноги теперь ПЕРВЫЙ элемент из 4-х ---
        # (вместо leg_margin = float(pos.step_margins[0]))
        # Рассчитываем 1/4 бюджета
        total_target = bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
        leg_margin = total_target / 4.0 # 1/4
        
        remain_side = pending_bias or pos.side

        if pending_hc is not None:
            new_close = float(pending_hc)
        else:
            # ➊ при flip без /hedge_close — пересчитать HC
            if rng_tac:
                new_close = planned_hc_price(
                    px, 
                    rng_tac["lower"],
                    rng_tac["upper"],
                    remain_side,
                    CONFIG.HEDGE_MODE,
                    tick,
                )
            else:
                # fallback: текущая рыночная
                new_close = px

        # ➋ сбросить ручные TAC’и, чтобы не перекрывали автогенерацию
        pos.manual_tac_price = None
        pos.manual_tac2_price = None

        # ➌ пересобрать план на новую сторону с новым HC
        new_pos, new_targets, fees_est = _plan_with_leg(
            symbol=symbol,
            leg_margin=leg_margin,
            remain_side=remain_side,
            entry_px=entry_px,
            close_px=new_close,
            bank=bank,
            rng_strat=rng_strat,
            tick=tick,
            growth=CONFIG.GROWTH_AFTER_HEDGE, # (growth=1.0)
        )

        new_pos.ordinary_targets = clip_targets_by_ml(new_pos, bank, fees_est, new_targets, tick)
        _sync_reserve3_flags(new_pos)

        b["position"] = new_pos
        b["fsm_state"] = int(FSM.MANAGING)

        lots_leg = margin_to_lots(symbol, leg_margin, price=entry_px, leverage=new_pos.leverage)
        levels_block = render_hedge_preview_block(
            symbol, new_pos, bank, fees_est, tick, new_close, lots_leg, leg_margin
        )
        await say(
            "♻️ План после ручного изменения хеджа\n"
            f"Bias: <b>{remain_side}</b>\n"
            f"HC: <code>{fmt(new_close)}</code>\n"
            f"{levels_block}"
        )
        return

    if pos and b.get("force_close"):
        if (not pos) or pos.steps_filled <= 0 or (b.get("fsm_state") not in (int(FSM.OPENED), int(FSM.MANAGING))):
            return
        exit_p = px
        time_min = (time.time() - pos.open_ts) / 60.0
        net_usd, net_pct = compute_net_pnl(pos, exit_p, CONFIG.FEE_TAKER, CONFIG.FEE_TAKER)
        await say(
            "🧰 <b>MANUAL_CLOSE</b>\n"
            f"Цена выхода: <code>{fmt(exit_p)}</code>\n"
            f"P&L (net)≈ {net_usd:+.2f} USD ({net_pct:+.2f}%)\n"
            f"Время в сделке: {time_min:.1f} мин"
        )
        b["force_close"] = False
        if CONFIG.REQUIRE_MANUAL_REOPEN_ON.get("manual_close", True):
            b["user_manual_mode"] = True
        pos.last_sl_notified_price = None
        b["position"] = None
        b["fsm_state"] = int(FSM.IDLE)
        return

    if not pos or not pos.from_hedge or b.get("fsm_state") != int(FSM.MANAGING):
        b.pop("cmd_strat_show", None)
        b.pop("cmd_strat_set", None)
        b.pop("cmd_strat_reset", None)
        b.pop("cmd_tac_set", None)
        b.pop("cmd_tac2_set", None)
        b.pop("cmd_tac_reset", None)
        return

    if b.pop("cmd_strat_show", False):
        cum_margin = _pos_total_margin(pos)
        fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
        await say(_strat_report_text(pos, px, tick, bank, fees_est, rng_strat, hdr="📋 STRAT (текущий план)"))

    _set_req = b.pop("cmd_strat_set", None)
    if _set_req is not None:
        # --- ПРАВКА 20: Упрощаем /strat set (только 1 цена) ---
        vals = []
        for v in _set_req:
            try:
                vals.append(float(str(v).replace(",", ".").strip()))
            except Exception:
                pass
        if len(vals) < 1:
            await say("❗ Укажите 1 цену: /strat set P1")
        else:
            q = quantize_to_tick(vals[0], tick) # Берем только первую цену
            
            base_off = getattr(pos, "ordinary_offset", 0)
            strat_idx = -1
            for j in range(base_off, len(pos.ordinary_targets)):
                 if str(pos.ordinary_targets[j].get("label", "")).startswith("STRAT"):
                    strat_idx = j
                    break

            if strat_idx == -1:
                 return await say("❗ Не найден STRAT 1 в текущем плане для замены.")
            
            # Проверка, что цена не слишком близко к TAC2
            tac2_idx = strat_idx - 1
            if tac2_idx >= 0:
                prev = pos.ordinary_targets[tac2_idx]["price"]
                min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
                if pos.side == "LONG" and q > prev - min_gap:
                    q = prev - min_gap
                if pos.side == "SHORT" and q < prev + min_gap:
                    q = prev + min_gap
            
            q_fixed = quantize_to_tick(q, tick)
            
            pos.ordinary_targets[strat_idx] = {"price": q_fixed, "label": "STRAT 1"}
            _sync_reserve3_flags(pos)

            pos.rebalance_tail_margins_excluding_reserve(bank)
            cum_margin = _pos_total_margin(pos)
            fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
            pos.ordinary_targets = clip_targets_by_ml(pos, bank, fees_est, pos.ordinary_targets, tick)
            _sync_reserve3_flags(pos)
            await say(_strat_report_text(pos, px, tick, bank, fees_est, rng_strat, hdr="✏️ STRAT 1 обновлён (маржа хвоста пересчитана)"))

    if b.pop("cmd_strat_reset", False):
        cum_margin = _pos_total_margin(pos)
        fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
        close_px = pos.hedge_close_px or pos.avg
        pos.manual_tac_price = None
        pos.manual_tac2_price = None
        pos.ordinary_targets = build_targets_with_tactical(pos, rng_strat, close_px, tick, bank, fees_est)
        pos.ordinary_offset = min(getattr(pos, "ordinary_offset", 0), len(pos.ordinary_targets))
        _sync_reserve3_flags(pos)
        await say(_strat_report_text(pos, px, tick, bank, fees_est, rng_strat, hdr="♻️ STRAT сброшен к авто-плану"))

    _tac_req = b.pop("cmd_tac_set", None)
    _tac2_req = b.pop("cmd_tac2_set", None)

    rebuild_tac = False
    tac_msg = ""

    if _tac_req is not None:
        v = None
        for it in (list(_tac_req) if isinstance(_tac_req, (list, tuple)) else [_tac_req]):
            try:
                v = float(str(it).replace(",", ".").strip())
                break
            except Exception:
                pass
        if v is None:
            await say("❗ Укажите цену TAC #1: /tac set PRICE")
        else:
            tac_q = quantize_to_tick(v, tick)
            pos.manual_tac_price = tac_q
            rebuild_tac = True
            tac_msg = f"✏️ TAC #1 обновлён вручную → <code>{fmt(tac_q)}</code>\n"

    if _tac2_req is not None:
        v = None
        for it in (list(_tac2_req) if isinstance(_tac2_req, (list, tuple)) else [_tac2_req]):
            try:
                v = float(str(it).replace(",", ".").strip())
                break
            except Exception:
                pass
        if v is None:
            await say("❗ Укажите цену TAC #2: /tac2 set PRICE")
        else:
            tac_q = quantize_to_tick(v, tick)
            pos.manual_tac2_price = tac_q
            rebuild_tac = True
            if not tac_msg:
                tac_msg = f"✏️ TAC #2 обновлён вручную → <code>{fmt(tac_q)}</code>\n"
            else:
                tac_msg += f"✏️ TAC #2 обновлён вручную → <code>{fmt(tac_q)}</code>\n"

    if b.pop("cmd_tac_reset", False):
        pos.manual_tac_price = None
        pos.manual_tac2_price = None
        rebuild_tac = True
        tac_msg = "♻️ TAC сброшен к авто-плану\n"

    if rebuild_tac:
        pos.rebalance_tail_margins_excluding_reserve(bank)
        
        cum_margin = _pos_total_margin(pos)
        fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
        base_close = pos.hedge_close_px or pos.avg

        pos.ordinary_targets = build_targets_with_tactical(pos, rng_strat, base_close, tick, bank, fees_est)
        pos.ordinary_targets = clip_targets_by_ml(pos, bank, fees_est, pos.ordinary_targets, tick)
        pos.ordinary_offset = min(getattr(pos, "ordinary_offset", 0), len(pos.ordinary_targets))
        _sync_reserve3_flags(pos)

        ml_now = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_est)
        ml_reserve = ml_reserve_pct_to_ml20(pos, px, bank, fees_est)
        ml_arrow = "↓" if pos.side == "LONG" else "↑"
        dist_now = ml_distance_pct(pos.side, px, ml_now)
        dist_txt = "N/A" if np.isnan(dist_now) else f"{dist_now:.2f}%"
        levels_block = render_remaining_levels_block(symbol, pos, bank, CONFIG.FEE_TAKER, tick)
        planned_now = _count_strats(pos.ordinary_targets[getattr(pos, "ordinary_offset", 0) :])

        await say(
            f"{tac_msg}"
            f"Средняя: <code>{fmt(pos.avg)}</code> (P/L 0) | TP: <code>{fmt(pos.tp_price)}</code>\n"
            f"ML(20%): {ml_arrow}<code>{fmt(ml_now)}</code> ({dist_txt} от текущей)\n"
            f"Запас маржи до ML20%: <b>{('∞' if not np.isfinite(ml_reserve) else f'{ml_reserve:.1f}%')}</b>\n"
            f"{levels_block}\n"
            f"(Осталось: {planned_now} из 1)" # Обновлено
        )


# ---------------------------------------------------------------------------
# Главный цикл сканера
# ---------------------------------------------------------------------------
async def _scanner_main(app, ns_key: str):
    """
    Главный цикл сканера: цены → диапазоны → входы → хедж/план → ручные команды.
    """
    bot_data = app.bot_data
    box = bot_data[BOXES_KEY][ns_key]
    chat_id = box["chat_id"]
    symbol = box["symbol"]

    say = box.get("say") or _make_say(app, chat_id)

    tick = default_tick(symbol) or 0.0001

    await say(f"⚙️ Сканер для <b>{symbol}</b> запущен.")

    # Первичная подкачка диапазонов
    rng_strat, rng_tac = await build_ranges(symbol)
    box["rng_strat"] = rng_strat
    box["rng_tac"] = rng_tac
    box["last_ranges_ts"] = time.time()
    box["m15_state"] = {}
    box["m15_sig"] = {}
    box.setdefault("user_manual_mode", False)
    box.setdefault("fsm_state", int(FSM.IDLE))
    box.setdefault("position", None)

    banks = app.bot_data.get(BANKS_KEY, {})
    fresh_bank = banks.get(ns_key)  # ns_key = _ns(chat_id, symbol)
    if fresh_bank is not None:
        box["bank_usd"] = float(fresh_bank)
    bank = float(box.get("bank_usd", CONFIG.SAFETY_BANK_USDT))
    
    # Снимок порогов
    if rng_strat and rng_tac:
        tac_lo = rng_tac["lower"]
        tac_hi = rng_tac["upper"]
        tac_w = tac_hi - tac_lo
        long_thr = quantize_to_tick(tac_lo + tac_w * 0.30, tick)
        short_thr = quantize_to_tick(tac_lo + tac_w * 0.70, tick)
        brk_up, brk_dn = break_levels(rng_strat)
        await say(
            "🎯 Пороги входа (TAC 30/70): "
            f"LONG ≤ <b>{fmt(long_thr)}</b>, SHORT ≥ <b>{fmt(short_thr)}</b>\n"
            "🧪 Диапазоны:\n"
            f"• STRAT: [{fmt(rng_strat['lower'])} … {fmt(rng_strat['upper'])}] w={rng_strat['width']:.5f}\n"
            f"• TAC (3d): [{fmt(tac_lo)} … {fmt(tac_hi)}] w={tac_w:.5f} (≈{tac_w / max(rng_strat['width'],1e-9):.0%} от STRAT)\n"
            f"🔐 Пробой STRAT: ↑{fmt(brk_up)} | ↓{fmt(brk_dn)}"
        )

    # основной цикл
    while not box.get("stop_flag"):
        # 1) обновление диапазонов раз в N минут
        now = time.time()
        if now - box.get("last_ranges_ts", 0) >= CONFIG.REBUILD_TACTICAL_EVERY_MIN * 60:
            try:
                rng_strat, rng_tac = await build_ranges(symbol)
                box["rng_strat"] = rng_strat
                box["rng_tac"] = rng_tac
                box["last_ranges_ts"] = now
            except Exception as e:
                log.exception("rebuild ranges failed")
                await say(f"⚠️ Не удалось обновить диапазоны: {e}")

        rng_strat = box.get("rng_strat")
        rng_tac = box.get("rng_tac")

        # 2) текущая цена
        try:
            df_entry = await maybe_await(fetch_ohlcv, symbol, CONFIG.TF_ENTRY, 5)
            if df_entry is None or df_entry.empty:
                raise RuntimeError("нет данных по символу")
            last_row = df_entry.iloc[-1]
            px = float(last_row["close"])
        except Exception as e:
            log.exception("price fetch failed")
            await say(f"⚠️ Ошибка получения цены: {e}")
            await asyncio.sleep(5)
            continue

        box["last_px"] = px

        # 3) M15-сигналы
        await _m15_state_step(box, symbol, say)

        banks = app.bot_data.get(BANKS_KEY, {})
        fresh_bank = banks.get(ns_key)
        if fresh_bank is not None:
            box["bank_usd"] = float(fresh_bank)
        bank = float(box.get("bank_usd", CONFIG.SAFETY_BANK_USDT))
        
        # 4) Ручные команды, если есть позиция
        if rng_strat:
            await _handle_manual_commands(
                box,
                symbol,
                say,
                px,
                tick,
                bank,
                rng_strat,
                rng_tac,  # ← передаём тактический диапазон
                None,
            )

        pos: Position | None = box.get("position")

        # 5) Если позиции нет и не стоит ручной режим — проверяем вход по TAC
        if pos is None and not box.get("user_manual_mode", False) and rng_tac and rng_strat:
            tac_lo = rng_tac["lower"]
            tac_hi = rng_tac["upper"]
            tac_w = tac_hi - tac_lo
            long_thr = quantize_to_tick(tac_lo + tac_w * 0.30, tick)
            short_thr = quantize_to_tick(tac_lo + tac_w * 0.70, tick)

            dist_long = max(0.0, long_thr - px)
            dist_short = max(0.0, px - short_thr)
            pct_to_short = (dist_short / px) * 100.0 if px else 0.0

            await say(
                f"Текущая: <b>{fmt(px)}</b>. "
                f"До LONG: {fmt(dist_long)} ({(dist_long/px*100 if px else 0):.2f}%), "
                f"до SHORT: {fmt(dist_short)} ({pct_to_short:.2f}%)."
            )

            # --- вход по нижней границе (LONG-бейс)
            if px <= long_thr + tick * CONFIG.WICK_HYST_TICKS:
                # мы в нижней части → хотим оставить LONG
                bias = "LONG"
                
                # --- ПРАВКА 21: Прямой расчет ноги (1/4 бюджета) ---
                total_target = bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
                total_levels = 1 + CONFIG.STRAT_LEVELS_AFTER_HEDGE # 1+3=4
                leg_margin = total_target / total_levels # 1/4 бюджета
                
                # Вызываем _plan_with_leg напрямую, без _fit_leg...
                pos_new, targets_new, fees_est = _plan_with_leg(
                    symbol,
                    leg_margin,
                    remain_side=bias,
                    entry_px=px,
                    close_px=planned_hc_price(px, tac_lo, tac_hi, bias, CONFIG.HEDGE_MODE, tick),
                    bank=bank,
                    rng_strat=rng_strat,
                    tick=tick,
                    growth=CONFIG.GROWTH_AFTER_HEDGE, # (growth=1.0)
                )
                
                # подрежем по ML
                pos_new.ordinary_targets = clip_targets_by_ml(pos_new, bank, fees_est, targets_new, tick)
                box["position"] = pos_new
                box["fsm_state"] = int(FSM.MANAGING)

                lots_leg = margin_to_lots(symbol, leg_margin, price=px, leverage=pos_new.leverage)
                hc_price = pos_new.hedge_close_px
                levels_block = render_hedge_preview_block(
                    symbol,
                    pos_new,
                    bank,
                    fees_est,
                    tick,
                    hc_price,
                    lots_leg,
                    leg_margin,
                )

                await say(
                    "🧱 <b>HEDGE OPEN [SHORT]</b>\n"
                    f"Цена: <code>{fmt(px)}</code> | Обе ноги по <b>{lots_leg:.2f}</b> lot\n"
                    f"Депозит (суммарно): <b>{leg_margin*2:.2f} USD</b> (по {leg_margin:.2f} на ногу)\n"
                    f"{levels_block}"
                )

        # 6) если позиция есть — обновим статус
        pos = box.get("position")
        if pos is not None:
            cum_margin = _pos_total_margin(pos)
            fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
            _update_status_snapshot(
                box,
                symbol=symbol,
                bank_fact=bank,
                bank_target=bank,
                pos=pos,
                scan_paused=False,
                rng_strat=rng_strat,
                rng_tac=rng_tac,
            )
            # можно раскомментировать для частых апдейтов:
            # await say(box["status_line"])

        await asyncio.sleep(CONFIG.SCAN_INTERVAL_SEC)

    # если вышли из цикла — сканер остановлен
    await say(f"⏹ Сканер для {symbol} остановлен.")


# ---------------------------------------------------------------------------
# ПУБЛИЧНО: старт, стоп, проверка
# ---------------------------------------------------------------------------
def _parse_chat_and_symbol(args, kwargs):
    """
    Поддерживаем ВСЕ варианты вызова:
    1) start_scanner_for_pair(app, "EURUSD", -123456)
    2) start_scanner_for_pair(app, -123456, "EURUSD")
    3) start_scanner_for_pair(app, symbol="EURUSD", chat_id=-123456)
    4) start_scanner_for_pair(app, chat_id=-123456, symbol="EURUSD")
    """
    sym = kwargs.get("symbol")
    cid = kwargs.get("chat_id")

    if sym is not None and cid is not None:
        return int(cid), str(sym).upper()

    if len(args) >= 2:
        a1, a2 = args[0], args[1]
        if isinstance(a1, str) and isinstance(a2, int):
            return a2, a1.upper()
        if isinstance(a1, int) and isinstance(a2, str):
            return a1, a2.upper()
        if sym is None:
            sym = a1 if isinstance(a1, str) else a2
        if cid is None:
            cid = a1 if isinstance(a1, int) else a2

    if sym is None:
        raise ValueError("symbol is required")
    if cid is None:
        raise ValueError("chat_id is required")

    return int(cid), str(sym).upper()


async def start_scanner_for_pair(app, *args, **kwargs):
    """
    Стартует (или не стартует, если уже есть) фоновую задачу сканера
    для конкретного чата и символа.
    Возвращает строку для отправки в Telegram.
    """
    chat_id, symbol = _parse_chat_and_symbol(args, kwargs)
    ns_key = _ns(chat_id, symbol)

    bot_data = app.bot_data
    bot_data.setdefault(TASKS_KEY, {})
    bot_data.setdefault(BOXES_KEY, {})
    bot_data.setdefault(BANKS_KEY, {})

    # Читаем банк через единый резолвер
    bank = _resolve_bank_usd(app, chat_id, symbol)

    # Создаём 'say' здесь, чтобы передать его в box
    say = _make_say(app, chat_id)

    # уже бежит?
    task = bot_data[TASKS_KEY].get(ns_key)
    if task and not task.done():
        return f"ℹ️ сканер по {symbol} уже запущен."

    # коробка состояния для этого сканера
    box = {
        "chat_id": chat_id,
        "symbol": symbol,
        "bank_usd": bank,
        "stop_flag": False,
        "user_manual_mode": False,
        "fsm_state": int(FSM.IDLE),
        "position": None,
        "m15_state": {},
        "m15_sig": {},
        "say": say,
        # флаги для /hedge_flip и /hedge_close
        "pending_hedge_bias": None,  # "LONG" / "SHORT"
        "pending_hc_price": None,    # float
    }
    bot_data[BOXES_KEY][ns_key] = box

    # сам цикл – корутина _scanner_main(...)
    task = app.create_task(_scanner_main(app, ns_key))
    bot_data[TASKS_KEY][ns_key] = task

    return f"✅ сканер по {symbol} запущен."


async def stop_scanner_for_pair(app, *args, **kwargs):
    """
    Останавливает сканер, если он был.
    Возвращает строку для Telegram.
    """
    chat_id, symbol = _parse_chat_and_symbol(args, kwargs)
    ns_key = _ns(chat_id, symbol)

    bot_data = app.bot_data
    tasks = bot_data.get(TASKS_KEY, {})
    boxes = bot_data.get(BOXES_KEY, {})

    # помечаем, что надо завершиться
    box = boxes.get(ns_key)
    if box:
        box["stop_flag"] = True

    task = tasks.get(ns_key)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    tasks.pop(ns_key, None)
    boxes.pop(ns_key, None)

    return f"🛑 сканер по {symbol} остановлен."


def is_scanner_running(app, *args, **kwargs) -> bool:
    """
    Просто говорит, есть ли активная задача.
    """
    chat_id, symbol = _parse_chat_and_symbol(args, kwargs)
    ns_key = _ns(chat_id, symbol)
    tasks = app.bot_data.get(TASKS_KEY, {})
    task = tasks.get(ns_key)
    return bool(task and not task.done())


__all__ = [
    "start_scanner_for_pair",
    "stop_scanner_for_pair",
    "is_scanner_running",
]
