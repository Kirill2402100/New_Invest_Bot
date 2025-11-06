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
# === Глобальная переработка auto_strat и build_targets (ПРАВКА 22) ========
# ===========================================================================

def auto_strat_targets_with_ml_buffer(
    pos: "Position",
    rng_strat: dict,
    entry: float,
    tick: float,
    bank: float,
    fees_est: float,
) -> list[dict]:
    """
    --- ПРАВКА 22: Глобальная переработка ---
    Эта функция теперь ищет *все* 3 уровня добора (TAC1, TAC2, STRAT1)
    и сама проверяет их на ML.
    """
    side = pos.side
    hc = float(entry)
    atr = float(rng_strat.get("atr1h", 0.0)) or 0.0

    g_step = max(tick * max(2, CONFIG.DCA_MIN_GAP_TICKS), atr * 0.01)
    g = max(g_step, hc * CONFIG.MIN_SPACING_PCT + tick * max(2, CONFIG.DCA_MIN_GAP_TICKS))

    def _strat1_at_g(gv: float) -> float:
        # Ищем старый STRAT 2, т.е. 2 * g
        if side == "LONG":
            return quantize_to_tick(hc - 2 * gv, tick)
        else:
            return quantize_to_tick(hc + 2 * gv, tick)

    thr3 = CONFIG.ML_BREAK_BUFFER_PCT # (Порог для STRAT1, старый thr3)
    thr2 = CONFIG.ML_BREAK_BUFFER_PCT2 # (Порог для TAC2, старый thr2)
    max_depth = max(atr * 4.0, g_step * 60)
    moved = 0.0

    while moved <= max_depth:
        # 1. Находим цену STRAT 1 (p1)
        p1 = _strat1_at_g(g)

        # 2. Находим цены TAC1 (t1) и TAC2 (t2)
        t1, t2 = _two_tacticals_between(hc, p1, side, tick)
        prices = [t1, t2, p1] # Это 3 наших уровня добора

        # 3. Проверяем зазор
        min_total = (
            max(tick * CONFIG.DCA_MIN_GAP_TICKS, hc * CONFIG.MIN_SPACING_PCT)
            + max(tick * CONFIG.DCA_MIN_GAP_TICKS, t1 * CONFIG.MIN_SPACING_PCT)
        )
        ok_corridor = (abs(t1 - hc) >= min_total)

        # 4. Проверяем ML буферы для 2-го и 3-го добора
        #    Используем 'prices' [t1, t2, p1]
        #    ML-буфер после 3-го уровня (STRAT 1)
        buf3 = ml_distance_pct(
            side, _break_price_for_side(rng_strat, side), _ml_after_k(pos, bank, fees_est, prices, 3)
        )
        #    ML-буфер после 2-го уровня (TAC 2)
        buf2 = ml_distance_pct(
            side, _break_price_for_side(rng_strat, side), _ml_after_k(pos, bank, fees_est, prices, 2)
        )
        ok_ml = (pd.notna(buf3) and buf3 >= thr3) and (pd.notna(buf2) and buf2 >= thr2)

        if ok_corridor and ok_ml:
            break

        g += g_step
        moved += g_step
    else:
        # Если не нашли «идеальный» g, берём последний рассчитанный
        p1 = _strat1_at_g(g)
        t1, t2 = _two_tacticals_between(hc, p1, side, tick)

    # Возвращаем 3 уровня добора
    return [
        {"price": t1, "label": "TAC #1"},
        {"price": t2, "label": "TAC #2"},
        {"price": p1, "label": "STRAT 1"},
    ]

def build_targets_with_tactical(
    pos: "Position",
    rng_strat: dict,
    close_px: float,
    tick: float,
    bank: float,
    fees_est: float,
) -> list[dict]:
    """
    --- ПРАВКА 22: Упрощение ---
    Эта функция теперь просто вызывает auto_strat... и применяет ручные override'ы.
    """
    base_for_strat = float(close_px)
    
    # 1. Получаем авто-уровни
    strat = auto_strat_targets_with_ml_buffer(
        pos, rng_strat, entry=base_for_strat, tick=tick, bank=bank, fees_est=fees_est
    )
    if not strat:
        return []

    # 2. Применяем ручные TAC, если они есть
    if pos.manual_tac_price is not None:
        tac1_q = quantize_to_tick(pos.manual_tac_price, tick)
        strat[0]["price"] = tac1_q
        # Если задан только TAC1, пересчитываем TAC2
        if pos.manual_tac2_price is None:
             tac2_q = _tactical_between(tac1_q, strat[2]["price"], pos.side, tick)
             strat[1]["price"] = tac2_q
    
    if pos.manual_tac2_price is not None:
        tac2_q = quantize_to_tick(pos.manual_tac2_price, tick)
        strat[1]["price"] = tac2_q
        # Если задан только TAC2, а TAC1 - авто
        if pos.manual_tac_price is None:
            tac1_q = _tactical_between(close_px, tac2_q, pos.side, tick)
            strat[0]["price"] = tac1_q

    # 3. Применяем клиппинг (если нужен, но auto_strat... уже должен был)
    # (Оставляем на случай ручных правок)
    gap_hc = max(tick * CONFIG.DCA_MIN_GAP_TICKS, close_px * CONFIG.MIN_SPACING_PCT)
    gap_s1 = max(tick * CONFIG.DCA_MIN_GAP_TICKS, strat[2]["price"] * CONFIG.MIN_SPACING_PCT)
    dist = abs(close_px - strat[2]["price"])
    min_total = gap_hc + gap_s1

    if dist > min_total:
         if pos.side == "LONG":
            hi = close_px - gap_hc
            lo = strat[2]["price"] + gap_s1
            strat[0]["price"] = min(max(strat[0]["price"], lo), hi)
            strat[1]["price"] = min(max(strat[1]["price"], lo), hi)
         else:
            lo = close_px + gap_hc
            hi = strat[2]["price"] - gap_s1
            strat[0]["price"] = min(max(strat[0]["price"], lo), hi)
            strat[1]["price"] = min(max(strat[1]["price"], lo), hi)

    return strat
# ===========================================================================
# === КОНЕЦ Глобальной переработки ========================================
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
    --- ПРАВКА 22 (v2): Глобальная переработка ---
    Эта функция теперь ищет *все* 3 уровня добора (TAC1, TAC2, STRAT1)
    и сама проверяет их на ML.
    """
    side = pos.side
    hc = float(entry)
    atr = float(rng_strat.get("atr1h", 0.0)) or 0.0

    g_step = max(tick * max(2, CONFIG.DCA_MIN_GAP_TICKS), atr * 0.01)
    g = max(g_step, hc * CONFIG.MIN_SPACING_PCT + tick * max(2, CONFIG.DCA_MIN_GAP_TICKS))

    def _strat1_at_g(gv: float) -> float:
        # Ищем старый STRAT 2, т.е. 2 * g
        if side == "LONG":
            return quantize_to_tick(hc - 2 * gv, tick)
        else:
            return quantize_to_tick(hc + 2 * gv, tick)

    thr3 = CONFIG.ML_BREAK_BUFFER_PCT # (Порог для STRAT1, старый thr3)
    thr2 = CONFIG.ML_BREAK_BUFFER_PCT2 # (Порог для TAC2, старый thr2)
    max_depth = max(atr * 4.0, g_step * 60)
    moved = 0.0
    
    # --- ИСПРАВЛЕНИЕ ОШИБКИ "ПРОПАВШИЙ STRAT 1" ---
    # Мы должны симулировать _ml_after_k с учетом *всех* предыдущих доборов.
    # Создадим временную позицию, чтобы передать правильные step_margins.
    
    class _SimPos:
        pass
    
    sim_pos = _SimPos()
    sim_pos.side = pos.side
    sim_pos.avg = pos.avg
    sim_pos.qty = pos.qty
    sim_pos.leverage = pos.leverage
    sim_pos.steps_filled = pos.steps_filled
    sim_pos.reserve_used = pos.reserve_used
    # Важно: передаем *всю* сетку маржи (4 уровня)
    sim_pos.step_margins = pos.step_margins 
    sim_pos.reserve_margin_usdt = 0.0
    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---


    while moved <= max_depth:
        # 1. Находим цену STRAT 1 (p1)
        p1 = _strat1_at_g(g)

        # 2. Находим цены TAC1 (t1) и TAC2 (t2)
        t1, t2 = _two_tacticals_between(hc, p1, side, tick)
        
        # --- ИСПРАВЛЕНИЕ ОШИБКИ ---
        # Правильный порядок доборов: [t1, t2, p1]
        prices = [t1, t2, p1] 
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        # 3. Проверяем зазор
        min_total = (
            max(tick * CONFIG.DCA_MIN_GAP_TICKS, hc * CONFIG.MIN_SPACING_PCT)
            + max(tick * CONFIG.DCA_MIN_GAP_TICKS, t1 * CONFIG.MIN_SPACING_PCT)
        )
        ok_corridor = (abs(t1 - hc) >= min_total)

        # 4. Проверяем ML буферы для 2-го и 3-го добора
        #    Используем 'prices' [t1, t2, p1]
        #    ML-буфер после 3-го уровня (STRAT 1)
        buf3 = ml_distance_pct(
            side, _break_price_for_side(rng_strat, side), _ml_after_k(sim_pos, bank, fees_est, prices, 3)
        )
        #    ML-буфер после 2-го уровня (TAC 2)
        buf2 = ml_distance_pct(
            side, _break_price_for_side(rng_strat, side), _ml_after_k(sim_pos, bank, fees_est, prices, 2)
        )
        ok_ml = (pd.notna(buf3) and buf3 >= thr3) and (pd.notna(buf2) and buf2 >= thr2)

        if ok_corridor and ok_ml:
            break

        g += g_step
        moved += g_step
    else:
        # Если не нашли «идеальный» g, берём последний рассчитанный
        p1 = _strat1_at_g(g)
        t1, t2 = _two_tacticals_between(hc, p1, side, tick)

    # Возвращаем 3 уровня добора
    return [
        {"price": t1, "label": "TAC #1"},
        {"price": t2, "label": "TAC #2"},
        {"price": p1, "label": "STRAT 1"},
    ]

def build_targets_with_tactical(
    pos: "Position",
    rng_strat: dict,
    close_px: float,
    tick: float,
    bank: float,
    fees_est: float,
) -> list[dict]:
    """
    --- ПРАВКА 22: Упрощение ---
    Эта функция теперь просто вызывает auto_strat... и применяет ручные override'ы.
    """
    base_for_strat = float(close_px)
    
    # 1. Получаем авто-уровни
    strat = auto_strat_targets_with_ml_buffer(
        pos, rng_strat, entry=base_for_strat, tick=tick, bank=bank, fees_est=fees_est
    )
    if not strat:
        return []

    # 2. Применяем ручные TAC, если они есть
    if pos.manual_tac_price is not None:
        tac1_q = quantize_to_tick(pos.manual_tac_price, tick)
        strat[0]["price"] = tac1_q
        # Если задан только TAC1, пересчитываем TAC2
        if pos.manual_tac2_price is None:
             tac2_q = _tactical_between(tac1_q, strat[2]["price"], pos.side, tick)
             strat[1]["price"] = tac2_q
    
    if pos.manual_tac2_price is not None:
        tac2_q = quantize_to_tick(pos.manual_tac2_price, tick)
        strat[1]["price"] = tac2_q
        # Если задан только TAC2, а TAC1 - авто
        if pos.manual_tac_price is None:
            tac1_q = _tactical_between(close_px, tac2_q, pos.side, tick)
            strat[0]["price"] = tac1_q

    # 3. Применяем клиппинг (если нужен, но auto_strat... уже должен был)
    # (Оставляем на случай ручных правок)
    gap_hc = max(tick * CONFIG.DCA_MIN_GAP_TICKS, close_px * CONFIG.MIN_SPACING_PCT)
    gap_s1 = max(tick * CONFIG.DCA_MIN_GAP_TICKS, strat[2]["price"] * CONFIG.MIN_SPACING_PCT)
    dist = abs(close_px - strat[2]["price"])
    min_total = gap_hc + gap_s1

    if dist > min_total:
         if pos.side == "LONG":
            hi = close_px - gap_hc
            lo = strat[2]["price"] + gap_s1
            strat[0]["price"] = min(max(strat[0]["price"], lo), hi)
            strat[1]["price"] = min(max(strat[1]["price"], lo), hi)
         else:
            lo = close_px + gap_hc
            hi = strat[2]["price"] - gap_s1
            strat[0]["price"] = min(max(strat[0]["price"], lo), hi)
            strat[1]["price"] = min(max(strat[1]["price"], lo), hi)

    return strat
# ===========================================================================
# === КОНЕЦ Глобальной переработки ========================================
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
        # --- ИСПРАВЛЕНИЕ ОШИБКИ "ПРОПАВШИЙ STRAT 1" ---
        # Индекс маржи = used_ord + i
        if used_ord + i >= len(pos.step_margins) or i >= len(prices):
            break
        m = float(pos.step_margins[used_ord + i])
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---
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
