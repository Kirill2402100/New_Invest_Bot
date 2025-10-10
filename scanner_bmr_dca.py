# scanner_bmr_dca.py — patched, full
# - безопасная нормализация символа (str|dict|list)
# - вход только через HEDGE (trend|revert), HC из единого расчёта
# - STRAT-хвост от цены закрытия хеджа + TAC между HC и STRAT#1 с зазорами (тик/0.35%)
# - ML-буфер ≥ 3% после STRAT#3 и пробоя; EXT-план после подтверждённого пробоя
# - 1m хвосты как триггеры; 15m ATR/фракталы — только уведомления
# - совместимость с fetch_ohlcv(symbol, tf, limit) из fx_feed; FA/Sheets удалены

from __future__ import annotations

import asyncio, time, logging, os, inspect
from typing import Optional
from datetime import datetime, timezone
from enum import IntEnum

import numpy as np
import pandas as pd
from telegram.ext import Application

# FA/Google Sheets удалены: все связанные настройки и логика вычищены

# === Forex адаптеры и фид ===
from fx_mt5_adapter import FX, margin_to_lots, default_tick
from fx_feed import fetch_ohlcv

log = logging.getLogger("bmr_dca_engine")
logging.getLogger("fx_feed").setLevel(logging.WARNING)

# Регистр для задач сканеров в рамках Telegram-приложения
TASKS_KEY = "scan_tasks"    # app.bot_data[TASKS_KEY] -> dict[ns_key] = asyncio.Task

# --- Status snapshot helper (для /status)
def _update_status_snapshot(box: dict, *, symbol: str, bank_fact: float, bank_target: float,
                            pos, scan_paused: bool, rng_strat, rng_tac):
    state = "ПАУЗА" if scan_paused else "РАБОТАЕТ"
    pos_line = "Нет активной позиции."
    if pos and getattr(pos, "steps_filled", 0) > 0:
        pos_line = f"{pos.side} steps {pos.steps_filled}/{pos.max_steps} | avg={fmt(pos.avg)} | tp={fmt(pos.tp_price)}"
    # значения, которые может читать обработчик /status
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

# Google Sheets: удалено

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
    # <<< MODIFIED: Added HEDGE_MODE switch
    HEDGE_MODE = "trend"  # "revert" (старая логика) или "trend" (новая)

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
    TF_TRIGGER = "1m"           # новый: поток для триггеров по хвостам

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
    WICK_HYST_TICKS = 1         # гистерезис для 1m-триггеров (±тик)

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
    # Рекомендации на 15m (спайк-режим)
    M15_RECO = {
        "ATR_LEN": 14,
        "TRIGGER_MULT": 1.5,    # свеча с (high-low) >= 1.5*ATR
        "CHECK_EVERY_SEC": 60,  # не чаще 1 раза в минуту тянуть 15m
        "MSG_COOLDOWN_SEC": 60
    }

    # FA полностью отключён

    ORDINARY_ADDS = 5  # столько обычных доборов после входа

    # Анти-слипание ценовых уровней
    DCA_MIN_GAP_TICKS = 2   # минимум 2 тика между целями

    # Показываем ML-цену при целевом Margin Level
    ML_TARGET_PCT = 20.0    # "ML цена (20%)"
    # Минимальный запас к ML(20%) после STRAT и пробоя STRAT
    ML_BREAK_BUFFER_PCT  = 3.0   # после 3-го STRAT
    ML_BREAK_BUFFER_PCT2 = 2.0   # после 2-го STRAT (новое, для более ранней страховки)
    ### NEW: минимальный разрыв между HC, TAC и STRAT#1 — 0.35%
    MIN_SPACING_PCT = 0.0035    # 0.35%
    # Требование к запасу после STRAT в «тиках»: не меньше шага между самими STRAT
    ML_REQ_GAP_MODE = "strat_spacing"  # ["strat_spacing"]

    # Расширение коридора после пробоя
    EXT_AFTER_BREAK = {
        "CONFIRM_BARS_5M": 6,  # сколько 5m-баров нужно удержаться за STRAT
        "EXTRA_LOOKBACK_DAYS": 10, # доп. история для «нового потолка/пола» (на TF_RANGE)
        "ATR_MULT_MIN": 2.0,   # минимальная ширина экстеншена в ATR
        "PRICE_EPS": 0.0015,       # небольшой буфер от уровня пробоя
    }
    
    # BOOST
    BOOST_MAX_STEPS = 3
    BREAK_MSG_COOLDOWN_SEC = 45

    # --- manual reopen policy ---
    # После закрытия сделки требовать ручной запуск нового цикла командой /open
    REQUIRE_MANUAL_REOPEN_ON = {
        "manual_close": True,  # после ручного закрытия
        "sl_hit": True,        # после SL/трейла
        "tp_hit": True,        # ⬅ по умолчанию тоже ждём /open
    }
    REMIND_MANUAL_MSG_COOLDOWN_SEC = 120  # раз в N секунд напоминать про /open

    # --- подтверждение пробоя STRAT ---
    # Прежде чем «замораживать» обычные усреднения/строить EXT,
    # проверяем пробой несколькими замерами
    BREAK_PROBE = {
        "SAMPLES": 3,      # число подтверждений
        "INTERVAL_SEC": 5,     # интервал между замерами, сек
        "TIMEOUT_SEC": 20,     # таймаут пробы, сек
    }

    # План после закрытия хеджа:
    # 1 шаг уже занят «оставшейся ногой» + (TAC + STRAT#1..#3) = 4 оставшихся шага
    STRAT_LEVELS_AFTER_HEDGE = 5

    # Более плавная лестница ДЛЯ периода «после хеджа»
    GROWTH_AFTER_HEDGE = 1.6

# ENV-переопределения
CONFIG.SYMBOL   = os.getenv("FX_SYMBOL", CONFIG.SYMBOL)
CONFIG.TF_ENTRY = os.getenv("TF_ENTRY", CONFIG.TF_ENTRY)
CONFIG.TF_RANGE = os.getenv("TF_RANGE", os.getenv("TF_TREND", CONFIG.TF_RANGE))

# ---------------------------------------------------------------------------
# Helpers (Sheets удалены)
# ---------------------------------------------------------------------------
async def maybe_await(func, *args, **kwargs):
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

# Sheets/FA: таблицы, заголовки, белые списки ключей — удалены

# === FUND_BOT / Targets — удалено ===

# все вспомогательные функции для Sheets — удалены

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

# Sheets логирование удалено

# FA policy & FUND_BOT weights — удалены (не используются)

# ---------------------------------------------------------------------------
# Formatting & maths
# ---------------------------------------------------------------------------

def fmt(p: float) -> str:
    if p is None or pd.isna(p): return "N/A"
    if p < 0.01: return f"{p:.6f}"
    if p < 1.0:  return f"{p:.5f}"
    return f"{p:.4f}"

def _display_label_ru(lbl: str) -> str:
    """Приводим подписи таргетов к единому виду для сообщения."""
    s = str(lbl or "")
    if s.upper().startswith("TAC"):
        return "TAC"
    if "33%" in s:
        return "STRAT #1 (33%)"
    if "66%" in s:
        return "STRAT #2 (66%)"
    if "100%" in s:
        return "STRAT #3 (100%, резерв)"
    return s

def _project_remaining_levels(symbol: str, pos: "Position", bank: float,
                              fee_taker: float, tick: float) -> list[dict]:
    """
    Для каждого оставшегося уровня считаем:
      price, label, step_margin, lots_at_level,
      free_margin_after, ml_pct_after — сразу после исполнения уровня.
    """
    if not getattr(pos, "ordinary_targets", None):
        return []
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    used_now = _pos_total_margin(pos)
    base_off = getattr(pos, "ordinary_offset", 0)
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    out = []
    avg0, qty0 = float(pos.avg), float(pos.qty)

    for i, t in enumerate(pos.ordinary_targets[base_off:], start=0):
        step_idx = used_ord + i
        if step_idx >= len(pos.step_margins):
            break
        price = float(t["price"])
        m = float(pos.step_margins[step_idx])
        notional = m * L
        dq = notional / max(price, 1e-12)
        qty_new = qty0 + dq
        avg_new = (avg0 * qty0 + price * dq) / max(qty_new, 1e-9) if qty0 > 0 else price
        used_new = used_now + m
        # «временная» позиция для расчёта equity/ML на цене price
        class _Tmp: pass
        tmp = _Tmp()
        tmp.side = pos.side; tmp.avg = avg_new; tmp.qty = qty_new; tmp.leverage = L
        tmp.steps_filled = 1; tmp.step_margins = [used_new]; tmp.reserve_used = False; tmp.reserve_margin_usdt = 0.0
        fees_est = (used_new * L) * fee_taker * CONFIG.LIQ_FEE_BUFFER
        eq = equity_at_price(tmp, price, bank, fees_est)
        free_after = max(eq - used_new, 0.0)
        ml_after = (eq / used_new) * 100.0 if used_new > 0 else float('inf')
        lots = margin_to_lots(symbol, m, price=price, leverage=L)
        out.append({
            "price": price,
            "label": _display_label_ru(t.get("label")),
            "step_margin": m,
            "lots": lots,
            "free_after": free_after,
            "ml_after": ml_after,
        })
    return out

def render_remaining_levels_block(symbol: str, pos: "Position", bank: float,
                                    fee_taker: float, tick: float) -> str:
    rows = _project_remaining_levels(symbol, pos, bank, fee_taker, tick)
    if not rows:
        return "Оставшиеся уровни: —"
    lines = ["Оставшиеся уровни (цена — добор → ПОСЛЕ ИСП.: свободная, уровень маржи):"]
    for r in rows:
        lines.append(
            f"• {r['label']}: {fmt(r['price'])} — {r['step_margin']:.0f} $ ≈ {r['lots']:.2f} lot → "
            f"{r['free_after']:.2f} $, {r['ml_after']:.1f}%"
        )
    return "\n".join(lines)

# === 1) новый хелпер для вывода "ML после ..." ===
def _ml_after_labels_line(pos: "Position", bank: float, fees_est: float) -> str:
    """
    Возвращает строку вида:
      ML после TAC: 1.2345 | +1: 1.2456 | +2: 1.2567 | +3: 1.2678
    Привязываем подписи к реальным целям (TAC, STRAT#1–#3),
    расчёт — от цены закрытия хеджа (hedge_close_px).
    """
    base_off = getattr(pos, "ordinary_offset", 0)
    used_ord_now = pos.steps_filled - (1 if pos.reserve_used else 0)
    avail_ord = max(0, len(pos.step_margins) - used_ord_now)
    avail_tgts = max(0, len(pos.ordinary_targets) - base_off)
    avail_k = min(4, avail_ord, avail_tgts)
    if avail_k <= 0:
        return ""
    ks = tuple(range(1, avail_k + 1))
    scen = _ml_multi_scenarios(pos, bank, fees_est, k_list=ks)
    labels = []
    for i in range(avail_k):
        raw = str(pos.ordinary_targets[base_off + i].get("label", ""))
        if i == 0 and raw.upper().startswith("TAC"):
            lab = "TAC"
        else:
            lab = f"+{i}" if i > 0 else _display_label_ru(raw)
        v = scen.get(i + 1)
        vtxt = "N/A" if (v is None or (isinstance(v, float) and np.isnan(v))) else fmt(v)
        labels.append(f"{lab}: {vtxt}")
    return "ML после " + " | ".join(labels)

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
    used = _pos_total_margin(pos) or 1e-9
    L = max(1, int(getattr(pos, "leverage", 1) or 1))

    if pos.side == "LONG":
        gross_usd = used * L * (exit_p / pos.avg - 1.0)
    else:
        gross_usd = used * L * (pos.avg / max(exit_p, 1e-12) - 1.0)

    entry_notional = used * L
    exit_notional  = exit_p * pos.qty
    fee_entry_usd  = entry_notional * fee_entry
    fee_exit_usd   = exit_notional  * fee_exit

    net_usd = gross_usd - fee_entry_usd - fee_exit_usd
    net_pct = (net_usd / used) * 100.0
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
    """Equity = банк + PnL - комиссии."""
    used = _pos_total_margin(pos)
    pnl  = _pnl_at_price(pos, price, used)
    return bank + pnl - max(fees_est, 0.0)

def ml_percent_now(pos, price: float, bank: float, fees_est: float) -> float:
    used = _pos_total_margin(pos)
    if used <= 0: 
        return float('inf')
    return (equity_at_price(pos, price, bank, fees_est) / used) * 100.0

def ml_reserve_pct_to_ml20(pos, price: float, bank: float, fees_est: float) -> float:
    """Сколько процентов маржи остаётся до порога 20%: ML% - 20%."""
    mlp = ml_percent_now(pos, price, bank, fees_est)
    return float('inf') if (not np.isfinite(mlp)) else (mlp - CONFIG.ML_TARGET_PCT)

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
    denom = 1.0 + base
    if denom <= 0:
        return float('nan')
    return pos.avg * denom if pos.side == "LONG" else pos.avg / denom

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

### NEW: 15m ФРАКТАЛЫ (Bill Williams style, центр в i)
def compute_fractals_15m(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Возвращает два Series длиной df: fractal_up, fractal_down (иначе NaN).
    df: колонки open,high,low,close,volume (как в d= m15 DataFrame)
    """
    h, l = df["high"], df["low"]
    n = len(df)
    up = pd.Series([np.nan]*n, index=df.index)
    dn = pd.Series([np.nan]*n, index=df.index)
    # фрактал формируется на центральной свече i при наличии i-2..i+2
    for i in range(2, n-2):
        if h.iloc[i-2] < h.iloc[i] and h.iloc[i-1] < h.iloc[i] and h.iloc[i+1] < h.iloc[i] and h.iloc[i+2] < h.iloc[i]:
            up.iloc[i] = h.iloc[i]
        if l.iloc[i-2] > l.iloc[i] and l.iloc[i-1] > l.iloc[i] and l.iloc[i+1] > l.iloc[i] and l.iloc[i+2] > l.iloc[i]:
            dn.iloc[i] = l.iloc[i]
    return up, dn

def _last_confirmed_fractals(fr_up: pd.Series, fr_dn: pd.Series) -> tuple[float|None, float|None]:
    """Последние подтверждённые уровни (до последней свечи включительно)."""
    up_val = pd.to_numeric(fr_up.dropna(), errors="coerce")
    dn_val = pd.to_numeric(fr_dn.dropna(), errors="coerce")
    last_up = float(up_val.iloc[-1]) if len(up_val) else None
    last_dn = float(dn_val.iloc[-1]) if len(dn_val) else None
    return last_up, last_dn

### NEW: подбор «тактической» точки между HC и STRAT#1
def _tactical_between(hc_px: float, strat1: float, side: str, tick: float) -> float:
    """
    Берём середину между HC и STRAT#1 и соблюдаем 0.35% до обеих точек.
    """
    if side == "LONG":
        mid = hc_px + 0.5*(strat1 - hc_px)
        min_gap = max(tick*CONFIG.DCA_MIN_GAP_TICKS, hc_px*CONFIG.MIN_SPACING_PCT)
        # вниз от HC и вверх от STRAT#1 на min_gap
        lo = hc_px - min_gap
        hi = strat1 + min_gap
        p = max(min(mid, lo), hi)  # зажимаем середину в коридоре [hi..lo]
    else:
        mid = hc_px + 0.5*(strat1 - hc_px)
        min_gap = max(tick*CONFIG.DCA_MIN_GAP_TICKS, hc_px*CONFIG.MIN_SPACING_PCT)
        lo = strat1 - min_gap
        hi = hc_px + min_gap
        p = min(max(mid, hi), lo)
    return quantize_to_tick(p, tick)

def build_targets_with_tactical(pos: "Position", rng_strat: dict, close_px: float, tick: float,
                                  bank: float, fees_est: float) -> list[dict]:
    """
    Строит список целей: [TAC] + [STRAT 33/66/100], причём STRAT вычисляются от 'базы'
    (по логике — от точки закрытия хеджа, а если вручную задан TAC — от него).
    ML-буфер контролируем автофункцией для STRAT; TAC вставляем с соблюдением 0.35%.
    """
    # 1) STRAT из авто-алгоритма: якорим у 100% от ЦЕНЫ ЗАКРЫТИЯ ХЕДЖА (всегда)
    base_for_strat = float(close_px)
    strat = auto_strat_targets_with_ml_buffer(pos, rng_strat, entry=base_for_strat,
                                              tick=tick, bank=bank, fees_est=fees_est)
    if not strat:
        return []
    # 2) TAC (ручной или авто mid), с соблюдением дистанций к HC и STRAT#1
    strat1 = strat[0]["price"]
    tac_px = float(pos.manual_tac_price) if pos.manual_tac_price is not None else _tactical_between(close_px, strat1, pos.side, tick)
    # Антислипание/зазоры: минимум 0.35% и ≥ 2 тика между HC и TAC, и TAC и STRAT#1.
    # Если коридор слишком узкий, пропорционально ужимаем зазоры так, чтобы TAC гарантированно
    # оставался между HC и STRAT#1.
    gap_hc = max(tick*CONFIG.DCA_MIN_GAP_TICKS, close_px*CONFIG.MIN_SPACING_PCT)
    gap_s1 = max(tick*CONFIG.DCA_MIN_GAP_TICKS, strat1  *CONFIG.MIN_SPACING_PCT)
    dist = abs(close_px - strat1)
    min_total = gap_hc + gap_s1
    if dist <= min_total:
        # распределяем доступное расстояние пропорционально исходным гэпам
        w_hc = gap_hc / max(min_total, 1e-12)
        w_s1 = 1.0 - w_hc
        gap_hc = max(tick*CONFIG.DCA_MIN_GAP_TICKS, dist * w_hc)
        gap_s1 = max(tick*CONFIG.DCA_MIN_GAP_TICKS, dist * w_s1)
    # Теперь клампы с «подрезанными» (или исходными) гэпами
    if pos.side == "LONG":
        upper_bound = close_px - gap_hc
        lower_bound = strat1 + gap_s1
        tac_px = max(min(tac_px, upper_bound), lower_bound)
    else:
        lower_bound = close_px + gap_hc
        upper_bound = strat1 - gap_s1
        tac_px = min(max(tac_px, lower_bound), upper_bound)
    tac_px = quantize_to_tick(tac_px, tick)
    # 3) итоговый список
    out = [{"price": tac_px, "label": "TAC"}] + strat
    return out

def break_distance_pcts(px: float, up: float, dn: float) -> tuple[float, float]:
    if px is None or px <= 0 or any(v is None or np.isnan(v) for v in (up, dn)):
        return float('nan'), float('nan')
    up_pct = max(0.0, (up / px - 1.0) * 100.0)
    dn_pct = max(0.0, (1.0 - dn / px) * 100.0)
    return up_pct, dn_pct

def quantize_to_tick(x: float | None, tick: float) -> float | None:
    if x is None or (isinstance(x, float) and np.isnan(x)): return x
    return round(round(x / tick) * tick, 10)

def _place_segment(start: float, end: float, count: int, tick: float,
                   include_end_last: bool, side: str) -> list[float]:
    """Равномерно распределяем точки между start и end. Анти-слипание: >= DCA_MIN_GAP_TICKS."""
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
        # кламп в коридор
        q = min(max(q, lo), hi)
        if i == 0:
            # первый — отступить от 'a' минимум на min_gap, но оставаться в коридоре
            q = (start - min_gap) if side == "LONG" else (start + min_gap)
            q = min(max(quantize_to_tick(q, tick), lo), hi)
        if (not out) or (side == "LONG" and q <= out[-1] - min_gap) or (side == "SHORT" and q >= out[-1] + min_gap):
            out.append(q)
    return out

def compute_corridor_targets(entry: float, side: str, rng_strat: dict, rng_tac: dict, tick: float) -> list[dict]:
    """
    Строим цели только в сторону ухудшения.
    Логика: 2 TAC + 3 STRAT (последний = STRAT 100%).
    Если TAC-сегмент короткий/нулевой — недостающие TAC-точки забираем
    с начала STRAT-сегмента (не сокращая общее количество шагов).
    """
    DESIRED_TAC = 2
    DESIRED_STRAT = 3

    if side == "LONG":
        tac_b    = min(entry, rng_tac["lower"])
        strat_b = min(entry, rng_strat["lower"])
        seg1 = _place_segment(entry, tac_b,   DESIRED_TAC,   tick, include_end_last=False, side=side)
        seg2 = _place_segment(tac_b,  strat_b, DESIRED_STRAT, tick, include_end_last=True,  side=side)
    else:
        tac_b    = max(entry, rng_tac["upper"])
        strat_b = max(entry, rng_strat["upper"])
        seg1 = _place_segment(entry, tac_b,   DESIRED_TAC,   tick, include_end_last=False, side=side)
        seg2 = _place_segment(tac_b,  strat_b, DESIRED_STRAT, tick, include_end_last=True,  side=side)

    # Если TAC-точек не хватает, «одалживаем» их с начала STRAT,
    # но ВСЕГДА сохраняем последний STRAT (100%) в конце.
    missing_tac = max(0, DESIRED_TAC - len(seg1))
    if missing_tac > 0 and len(seg2) > 0:
        # нельзя забрать последний «якорь» (STRAT 100%)
        can_take = max(0, len(seg2) - 1)
        take = min(missing_tac, can_take)
        if take > 0:
            seg1 = seg1 + seg2[:take]
            seg2 = seg2[take:]

    # Подписи (берём «столько, сколько есть»)
    def _labels(prefix: str, n: int, include_end: bool) -> list[str]:
        if n <= 0: return []
        if include_end:
            fr = [(i + 1) / n for i in range(n)]  # 50..100 (n=2) | 33..100 (n=3)
        else:
            fr = [(i + 1) / (n + 1) for i in range(n)]  # 33..66 (n=2)
        return [f"{prefix} {int(round(f * 100))}%" for f in fr]

    labs_tac   = _labels("TAC",   max(len(seg1), 0), include_end=False)
    labs_strat = _labels("STRAT", max(len(seg2), 0), include_end=True)

    targets: list[dict] = []
    for p, lab in zip(seg1, labs_tac):
        targets.append({"price": p, "label": lab})
    for p, lab in zip(seg2, labs_strat):
        targets.append({"price": p, "label": lab})

    # Дедуп по тику, порядок сохраняем
    return merge_targets_sorted(side, tick, entry, targets)

def compute_strategic_targets_only(entry: float, side: str, rng_strat: dict, tick: float, levels: int = 3) -> list[dict]:
    """Только STRAT в сторону ухудшения (последний — 100%)."""
    assert tick is not None, "tick must be provided"
    if levels <= 0: 
        return []
    if side == "LONG":
        strat_b = min(entry, rng_strat["lower"])
        seg = _place_segment(entry, strat_b, levels, tick, include_end_last=True, side=side)
    else:
        strat_b = max(entry, rng_strat["upper"])
        seg = _place_segment(entry, strat_b, levels, tick, include_end_last=True, side=side)
    labs = [f"STRAT {int(round((i + 1) / max(len(seg),1) * 100))}%" for i in range(len(seg))]
    out = [{"price": p, "label": lab} for p, lab in zip(seg, labs)]
    return merge_targets_sorted(side, tick, entry, out)

def merge_targets_sorted(side: str, tick: float, entry: float, targets: list[dict]) -> list[dict]:
    dedup = []
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    for t in targets:
        if not dedup or (side == "SHORT" and t["price"] >= dedup[-1]["price"] + min_gap) or \
           (side == "LONG"  and t["price"] <= dedup[-1]["price"] - min_gap):
            dedup.append(t)
    return dedup

def _advance_pointer(pos, tick):
    if pos.last_filled_q is None:
        return
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    base = getattr(pos, "ordinary_offset", 0)
    while base < len(pos.ordinary_targets):
        p = pos.ordinary_targets[base]["price"]
        ok = (pos.side == "SHORT" and p >= pos.last_filled_q + min_gap) or \
             (pos.side == "LONG"  and p <= pos.last_filled_q - min_gap)
        if ok: break
        base += 1
    pos.ordinary_offset = base

def next_pct_target(pos):
    if not getattr(pos, "ordinary_targets", None):
        return None
    used_dca = max(0, (pos.steps_filled - (1 if pos.reserve_used else 0)) - 1)
    base = getattr(pos, "ordinary_offset", 0)
    abs_idx = max(base, used_dca)       # <-- ключевая правка
    if 0 <= abs_idx < len(pos.ordinary_targets):
        t = pos.ordinary_targets[abs_idx]
        # если это «резервный» STRAT#3 и он ещё не готов — пропускаем
        if getattr(pos, "reserve3_price", None) is not None and \
           abs(float(t["price"]) - float(pos.reserve3_price)) < 1e-12 and \
           (not getattr(pos, "reserve3_ready", False)) and (not getattr(pos, "reserve3_done", False)):
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

async def plan_extension_after_break(symbol: str, pos: "Position",
                                     rng_strat: dict, rng_tac: dict,
                                     px: float, tick: float) -> list[dict]:
    """
    Дорисовывает оставшиеся обычные уровни после подтверждённого пробоя STRAT.
    Не трогает уже 'израсходованные' цели. Возвращает новый список ordinary_targets.
    """
    # Сколько обычных шагов уже использовано (включая OPEN, без резерва)
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    remaining = max(0, pos.ord_levels - used_ord)
    if remaining <= 0:
        return pos.ordinary_targets

    # Подтягиваем расширенную историю на TF_RANGE
    ext_hours = max(
        (CONFIG.TACTICAL_LOOKBACK_DAYS + CONFIG.EXT_AFTER_BREAK["EXTRA_LOOKBACK_DAYS"]) * 24,
        CONFIG.TACTICAL_LOOKBACK_DAYS * 24
    )
    try:
        ext_df = await maybe_await(fetch_ohlcv, symbol, CONFIG.TF_RANGE, ext_hours)
    except Exception:
        ext_df = None
    ext_rng = await build_range_from_df(ext_df, min_atr_mult=1.5) if (ext_df is not None and not ext_df.empty) else None

    # Минимальная «добавка» по ширине через ATR
    atr1h = float(rng_strat.get("atr1h", 0.0))
    atr_guard = CONFIG.EXT_AFTER_BREAK["ATR_MULT_MIN"] * max(atr1h, 1e-12)

    # Конечная точка расширения: «новый потолок/пол»
    if pos.side == "SHORT":  # пробой вверх, строим ПОДАЛЬШЕ вверх
        candidates = [
            px + atr_guard,
            rng_strat["upper"],
            (ext_rng and ext_rng["upper"]) or rng_strat["upper"],
        ]
        end = max([v for v in candidates if np.isfinite(v)])
        start = px
    else:                                                         # LONG: пробой вниз, строим ПОДАЛЬШЕ вниз
        candidates = [
            px - atr_guard,
            rng_strat["lower"],
            (ext_rng and ext_rng["lower"]) or rng_strat["lower"],
        ]
        end = min([v for v in candidates if np.isfinite(v)])
        start = px

    # Равномерно раскидываем оставшиеся уровни, последний — у "нового потолка/пола"
    seg = _place_segment(start, end, remaining, tick, include_end_last=True, side=pos.side)
    if not seg:
        return pos.ordinary_targets

    # Сохраняем уже пройденные цели
    keep_idx = max(getattr(pos, "ordinary_offset", 0), max(0, used_ord - 1))
    already = pos.ordinary_targets[:min(keep_idx, len(pos.ordinary_targets))]
    
    new_labels = [f"EXT {int(round((i + 1) / len(seg) * 100))}%" for i in range(len(seg))]
    ext_targets = [{"price": p, "label": lab} for p, lab in zip(seg, new_labels)]

    return already + ext_targets
# ---------------------------------------------------------------------------
# Диапазоны/индикаторы
# ---------------------------------------------------------------------------
async def build_range_from_df(df: Optional[pd.DataFrame], min_atr_mult: float):
    if df is None or df.empty: return None
    want = ["open","high","low","close","volume"]
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
    return {"lower": lower, "upper": upper, "mid": mid, "atr1h": atr1h, "width": upper-lower}

async def build_ranges(symbol: str):
    s_limit = CONFIG.STRATEGIC_LOOKBACK_DAYS * 24
    t_limit = CONFIG.TACTICAL_LOOKBACK_DAYS  * 24
    s_df_task = asyncio.create_task(maybe_await(fetch_ohlcv, symbol, CONFIG.TF_RANGE, s_limit))
    t_df_task = asyncio.create_task(maybe_await(fetch_ohlcv, symbol, CONFIG.TF_RANGE, t_limit))
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
    vol = df["volume"]
    rm = vol.rolling(CONFIG.VOL_WIN, min_periods=1).mean().iloc[-1]
    rs = vol.rolling(CONFIG.VOL_WIN, min_periods=2).std().iloc[-1]
    vol_z = (vol.iloc[-1] - rm) / max(rs, 1e-9)
    st = ta_supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3.0)
    dir_now  = int(st["direction"].iloc[-1])
    dir_prev = int(st["direction"].iloc[-2]) if len(st) > 1 else dir_now
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
# STRAT ML-buffer helpers (3% после #3 и пробоя STRAT)
# ---------------------------------------------------------------------------
def _break_price_for_side(rng_strat: dict, side: str) -> float:
    up, dn = break_levels(rng_strat)
    return dn if side == "LONG" else up

def _simulate_after_k(pos: "Position", prices: list[float], k: int) -> tuple[float, float, float]:
    """
    Симуляция состояния после k будущих стратегических шагов по заданным ценам (len(prices) >= k).
    Возвращает (avg, qty, used_margin) после применения k шагов к текущему pos.
    """
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    avg = float(pos.avg)
    qty = float(pos.qty)
    used = float(_pos_total_margin(pos))
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    for i in range(k):
        if used_ord + i >= len(pos.step_margins) or i >= len(prices): break
        m = float(pos.step_margins[used_ord + i])
        p = float(prices[i])
        dq = (m * L) / max(p, 1e-12)
        qty_new = qty + dq
        avg = (avg * qty + p * dq) / max(qty_new, 1e-9)
        qty = qty_new
        used += m
    return avg, qty, used

def _ml_after_k(pos: "Position", bank: float, fees_est: float, targets: list[float], k: int) -> float:
    """
    ML(20%) после применения k будущих шагов по ценам из targets.
    """
    class _Tmp: pass
    avg, qty, used = _simulate_after_k(pos, targets, k)
    t = _Tmp()
    t.side = pos.side; t.avg = avg; t.qty = qty; t.leverage = pos.leverage
    t.steps_filled = 1; t.step_margins = [used]; t.reserve_used = False; t.reserve_margin_usdt = 0.0
    return ml_price_at(t, CONFIG.ML_TARGET_PCT, bank, fees_est)

# --- helpers для требований по запасу ---
def _extract_strat_prices(targets: list[dict]) -> list[float]:
    """Возвращает цены STRAT-таргетов (без TAC) в порядке ухудшения."""
    out = []
    for t in targets:
        lab = str(t.get("label","")).upper()
        if lab.startswith("STRAT"):
            out.append(float(t["price"]))
    return out

def _gap_ticks_to_ml_after_k(pos: "Position", bank: float, fees_est: float,
                              strat_prices: list[float], k: int, tick: float) -> float:
    """
    Запас (в тиках) от цены k-го STRAT до ML(20%) после исполнения k шагов.
    """
    if k <= 0 or k > len(strat_prices): 
        return float('nan')
    mlk = _ml_after_k(pos, bank, fees_est, strat_prices[:k], k)
    if np.isnan(mlk): 
        return float('nan')
    p_k = strat_prices[k-1]
    dpx = (p_k - mlk) if pos.side=="LONG" else (mlk - p_k)
    return abs(dpx / max(tick, 1e-12))

def _strat_spacings_in_ticks(side: str, strat_prices: list[float], tick: float) -> list[float]:
    """
    Расстояния между соседними STRAT в тиках (|p_i - p_{i+1}|/tick).
    """
    gaps = []
    for i in range(len(strat_prices)-1):
        gaps.append(abs((strat_prices[i] - strat_prices[i+1]) / max(tick, 1e-12)))
    return gaps

def _ml_buffer_after_3(pos: "Position", bank: float, fees_est: float,
                       rng_strat: dict, t1: float, t2: float, t3: float) -> float:
    """
    Возвращает запас к ML(20%) в процентах (>=0 — безопасный) от цены пробоя STRAT
    после трёх будущих шагов по t1..t3.
    """
    ml3 = _ml_after_k(pos, bank, fees_est, [t1, t2, t3], 3)
    brk = _break_price_for_side(rng_strat, pos.side)
    return ml_distance_pct(pos.side, brk, ml3)

def _linspace_exclusive(a: float, b: float, n: int, include_end: bool, tick: float, side: str) -> list[float]:
    if n <= 0: return []
    fr = [(i+1)/n for i in range(n)] if include_end else [(i+1)/(n+1) for i in range(n)]
    raw = [a + (b - a)*f for f in fr]
    out = []
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    lo, hi = (min(a,b), max(a,b))
    for i, x in enumerate(raw):
        q = quantize_to_tick(x, tick)
        # кламп в коридор
        q = min(max(q, lo), hi)
        if i == 0:
            # первый — отступить от 'a' минимум на min_gap, но оставаться в коридоре
            q = (a - min_gap) if side == "LONG" else (a + min_gap)
            q = min(max(quantize_to_tick(q, tick), lo), hi)
        if (not out) or (side == "LONG" and q <= out[-1] - min_gap) or (side == "SHORT" and q >= out[-1] + min_gap):
            out.append(q)
    return out

def auto_strat_targets_with_ml_buffer(pos: "Position", rng_strat: dict, entry: float, tick: float,
                                        bank: float, fees_est: float) -> list[dict]:
    """
    Строит 3 STRAT-цели от entry в сторону «ухудшения» так, чтобы запас
    к ML(20%) от цены пробоя STRAT после 3-й ступени был >= CONFIG.ML_BREAK_BUFFER_PCT.
    Алгоритм: якорим #3 у STRAT(100%), проверяем буфер; если < порога — углубляем #3,
    #1/#2 — равномерно между entry и новым #3. Соблюдаем квантование и gap.
    """
    side = pos.side
    # базовая конечная точка — STRAT 100%
    end = rng_strat["lower"] if side=="LONG" else rng_strat["upper"]
    # стартовая равномерная раскладка
    p = _linspace_exclusive(entry, end, 3, include_end=True, tick=tick, side=side)
    if len(p) < 3:
        return [{"price": x, "label": lab} for x,lab in zip(p, ["STRAT 33%","STRAT 66%","STRAT 100%"][:len(p)])]
    p1, p2, p3 = p
    # Оценим буферы после 2-х и 3-х шагов
    buf3 = _ml_buffer_after_3(pos, bank, fees_est, rng_strat, p1, p2, p3)
    buf2 = ml_distance_pct(pos.side,
                           _break_price_for_side(rng_strat, pos.side),
                           _ml_after_k(pos, bank, fees_est, [p1, p2], 2))
    thr3 = CONFIG.ML_BREAK_BUFFER_PCT
    thr2 = CONFIG.ML_BREAK_BUFFER_PCT2
    # если оба буфера уже ок — возвращаем
    if (pd.notna(buf3) and buf3 >= thr3) and (pd.notna(buf2) and buf2 >= thr2):
        labs = ["STRAT 33%","STRAT 66%","STRAT 100%"]
        return [{"price": p1, "label": labs[0]}, {"price": p2, "label": labs[1]}, {"price": p3, "label": labs[2]}]
    # иначе — углубляем p3 ступенчато до выполнения условия или до стопа
    atr = float(rng_strat.get("atr1h", 0.0)) or max(abs(end-entry), 1e-6)*0.02
    step = max(tick*max(4, CONFIG.DCA_MIN_GAP_TICKS), atr*0.05)  # ~5% ATR шаг
    max_depth = atr * 4.0  # не уходим слишком далеко: до ~4 ATR
    moved = 0.0
    while moved <= max_depth:
        # двигаем p3 глубже в сторону «минуса»
        p3 = (p3 - step) if side=="LONG" else (p3 + step)
        p3 = quantize_to_tick(p3, tick)
        # пересобираем p1,p2 равномерно между entry и p3 (end не нужен)
        mid = _linspace_exclusive(entry, p3, 3, include_end=True, tick=tick, side=side)
        if len(mid) < 3:
            moved += step
            continue
        p1, p2, p3 = mid
        buf3 = _ml_buffer_after_3(pos, bank, fees_est, rng_strat, p1, p2, p3)
        buf2 = ml_distance_pct(pos.side,
                               _break_price_for_side(rng_strat, pos.side),
                               _ml_after_k(pos, bank, fees_est, [p1, p2], 2))
        if (pd.notna(buf3) and buf3 >= thr3) and (pd.notna(buf2) and buf2 >= thr2):
            break
        moved += step
    labs = ["STRAT 33%","STRAT 66%","STRAT 100% (RESERVE)"]
    return [{"price": p1, "label": labs[0]},
            {"price": p2, "label": labs[1]},
            {"price": p3, "label": labs[2], "reserve3": True}]

def _strat_report_text(pos: "Position", px: float, tick: float, bank: float,
                       fees_est: float, rng_strat: dict, hdr: str) -> str:
    """
    Формирует текст отчёта для /strat show|set|reset: цели, размеры USD/лот,
    ML сейчас и после +1/+2/+3, буфер после #3.
    """
    lines = [hdr]
    # Ближайшие 3 цели c дистанциями (для отображения)
    base_off = getattr(pos, "ordinary_offset", 0)
    tgts = pos.ordinary_targets[base_off:base_off+3]
    if getattr(pos, "hedge_close_px", None) is not None:
        dt = abs((pos.hedge_close_px - px) / max(tick,1e-12))
        dp = abs((pos.hedge_close_px/max(px,1e-12)-1.0)*100.0)
        lines.append(f"HC) <code>{fmt(pos.hedge_close_px)}</code> — Δ≈ {dt:.0f} тик. ({dp:.2f}%)")
    for i, t in enumerate(tgts, start=1):
        dt = abs((t["price"] - px) / max(tick,1e-12))
        dp = abs((t["price"]/max(px,1e-12)-1.0)*100.0)
        lines.append(f"{i}) <code>{fmt(t['price'])}</code> ({t['label']}) — Δ≈ {dt:.0f} тик. ({dp:.2f}%)")
    # ML сейчас и после 1/2/3
    ml_now = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_est)
    avail = min(3, len(pos.step_margins)-(pos.steps_filled-(1 if pos.reserve_used else 0)), len(tgts))
    ks = tuple(range(1, avail+1))
    scen = _ml_multi_scenarios(pos, bank, fees_est, k_list=ks) if avail>0 else {}
    def _fmt_ml(v): return "N/A" if (v is None or np.isnan(v)) else fmt(v)
    arrow = "↓" if pos.side=="LONG" else "↑"
    dist_now = ml_distance_pct(pos.side, px, ml_now)
    dist_txt = "N/A" if np.isnan(dist_now) else f"{dist_now:.2f}%"
    lines.append(f"ML(20%): {arrow}<code>{fmt(ml_now)}</code> ({dist_txt} от текущей)")
    lines.append(f"ML после +1: {_fmt_ml(scen.get(1))} | +2: {_fmt_ml(scen.get(2))} | +3: {_fmt_ml(scen.get(3))}")
    # Буфер к ML после #3 и пробоя — СТРОГО по STRAT#1/#2/#3
    strat_only = [t for t in pos.ordinary_targets[base_off:] 
                  if str(t.get("label","")).upper().startswith("STRAT")][:3]
    if len(strat_only) == 3:
        buf = _ml_buffer_after_3(pos, bank, fees_est, rng_strat,
                                 strat_only[0]["price"], strat_only[1]["price"], strat_only[2]["price"])
        st = "OK" if (pd.notna(buf) and buf >= CONFIG.ML_BREAK_BUFFER_PCT) else "FAIL"
        brk_up, brk_dn = break_levels(rng_strat)
        brk = brk_dn if pos.side=="LONG" else brk_up
        lines.append(f"Буфер после #3 и пробоя STRAT (от <code>{fmt(brk)}</code>) → {buf:.2f}% [{st}] (порог {CONFIG.ML_BREAK_BUFFER_PCT:.2f}%)")
    return "\n".join(lines)

class FSM(IntEnum):
    IDLE = 0   # нет позиции
    OPENED = 1 # открыт 1-й шаг, идёт первичное оповещение
    MANAGING = 2 # можно ADD/RETEST/TRAIL/EXIT

# --- FIX 2: единая синхронизация reserve3 после любых перестроений ---

def _sync_reserve3_flags(pos: "Position"):
    """После изменения ordinary_targets привести в соответствие reserve3_*."""
    pos.reserve3_price = None
    pos.reserve3_armed = False
    pos.reserve3_ready = False
    pos.reserve3_done  = False
    for t in pos.ordinary_targets:
        if t.get("reserve3"):
            pos.reserve3_price = float(t["price"])
            break

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
        self.break_dir: str | None = None
        self.break_confirm_bars: int = 0
        self.extension_planned: bool = False
        self.ordinary_offset: int = 0
        # антидубли
        self.last_filled_q: float | None = None   # последний исполненный уровень (квантованный в тик)
        self.last_px: float | None = None         # предыдущая наблюдённая цена (для кросс-овера)
        # --- режим «после хеджа» ---
        self.hedge_entry_px: float | None = None  # по какой цене формировалась нога после закрытия хеджа
        self.from_hedge: bool = False
        # цена закрытия «прибыльной» ноги хеджа (для отображения рядом с целями)
        self.hedge_close_px: float | None = None
        # --- STRAT #3 как «резерв» ---
        self.reserve3_price: float | None = None    # цена 3-го STRAT как резервного
        self.reserve3_armed: bool = False           # цена прошла глубже (armed)
        self.reserve3_ready: bool = False           # был возврат через уровень (подтверждение разворота)
        self.reserve3_done: bool = False
        ### NEW: ручная «тактическая» точка (если задана)
        self.manual_tac_price: float | None = None

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
# HEDGE helpers (состояние и расчёты)
# ---------------------------------------------------------------------------
def planned_hc_price(entry: float, tac_lo: float, tac_hi: float, bias: str, mode: str, tick: float) -> float:
    """
    HC для хеджа:
    - mode == "trend": от точки входа на размер 'коридора' между TAC30 и TAC70
                           LONG-bias → вверх, SHORT-bias → вниз
                           HC = entry ± (tac_hi - tac_lo)
    - mode == "revert": закрытие на противоположном TAC (как раньше)
    """
    if mode == "trend":
        span = float(tac_hi) - float(tac_lo)  # расстояние между 30% и 70%
        px = entry + span if bias == "LONG" else entry - span
    else:  # "revert"
        # противоположный TAC относительно ожидаемого профита
        px = tac_hi if bias == "LONG" else tac_lo
    return quantize_to_tick(px, tick)

def _sum_first_n(lst: list[float], n: int) -> float:
    return sum(lst[:max(0, min(n, len(lst)))]) if lst else 0.0

def _wick_reached(side: str, tgt: float, lo: float, hi: float, tick: float, hyst_ticks: int = 1) -> bool:
    """Проверка по хвостам 1m с гистерезисом ±N тиков."""
    if any(v is None or np.isnan(v) for v in (tgt, lo, hi)): 
        return False
    buf = max(1, int(hyst_ticks)) * tick
    if side == "LONG":
        # «в сторону ухудшения» для LONG — вниз: достигнуто, если low <= tgt - buf
        return lo <= (tgt - buf)
    else:
        # для SHORT — вверх: достигнуто, если high >= tgt + buf
        return hi >= (tgt + buf)

def _tp_sl_hit(side: str, tp: float | None, sl: float | None, lo: float, hi: float, tick: float, hyst_ticks: int = 1) -> tuple[bool, bool]:
    """TP/SL по хвостам 1m: TP — «в прибыль», SL — «в убыток»."""
    buf = max(1, int(hyst_ticks)) * tick
    tp_hit = False; sl_hit = False
    if tp is not None and not np.isnan(tp):
        if side == "LONG":   tp_hit = hi >= (tp + buf)
        else:                tp_hit = lo <= (tp - buf)
    if sl is not None and not np.isnan(sl):
        if side == "LONG":   sl_hit = lo <= (sl - buf)
        else:                sl_hit = hi >= (sl + buf)
    return tp_hit, sl_hit

def _ml_multi_scenarios(pos: "Position", bank: float, fees_est: float, k_list=(1,2,3)) -> dict[int, float]:
    """
    Прогноз ML-20% 'после k шагов': симуляция добавления k следующих стратегических шагов.
    Возвращает {k: ml_price_after_k}.
    """
    out = {}
    # копии параметров
    avg0 = pos.avg
    qty0 = pos.qty
    used0 = _pos_total_margin(pos)
    L = max(1, int(getattr(pos, "leverage", 1) or 1))
    # индексы будущих шагов
    base = getattr(pos, "ordinary_offset", 0)
    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
    next_idx = used_ord
    for k in k_list:
        avg = avg0
        qty = qty0
        used = used0
        for i in range(k):
            if next_idx + i >= len(pos.step_margins): break
            if base + i >= len(pos.ordinary_targets): break
            m = pos.step_margins[next_idx + i]
            p = pos.ordinary_targets[base + i]["price"]
            notional = m * L
            dq = notional / max(p, 1e-9)
            qty += dq
            avg = (avg * (qty - dq) + p * dq) / max(qty, 1e-9)
            used += m
        if used > 0:
            class _Tmp: pass
            t = _Tmp()
            t.side = pos.side
            t.avg = avg
            t.leverage = L
            t.qty = qty
            # >>> важно для _pos_total_margin:
            t.steps_filled = 1
            t.step_margins = [used]
            t.reserve_used = False
            t.reserve_margin_usdt = 0.0
            out[k] = ml_price_at(t, CONFIG.ML_TARGET_PCT, bank, fees_est)
        else:
            out[k] = float('nan')
    return out

# === 2) сайзер первой ноги хеджа — гарантирует 3 STRAT после закрытия ===
def _count_strats(targets: list[dict]) -> int:
    return sum(1 for t in targets if str(t.get("label","")).upper().startswith("STRAT"))

def _plan_with_leg(symbol: str, leg_margin: float, remain_side: str, entry_px: float,
                   close_px: float, bank: float, rng_strat: dict, tick: float,
                   growth: float) -> tuple["Position", list[dict], float]:
    """
    Строит виртуальную позицию 'после хеджа' при заданной марже первой ноги.
    Возвращает (pos, targets_clipped, fees_est).
    """
    pos = Position(remain_side, signal_id="SIZER", leverage=CONFIG.LEVERAGE, owner_key="SIZER")
    pos.plan_with_reserve(bank, growth, CONFIG.STRAT_LEVELS_AFTER_HEDGE)
    pos.step_margins[0] = float(leg_margin)
    _ = pos.add_step(entry_px)
    pos.rebalance_tail_margins_excluding_reserve(bank)
    pos.from_hedge = True
    pos.hedge_entry_px = entry_px
    pos.hedge_close_px = close_px
    cum_margin = _pos_total_margin(pos)
    fees_est   = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
    pos.ordinary_targets = build_targets_with_tactical(
        pos, rng_strat, close_px, tick, bank, fees_est
    )
    # вместо обрезания целей — сначала пробуем вписаться сайзингом
    # (обрезка останется как страховка уже после финальной подгонки)
    pos.ordinary_offset = 0
    _sync_reserve3_flags(pos)
    return pos, pos.ordinary_targets, fees_est

def fit_leg_margin_for_three_strats(symbol: str, leg_margin_init: float, remain_side: str,
                                      entry_px: float, close_px: float, bank: float,
                                      rng_strat: dict, tick: float, growth: float
                                     ) -> tuple[float, "Position", list[dict], float]:
    """
    Подбирает маржу первой ноги (≤ leg_margin_init), чтобы после закрытия хеджа
    поместились TAC + STRAT #1/#2/#3 (без срезания ML-клиппером).
    """
    s_min = 0.25
    lo, hi = s_min, 1.0

    def _ok(pos, targets, fees) -> bool:
        s = _extract_strat_prices(targets)
        if len(s) < 3:
            return False
        # требуемые «мин. запасы» в тиках — по дистанциям между STRAT
        req = _strat_spacings_in_ticks(pos.side, s, tick)  # [|S1-S2|, |S2-S3|]
        # фактический запас после 2-го и 3-го STRAT
        g2 = _gap_ticks_to_ml_after_k(pos, bank, fees, s, 2, tick)
        g3 = _gap_ticks_to_ml_after_k(pos, bank, fees, s, 3, tick)
        req1 = req[0]                 # S1-S2 всегда есть при len(s)>=3
        req2 = req[1] if len(req) > 1 else req1
        ok2 = (not np.isnan(g2)) and (g2 + 1e-9 >= req1)
        ok3 = (not np.isnan(g3)) and (g3 + 1e-9 >= req2)
        return ok2 and ok3

    p0, tg0, f0 = _plan_with_leg(symbol, leg_margin_init, remain_side, entry_px, close_px,
                                 bank, rng_strat, tick, growth)
    if _count_strats(tg0) >= 3 and _ok(p0, tg0, f0):
        return leg_margin_init, p0, tg0, f0

    best = None
    for _ in range(28):
        mid = (lo + hi) / 2.0
        leg = leg_margin_init * mid
        pos, targets, fees = _plan_with_leg(symbol, leg, remain_side, entry_px, close_px,
                                            bank, rng_strat, tick, growth)
        if (_count_strats(targets) >= 3) and _ok(pos, targets, fees):
            best = (leg, pos, targets, fees)
            hi = mid
        else:
            lo = mid
    if best is not None:
        return best
    # fallback: возьмём более осторожный (уменьшенный) leg и после — ML-клиппинг
    leg = leg_margin_init * hi
    pos, targets, fees = _plan_with_leg(symbol, leg, remain_side, entry_px, close_px,
                                        bank, rng_strat, tick, growth)
    pos.ordinary_targets = clip_targets_by_ml(pos, bank=bank, fees_est=fees,
                                              targets=targets, tick=tick)
    return leg, pos, pos.ordinary_targets, fees

def clip_targets_by_ml(pos: "Position", bank: float, fees_est: float,
                       targets: list[dict], tick: float, safety_ticks: int = 2) -> list[dict]:
    """
    Последовательно симулируем доборы и отсекаем цели, которые залезают за ML(20%).
    Для LONG цели не должны опускаться ниже ML(20%) + safety, для SHORT — подниматься выше ML(20%) - safety.
    Всё, что не поместилось, потом достроится как EXT после подтверждённого пробоя.
    """
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
        # прогнозное состояние после этого добора
        qty_new = qty + dq
        avg_new = (avg * qty + price * dq) / max(qty_new, 1e-9)
        used_new = used + m
        class _Tmp: pass
        tmp = _Tmp()
        tmp.side = pos.side; tmp.avg = avg_new; tmp.leverage = L; tmp.qty = qty_new
        tmp.steps_filled = 1; tmp.step_margins = [used_new]; tmp.reserve_used = False; tmp.reserve_margin_usdt = 0.0
        ml_guard = ml_price_at(tmp, CONFIG.ML_TARGET_PCT, bank, fees_est)
        if np.isnan(ml_guard):
            break
        buf = max(1, int(safety_ticks)) * tick
        ok = (price > ml_guard + buf) if pos.side == "LONG" else (price < ml_guard - buf)
        if not ok:
            break
        # принимаем цель и продвигаем состояние
        out.append(t)
        qty, avg, used = qty_new, avg_new, used_new
    return out

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
    так и с позиционным запуском (app, broadcast, box).
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
    
    b = root.setdefault(ns_key, {})      # <- у каждого символа/чата свой карман
    # при запуске процесса гарантируем «включённость»
    b["bot_on"] = True
    b.setdefault("position", None)
    b.setdefault("fsm_state", int(FSM.IDLE))
    b.setdefault("intro_done", False)
    b["owner_key"] = ns_key              # полезно видеть в логах
    b["chat_id"]   = target_chat_id

    # Google Sheets: полностью удалено; sheet = None
    sheet = None

    # ---- SYMBOL ----
    if symbol not in FX:
        log.critical(f"Unsupported FX symbol: {symbol}. Supported: {list(FX.keys())}")
        return

    tick = default_tick(symbol)
    b["price_tick"] = tick
    log.info(f"Successfully initialized for Forex symbol {symbol} with tick size {tick}")

    # Sheets подготовка: удалено

    # Локальный хэлпер на рассылку
    async def say(txt: str):
        now_ts = time.time()
        # не дублируем точь-в-точь одинаковое сообщение чаще, чем раз в 20с
        if txt == b.get("last_msg_text") and (now_ts - b.get("last_msg_ts", 0.0) < 20):
            return
        b["last_msg_text"] = txt
        b["last_msg_ts"] = now_ts
        await broadcast(app, txt, target_chat_id=target_chat_id)

    # Напоминание о ручном запуске нового цикла
    async def _remind_manual_open():
        if time.time() - b.get("manual_remind_ts", 0) >= CONFIG.REMIND_MANUAL_MSG_COOLDOWN_SEC:
            await say("⏸ Ручной режим: чтобы запустить новый цикл, отправьте <b>/open</b>.")
            b["manual_remind_ts"] = time.time()

    # ---

    rng_strat, rng_tac = None, None
    # Хелпер: активен ли хедж (считать как «позиция есть»)
    def _hedge_active() -> bool:
        h = b.get("hedge")
        return bool(h and h.get("active"))
    # состояние 1m-хвоста
    b.setdefault("m1_minute_id", None)
    b.setdefault("m1_hi", None)
    b.setdefault("m1_lo", None)
    # состояние хеджа
    b.setdefault("hedge", None)  # {"active":bool,"bias":"LONG"/"SHORT","entry_px":float,"leg_margin":float,"lots":float}
    # состояние рекомендаций по 15m
    b.setdefault("m15_state", {"active": False, "dir": None, "ext_hi": None, "ext_lo": None,
                               "ext_candle_low": None, "ext_candle_hi": None, "last_ts": 0.0, "phase": "idle"})
    ### NEW: антиспам по 15m-свечам — не более 1 сообщения каждого типа на свече
    b.setdefault("m15_sig", {"bar_id": None, "atr_sent": False, "fr_up_sent": False, "fr_dn_sent": False})
    last_build_strat = 0.0
    last_build_tac = 0.0

    while b.get("bot_on", True):
        try:
            # Факт банка — как и раньше
            bank = float(b.get("safety_bank_usdt", CONFIG.SAFETY_BANK_USDT))

            # Целевой банк: просто факт (FUND_BOT цели удалены)
            bank_target = float(bank)

            # Синхронизируем в box — чтобы with_banks() всегда брал консистентные значения
            b["bank_target_usdt"] = bank_target
            b["safety_bank_usdt"] = bank
            # для обратной совместимости со старым /status
            b["status_bank"] = {"fact": bank, "target": bank_target}
            
            fee_maker = float(b.get("fee_maker", CONFIG.FEE_MAKER))
            fee_taker = float(b.get("fee_taker", CONFIG.FEE_TAKER))

            now = time.time()
            
            def with_banks(payload: dict) -> dict:
                payload["Bank_Target_USDT"] = bank_target
                payload["Bank_Fact_USDT"] = bank
                return payload

            # перечитывание FA/таргетов удалено

            # FA выключен: не влияет на управление
            
            # Банк не масштабируем (веса FUND_BOT удалены)
            weight = 100
            def _alloc_bank(bank: float, weight: int = 100) -> float:
                return bank
            
            reserve_off = False

            idle_for_rebuild = not (b.get("position") or _hedge_active())
            need_build_strat = (rng_strat is None) or ((now - last_build_strat > CONFIG.REBUILD_RANGE_EVERY_MIN*60) and idle_for_rebuild)
            need_build_tac   = (rng_tac   is None) or ((now - last_build_tac   > CONFIG.REBUILD_TACTICAL_EVERY_MIN*60) and idle_for_rebuild)
            if need_build_strat or need_build_tac:
                s, t = await build_ranges(symbol)
                if need_build_strat and s:
                    rng_strat = s; last_build_strat = now
                    if not _hedge_active():
                        b["intro_done"] = False
                    b["rng_strat_cache"] = s
                    log.info(f"[RANGE-STRAT] lower={fmt(rng_strat['lower'])} upper={fmt(rng_strat['upper'])} width={fmt(rng_strat['width'])}")
                if need_build_tac and t:
                    rng_tac = t; last_build_tac = now
                    if not _hedge_active():
                        b["intro_done"] = False
                    b["rng_tac_cache"] = t
                    log.info(f"[RANGE-TAC]    lower={fmt(rng_tac['lower'])} upper={fmt(rng_tac['upper'])} width={fmt(rng_tac['width'])}")

            # Если нет диапазонов — попробуем взять кеш и, при неудаче, сообщим «нет данных — пауза»
            if not (rng_strat and rng_tac):
                rng_strat = rng_strat or b.get("rng_strat_cache")
                rng_tac   = rng_tac   or b.get("rng_tac_cache")
                if not (rng_strat and rng_tac):
                    paused_msg_cool = 3600  # не чаще 1 раза в час
                    if time.time() - b.get("stale_notice_ts", 0) > paused_msg_cool:
                        await say("⏸ Нет данных — пауза. Нет свежих котировок/диапазонов, сканер на паузе до открытия рынка.")
                        b["stale_notice_ts"] = time.time()
                    b["scan_paused"] = True
                    _update_status_snapshot(b, symbol=symbol, bank_fact=bank, bank_target=bank_target,
                                            pos=b.get("position"), scan_paused=True,
                                            rng_strat=None, rng_tac=None)
                    await asyncio.sleep(30)
                    continue

            # 5m данные — ограничим кол-во баров
            bars_needed = max(60, CONFIG.VOL_WIN + CONFIG.ADX_LEN + 20)
            ohlc5_df = await maybe_await(fetch_ohlcv, symbol, CONFIG.TF_ENTRY, bars_needed)
            if ohlc5_df is None or ohlc5_df.empty:
                log.warning("Could not fetch 5m OHLCV data. Pausing.")
                if time.time() - b.get("stale_notice_ts", 0) > 3600:
                    await say("⏸ Нет данных — пауза. Нет свежих котировок (5m).")
                    b["stale_notice_ts"] = time.time()
                b["scan_paused"] = True
                _update_status_snapshot(b, symbol=symbol, bank_fact=bank, bank_target=bank_target,
                                        pos=b.get("position"), scan_paused=True,
                                        rng_strat=rng_strat, rng_tac=rng_tac)
                await asyncio.sleep(30); continue

            # Пауза — только из-за несвежих данных; FA не влияет
            if not _is_df_fresh(ohlc5_df, max_age_min=15):
                b["scan_paused"] = True
            else:
                b["scan_paused"] = False
            manage_only_flag = b.get("scan_paused", False)
            
            pos: Position | None = b.get("position")
            if pos and getattr(pos, "owner_key", None) not in (None, b["owner_key"]):
                await asyncio.sleep(1); continue # На всякий случай не трогаем чужую позицию

            if pos and b.get("fsm_state") == int(FSM.MANAGING) and pos.steps_filled > 0:
                if False and pos.reserve_available and not pos.reserve_used:
                    pos.reserve_available = False
                    pos.reserve_margin_usdt = 0.0
                    pos.max_steps = max(pos.steps_filled, pos.ord_levels)

                new_alloc_bank = _alloc_bank(bank, weight)
                if new_alloc_bank > 0 and pos.steps_filled < pos.ord_levels:
                    if pos.alloc_bank_planned <= 0 or abs(new_alloc_bank - pos.alloc_bank_planned) / max(pos.alloc_bank_planned, 1e-9) > 0.05:
                        pos.rebalance_tail_margins_excluding_reserve(new_alloc_bank)
                        pos.alloc_bank_planned = new_alloc_bank

            df5 = ohlc5_df.iloc[:, -5:].copy()
            df5.columns = ["open", "high", "low", "close", "volume"]
            try:
                ind = compute_indicators_5m(df5)
            except (ValueError, IndexError) as e:
                log.warning(f"Indicator calculation failed: {e}. Skipping cycle.")
                await asyncio.sleep(2); continue

            px = float(df5["close"].iloc[-1])

            # ------------------------------------------------------------------
            # НЕМЕДЛЕННЫЙ РУЧНОЙ ВХОД ЧЕРЕЗ ХЕДЖ (без ожидания TAC/STRAT)
            # ------------------------------------------------------------------
            # Поддерживаем оба ключа: новый cmd_force_open_now_dir и старый cmd_open_manual_dir
            manual_dir = b.pop("cmd_force_open_now_dir", None) or b.pop("cmd_open_manual_dir", None)
            if manual_dir in ("LONG", "SHORT"):
                # Защита от дубля: если уже есть активная позиция/цикл — не открываем
                if pos or _hedge_active():
                    await say(
                        f"⚠️ Уже есть активная позиция по <b>{symbol}</b> — "
                        f"игнорирую ручной старт <b>{manual_dir}</b>."
                    )
                else:
                    # Снимаем ручной режим, чтобы цикл не стопорился
                    b["user_manual_mode"] = False
                    # Входной ценой считаем ту, что пришла с команды (если есть), иначе — текущий px
                    entry_px = b.pop("cmd_force_open_now_entry_px", None)
                    try:
                        entry_px = float(entry_px) if entry_px is not None else float(px)
                    except Exception:
                        entry_px = float(px)

                    # Направление «остающейся ноги» и bias прибыльной ноги
                    remain_side = manual_dir
                    bias_side = ("SHORT" if remain_side == "LONG" else "LONG")

                    # TAC-границы того же окна, что использует сканер
                    tac_lo = rng_tac["lower"] + 0.30 * rng_tac["width"]
                    tac_hi = rng_tac["lower"] + 0.70 * rng_tac["width"]

                    # HC строго от entry_px (замораживаем расчёт)
                    planned_hc_px = planned_hc_price(entry_px, tac_lo, tac_hi, bias_side, CONFIG.HEDGE_MODE, tick)

                    # Общее планирование как в авто-входе, но банк — через веса FUND_BOT
                    ord_levels_tmp = min(CONFIG.DCA_LEVELS - 1, 1 + CONFIG.ORDINARY_ADDS)
                    growth = choose_growth(ind, rng_strat, rng_tac)
                    alloc_bank = _alloc_bank(bank, weight)  # <<<< было: bank
                    total_target = alloc_bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
                    margins_full = plan_margins_bank_first(total_target, ord_levels_tmp + 1, growth)
                    margin_3 = _sum_first_n(margins_full, 3)
                    if margin_3 > total_target * 0.5:
                        scale = (total_target * 0.5) / margin_3
                        margin_3 *= scale
                    # --- сайзер ноги хеджа: гарантируем TAC + STRAT #1/#2/#3 после закрытия
                    remain_side = manual_dir
                    growth_after = CONFIG.GROWTH_AFTER_HEDGE
                    alloc_bank_after = _alloc_bank(bank, weight)
                    leg_sized, _pos, _targets, _fees_est = fit_leg_margin_for_three_strats(
                        symbol, margin_3, remain_side, entry_px, planned_hc_px,
                        alloc_bank_after, rng_strat, tick, growth_after
                    )
                    lots_per_leg = margin_to_lots(symbol, leg_sized, price=entry_px, leverage=CONFIG.LEVERAGE)
                    dep_total = 2 * leg_sized
                    # Фиксируем entry_px и уже «ужатую» маржу внутри состояния хеджа
                    b["hedge"] = {"active": True, "bias": bias_side, "entry_px": entry_px,
                                  "hc_px": planned_hc_px,
                                  "leg_margin": leg_sized, "lots_per_leg": lots_per_leg, "ts": time.time()}
                    ### NEW: сброс одноразового алерта «закрыть хедж»
                    b["hedge_close_notified"] = False

                    # Полноценное «HEDGE OPEN»-сообщение как при авто-входе
                    _hc_dticks = abs((planned_hc_px - entry_px) / max(tick, 1e-12))
                    _hc_dpct   = abs((planned_hc_px / max(entry_px, 1e-12) - 1.0) * 100.0)
                    # Обновляем снапшот статуса
                    b["status_snapshot"] = b.get("status_snapshot", {})
                    b["status_snapshot"]["state"] = "OPENING"

                    # _pos, _fees_est уже посчитаны сайзером выше

                    _ml_now    = ml_price_at(_pos, CONFIG.ML_TARGET_PCT, alloc_bank_after, _fees_est)
                    _ml_arrow  = "↓" if remain_side == "LONG" else "↑"
                    _dist_now  = ml_distance_pct(_pos.side, px, _ml_now)  # расстояние — от текущей цены рынка
                    _ml_after_line = _ml_after_labels_line(_pos, alloc_bank_after, _fees_est)
                    _ml_reserve = ml_reserve_pct_to_ml20(_pos, px, alloc_bank_after, _fees_est)
                    _nxt = _pos.ordinary_targets[0] if _pos.ordinary_targets else None
                    _nxt_txt = "N/A" if _nxt is None else f"{fmt(_nxt['price'])} ({_nxt['label']})"
                    _nxt_margin = _pos.step_margins[1] if len(_pos.step_margins) > 1 else None
                    if _nxt and _nxt_margin:
                        _nxt_lots = margin_to_lots(symbol, _nxt_margin, price=_nxt['price'], leverage=_pos.leverage)
                        _nxt_dep_txt = f"{_nxt_margin:.2f} USD ≈ {_nxt_lots:.2f} lot"
                    else:
                        _nxt_dep_txt = "N/A"
                    _levels_block = render_remaining_levels_block(symbol, _pos, alloc_bank_after, CONFIG.FEE_TAKER, tick)

                    await say(
                        f"🧷 HEDGE OPEN [{bias_side}] \n"
                        f"Цена: <code>{fmt(entry_px)}</code> | Обе ноги по <b>{lots_per_leg:.2f} lot</b>\n"
                        f"Депозит (суммарно): <b>{dep_total:.2f} USD</b> (по <b>{leg_sized:.2f}</b> на ногу)\n"
                        f"План HC: HC) <code>{fmt(planned_hc_px)}</code> — Δ≈ {_hc_dticks:.0f} тик. ({_hc_dpct:.2f}%)\n"
                        f"⚙️ Превью после закрытия хеджа (останется <b>{remain_side}</b>):\n"
                        f"Средняя: <code>{fmt(_pos.avg)}</code> (P/L 0) | TP: <code>{fmt(_pos.tp_price)}</code>\n"
                        f"ML(20%): {_ml_arrow}<code>{fmt(_ml_now)}</code> ({'N/A' if np.isnan(_dist_now) else f'{_dist_now:.2f}%'} от текущей)\n"
                        f"Запас маржи до ML20%: <b>{('∞' if not np.isfinite(_ml_reserve) else f'{_ml_reserve:.1f}%')}</b>\n"
                        f"{_ml_after_line}\n"
                        f"{_levels_block}\n"
                        f"Сигнал на ЗАКРЫТИЕ хеджа придёт при касании целевой цены HC по 1m-хвосту."
                    )
                    # Sheets логирование удалено

                    # Продолжаем следующую итерацию (не уходим в обычные триггеры)
                    continue

            # ---- 15m ATR/Fractals (только уведомления; БЕЗ заморозок) ----
            try:
                if now - b["m15_state"].get("last_fetch", 0.0) >= CONFIG.M15_RECO["CHECK_EVERY_SEC"]:
                    m15_df = await maybe_await(fetch_ohlcv, symbol, "15m", max(50, CONFIG.M15_RECO["ATR_LEN"] + 10))
                    b["m15_state"]["last_fetch"] = now
                    if m15_df is not None and not m15_df.empty:
                        d = m15_df.iloc[:, -5:].copy(); d.columns = ["open","high","low","close","volume"]
                        atr15 = ta_atr(d["high"], d["low"], d["close"], length=CONFIG.M15_RECO["ATR_LEN"]).iloc[-1]
                        hi = float(d["high"].iloc[-1]); lo = float(d["low"].iloc[-1])
                        rng = hi - lo
                        # антиспам: сброс флагов на новой 15m-свече
                        bar_id = d.index[-1]
                        if b["m15_sig"].get("bar_id") != bar_id:
                            b["m15_sig"].update({"bar_id": bar_id, "atr_sent": False, "fr_up_sent": False, "fr_dn_sent": False})
                        # === ATR-событие (без «заморозок») ===
                        if atr15 > 0 and rng >= CONFIG.M15_RECO["TRIGGER_MULT"] * float(atr15):
                            if not b["m15_sig"]["atr_sent"]:
                                dir_txt = "вверх" if (d["close"].iloc[-1] >= d["open"].iloc[-1]) else "вниз"
                                await say(f"📢 Бар выше 1.5 ATR {dir_txt}")
                                b["m15_sig"]["atr_sent"] = True

                        # === ФРАКТАЛЫ (15m) ===
                        fr_up, fr_dn = compute_fractals_15m(d)
                        last_up, last_dn = _last_confirmed_fractals(fr_up, fr_dn)
                        if (last_up is not None) and (hi >= last_up) and (not b["m15_sig"]["fr_up_sent"]):
                            await say("📐 Фрактал вверх пройден")
                            b["m15_sig"]["fr_up_sent"] = True
                        if (last_dn is not None) and (lo <= last_dn) and (not b["m15_sig"]["fr_dn_sent"]):
                            await say("📐 Фрактал вниз пройден")
                            b["m15_sig"]["fr_dn_sent"] = True
            except Exception:
                log.exception("m15 recommendations failed")

            # ---- 1m: обновляем хвост текущей минуты ----
            try:
                m1 = await maybe_await(fetch_ohlcv, symbol, CONFIG.TF_TRIGGER, 2)
            except Exception:
                m1 = None
            if m1 is not None and not m1.empty:
                m1c = m1.iloc[:, -5:].copy()
                m1c.columns = ["open","high","low","close","volume"]
                # последняя (текущая) минутка
                m_hi = float(m1c["high"].iloc[-1]); m_lo = float(m1c["low"].iloc[-1])
                # идентификатор минуты
                last_idx = m1.index[-1]
                m_id = (pd.to_datetime(last_idx).floor("1min") if isinstance(last_idx, (pd.Timestamp, np.datetime64))
                        else pd.Timestamp.utcnow().floor("1min"))
                if b.get("m1_minute_id") != m_id:
                    b["m1_minute_id"] = m_id
                    b["m1_hi"] = m_hi
                    b["m1_lo"] = m_lo
                else:
                    b["m1_hi"] = max(float(b["m1_hi"]), m_hi) if b.get("m1_hi") is not None else m_hi
                    b["m1_lo"] = min(float(b["m1_lo"]), m_lo) if b.get("m1_lo") is not None else m_lo
            else:
                # фолбэк: хотя бы обновлять экстремумы текущей «минуты» по последней цене
                m_id_fb = pd.Timestamp.utcnow().floor("1min")
                if b.get("m1_minute_id") != m_id_fb:
                    b["m1_minute_id"] = m_id_fb
                    b["m1_hi"] = px; b["m1_lo"] = px
                else:
                    b["m1_hi"] = max(float(b["m1_hi"] or px), px)
                    b["m1_lo"] = min(float(b["m1_lo"] or px), px)
            m1_hi = b.get("m1_hi"); m1_lo = b.get("m1_lo")

            # запомним прошлую цену для кросс-овера
            prev_px = None
            if b.get("position"):
                prev_px = b["position"].last_px
                b["position"].last_px = px

            # Интро показываем только когда нет ни позиции, ни активного хеджа
            if (not b.get("intro_done")) and (pos is None) and (not _hedge_active()):
                p30_t = rng_tac["lower"] + 0.30 * rng_tac["width"]
                p70_t = rng_tac["lower"] + 0.70 * rng_tac["width"]
                d_to_long  = max(0.0, px - p30_t)
                d_to_short = max(0.0, p70_t - px)
                pct_to_long  = (d_to_long  / max(px, 1e-9)) * 100
                pct_to_short = (d_to_short / max(px, 1e-9)) * 100
                brk_up, brk_dn = break_levels(rng_strat)
                width_ratio = (rng_tac["width"] / max(rng_strat["width"], 1e-9)) * 100.0
                await say(
                    "🎯 Пороги входа (<b>TAC 30/70</b>): LONG ≤ <code>{}</code>, SHORT ≥ <code>{}</code>\n"
                    "📏 Диапазоны:\n"
                    "• STRAT: [{} … {}] w={}\n"
                    "• TAC (3d): [{} … {}] w={} (≈{:.0f}% от STRAT)\n"
                    "🔓 Пробой STRAT: ↑{} | ↓{}\n"
                    "Текущая: {}. До LONG: {} ({:.2f}%), до SHORT: {} ({:.2f}%).".format(
                        fmt(p30_t), fmt(p70_t),
                        fmt(rng_strat['lower']), fmt(rng_strat['upper']), fmt(rng_strat['width']),
                        fmt(rng_tac['lower']),   fmt(rng_tac['upper']),   fmt(rng_tac['width']), width_ratio,
                        fmt(brk_up), fmt(brk_dn),
                        fmt(px), fmt(d_to_long), pct_to_long, fmt(d_to_short), pct_to_short
                    )
                )
                b["intro_done"] = True

            # Обновляем снапшот статуса на каждом цикле
            _update_status_snapshot(b, symbol=symbol, bank_fact=bank, bank_target=bank_target,
                                    pos=pos, scan_paused=b.get("scan_paused", False),
                                    rng_strat=rng_strat, rng_tac=rng_tac)

            # -------- STRAT commands handling --------
            # Работают ТОЛЬКО когда есть позиция после хеджа (3 стратегических шага «хвоста»)
            if b.pop("cmd_strat_show", False):
                if not pos or not pos.from_hedge or b.get("fsm_state") != int(FSM.MANAGING):
                    await say("ℹ️ Нет активной позиции «после хеджа» — показать нечего.")
                else:
                    cum_margin = _pos_total_margin(pos)
                    fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                    await say(_strat_report_text(pos, px, tick, bank, fees_est, rng_strat, hdr="📋 STRAT (текущий план)"))

            _set_req = b.pop("cmd_strat_set", None)
            if _set_req is not None:
                if not pos or not pos.from_hedge or b.get("fsm_state") != int(FSM.MANAGING):
                    await say("ℹ️ Нет активной позиции «после хеджа» — менять STRAT-точки нельзя.")
                else:
                    # parse & sanitize
                    vals = []
                    for v in _set_req:
                        try: vals.append(float(str(v).replace(",",".").strip()))
                        except: pass
                    if len(vals) < 1:
                        await say("❗ Укажите 1–3 цен: /strat set P1 P2 P3")
                    else:
                        # дополним до 3 последним значением
                        while len(vals) < 3: vals.append(vals[-1])
                        # квантование и упорядочивание «в минус» относительно стороны
                        q = [quantize_to_tick(x, tick) for x in vals[:3]]
                        if pos.side=="LONG":  q = sorted(q, reverse=True)  # вниз → убывание цен
                        else:                 q = sorted(q)              # вверх → возрастание
                        # обеспечиваем минимум разрыва
                        min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
                        q_fixed = []
                        for i,x in enumerate(q):
                            if i==0:
                                # первый — не ближе min_gap к entry
                                base_off = min(getattr(pos,"ordinary_offset",0), max(0, len(pos.ordinary_targets)-1))
                                base = pos.avg if pos.steps_filled <= 1 or not pos.ordinary_targets else pos.ordinary_targets[base_off]["price"]
                                if pos.side=="LONG" and x > base - min_gap: x = base - min_gap
                                if pos.side=="SHORT" and x < base + min_gap: x = base + min_gap
                            else:
                                prev = q_fixed[-1]
                                if pos.side=="LONG" and x > prev - min_gap: x = prev - min_gap
                                if pos.side=="SHORT" and x < prev + min_gap: x = prev + min_gap
                            q_fixed.append(quantize_to_tick(x, tick))
                        # применяем ТОЛЬКО к STRAT-целям (TAC сохраняем, если есть)
                        base_off = getattr(pos, "ordinary_offset", 0)
                        # найдём индексы первых трёх STRAT-таргетов после base_off
                        idxs = [j for j in range(base_off, len(pos.ordinary_targets)) 
                                if str(pos.ordinary_targets[j].get("label","")).startswith("STRAT")][:3]
                        labels = ["STRAT 33%","STRAT 66%","STRAT 100%"]
                        for i, j in enumerate(idxs):
                            pos.ordinary_targets[j] = {"price": q_fixed[i], "label": labels[i]}
                        # заново помечаем третий как резерв
                        for t in pos.ordinary_targets:
                            t.pop("reserve3", None)
                        if len(idxs) >= 3:
                            pos.ordinary_targets[idxs[2]]["reserve3"] = True
                        _sync_reserve3_flags(pos)
                        cum_margin = _pos_total_margin(pos)
                        fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                        await say(_strat_report_text(pos, px, tick, bank, fees_est, rng_strat, hdr="✏️ STRAT обновлён вручную (TAC сохранён)"))
                        # Sheets логирование удалено

            if b.pop("cmd_strat_reset", False):
                if not pos or not pos.from_hedge or b.get("fsm_state") != int(FSM.MANAGING):
                    await say("ℹ️ Нет активной позиции «после хеджа» — сбрасывать нечего.")
                else:
                    cum_margin = _pos_total_margin(pos)
                    fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                    # сбрасываем сразу весь хвост к авто: TAC (авто) + STRAT от цены закрытия хеджа
                    close_px = pos.hedge_close_px or pos.avg
                    pos.manual_tac_price = None
                    pos.ordinary_targets = build_targets_with_tactical(pos, rng_strat, close_px, tick, bank, fees_est)
                    pos.ordinary_offset = min(getattr(pos,"ordinary_offset",0), len(pos.ordinary_targets))
                    _sync_reserve3_flags(pos)
                    await say(_strat_report_text(pos, px, tick, bank, fees_est, rng_strat, hdr="♻️ STRAT сброшен к авто-плану"))
                    # Sheets логирование удалено
            # -------- /STRAT commands end --------

            ### NEW: /tac set|reset — ручная тактическая точка между HC и STRAT#1
            _tac_req = b.pop("cmd_tac_set", None)
            if _tac_req is not None:
                if not pos or not pos.from_hedge or b.get("fsm_state") != int(FSM.MANAGING):
                    await say("ℹ️ Нет активной позиции «после хеджа» — задавать TAC нельзя.")
                else:
                    # берём первый аргумент как цену
                    v = None
                    for it in (list(_tac_req) if isinstance(_tac_req,(list,tuple)) else [_tac_req]):
                        try: v = float(str(it).replace(",",".").strip()); break
                        except: pass
                    if v is None:
                        await say("❗ Укажите цену TAC: /tac set PRICE")
                    else:
                        tac_q = quantize_to_tick(v, tick)
                        pos.manual_tac_price = tac_q
                        cum_margin = _pos_total_margin(pos)
                        fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                        base_close = pos.hedge_close_px or pos.avg
                        pos.ordinary_targets = build_targets_with_tactical(pos, rng_strat, base_close, tick, bank, fees_est)
                        pos.ordinary_offset = min(getattr(pos,"ordinary_offset",0), len(pos.ordinary_targets))
                        _sync_reserve3_flags(pos)
                        await say(f"✏️ TAC обновлён вручную → <code>{fmt(tac_q)}</code>")
            if b.pop("cmd_tac_reset", False):
                if not pos or not pos.from_hedge or b.get("fsm_state") != int(FSM.MANAGING):
                    await say("ℹ️ Нет активной позиции «после хеджа» — сбрасывать TAC нечего.")
                else:
                    pos.manual_tac_price = None
                    cum_margin = _pos_total_margin(pos)
                    fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                    base_close = pos.hedge_close_px or pos.avg
                    pos.ordinary_targets = build_targets_with_tactical(pos, rng_strat, base_close, tick, bank, fees_est)
                    pos.ordinary_offset = min(getattr(pos,"ordinary_offset",0), len(pos.ordinary_targets))
                    _sync_reserve3_flags(pos)
                    await say("♻️ TAC сброшен к авто-плану")

            # Диагностика целей FUND_BOT — удалена

            # Ручное закрытие
            if pos and b.get("force_close"):
                # Разрешаем ручное закрытие как в OPENED, так и в MANAGING
                if (not pos) or pos.steps_filled <= 0 or (b.get("fsm_state") not in (int(FSM.OPENED), int(FSM.MANAGING))):
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
                # Sheets логирование удалено
                b["force_close"] = False
                # Включаем «ручной режим» до явного /open
                if CONFIG.REQUIRE_MANUAL_REOPEN_ON.get("manual_close", True):
                    b["user_manual_mode"] = True
                    await _remind_manual_open()
                pos.last_sl_notified_price = None
                b["position"] = None
                b["fsm_state"] = int(FSM.IDLE)
                continue

            # --- Проба пробоя STRAT (без заморозок; используем для EXT-подтверждения) ---
            if pos:
                brk_up, brk_dn = break_levels(rng_strat)
                eps = CONFIG.EXT_AFTER_BREAK["PRICE_EPS"]

                def _is_beyond(px_val, up, dn, eps_, direction):
                    if direction == "up":
                        return px_val >= up * (1.0 + eps_)
                    else:
                        return px_val <= dn * (1.0 - eps_)

                # направление потенциального пробоя
                dir_now = "up" if px >= brk_up * (1.0 + eps) else ("down" if px <= brk_dn * (1.0 - eps) else None)
                probe = b.get("break_probe")  # {"dir","hits","started","last_ts"} | None
                now_ts = time.time()

                # старт пробы
                if dir_now and (probe is None):
                    b["break_probe"] = {"dir": dir_now, "hits": 1, "started": now_ts, "last_ts": now_ts}

                # апдейт пробы
                probe = b.get("break_probe")
                if probe:
                    if not _is_beyond(px, brk_up, brk_dn, eps, probe["dir"]):
                        # вышли из зоны — отменяем пробу
                        b["break_probe"] = None
                    else:
                        # добираем замеры раз в INTERVAL_SEC
                        if now_ts - probe["last_ts"] >= CONFIG.BREAK_PROBE["INTERVAL_SEC"]:
                            probe["hits"] += 1
                            probe["last_ts"] = now_ts
                        # таймаут пробы
                        if now_ts - probe["started"] > CONFIG.BREAK_PROBE["TIMEOUT_SEC"]:
                            b["break_probe"] = None
                        # подтверждение пробы — помечаем направление, но НЕ замораживаем усреднения
                        probe = b.get("break_probe")
                        if probe and probe["hits"] >= CONFIG.BREAK_PROBE["SAMPLES"]:
                            pos.break_dir = probe["dir"]
                            pos.break_confirm_bars = 0
                            pos.extension_planned = False
                            b["break_last_bar_id"] = None
                            b["break_probe"] = None

            # Подтверждение пробоя (без заморозок) и дорисовка коридора
            if pos and (pos.break_dir is not None):
                if not pos or b.get("fsm_state") != int(FSM.MANAGING) or pos.steps_filled <= 0:
                    await asyncio.sleep(1); continue
                brk_up, brk_dn = break_levels(rng_strat)
                eps = CONFIG.EXT_AFTER_BREAK["PRICE_EPS"]
                still_beyond = (px >= brk_up * (1.0 + eps)) if pos.break_dir == "up" else (px <= brk_dn * (1.0 - eps))

                last_idx = df5.index[-1]
                if isinstance(last_idx, (pd.Timestamp, np.datetime64)):
                    curr_bar_id = pd.to_datetime(last_idx).floor("5min")
                elif "time" in ohlc5_df.columns:
                    curr_bar_id = pd.to_datetime(ohlc5_df["time"].iloc[-1]).floor("5min")
                else:
                    curr_bar_id = pd.Timestamp.utcnow().floor("5min")

                last_seen = b.get("break_last_bar_id")
                if last_seen is None or curr_bar_id != last_seen:
                    pos.break_confirm_bars = (pos.break_confirm_bars + 1) if still_beyond else 0
                    b["break_last_bar_id"] = curr_bar_id

                re_up = rng_strat["upper"] * (1.0 + CONFIG.REENTRY_BAND)
                re_dn = rng_strat["lower"] * (1.0 - CONFIG.REENTRY_BAND)
                back_inside = (re_dn <= px <= re_up)

                if (not still_beyond) and back_inside and pos.break_confirm_bars == 0:
                    pos.break_dir = None
                    pos.extension_planned = False

                if (not pos.extension_planned) and (pos.break_confirm_bars >= CONFIG.EXT_AFTER_BREAK["CONFIRM_BARS_5M"]):
                    used_ord_now = pos.steps_filled - (1 if pos.reserve_used else 0)
                    keep_idx = max(getattr(pos, "ordinary_offset", 0), max(0, used_ord_now - 1))
                    new_targets = await plan_extension_after_break(symbol, pos, rng_strat, rng_tac, px, tick)
                    
                    if len(new_targets) > len(pos.ordinary_targets):
                        pos.ordinary_targets = new_targets
                        pos.extension_planned = True
                        pos.break_dir = None
                        pos.ordinary_offset = min(keep_idx, len(pos.ordinary_targets))
                        # Sheets логирование удалено

            # ===== ВХОД ЧЕРЕЗ ХЕДЖ (две ноги) =====
            # Авто-вход от сканера (как раньше)
            if (not pos) and (b.get("hedge") is None) and (not manage_only_flag) and (not b.get("user_manual_mode", False)):
                # TAC-границы
                tac_lo = rng_tac["lower"] + 0.30 * rng_tac["width"]
                tac_hi = rng_tac["lower"] + 0.70 * rng_tac["width"]
                can_long  = (m1_lo is not None) and (m1_lo <= tac_lo - CONFIG.WICK_HYST_TICKS * tick)
                can_short = (m1_hi is not None) and (m1_hi >= tac_hi + CONFIG.WICK_HYST_TICKS * tick)
                # разрешаем вход, если коснулись одного из TAC
                if can_long or can_short:
                    # <<< MODIFIED: Bias logic depends on HEDGE_MODE
                    if CONFIG.HEDGE_MODE == "trend":
                        # верхний триггер → LONG-bias, нижний → SHORT-bias
                        bias_side = "LONG" if can_short else "SHORT"
                    else: # revert
                        # как было: нижний → LONG-bias, верхний → SHORT-bias
                        bias_side = "LONG" if can_long else "SHORT"

                    # план «старой» лестницы, чтобы посчитать сумму (OPEN + TAC-33 + TAC-67)
                    ord_levels_tmp = min(CONFIG.DCA_LEVELS - 1, 1 + CONFIG.ORDINARY_ADDS)
                    growth = choose_growth(ind, rng_strat, rng_tac)
                    # FA OFF: без урезаний
                    alloc_bank = _alloc_bank(bank, weight)
                    total_target = alloc_bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
                    margins_full = plan_margins_bank_first(total_target, ord_levels_tmp + 1, growth)
                    margin_3 = _sum_first_n(margins_full, 3)  # на КАЖДУЮ ногу
                    # Гарантия: 2*leg_margin <= 70% банка
                    total_target = alloc_bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
                    if margin_3 > total_target * 0.5:
                        scale = (total_target * 0.5) / margin_3
                        margin_3 *= scale

                    # <<< MODIFIED: Calculate HC price using helper and store it
                    planned_hc_px = planned_hc_price(px, tac_lo, tac_hi, bias_side, CONFIG.HEDGE_MODE, tick)

                    # --- Превью после закрытия хеджа: остаётся противоположная нога ---
                    remain_side = "SHORT" if bias_side == "LONG" else "LONG"
                    # после хеджа льем более плавную лестницу
                    growth_after = CONFIG.GROWTH_AFTER_HEDGE
                    alloc_bank_after = _alloc_bank(bank, weight)
                    # Подберём маржу первой ноги так, чтобы поместились 3 STRAT
                    leg_sized, _pos, _targets, _fees_est = fit_leg_margin_for_three_strats(
                        symbol, margin_3, remain_side, px, planned_hc_px,
                        alloc_bank_after, rng_strat, tick, growth_after
                    )
                    lots_per_leg = margin_to_lots(symbol, leg_sized, price=px, leverage=CONFIG.LEVERAGE)
                    dep_total = 2 * leg_sized

                    b["hedge"] = {
                        "active": True,
                        "bias": bias_side,  # где «ждём» профит
                        "entry_px": px,
                        "hc_px": planned_hc_px, # <<< MODIFIED: Store target close price
                        "leg_margin": leg_sized,
                        "lots_per_leg": lots_per_leg,
                        "ts": time.time()
                    }
                    ### NEW: сброс одноразового алерта «закрыть хедж»
                    b["hedge_close_notified"] = False
                    
                    _hc_dticks = abs((planned_hc_px - px) / max(tick, 1e-12))
                    _hc_dpct   = abs((planned_hc_px / max(px, 1e-12) - 1.0) * 100.0)

                    # ML/риски и план следующего добора
                    _ml_now    = ml_price_at(_pos, CONFIG.ML_TARGET_PCT, alloc_bank_after, _fees_est)
                    _ml_arrow  = "↓" if remain_side == "LONG" else "↑"
                    _dist_now  = ml_distance_pct(_pos.side, px, _ml_now)
                    _dist_now_txt = "N/A" if np.isnan(_dist_now) else f"{_dist_now:.2f}%"
                    _ml_after_line = _ml_after_labels_line(_pos, alloc_bank_after, _fees_est)
                    _ml_reserve = ml_reserve_pct_to_ml20(_pos, px, alloc_bank_after, _fees_est)
                    _levels_block = render_remaining_levels_block(symbol, _pos, alloc_bank_after, CONFIG.FEE_TAKER, tick)
                    def _fmt_ml(v): return "N/A" if (v is None or np.isnan(v)) else fmt(v)
                    _planned_now = len([t for t in _pos.ordinary_targets[getattr(_pos,"ordinary_offset",0):]
                                        if str(t.get("label","")).startswith("STRAT")])

                    await say(
                        f"🧷 HEDGE OPEN [{bias_side}] \n"
                        f"Цена: <code>{fmt(px)}</code> | Обе ноги по <b>{lots_per_leg:.2f} lot</b>\n"
                        f"Депозит (суммарно): <b>{dep_total:.2f} USD</b> (по <b>{leg_sized:.2f}</b> на ногу)\n"
                        f"План HC: HC) <code>{fmt(planned_hc_px)}</code> — Δ≈ {_hc_dticks:.0f} тик. ({_hc_dpct:.2f}%)\n"
                        f"⚙️ Превью после закрытия хеджа (останется <b>{remain_side}</b>):\n"
                        f"Средняя: <code>{fmt(_pos.avg)}</code> (P/L 0) | TP: <code>{fmt(_pos.tp_price)}</code>\n"
                        f"ML(20%): {_ml_arrow}<code>{fmt(_ml_now)}</code> ({_dist_now_txt} от текущей)\n"
                        f"Запас маржи до ML20%: <b>{('∞' if not np.isfinite(_ml_reserve) else f'{_ml_reserve:.1f}%')}</b>\n"
                        f"{_ml_after_line}\n"
                        f"{_levels_block}\n"
                        f"(Осталось: {_planned_now} из 3)\n"
                        f"Сигнал на ЗАКРЫТИЕ хеджа придёт при касании целевой цены HC по 1m-хвосту."
                    )
                    # Sheets логирование удалено

            # СИГНАЛ «закрыть хедж» (по противоположному TAC, по хвостам 1m)
            # + АКТИВНОЕ ОБНОВЛЕНИЕ/ПЕРЕВОРОТ BIAS, если возник «противоположный вход»
            if (b.get("hedge") and b["hedge"].get("active")):
                tac_lo = rng_tac["lower"] + 0.30 * rng_tac["width"]
                tac_hi = rng_tac["lower"] + 0.70 * rng_tac["width"]
                bias = b["hedge"]["bias"]

                # --- РУЧНОЙ ПЕРЕВОРОТ ХЕДЖА ---
                _flip = b.pop("cmd_hedge_flip", None)
                if _flip:
                    if not (b.get("hedge") and b["hedge"].get("active")):
                        await say("ℹ️ Хедж не активен — переворот невозможен.")
                    else:
                        # 1) аргумент команды теперь трактуем как 'remain side' (что должно остаться)
                        desired_remain = str(_flip).upper() if isinstance(_flip, str) else None
                        curr_bias = b["hedge"]["bias"]

                        if desired_remain in ("LONG","SHORT"):
                            new_bias = "SHORT" if desired_remain == "LONG" else "LONG"
                        else:
                            # если аргумент не задан, просто инвертируем текущий bias
                            new_bias = "SHORT" if curr_bias == "LONG" else "LONG"
                            desired_remain = "SHORT" if new_bias == "LONG" else "LONG"

                        b["hedge"]["bias"] = new_bias

                        # 2) TAC-уровни и HC считаем ОТ ТОЧКИ ВХОДА ХЕДЖА
                        tac_lo = rng_tac["lower"] + 0.30 * rng_tac["width"]
                        tac_hi = rng_tac["lower"] + 0.70 * rng_tac["width"]
                        entry_px0 = float(b["hedge"]["entry_px"])
                        planned_hc_px = quantize_to_tick(
                            planned_hc_price(entry_px0, tac_lo, tac_hi, new_bias, CONFIG.HEDGE_MODE, tick),
                            tick
                        )
                        b["hedge"]["hc_px"] = planned_hc_px

                        # 3) Превью позиции после закрытия хеджа уже строим в сторону desired_remain
                        remain_side = desired_remain
                        leg_margin  = float(b["hedge"]["leg_margin"])
                        alloc_bank_after = _alloc_bank(bank, weight)
                        growth_after = CONFIG.GROWTH_AFTER_HEDGE

                        # Превью без изменения маржи (факт уже в рынке)
                        _pos, _, _fees = _plan_with_leg(symbol, leg_margin, remain_side, entry_px0,
                                                        planned_hc_px, alloc_bank_after, rng_strat, tick, growth_after)

                        _ml_now = ml_price_at(_pos, CONFIG.ML_TARGET_PCT, alloc_bank_after, _fees)
                        _ml_arrow = "↓" if remain_side == "LONG" else "↑"
                        _dist_now = ml_distance_pct(_pos.side, px, _ml_now)
                        _dist_now_txt = "N/A" if np.isnan(_dist_now) else f"{_dist_now:.2f}%"
                        _avail = min(3, len(_pos.step_margins) - 1, len(_pos.ordinary_targets))
                        _scen = _ml_multi_scenarios(_pos, alloc_bank_after, _fees, k_list=tuple(range(1, _avail + 1)))
                        def _fmt_ml(v): 
                            return "N/A" if (v is None or np.isnan(v)) else fmt(v)
                        _ml_after_line = _ml_after_labels_line(_pos, alloc_bank_after, _fees)
                        _levels_block = render_remaining_levels_block(symbol, _pos, alloc_bank_after, CONFIG.FEE_TAKER, tick)
                        
                        await say(
                            f"🔁 HEDGE FLIP [Останется: {remain_side}] \n"
                            f"HC теперь: <code>{fmt(planned_hc_px)}</code>\n"
                            f"Останется: <b>{remain_side}</b>\n"
                            f"Средняя: <code>{fmt(_pos.avg)}</code> (P/L 0) | TP: <code>{fmt(_pos.tp_price)}</code>\n"
                            f"ML(20%): {_ml_arrow}<code>{fmt(_ml_now)}</code> "
                            f"({_dist_now_txt} от текущей)\n"
                            f"{_ml_after_line}\n"
                            f"{_levels_block}"
                        )
                # сигнал «закрыть хедж» (только уведомление) — по целевой hc_px, если она задана
                bias_now = b["hedge"]["bias"]
                hc_px = b["hedge"].get("hc_px")  # может отсутствовать для старых записей
                buf = CONFIG.WICK_HYST_TICKS * tick
                if hc_px is not None:
                    need_close = (
                        (bias_now == "LONG"  and (b.get("m1_hi") is not None) and (b["m1_hi"] >= hc_px + buf)) or
                        (bias_now == "SHORT" and (b.get("m1_lo") is not None) and (b["m1_lo"] <= hc_px - buf))
                    )
                else:
                    # фолбэк на TAC-границы, если hc_px не задан
                    need_close = (
                        (bias_now == "LONG"  and (b.get("m1_hi") is not None) and (b["m1_hi"] >= tac_hi + buf)) or
                        (bias_now == "SHORT" and (b.get("m1_lo") is not None) and (b["m1_lo"] <= tac_lo - buf))
                    )
                if need_close and (not b.get("hedge_close_notified", False)):
                    b["hedge_close_notified"] = True
                    side_win = "LONG" if bias_now == "LONG" else "SHORT"
                    await say(
                        f"📣 Сигнал: <b>закрыть хедж</b> — закройте прибыльную ногу <b>{side_win}</b>.\n"
                        f"Отправьте фактическую цену закрытия командой: <code>/хедж_закрытие PRICE</code>"
                    )

            # Если пришла команда /хедж_закрытие PRICE — оформляем одиночную позицию с оставшейся ногой
            if (b.get("hedge") and b["hedge"].get("active") and b.get("hedge_close_price") is not None):
                try:
                    close_px = float(b.get("hedge_close_price"))
                except Exception:
                    close_px = px
                h = b["hedge"]; h["active"] = False
                b["hedge_close_notified"] = False
                bias = h["bias"]
                # остаётся противоположная, «минусовая» нога
                remain_side = "SHORT" if bias == "LONG" else "LONG"
                leg_margin = float(h["leg_margin"])  # уже «ужатая» на этапе HEDGE OPEN
                entry_px = float(h["entry_px"])
                pos = Position(remain_side, signal_id=f"{symbol.replace('/','')} {int(time.time())}",
                               leverage=CONFIG.LEVERAGE, owner_key=b["owner_key"])
                # план STRAT: только стратегические уровни
                ord_levels = CONFIG.STRAT_LEVELS_AFTER_HEDGE
                alloc_bank = _alloc_bank(bank, weight)
                # после хеджа — фиксированный более плавный рост
                pos.plan_with_reserve(alloc_bank, CONFIG.GROWTH_AFTER_HEDGE, ord_levels)
                # первый шаг = объём оставшейся ноги хеджа
                pos.step_margins[0] = leg_margin
                pos.alloc_bank_planned = alloc_bank
                # оформить «первый шаг» по цене входа хеджа
                _ = pos.add_step(entry_px)
                # чтобы суммарное потребление ≤ 70% банка:
                pos.rebalance_tail_margins_excluding_reserve(alloc_bank)
                pos.from_hedge = True
                pos.hedge_entry_px = entry_px
                # STRAT-цели с ML-буфером строим ОТ ЦЕНЫ ЗАКРЫТИЯ ХЕДЖА (важно!) и добавляем TAC:
                cum_margin   = _pos_total_margin(pos)
                cum_notional = cum_margin * pos.leverage
                fees_paid_est = cum_notional * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                # сначала посчитаем STRAT от close_px, затем вставим TAC и прогоняем клиппер по ML
                pos.hedge_close_px = close_px
                pos.ordinary_targets = build_targets_with_tactical(pos, rng_strat, close_px, tick, alloc_bank, fees_paid_est)
                pos.ordinary_targets = clip_targets_by_ml(pos, bank=alloc_bank, fees_est=fees_paid_est,
                                                          targets=pos.ordinary_targets, tick=tick)
                pos.ordinary_offset = 0
                _sync_reserve3_flags(pos)

                # --- СООБЩЕНИЕ ПОСЛЕ ЗАКРЫТИЯ ХЕДЖА ---
                ml_now = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_paid_est)
                ml_reserve = ml_reserve_pct_to_ml20(pos, px, bank, fees_paid_est)
                dist_now = ml_distance_pct(pos.side, px, ml_now)
                ml_arrow = "↓" if pos.side == "LONG" else "↑"
                dist_now_txt = "N/A" if np.isnan(dist_now) else f"{dist_now:.2f}%"
                avail = min(3, len(pos.step_margins)-1, len(pos.ordinary_targets))
                scen = _ml_multi_scenarios(pos, bank, fees_paid_est, k_list=tuple(range(1, avail+1)))
                def _fmt_ml(v): 
                    return "N/A" if (v is None or np.isnan(v)) else fmt(v)
                levels_block = render_remaining_levels_block(symbol, pos, bank, CONFIG.FEE_TAKER, tick)
                # сколько именно STRAT осталось (без TAC)
                planned_now = len([t for t in pos.ordinary_targets[getattr(pos,"ordinary_offset",0):]
                                         if str(t.get("label","")).startswith("STRAT")])
                remain_side = "SHORT" if bias == "LONG" else "LONG"
                await say(
                    f"✅ Хедж закрыт (по команде). Оставлена нога: <b>{remain_side}</b>\n"
                    f"Цена входа хеджа: <code>{fmt(entry_px)}</code> | Закрытие второй ноги: <code>{fmt(close_px)}</code>\n"
                    f"Средняя: <code>{fmt(pos.avg)}</code> (P/L 0) | TP: <code>{fmt(pos.tp_price)}</code>\n"
                    f"ML(20%): {ml_arrow}<code>{fmt(ml_now)}</code> ({dist_now_txt} от текущей)\n"
                    f"Запас маржи до ML20%: <b>{('∞' if not np.isfinite(ml_reserve) else f'{ml_reserve:.1f}%')}</b>\n"
                    f"{_ml_after_labels_line(pos, bank, fees_paid_est)}\n"
                    f"{levels_block}\n"
                    f"(Осталось: {planned_now} из 3)"
                )
                b["position"] = pos
                b["fsm_state"] = int(FSM.MANAGING)
                # очистим команду
                b["hedge_close_price"] = None

                # Sheets логирование удалено
                # дальше обычное MANAGING
                continue

            # ===== (старый) BOOST-вход и обычный OPEN отключён — вход теперь только через хедж =====
            # (оставляем BOOST-блок нетронутым, но он не будет вызван, т.к. OPEN не создаётся)

            if pos:
                if not pos or b.get("fsm_state") == int(FSM.IDLE) or pos.steps_filled <= 0:
                    pass
                else:
                    used_ord = pos.steps_filled - (1 if pos.reserve_used else 0)
                    nxt = next_pct_target(pos)
                    is_open_event = b.get("fsm_state") == int(FSM.OPENED) and pos.steps_filled == 1
                    
                    # --- ТРИГГЕРЫ ПО ХВОСТАМ 1m ---
                    reached = False
                    if (b.get("fsm_state") == int(FSM.MANAGING)) and (nxt is not None):
                        tgt = float(nxt["price"])
                        reached = _wick_reached(pos.side, tgt, m1_lo, m1_hi, tick, CONFIG.WICK_HYST_TICKS)
                        # антидребезг: не триггерим тот же самый квантованный уровень
                        if reached:
                            tgt_q = quantize_to_tick(tgt, tick)
                            if pos.last_filled_q is not None and tgt_q == pos.last_filled_q:
                                reached = False
                    # --- Логика резерва для STRAT #3 ---
                    # если #3 — резервный, сначала «армим» при проходе глубже, затем ждём возврат через уровень
                    if getattr(pos, "reserve3_price", None) is not None and not pos.reserve3_done:
                        buf = max(1, int(CONFIG.WICK_HYST_TICKS)) * tick
                        r3 = float(pos.reserve3_price)
                        if pos.side == "LONG":
                            if not pos.reserve3_armed and (m1_lo is not None) and (m1_lo <= r3 - buf):
                                pos.reserve3_armed = True
                            if pos.reserve3_armed and (m1_hi is not None) and (m1_hi >= r3 + buf):
                                pos.reserve3_ready = True
                        else:
                            if not pos.reserve3_armed and (m1_hi is not None) and (m1_hi >= r3 + buf):
                                pos.reserve3_armed = True
                            if pos.reserve3_armed and (m1_lo is not None) and (m1_lo <= r3 - buf):
                                pos.reserve3_ready = True
                    # финальный флаг события добора:
                    is_add_event = reached
                    # если следующий таргет — именно резервный #3, то требуем reserve3_ready
                    if nxt is None and getattr(pos, "reserve3_price", None) is not None:
                        # next_pct_target вернул None потому что #3 ещё не готов
                        is_add_event = False
                    
                    now_ts = time.time()
                    allowed_now = is_open_event or (pos.last_add_ts is None) or ((now_ts - pos.last_add_ts) >= CONFIG.ADD_COOLDOWN_SEC)
                    if (is_open_event or is_add_event) and (used_ord < pos.ord_levels) and allowed_now:
                        if is_add_event:
                            alloc_bank = _alloc_bank(bank, weight)
                            fill_px = float(nxt["price"]) if nxt is not None else float(px)
                            margin, _ = pos.add_step(fill_px)
                            pos.rebalance_tail_margins_excluding_reserve(alloc_bank)
                            # пометим уровень как «израсходованный»
                            pos.last_filled_q = quantize_to_tick(nxt["price"], tick) if nxt else None
                            _advance_pointer(pos, tick)
                            # если исполнился резервный #3 — зафиксируем
                            if getattr(pos, "reserve3_price", None) is not None and nxt is not None and \
                               abs(float(nxt["price"]) - float(pos.reserve3_price)) < 1e-12:
                                pos.reserve3_done = True
                        else:
                            margin = pos.step_margins[0]

                        nxt2 = next_pct_target(pos)
                        base = getattr(pos, "ordinary_offset", 0)
                        total_ord = max(0, min(len(pos.ordinary_targets) - base, pos.ord_levels - 1 - base))
                        used_ord_after = pos.steps_filled - (1 if pos.reserve_used else 0)
                        used_dca = max(0, used_ord_after - 1)
                        remaining = max(0, total_ord - used_dca)
                        next_idx = used_ord_after
                        nxt2_margin = pos.step_margins[next_idx] if next_idx < len(pos.step_margins) else None
                        
                        # лоты считаем по фактической цене исполнения уровня (fill_px для ADD, px для OPEN)
                        lots = margin_to_lots(symbol, margin, price=(fill_px if is_add_event else px), leverage=pos.leverage)
                        cum_margin = _pos_total_margin(pos)
                        cum_notional = cum_margin * pos.leverage
                        fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER

                        ml_price = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_paid_est)
                        ml_reserve = ml_reserve_pct_to_ml20(pos, px, bank, fees_paid_est)
                        dist_ml_pct = ml_distance_pct(pos.side, px, ml_price)
                        dist_txt = "N/A" if np.isnan(dist_ml_pct) else f"{dist_ml_pct:.2f}%"
                        # запас хода до ML в тиках
                        if ml_price is None or np.isnan(ml_price):
                            dist_ml_ticks_txt = "N/A"
                        else:
                            dist_ml_ticks = abs((ml_price - px) / max(tick, 1e-12))
                            dist_ml_ticks_txt = f"{dist_ml_ticks:.0f} тик."
                        ml_arrow = "↓" if pos.side == "LONG" else "↑"
                        # ML после будущих стратегических шагов
                        used_ord_now = pos.steps_filled - (1 if pos.reserve_used else 0)
                        base_off   = getattr(pos, "ordinary_offset", 0)
                        avail_ord  = max(0, len(pos.step_margins)        - used_ord_now)
                        avail_tgts = max(0, len(pos.ordinary_targets) - base_off)
                        avail_k    = min(3, avail_ord, avail_tgts)
                        k_list     = tuple(range(1, avail_k + 1)) if avail_k > 0 else ()
                        scen = _ml_multi_scenarios(pos, bank, fees_paid_est, k_list=k_list)
                        def _fmt_ml(v):
                            return "N/A" if (v is None or np.isnan(v)) else fmt(v)

                        # Блок с оставшимися уровнями (свободная маржа и ML после исполнения каждого)
                        levels_block = render_remaining_levels_block(symbol, pos, bank, CONFIG.FEE_TAKER, tick)

                        # Сообщение об исполнении шага / оформлении открытия
                        if is_add_event:
                            exec_label = (nxt.get("label") if nxt else "N/A")
                            exec_price = fmt(nxt["price"]) if nxt else fmt(px)
                            hdr = f"➕ Исполнен уровень: <b>{exec_label}</b> по <code>{exec_price}</code>"
                        else:
                            hdr = f"✅ Открытие оформлено по <code>{fmt(px)}</code>"

                        msg_lines = [
                            hdr,
                            f"Добор: <b>{margin:.2f} USD</b> ≈ {lots:.2f} lot",
                            f"Средняя: <code>{fmt(pos.avg)}</code> (P/L 0) | TP: <code>{fmt(pos.tp_price)}</code>",
                            margin_line(pos, bank, px, fees_paid_est),
                            f"ML(20%): {ml_arrow}<code>{fmt(ml_price)}</code> ({dist_txt} от текущей, {dist_ml_ticks_txt})",
                            f"Запас маржи до ML20%: <b>{('∞' if not np.isfinite(ml_reserve) else f'{ml_reserve:.1f}%')}</b>",
                        ]
                        ml_after = _ml_after_labels_line(pos, bank, fees_paid_est)
                        if ml_after:
                            msg_lines.append(ml_after)
                        # старую строку "ML после +1|+2|+3" больше не выводим

                        msg_lines.append(levels_block)

                        await say("\n".join(msg_lines))

                        # Sheets логирование удалено
                        # если сейчас был OPENED/ADD — завершаем итерацию, TP/SL проверим в следующей
                        continue

                    # --- TP/SL и трейлинг (минимальный контроль, выполняется если не было ADD/OPEN в эту итерацию) ---
                    tp_hit, sl_hit = _tp_sl_hit(
                        pos.side, pos.tp_price, pos.sl_price,
                        m1_lo or px, m1_hi or px, tick, CONFIG.WICK_HYST_TICKS
                    )

                    if tp_hit or sl_hit:
                        exit_reason = "TP_HIT" if tp_hit else "SL_HIT"
                        exit_p = pos.tp_price if tp_hit else pos.sl_price or px
                        time_min = (time.time() - pos.open_ts) / 60.0
                        net_usd, net_pct = compute_net_pnl(pos, exit_p, fee_taker, fee_taker)

                        await say(
                            f"🏁 <b>{exit_reason}</b>\n"
                            f"Цена выхода: <code>{fmt(exit_p)}</code>\n"
                            f"P&L (net)≈ {net_usd:+.2f} USD ({net_pct:+.2f}%)\n"
                            f"Время в сделке: {time_min:.1f} мин"
                        )

                        # Sheets логирование удалено

                        # Включаем ручной режим при выходе (если включено в политике)
                        if (tp_hit and CONFIG.REQUIRE_MANUAL_REOPEN_ON.get("tp_hit", True)) or \
                           (sl_hit and CONFIG.REQUIRE_MANUAL_REOPEN_ON.get("sl_hit", True)):
                            b["user_manual_mode"] = True
                            await _remind_manual_open()

                        b["position"] = None
                        b["fsm_state"] = int(FSM.IDLE)
                        continue
                # --- конец блока pos ---

            # Немного подождём до следующего цикла
            await asyncio.sleep(CONFIG.SCAN_INTERVAL_SEC)
            continue

        except Exception:
            log.exception("scanner_main_loop iteration failed")
            # В случае ошибки — короткая пауза
            await asyncio.sleep(1.0)

# ---------------------------------------------------------------------------
# Public scanner controls (exports for main.py)
# ---------------------------------------------------------------------------

def _ns_key(symbol: str, chat_id: int | None) -> str:
    return f"{_norm_symbol(symbol)}|{chat_id or 'default'}"

def is_scanner_running(app: Application, symbol: str, chat_id: int | None) -> bool:
    """Проверка: крутится ли таск сканера для данного (symbol|chat)."""
    tasks = app.bot_data.get(TASKS_KEY) or {}
    key = _ns_key(symbol, chat_id)
    t: asyncio.Task | None = tasks.get(key)
    return bool(t and not t.done())

async def start_scanner_for_pair(
    app: Application,
    broadcast,
    *,
    symbol: str,
    chat_id: int | None = None,
    botbox: dict | None = None,
) -> str:
    """Запустить сканер для пары. Идемпотентно: при повторном вызове просто вернёт сообщение."""
    key = _ns_key(symbol, chat_id)
    tasks: dict = app.bot_data.setdefault(TASKS_KEY, {})

    # Уже запущен?
    t: asyncio.Task | None = tasks.get(key)
    if t and not t.done():
        return f"Сканер для <b>{_norm_symbol(symbol)}</b> уже запущен."

    # Сбрасываем флаг остановки для бокса, чтобы цикл вошёл в while
    box_root = botbox if botbox is not None else app.bot_data
    slot = box_root.setdefault(key, {})
    slot["bot_on"] = True

    # Стартуем фоновую задачу
    async def _run():
        try:
            await scanner_main_loop(
                app,
                broadcast,
                symbol_override=_norm_symbol(symbol),
                target_chat_id=chat_id,
                botbox=botbox,
            )
        except asyncio.CancelledError:
            log.info("Scanner task %s cancelled", key)
            raise
        except Exception:
            log.exception("Scanner task %s crashed", key)

    t = asyncio.create_task(_run(), name=f"scan:{key}")
    tasks[key] = t

    # Авто-очистка словаря задач
    def _cleanup(_):
        curr = app.bot_data.get(TASKS_KEY) or {}
        if curr.get(key) is t:
            curr.pop(key, None)
    t.add_done_callback(_cleanup)

    log.info("Started scanner task for %s", key)
    return f"🚀 Сканер для <b>{_norm_symbol(symbol)}</b> запущен."

async def stop_scanner_for_pair(
    app: Application,
    *,
    symbol: str,
    chat_id: int | None = None,
    hard: bool = False,
    botbox: dict | None = None,
) -> str:
    """Остановить сканер: мягко (bot_on=False) или жёстко (cancel())."""
    key = _ns_key(_norm_symbol(symbol), chat_id)
    tasks: dict = app.bot_data.get(TASKS_KEY) or {}
    t: asyncio.Task | None = tasks.get(key)

    # Попросим цикл завершиться — в том же root, что использовался при старте
    box_root = botbox if botbox is not None else app.bot_data
    slot = box_root.get(key)
    if slot is not None:
        slot["bot_on"] = False

    if t and not t.done():
        if hard:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            msg = f"⛔️ Сканер для <b>{_norm_symbol(symbol)}</b> остановлен (hard)."
        else:
            msg = f"🛑 Сканеру для <b>{_norm_symbol(symbol)}</b> дан сигнал остановки."
    else:
        msg = f"ℹ️ Сканер для <b>{_norm_symbol(symbol)}</b> не запущен."

    return msg

# --- Backward-compat aliases (на случай других импортов в main.py) ---
# Если где-то ожидают альтернативные имена — экспортируем их тоже.
start_pair_scanner = start_scanner_for_pair
stop_pair_scanner = stop_scanner_for_pair

def is_pair_scanner_running(app: Application, symbol: str, chat_id: int | None) -> bool:
    return is_scanner_running(app, symbol, chat_id)
