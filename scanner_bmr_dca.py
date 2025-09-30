# scanner_bmr_dca.py — patched, full
# - безопасная нормализация символа (str|dict|list)
# - автосоздание листов BMR_DCA_<SYMBOL> и дублирование логов туда
# - интеграция ФА-бота (risk/bias/ttl/updated_at) + периодическое обновление
# - совместимость с fetch_ohlcv(symbol, tf, limit) из fx_feed

from __future__ import annotations

import asyncio, time, logging, json, os, inspect, numbers, ast
from typing import Optional
from datetime import datetime, timezone
from enum import IntEnum
from contextlib import suppress

import numpy as np
import pandas as pd
from telegram.ext import Application
import gspread
from gspread.utils import rowcol_to_a1

# === Forex адаптеры и фид ===
from fx_mt5_adapter import FX, margin_to_lots, default_tick
from fx_feed import fetch_ohlcv

import trade_executor

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

# === G-Sheets Concurrency & Retry Control ===
_GS_SEM = asyncio.Semaphore(int(os.getenv("GS_MAX_CONC", "3")))

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
    TF_TRIGGER = "1m"          # новый: поток для триггеров по хвостам

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

    # ФА-политика
    FA_REFRESH_SEC = 300  # перечитывать раз в 5 минут

    ORDINARY_ADDS = 5  # столько обычных доборов после входа

    # Анти-слипание ценовых уровней
    DCA_MIN_GAP_TICKS = 2   # минимум 2 тика между целями

    # Показываем ML-цену при целевом Margin Level
    ML_TARGET_PCT = 20.0     # "ML цена (20%)"
    # Минимальный запас к ML(20%) после третьего STRAT-усреднения и пробоя STRAT
    ML_BREAK_BUFFER_PCT = 3.0

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
        "SAMPLES": 3,        # число подтверждений
        "INTERVAL_SEC": 5,     # интервал между замерами, сек
        "TIMEOUT_SEC": 20,     # таймаут пробы, сек
    }

    # План STRAT после закрытия хеджа:
    # 1 шаг уже занят «оставшейся ногой» хеджа + 3 будущих STRAT-усреднения
    STRAT_LEVELS_AFTER_HEDGE = 4

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
SAFE_LOG_KEYS |= {"Bank_Target_USDT", "Bank_Fact_USDT", "Chat_ID", "Owner_Key", "FA_Risk", "FA_Bias"}

BMR_HEADERS_FALLBACK = [
    "Event","Event_ID","Timestamp_UTC","Pair","Side","Signal_ID","Leverage",
    "Step_No","Step_Margin_USDT","Cum_Margin_USDT","Entry_Price","Avg_Price",
    "TP_Pct","TP_Price","SL_Price","Liq_Est_Price","Next_DCA_Price","Next_DCA_Label",
    "ATR_5m","ATR_1h","RSI_5m","ADX_5m","Supertrend","Vol_z",
    "Range_Lower","Range_Upper","Range_Width",
    "Fee_Rate_Maker","Fee_Rate_Taker","Fee_Est_USDT",
    "PNL_Realized_USDT","PNL_Realized_Pct","Time_In_Trade_min","Trail_Stage",
    "Triggered_Label","Chat_ID","Owner_Key","FA_Risk","FA_Bias",
    "Bank_Target_USDT", "Bank_Fact_USDT"
]

PAIR_KEY = {
    "USDJPY": "JPY",
    "EURCHF": "CHF",
    "EURUSD": "EUR",
    "GBPUSD": "GBP",
}

# === FUND_BOT targets ===
TARGET_WS = os.getenv("TARGET_WS", "FUND_BOT")
PAIR_LIST = ["USDJPY","EURCHF","EURUSD","GBPUSD"]

# {"USDJPY": {"value": 1120.0, "ts": "2025-09-09T..."}, ...}
targets_cache: dict[str, dict] = {}
ws_cache: dict[str, gspread.Worksheet] = {}

async def _gs_call(func, *args, **kwargs):
    delay = 0.5
    last_exc = None
    for _ in range(5):
        async with _GS_SEM:
            try:
                return await maybe_await(func, *args, **kwargs)
            except gspread.exceptions.APIError as e:
                s = str(e)
                # расширим список временных ошибок
                if any(x.lower() in s.lower() for x in ("429", "rateLimitExceeded", "internalError", "backendError", "503", "timeout")):
                    last_exc = e
                    await asyncio.sleep(delay); delay *= 2
                    continue
                raise
    # если так и не получилось — явно бросаем, чтобы внешняя логика не получила None
    raise last_exc or RuntimeError("GSheets call failed without explicit error")

async def refresh_targets_from_fund_ws(sh, box=None) -> bool:
    """
    Прочитать последнюю строку с action=='alloc' из листа TARGET_WS
    (по умолчанию FUND_BOT), распарсить note как JSON вида:
      {"USDJPY":1120,"AUDUSD":700,"EURUSD":560,"GBPUSD":420}
    и обновить targets_cache (+продублировать в box[ns_key]["bank_target_usdt"]).
    """
    try:
        ws = await _gs_call(sh.worksheet, TARGET_WS)
        rows = await _gs_call(ws.get_all_records)
        if not rows:
            return False
        filter_chat = os.getenv("FUND_ALLOC_CHAT_ID")
        last_alloc = None
        for r in reversed(rows):
            if str(r.get("action","")).lower() == "alloc" and (not filter_chat or str(r.get("chat_id","")) == filter_chat):
                last_alloc = r
                break
        if not last_alloc:
            return False

        note_raw = last_alloc.get("note") or "{}"
        try:
            alloc = json.loads(note_raw)
        except Exception:
            try:
                alloc = ast.literal_eval(note_raw)
            except Exception:
                alloc = {}

        ts = str(last_alloc.get("ts") or "")
        updated = False
        for sym in PAIR_LIST:
            try:
                val = float(alloc.get(sym, 0) or 0)
            except Exception:
                val = 0.0
            if val > 0:
                targets_cache[sym] = {"value": val, "ts": ts}
                if box is not None:
                    # если знаешь ID торгового чата для пары — можно задать через ENV <SYM>_CHAT_ID
                    chat_env = os.getenv(f"{sym}_CHAT_ID")
                    ns_key = f"{sym}|{chat_env or 'default'}"
                    slot = box.setdefault(ns_key, {})
                    slot["bank_target_usdt"] = val
                updated = True
        return updated
    except gspread.WorksheetNotFound:
        log.warning(f"{TARGET_WS} sheet not found for targets.")
        return False
    except Exception:
        log.exception("refresh_targets_from_fund_ws failed")
        return False

def _diag_targets_snapshot() -> str:
    lines = []
    for sym in PAIR_LIST:
        t = targets_cache.get(sym)
        if t and "value" in t:
            lines.append(f"{sym}  target={t['value']:.2f}  source=FUND_BOT  ts={t.get('ts','')}")
        else:
            lines.append(f"{sym}  target=N/A  source=fallback")
    return "🧪 Targets snapshot:\n" + "\n".join(lines)

def _get_master_headers(sh) -> list[str]:
    # Всегда фиксированный, чистый порядок колонок
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

async def _gs_append_row(ws, row):
    await _gs_call(ws.append_row, row, value_input_option="USER_ENTERED")

async def _ensure_ws(sh, name: str, headers: list[str]):
    try:
        ws = await _gs_call(sh.worksheet, name)
        # если шапка на листе не совпадает по длине/порядку — перепишем строку 1
        existing = await _gs_call(ws.row_values, 1)
        if existing != headers:
            rng = f"A1:{rowcol_to_a1(1, len(headers))}"
            await _gs_call(ws.update, rng, [headers])
        return ws
    except gspread.WorksheetNotFound:
        ws = await _gs_call(sh.add_worksheet, title=name, rows=2000, cols=max(20, len(headers)))
        if headers:
            rng = f"A1:{rowcol_to_a1(1, len(headers))}"
            await _gs_call(ws.update, rng, [headers])
        return ws

async def ensure_ws_cached(sh, name, headers):
    ws = ws_cache.get(name)
    if inspect.iscoroutine(ws):
        ws = await ws
        ws_cache[name] = ws
    if ws is None:
        ws = await _ensure_ws(sh, name, headers)
        ws_cache[name] = ws
    return ws

async def log_event_safely(payload: dict, sh: gspread.Spreadsheet | None = None):
    # Пишем только в лист по символу
    try:
        if sh is None:
            creds_json = os.environ.get("GOOGLE_CREDENTIALS")
            sheet_key  = os.environ.get("SHEET_ID")
            if not (creds_json and sheet_key): return
            gc = gspread.service_account_from_dict(json.loads(creds_json))
            sh = gc.open_by_key(sheet_key)
            
        sym = str(payload.get("Pair") or payload.get("pair") or "").upper()
        if not sym: return
        headers = _get_master_headers(sh)
        ws2 = await ensure_ws_cached(sh, f"BMR_DCA_{sym}", headers)
        await _gs_append_row(ws2, [_clean(payload.get(k)) for k in headers])
    except Exception:
        log.exception("[SHEETS] per-symbol log failed")

# ---- FA POLICY & WEIGHTS ----

async def read_fa_policy(symbol: str, sh: gspread.Spreadsheet | None = None) -> dict:
    """Читает политику из листа FA_Signals:
    pair, risk(Green/Amber/Red), bias(neutral/long-only/short-only), ttl (мин), updated_at (ISO),
    scan_lock_until, reserve_off, dca_scale, reason. При просрочке TTL возвращает {}.
    """
    try:
        if sh is None:
            creds_json = os.environ.get("GOOGLE_CREDENTIALS"); sheet_key = os.environ.get("SHEET_ID")
            if not (creds_json and sheet_key): return {}
            gc = gspread.service_account_from_dict(json.loads(creds_json))
            sh = gc.open_by_key(sheet_key)
            
        ws = await _gs_call(sh.worksheet, "FA_Signals")
        rows = await _gs_call(ws.get_all_records)
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
                try:
                    dca_scale = float(r.get("dca_scale") or 1.0)
                except: dca_scale = 1.0
                return {
                    "risk": risk, "bias": bias, "ttl": ttl, "updated_at": updated_at,
                    "scan_lock_until": scan_lock_until, "reserve_off": reserve_off, "dca_scale": dca_scale,
                    "reason": str(r.get("reason","")).strip()
                }
        return {}
    except Exception:
        log.exception("read_fa_policy failed")
        return {}

async def read_fund_bot_weights(sh) -> dict:
    try:
        ws = await _gs_call(sh.worksheet, "FUND_BOT")
        rc = ws.row_count
        start = max(2, rc - 200)
        rows = await _gs_call(ws.get, f"A{start}:E{rc}")
        if not rows:
            return {}
        # ищем последнюю непустую строку
        for row in reversed(rows):
            cell = row[4] if len(row) >= 5 else ""
            cell = (str(cell) if cell is not None else "").strip()
            if cell:
                try:
                    return json.loads(cell)
                except Exception:
                    try:
                        return ast.literal_eval(cell)
                    except Exception:
                        log.warning(f"Bad weights_json: {cell}")
                        return {}
        return {}
    except gspread.WorksheetNotFound:
        log.warning("FUND_BOT sheet not found. Using default weights.")
        return {}
    except Exception:
        log.exception("read_fund_bot_weights failed")
        return {}

def target_weight_for_pair(pair: str, weights: dict, default: int = 25) -> int:
    k = PAIR_KEY.get(pair)
    return int(weights.get(k, default)) if k else default

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
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    if abs(path) < min_gap:
        return []

    if include_end_last and count >= 1:
        # фракции: 1/n, 2/n, ..., 1.0  (нет 0.0)
        fracs = [(i + 1) / count for i in range(count)]
    else:
        # по-прежнему исключаем оба конца
        fracs = [(i + 1) / (count + 1) for i in range(count)]
        
    raw = [start + path * f for f in fracs]
    # Квантование и анти-слипание
    out: list[float] = []
    for x in raw:
        q = quantize_to_tick(x, tick)
        if q is None:
            continue
        # гарантируем отступ минимум в min_gap от точки start
        if abs(q - start) < min_gap:
            continue
        if not out or (abs(q - out[-1]) >= min_gap):
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
        seg1 = _place_segment(entry, tac_b,   DESIRED_TAC,   tick, include_end_last=False)  # TAC
        seg2 = _place_segment(tac_b,  strat_b, DESIRED_STRAT, tick, include_end_last=True)   # STRAT
    else:
        tac_b    = max(entry, rng_tac["upper"])
        strat_b = max(entry, rng_strat["upper"])
        seg1 = _place_segment(entry, tac_b,   DESIRED_TAC,   tick, include_end_last=False)
        seg2 = _place_segment(tac_b,  strat_b, DESIRED_STRAT, tick, include_end_last=True)

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
        seg = _place_segment(entry, strat_b, levels, tick, include_end_last=True)
    else:
        strat_b = max(entry, rng_strat["upper"])
        seg = _place_segment(entry, strat_b, levels, tick, include_end_last=True)
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
    abs_idx = max(base, used_dca)      # <-- ключевая правка
    return pos.ordinary_targets[abs_idx] if 0 <= abs_idx < len(pos.ordinary_targets) else None

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
    else:                                 # LONG: пробой вниз, строим ПОДАЛЬШЕ вниз
        candidates = [
            px - atr_guard,
            rng_strat["lower"],
            (ext_rng and ext_rng["lower"]) or rng_strat["lower"],
        ]
        end = min([v for v in candidates if np.isfinite(v)])
        start = px

    # Равномерно раскидываем оставшиеся уровни, последний — у "нового потолка/пола"
    seg = _place_segment(start, end, remaining, tick, include_end_last=True)
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
    if include_end: fr = [(i+1)/n for i in range(n)]
    else:           fr = [(i+1)/(n+1) for i in range(n)]
    raw = [a + (b - a)*f for f in fr]
    out = []
    min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
    for x in raw:
        q = quantize_to_tick(x, tick)
        if not out:
            # первый всегда должен отходить от a минимум на min_gap
            if abs(q - a) < min_gap: 
                q = a - min_gap if side=="LONG" else a + min_gap
        if not out or (side=="LONG" and q <= out[-1]-min_gap) or (side=="SHORT" and q >= out[-1]+min_gap):
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
    buf = _ml_buffer_after_3(pos, bank, fees_est, rng_strat, p1, p2, p3)
    # если буфер уже ок — возвращаем
    if pd.notna(buf) and buf >= CONFIG.ML_BREAK_BUFFER_PCT:
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
        buf = _ml_buffer_after_3(pos, bank, fees_est, rng_strat, p1, p2, p3)
        if pd.notna(buf) and buf >= CONFIG.ML_BREAK_BUFFER_PCT:
            break
        moved += step
    labs = ["STRAT 33%","STRAT 66%","STRAT 100%"]
    return [{"price": p1, "label": labs[0]}, {"price": p2, "label": labs[1]}, {"price": p3, "label": labs[2]}]

def _strat_report_text(pos: "Position", px: float, tick: float, bank: float,
                       fees_est: float, rng_strat: dict, hdr: str) -> str:
    """
    Формирует текст отчёта для /strat show|set|reset: цели, размеры USD/лот,
    ML сейчас и после +1/+2/+3, буфер после #3.
    """
    lines = [hdr]
    # Ближайшие 3 цели c дистанциями
    tgts = pos.ordinary_targets[getattr(pos, "ordinary_offset", 0):getattr(pos, "ordinary_offset", 0)+3]
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
    # Буфер к ML после #3 и пробоя
    if len(tgts) >= 3:
        buf = _ml_buffer_after_3(pos, bank, fees_est, rng_strat, tgts[0]["price"], tgts[1]["price"], tgts[2]["price"])
        st = "OK" if (pd.notna(buf) and buf >= CONFIG.ML_BREAK_BUFFER_PCT) else "FAIL"
        brk_up, brk_dn = break_levels(rng_strat)
        brk = brk_dn if pos.side=="LONG" else brk_up
        lines.append(f"Буфер после #3 и пробоя STRAT (от <code>{fmt(brk)}</code>) → {buf:.2f}% [{st}] (порог {CONFIG.ML_BREAK_BUFFER_PCT:.2f}%)")
    return "\n".join(lines)

class FSM(IntEnum):
    IDLE = 0   # нет позиции
    OPENED = 1 # открыт 1-й шаг, идёт первичное оповещение
    MANAGING = 2 # можно ADD/RETEST/TRAIL/EXIT

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
    
    b = root.setdefault(ns_key, {})     # <- у каждого символа/чата свой карман
    # при запуске процесса гарантируем «включённость»
    b["bot_on"] = True
    b.setdefault("position", None)
    b.setdefault("fsm_state", int(FSM.IDLE))
    b.setdefault("intro_done", False)
    b["owner_key"] = ns_key              # полезно видеть в логах
    b["chat_id"]   = target_chat_id

    # Google Sheets (необязательно)
    sheet = None
    try:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
        sheet_key  = os.environ.get("SHEET_ID")
        if creds_json and sheet_key:
            gc = gspread.service_account_from_dict(json.loads(creds_json))
            sheet = gc.open_by_key(sheet_key)
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
    if sheet:
        try:
            headers = _get_master_headers(sheet)
            await _ensure_ws(sheet, f"BMR_DCA_{symbol}", headers)
            # NEW: первичная подтяжка alloc-таргетов из FUND_BOT
            try:
                await refresh_targets_from_fund_ws(sheet, root)
            except Exception:
                log.exception("initial refresh_targets_from_fund_ws failed")
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

    # Напоминание о ручном запуске нового цикла
    async def _remind_manual_open():
        if time.time() - b.get("manual_remind_ts", 0) >= CONFIG.REMIND_MANUAL_MSG_COOLDOWN_SEC:
            await say("⏸ Ручной режим: чтобы запустить новый цикл, отправьте <b>/open</b>.")
            b["manual_remind_ts"] = time.time()

    # ---

    # ФА-политика
    fa = await read_fa_policy(symbol, sheet)
    fund_weights = await read_fund_bot_weights(sheet) if sheet else {}
    last_fa_read = 0.0
    last_targets_read = 0.0

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
    last_flush = 0
    last_build_strat = 0.0
    last_build_tac = 0.0

    while b.get("bot_on", True):
        try:
            # Факт банка — как и раньше
            bank = float(b.get("safety_bank_usdt", CONFIG.SAFETY_BANK_USDT))

            # Истинный таргет: 1) из box (если уже положен), 2) из локального кэша FUND_BOT,
            # 3) в крайнем случае — фолбэк на факт
            _tgt = b.get("bank_target_usdt")
            if _tgt is None:
                tce = targets_cache.get(symbol)
                if tce:
                    _tgt = tce.get("value")
            bank_target = float(_tgt if _tgt is not None else bank)

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

            # периодически перечитываем ФА и таргеты
            if now - last_fa_read > CONFIG.FA_REFRESH_SEC:
                fa = await read_fa_policy(symbol, sheet)
                if sheet:
                    fund_weights = await read_fund_bot_weights(sheet)
                last_fa_read = now

                # тихое окно от фунд-бота
                scan_until = pd.to_datetime(fa.get("scan_lock_until"), utc=True) if fa.get("scan_lock_until") else None
                b["fa_scan_lock"] = bool(scan_until and pd.Timestamp.now(tz="UTC") < scan_until)
                b["fa_scan_until_ts"] = float(scan_until.timestamp()) if scan_until is not None else None

                # --- детекция изменения FA и оповещение/логирование ---
                prev = (
                    b.get("fa_risk"),
                    b.get("fa_bias"),
                    b.get("fa_dca_scale"),
                    b.get("fa_reserve_off"),
                    b.get("fa_scan_until_iso"),
                )

                # записываем свежие значения в box
                b["fa_risk"] = (fa.get("risk") or "Green").capitalize()
                b["fa_bias"] = (fa.get("bias") or "neutral").lower()
                b["fa_dca_scale"] = float(fa.get("dca_scale") or 1.0)
                b["fa_reserve_off"] = bool(fa.get("reserve_off"))
                b["fa_scan_until_iso"] = fa.get("scan_lock_until") or ""
                b["fa_reason"] = (fa.get("reason") or "").strip()

                changed = prev != (
                    b["fa_risk"], b["fa_bias"], b["fa_dca_scale"], b["fa_reserve_off"], b["fa_scan_until_iso"]
                )

                if changed:
                    # 1) короткое уведомление в канал
                    emoji = {"Green":"✅","Amber":"🟡","Red":"🛑"}.get(b["fa_risk"], "✅")
                    lock_txt = ""
                    if b["fa_scan_until_iso"]:
                        try:
                            hhmm = pd.to_datetime(b["fa_scan_until_iso"], utc=True).strftime("%H:%M")
                            lock_txt = f"\n• тихое окно до {hhmm} (UTC)"
                        except Exception:
                            pass
                    dca_txt = ""
                    if b["fa_dca_scale"] < 1.0:
                        dca_txt = f"\n• dca_scale={b['fa_dca_scale']:.2f}"
                    res_txt = " (резерв отключён)" if b["fa_reserve_off"] else ""
                    rsn_txt = f"\n• reason: {b['fa_reason']}" if b.get("fa_reason") else ""

                    await say(
                        f"🔔 Обновлён FA-статус по <b>{symbol}</b> → {emoji} <b>{b['fa_risk']}</b>{res_txt}\n"
                        f"• bias: <b>{b['fa_bias']}</b>{dca_txt}{lock_txt}{rsn_txt}"
                    )

                    # 2) «маячок» в BMR_DCA_<SYM>, чтобы FA_Risk/FA_Bias были актуальны в логах
                    try:
                        await log_event_safely(with_banks({
                            "Event": "FA_STATUS",
                            "Event_ID": f"FA_{symbol}_{int(time.time())}",
                            "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            "Pair": symbol, "FA_Risk": b["fa_risk"], "FA_Bias": b["fa_bias"],
                            "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                        }), sheet)
                    except Exception:
                        log.exception("log_event_safely(FA_STATUS) failed")

            if sheet and (now - last_targets_read > 180):  # ~3 минуты
                try:
                    refreshed = await refresh_targets_from_fund_ws(sheet, root)
                    if refreshed:
                        log.info("FUND_BOT targets refreshed.")
                except Exception:
                    log.exception("periodic refresh_targets_from_fund_ws failed")
                last_targets_read = now

            # применяем FA: risk Red -> управляем, но не открываем; Amber -> консервативней
            fa_risk = b.get("fa_risk", (fa.get("risk") or "Green").capitalize())
            fa_bias = b.get("fa_bias", (fa.get("bias") or "neutral").lower())

            # --- вес из FUND_BOT (для политики), но банк пары не масштабируем по умолчанию
            weight = target_weight_for_pair(symbol, fund_weights)
            SCALE_BANK_BY_WEIGHTS = os.getenv("SCALE_BANK_BY_WEIGHTS", "0").lower() in ("1","true","yes","on")

            def _alloc_bank(bank: float, weight: int) -> float:
                if SCALE_BANK_BY_WEIGHTS:
                    w = max(0.0, min(1.0, float(weight) / 100.0))
                    return bank * w
                return bank # по умолчанию банк уже "пер-пара"
            
            reserve_off = bool(fa.get("reserve_off"))

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
                        else:                 q = sorted(q)                 # вверх → возрастание
                        # обеспечиваем минимум разрыва
                        min_gap = tick * CONFIG.DCA_MIN_GAP_TICKS
                        q_fixed = []
                        for i,x in enumerate(q):
                            if i==0:
                                # первый — не ближе min_gap к entry
                                base = pos.avg if pos.steps_filled<=1 else pos.ordinary_targets[getattr(pos,"ordinary_offset",0)]["price"]
                                if pos.side=="LONG" and x > base - min_gap: x = base - min_gap
                                if pos.side=="SHORT" and x < base + min_gap: x = base + min_gap
                            else:
                                prev = q_fixed[-1]
                                if pos.side=="LONG" and x > prev - min_gap: x = prev - min_gap
                                if pos.side=="SHORT" and x < prev + min_gap: x = prev + min_gap
                            q_fixed.append(quantize_to_tick(x, tick))
                        # применяем
                        base_off = getattr(pos, "ordinary_offset", 0)
                        labels = ["STRAT 33%","STRAT 66%","STRAT 100%"]
                        for i in range(3):
                            if base_off+i < len(pos.ordinary_targets):
                                pos.ordinary_targets[base_off+i] = {"price": q_fixed[i], "label": labels[i]}
                            else:
                                pos.ordinary_targets.append({"price": q_fixed[i], "label": labels[i]})
                        cum_margin = _pos_total_margin(pos)
                        fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                        await say(_strat_report_text(pos, px, tick, bank, fees_est, rng_strat, hdr="✏️ STRAT обновлён вручную"))
                        # лог в шиты
                        try:
                            await log_event_safely({
                                "Event":"STRAT_SET","Event_ID":f"STRAT_SET_{pos.signal_id}_{int(time.time())}",
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol,"Side": pos.side,"Signal_ID": pos.signal_id,
                                "Next_DCA_Price": pos.ordinary_targets[base_off]["price"] if pos.ordinary_targets else "",
                                "Next_DCA_Label": pos.ordinary_targets[base_off]["label"] if pos.ordinary_targets else "",
                                "Chat_ID": b.get("chat_id") or "","Owner_Key": b.get("owner_key") or "",
                                "FA_Risk": b.get("fa_risk") or "","FA_Bias": b.get("fa_bias") or "",
                                "Bank_Target_USDT": bank_target, "Bank_Fact_USDT": bank
                            })
                        except: pass

            if b.pop("cmd_strat_reset", False):
                if not pos or not pos.from_hedge or b.get("fsm_state") != int(FSM.MANAGING):
                    await say("ℹ️ Нет активной позиции «после хеджа» — сбрасывать нечего.")
                else:
                    cum_margin = _pos_total_margin(pos)
                    fees_est = (cum_margin * pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                    entry_px = pos.hedge_entry_px or pos.avg
                    pos.ordinary_targets = auto_strat_targets_with_ml_buffer(pos, rng_strat, entry=entry_px,
                                                                             tick=tick, bank=bank, fees_est=fees_est)
                    pos.ordinary_offset = min(getattr(pos,"ordinary_offset",0), len(pos.ordinary_targets))
                    await say(_strat_report_text(pos, px, tick, bank, fees_est, rng_strat, hdr="♻️ STRAT сброшен к авто-плану"))
                    try:
                        await log_event_safely({
                            "Event":"STRAT_RESET","Event_ID":f"STRAT_RESET_{pos.signal_id}_{int(time.time())}",
                            "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            "Pair": symbol,"Side": pos.side,"Signal_ID": pos.signal_id,
                            "Chat_ID": b.get("chat_id") or "","Owner_Key": b.get("owner_key") or "",
                            "FA_Risk": b.get("fa_risk") or "","FA_Bias": b.get("fa_bias") or "",
                            "Bank_Target_USDT": bank_target, "Bank_Fact_USDT": bank
                        })
                    except: pass
            # -------- /STRAT commands end --------

            # Диагностика по флагу
            if b.pop("cmd_diag_targets", False):
                await say(_diag_targets_snapshot())

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
                await log_event_safely(with_banks({
                    "Event_ID": f"MANUAL_CLOSE_{pos.signal_id}", "Signal_ID": pos.signal_id,
                    "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "Pair": symbol, "Side": pos.side, "Event": "MANUAL_CLOSE",
                    "PNL_Realized_USDT": net_usd, "PNL_Realized_Pct": net_pct,
                    "Time_In_Trade_min": time_min,
                    "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                    "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                }), sheet)
                b["force_close"] = False
                # Включаем «ручной режим» до явного /open
                if CONFIG.REQUIRE_MANUAL_REOPEN_ON.get("manual_close", True):
                    b["user_manual_mode"] = True
                    await _remind_manual_open()
                pos.last_sl_notified_price = None
                b["position"] = None
                b["fsm_state"] = int(FSM.IDLE)
                continue

            # --- Проба пробоя STRAT перед заморозкой обычных усреднений ---
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
                    await say("🧪 Вижу возможный пробой STRAT — жду подтверждения…")

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
                        # подтверждение пробы — включаем штатную механику пробоя
                        probe = b.get("break_probe")
                        if probe and probe["hits"] >= CONFIG.BREAK_PROBE["SAMPLES"]:
                            pos.freeze_ordinary = True
                            pos.break_dir = probe["dir"]
                            pos.break_confirm_bars = 0
                            pos.extension_planned = False
                            b["break_last_bar_id"] = None
                            b["break_probe"] = None
                            if now_ts - b.get("break_toggle_ts", 0) >= CONFIG.BREAK_MSG_COOLDOWN_SEC:
                                b["break_toggle_ts"] = now_ts
                                await say("📌 Пробой STRAT подтверждён — обычные усреднения заморожены. Резерв держим на ретест.")

            # Пробой диапазона — заморозка обычных усреднений, включаем режим ретеста (штатная логика)
            # Не замораживаем, пока идёт «проба» подтверждения (break_probe)
            if pos and not b.get("break_probe"):
                if not pos or b.get("fsm_state") != int(FSM.MANAGING) or pos.steps_filled <= 0:
                    await asyncio.sleep(1); continue
                brk_up, brk_dn = break_levels(rng_strat)
                eps = CONFIG.EXT_AFTER_BREAK["PRICE_EPS"]
                on_break = (px >= brk_up * (1.0 + eps)) or (px <= brk_dn * (1.0 - eps))
                if on_break and not pos.freeze_ordinary:
                    pos.freeze_ordinary = True
                    pos.break_dir = "up" if px >= brk_up else "down"
                    pos.break_confirm_bars = 0
                    pos.extension_planned = False
                    now_ts = time.time()
                    if now_ts - b.get("break_toggle_ts", 0) >= CONFIG.BREAK_MSG_COOLDOWN_SEC:
                        b["break_toggle_ts"] = now_ts
                        await say("📌 Пробой STRAT — обычные усреднения заморожены. Резерв держим на ретест.")
            
            # --- Подтверждение пробоя и дорисовка коридора ---
            if pos and pos.freeze_ordinary:
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
                    pos.freeze_ordinary = False
                    pos.break_dir = None
                    pos.extension_planned = False
                    now_ts = time.time()
                    if now_ts - b.get("break_toggle_ts", 0) >= CONFIG.BREAK_MSG_COOLDOWN_SEC:
                        b["break_toggle_ts"] = now_ts
                        await say("🔄 Пробой не подтвердился — вернулись в STRAT. Разморозил обычные усреднения.")

                if (not pos.extension_planned) and (pos.break_confirm_bars >= CONFIG.EXT_AFTER_BREAK["CONFIRM_BARS_5M"]):
                    used_ord_now = pos.steps_filled - (1 if pos.reserve_used else 0)
                    keep_idx = max(getattr(pos, "ordinary_offset", 0), max(0, used_ord_now - 1))
                    new_targets = await plan_extension_after_break(symbol, pos, rng_strat, rng_tac, px, tick)
                    
                    if len(new_targets) > len(pos.ordinary_targets):
                        pos.ordinary_targets = new_targets
                        pos.extension_planned = True
                        pos.freeze_ordinary = False
                        pos.ordinary_offset = min(keep_idx, len(pos.ordinary_targets))
                        await say("↗️ Пробой подтверждён — расширил коридор и достроил уровни EXT. Возобновляю обычные усреднения.")
                        try:
                            await log_event_safely(with_banks({
                                "Event": "EXT_PLAN", "Event_ID": f"EXT_{pos.signal_id}_{int(time.time())}",
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol, "Side": pos.side, "Signal_ID": pos.signal_id,
                                "Triggered_Label": "BREAK_CONFIRMED",
                                "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                                "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                            }), sheet)
                        except Exception:
                            log.exception("log EXT_PLAN failed")

            # ===== ВХОД ЧЕРЕЗ ХЕДЖ (две ноги) =====
            if (not pos) and (b.get("hedge") is None) and (not manage_only_flag) and (not b.get("user_manual_mode", False)):
                # TAC-границы
                tac_lo = rng_tac["lower"] + 0.30 * rng_tac["width"]
                tac_hi = rng_tac["lower"] + 0.70 * rng_tac["width"]
                can_long  = (m1_lo is not None) and (m1_lo <= tac_lo - CONFIG.WICK_HYST_TICKS * tick)
                can_short = (m1_hi is not None) and (m1_hi >= tac_hi + CONFIG.WICK_HYST_TICKS * tick)
                # bias от FA
                if fa_bias == "long-only":
                    can_short = False
                if fa_bias == "short-only":
                    can_long = False
                # разрешаем вход, если коснулись одного из TAC
                if can_long or can_short:
                    bias_side = "LONG" if can_long else "SHORT"

                    # план «старой» лестницы, чтобы посчитать сумму (OPEN + TAC-33 + TAC-67)
                    ord_levels_tmp = min(CONFIG.DCA_LEVELS - 1, 1 + CONFIG.ORDINARY_ADDS)
                    growth = choose_growth(ind, rng_strat, rng_tac)
                    if fa_risk == "Amber":
                        ord_levels_tmp = max(1, ord_levels_tmp - 1)
                        growth = min(growth, CONFIG.AUTO_ALLOC["growth_A"])
                    alloc_bank = _alloc_bank(bank, weight)
                    total_target = alloc_bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
                    margins_full = plan_margins_bank_first(total_target, ord_levels_tmp + 1, growth)
                    margin_3 = _sum_first_n(margins_full, 3)  # на КАЖДУЮ ногу
                    # Гарантия: 2*leg_margin <= 70% банка
                    total_target = alloc_bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
                    if margin_3 > total_target * 0.5:
                        scale = (total_target * 0.5) / margin_3
                        margin_3 *= scale
                    # лоты/депозит на ногу
                    lots_per_leg = margin_to_lots(symbol, margin_3, price=px, leverage=CONFIG.LEVERAGE)
                    dep_total = 2 * margin_3
                    b["hedge"] = {
                        "active": True,
                        "bias": bias_side,     # где «ждём» профит
                        "entry_px": px,
                        "leg_margin": margin_3,
                        "lots_per_leg": lots_per_leg,
                        "ts": time.time()
                    }
                    # --- Плановая точка закрытия хеджа = противоположный TAC ---
                    planned_hc_px = (rng_tac["lower"] + 0.70 * rng_tac["width"]) if bias_side == "LONG" \
                                    else (rng_tac["lower"] + 0.30 * rng_tac["width"])
                    _hc_dticks = abs((planned_hc_px - px) / max(tick, 1e-12))
                    _hc_dpct   = abs((planned_hc_px / max(px, 1e-12) - 1.0) * 100.0)

                    # --- Превью после закрытия хеджа: остаётся противоположная нога ---
                    remain_side = "SHORT" if bias_side == "LONG" else "LONG"
                    ord_levels_after = CONFIG.STRAT_LEVELS_AFTER_HEDGE
                    growth_after = choose_growth(ind, rng_strat, rng_tac)
                    if fa_risk == "Amber":
                        growth_after = min(growth_after, CONFIG.AUTO_ALLOC["growth_A"])
                    alloc_bank_after = _alloc_bank(bank, weight)

                    # Собираем «виртуальную» позицию как если бы хедж уже закрыли
                    _pos = Position(remain_side, signal_id=f"{symbol.replace('/','')} PREVIEW",
                                    leverage=CONFIG.LEVERAGE, owner_key=b["owner_key"])
                    _pos.plan_with_reserve(alloc_bank_after, growth_after, ord_levels_after)
                    _pos.step_margins[0] = margin_3         # первый шаг = оставшаяся нога хеджа
                    if bool(fa.get("reserve_off")):
                        _pos.reserve_available = False
                        _pos.reserve_margin_usdt = 0.0
                        _pos.max_steps = _pos.ord_levels
                    # чтобы суммарное потребление ≤ 70% банка:
                    _pos.rebalance_tail_margins_excluding_reserve(alloc_bank_after)
                    _ = _pos.add_step(px)                   # оформляем первый шаг по текущей цене
                    _pos.from_hedge = True
                    _pos.hedge_entry_px = px
                    _pos.hedge_close_px = planned_hc_px
                    # Авто-раскладка с ML-буфером 3% после #3 и пробоя:
                    _cum_margin = _pos_total_margin(_pos)
                    _fees_est   = (_cum_margin * _pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                    _pos.ordinary_targets = auto_strat_targets_with_ml_buffer(_pos, rng_strat, entry=px,
                                                                              tick=tick, bank=bank, fees_est=_fees_est)
                    _pos.ordinary_offset = 0

                    # ML/риски и план следующего добора
                    _ml_now     = ml_price_at(_pos, CONFIG.ML_TARGET_PCT, bank, _fees_est)
                    _ml_arrow   = "↓" if remain_side == "LONG" else "↑"
                    _dist_now   = ml_distance_pct(_pos.side, px, _ml_now)
                    _dist_now_txt = "N/A" if np.isnan(_dist_now) else f"{_dist_now:.2f}%"
                    _avail = min(3, len(_pos.step_margins)-1, len(_pos.ordinary_targets))
                    _scen = _ml_multi_scenarios(_pos, bank, _fees_est, k_list=tuple(range(1, _avail+1)))
                    def _fmt_ml(v): return "N/A" if (v is None or np.isnan(v)) else fmt(v)

                    _nxt = _pos.ordinary_targets[0] if _pos.ordinary_targets else None
                    _nxt_txt = "N/A" if _nxt is None else f"{fmt(_nxt['price'])} ({_nxt['label']})"
                    _nxt_margin = _pos.step_margins[1] if len(_pos.step_margins) > 1 else None
                    if _nxt and _nxt_margin:
                        _nxt_lots = margin_to_lots(symbol, _nxt_margin, price=_nxt['price'], leverage=_pos.leverage)
                        _nxt_dep_txt = f"{_nxt_margin:.2f} USD ≈ {_nxt_lots:.2f} lot"
                    else:
                        _nxt_dep_txt = "N/A"
                    _total_ord = 3  # по политике остаётся 3 STRAT-усреднения
                    _planned_now = min(_total_ord, len(_pos.ordinary_targets))
                    _remaining = _planned_now
                    _to_ext = max(0, _total_ord - _planned_now)

                    # Ближайшие цели (HC + первые 3 STRAT) с расстояниями
                    _targets_lines = []
                    _hc_dticks_txt = f"{_hc_dticks:.0f} тик."
                    _hc_dpct_txt   = f"{_hc_dpct:.2f}%"
                    _targets_lines.append(f"HC) <code>{fmt(planned_hc_px)}</code> (opp. TAC) — Δ≈ {_hc_dticks_txt} ({_hc_dpct_txt})")
                    for i, t in enumerate(_pos.ordinary_targets[:3], start=1):
                        _dticks = abs((t['price'] - px) / max(tick, 1e-12))
                        _dpct   = abs((t['price'] / max(px, 1e-12) - 1.0) * 100.0)
                        _targets_lines.append(f"{i}) <code>{fmt(t['price'])}</code> ({t['label']}) — Δ≈ {_dticks:.0f} тик. ({_dpct:.2f}%)")
                    _targets_block = "\n".join(_targets_lines) if _targets_lines else "—"

                    # размеры всех трёх STRAT-доборов (USD и лоты)
                    _sizes_lines = []
                    _next_idx = 1  # первый шаг уже занят
                    for j, t in enumerate(_pos.ordinary_targets[:3], start=1):
                        idx = _next_idx + (j - 1)
                        if idx >= len(_pos.step_margins): break
                        m = _pos.step_margins[idx]
                        lots_j = margin_to_lots(symbol, m, price=t['price'], leverage=_pos.leverage)
                        _sizes_lines.append(f"{j}) {m:.2f} USD ≈ {lots_j:.2f} lot")
                    _sizes_block = "\n".join(_sizes_lines) if _sizes_lines else "—"

                    await say(
                        f"🧷 HEDGE OPEN [{bias_side}] \n"
                        f"Цена: <code>{fmt(px)}</code> | Обе ноги по <b>{lots_per_leg:.2f} lot</b>\n"
                        f"Депозит (суммарно): <b>{dep_total:.2f} USD</b> (по <b>{margin_3:.2f}</b> на ногу)\n"
                        f"План HC: HC) <code>{fmt(planned_hc_px)}</code> — Δ≈ {_hc_dticks:.0f} тик. ({_hc_dpct:.2f}%)\n"
                        f"⚙️ Превью после закрытия хеджа (останется <b>{remain_side}</b>):\n"
                        f"ML(20%): {_ml_arrow}<code>{fmt(_ml_now)}</code> ({_dist_now_txt} от текущей)\n"
                        f"ML после +1: {_fmt_ml(_scen.get(1))} | +2: {_fmt_ml(_scen.get(2))} | +3: {_fmt_ml(_scen.get(3))}\n"
                        f"След. STRAT: <code>{_nxt_txt}</code>\n"
                        f"Ближайшие STRAT цели:\n{_targets_block}\n"
                        f"Размеры STRAT доборов:\n{_sizes_block}\n"
                        f"Плановый добор: <b>{_nxt_dep_txt}</b> (осталось: {_remaining} из 3){' — ещё ' + str(_to_ext) + ' уйдут в EXT' if _to_ext>0 else ''}\n"
                        f"Сигнал на ЗАКРЫТИЕ хеджа придёт при касании противоположного TAC по 1m-хвосту."
                    )
                    # запишем в лог
                    try:
                        await log_event_safely(with_banks({
                            "Event": "HEDGE_OPEN", "Event_ID": f"HEDGE_{symbol}_{int(time.time())}",
                            "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            "Pair": symbol, "Side": bias_side, "Signal_ID": f"{symbol} {int(now)}",
                            "Step_Margin_USDT": margin_3, "Cum_Margin_USDT": dep_total, "Entry_Price": px,
                            "ATR_5m": ind["atr5m"], "ATR_1h": rng_strat["atr1h"],
                            "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                            "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                        }), sheet)
                    except Exception:
                        log.exception("log HEDGE_OPEN failed")

            # СИГНАЛ «закрыть хедж» (по противоположному TAC, по хвостам 1m)
            # + АКТИВНОЕ ОБНОВЛЕНИЕ/ПЕРЕВОРОТ BIAS, если возник «противоположный вход»
            if (b.get("hedge") and b["hedge"].get("active")):
                tac_lo = rng_tac["lower"] + 0.30 * rng_tac["width"]
                tac_hi = rng_tac["lower"] + 0.70 * rng_tac["width"]
                bias = b["hedge"]["bias"]
                # Детекция условий входа в обе стороны (как при старте)
                can_long  = (b.get("m1_lo") is not None) and (b["m1_lo"] <= tac_lo - CONFIG.WICK_HYST_TICKS * tick)
                can_short = (b.get("m1_hi") is not None) and (b["m1_hi"] >= tac_hi + CONFIG.WICK_HYST_TICKS * tick)

                # Если во время хеджа возник «противоположный вход» — переворачиваем bias и пересчитываем превью
                flip_needed = (bias == "LONG" and can_short) or (bias == "SHORT" and can_long)
                if flip_needed and (time.time() - b.get("hedge_flip_ts", 0) > 10):
                    b["hedge_flip_ts"] = time.time()
                    new_bias = "LONG" if can_long else "SHORT"
                    b["hedge"]["bias"] = new_bias
                    planned_hc_px = (tac_hi if new_bias == "LONG" else tac_lo)
                    # Превью «что останется после закрытия хеджа» в НОВУЮ сторону
                    remain_side = "SHORT" if new_bias == "LONG" else "LONG"
                    entry_px0   = float(b["hedge"]["entry_px"])
                    leg_margin  = float(b["hedge"]["leg_margin"])
                    alloc_bank_after = _alloc_bank(bank, target_weight_for_pair(symbol, fund_weights))
                    growth_after = choose_growth(ind, rng_strat, rng_tac)
                    if fa_risk == "Amber":
                        growth_after = min(growth_after, CONFIG.AUTO_ALLOC["growth_A"])
                    _pos = Position(remain_side, signal_id=f"{symbol.replace('/','')} PREVIEW",
                                    leverage=CONFIG.LEVERAGE, owner_key=b["owner_key"])
                    _pos.plan_with_reserve(alloc_bank_after, growth_after, CONFIG.STRAT_LEVELS_AFTER_HEDGE)
                    _pos.step_margins[0] = leg_margin
                    if bool(fa.get("reserve_off")):
                        _pos.reserve_available = False
                        _pos.reserve_margin_usdt = 0.0
                        _pos.max_steps = _pos.ord_levels
                    _pos.rebalance_tail_margins_excluding_reserve(alloc_bank_after)
                    _ = _pos.add_step(entry_px0)
                    _pos.from_hedge = True
                    _pos.hedge_entry_px = entry_px0
                    _pos.hedge_close_px = planned_hc_px
                    _cum = _pos_total_margin(_pos)
                    _fees = (_cum * _pos.leverage) * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                    _pos.ordinary_targets = auto_strat_targets_with_ml_buffer(
                        _pos, rng_strat, entry=entry_px0, tick=tick, bank=bank, fees_est=_fees
                    )
                    _pos.ordinary_offset = 0
                    # Короткое обновлённое превью
                    _ml_now = ml_price_at(_pos, CONFIG.ML_TARGET_PCT, bank, _fees)
                    _ml_arrow = "↓" if remain_side == "LONG" else "↑"
                    _dist_now = ml_distance_pct(_pos.side, px, _ml_now)
                    _dist_now_txt = "N/A" if np.isnan(_dist_now) else f"{_dist_now:.2f}%"
                    _nxt = _pos.ordinary_targets[0] if _pos.ordinary_targets else None
                    _nxt_margin = _pos.step_margins[1] if len(_pos.step_margins) > 1 else None
                    if _nxt and _nxt_margin:
                        _nxt_lots = margin_to_lots(symbol, _nxt_margin, price=_nxt["price"], leverage=_pos.leverage)
                        _nxt_dep_txt = f"{_nxt_margin:.2f} USD ≈ {_nxt_lots:.2f} lot"
                    else:
                        _nxt_dep_txt = "N/A"
                    _nxt_txt = "N/A" if not _nxt else f"{fmt(_nxt['price'])} ({_nxt['label']})"
                    await say(
                        f"🔁 HEDGE UPDATE [{new_bias}] \n"
                        f"HC теперь: <code>{fmt(planned_hc_px)}</code>\n"
                        f"Останется: <b>{remain_side}</b> | ML(20%): {_ml_arrow}<code>{fmt(_ml_now)}</code> "
                        f"({_dist_now_txt} от текущей)\n"
                        f"След. STRAT: <code>{_nxt_txt}</code>\n"
                        f"Плановый добор: <b>{_nxt_dep_txt}</b>"
                    )

                # если bias LONG (вход от 30%), закрыть при касании 70% (hi >= tac_hi+buf) — закрываем «плюсовую» LONG
                # если bias SHORT (вход от 70%), закрыть при касании 30% (lo <= tac_lo-buf) — закрываем «плюсовую» SHORT
                need_close = (bias == "LONG" and (m1_hi is not None) and (m1_hi >= tac_hi + CONFIG.WICK_HYST_TICKS * tick)) or \
                             (bias == "SHORT" and (m1_lo is not None) and (m1_lo <= tac_lo - CONFIG.WICK_HYST_TICKS * tick))
                if need_close and (time.time() - b.get("hedge_close_notice_ts", 0) > 10):
                    b["hedge_close_notice_ts"] = time.time()
                    side_win = "LONG" if bias == "LONG" else "SHORT"
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
                bias = h["bias"]
                # остаётся противоположная, «минусовая» нога
                remain_side = "SHORT" if bias == "LONG" else "LONG"
                leg_margin = float(h["leg_margin"])
                entry_px = float(h["entry_px"])
                pos = Position(remain_side, signal_id=f"{symbol.replace('/','')} {int(time.time())}",
                               leverage=CONFIG.LEVERAGE, owner_key=b["owner_key"])
                # план STRAT: только стратегические уровни
                ord_levels = CONFIG.STRAT_LEVELS_AFTER_HEDGE
                growth = choose_growth(ind, rng_strat, rng_tac)
                if fa_risk == "Amber":
                    growth = min(growth, CONFIG.AUTO_ALLOC["growth_A"])
                alloc_bank = _alloc_bank(bank, weight)
                pos.plan_with_reserve(alloc_bank, growth, ord_levels)
                # первый шаг = объём оставшейся ноги хеджа
                pos.step_margins[0] = leg_margin
                pos.alloc_bank_planned = alloc_bank
                if reserve_off:
                    pos.reserve_available = False
                    pos.reserve_margin_usdt = 0.0
                    pos.max_steps = pos.ord_levels
                # чтобы суммарное потребление ≤ 70% банка:
                pos.rebalance_tail_margins_excluding_reserve(alloc_bank)
                # оформить «первый шаг» по цене входа хеджа
                _ = pos.add_step(entry_px)
                pos.from_hedge = True
                pos.hedge_entry_px = entry_px
                # сохраним точку закрытия хеджа, чтобы показывать её вместе с целями
                pos.hedge_close_px = close_px
                # STRAT-цели с ML-буфером 3% после #3 и пробоя:
                cum_margin   = _pos_total_margin(pos)
                cum_notional = cum_margin * pos.leverage
                fees_paid_est = cum_notional * CONFIG.FEE_TAKER * CONFIG.LIQ_FEE_BUFFER
                pos.ordinary_targets = auto_strat_targets_with_ml_buffer(pos, rng_strat, entry=entry_px,
                                                                         tick=tick, bank=bank, fees_est=fees_paid_est)
                pos.ordinary_offset = 0
                b["position"] = pos
                b["fsm_state"] = int(FSM.MANAGING)
                # очистим команду
                b["hedge_close_price"] = None

                # отчёт + ML-прогнозы
                ml_now = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_paid_est)
                dist_now = ml_distance_pct(pos.side, px, ml_now)
                ml_arrow = "↓" if pos.side == "LONG" else "↑"
                dist_now_txt = "N/A" if np.isnan(dist_now) else f"{dist_now:.2f}%"
                avail = min(3, len(pos.step_margins)-1, len(pos.ordinary_targets))
                scen = _ml_multi_scenarios(pos, bank, fees_paid_est, k_list=tuple(range(1, avail+1)))
                def _fmt_ml(v): 
                    return "N/A" if (v is None or np.isnan(v)) else fmt(v)
                
                next_strat_line = "N/A"
                if pos.ordinary_targets:
                    next_strat_line = f"<code>{fmt(pos.ordinary_targets[0]['price'])} ({pos.ordinary_targets[0]['label']})</code>"
                
                # блок «ближайшие цели»: сперва точка закрытия хеджа, затем STRAT-цели
                hc_line = "—"
                if pos.hedge_close_px is not None:
                    _d_ticks = abs((pos.hedge_close_px - px) / max(tick, 1e-12))
                    _d_pct   = abs((pos.hedge_close_px / max(px, 1e-12) - 1.0) * 100.0)
                    hc_line = f"HC) <code>{fmt(pos.hedge_close_px)}</code> (HEDGE CLOSE) — Δ≈ {_d_ticks:.0f} тик. ({_d_pct:.2f}%)"
                strat_lines = []
                for i, t in enumerate(pos.ordinary_targets[:3], start=1):
                    d_ticks = abs((t['price'] - px) / max(tick, 1e-12))
                    d_pct   = abs((t['price'] / max(px, 1e-12) - 1.0) * 100.0)
                    strat_lines.append(f"{i}) <code>{fmt(t['price'])}</code> ({t['label']}) — Δ≈ {d_ticks:.0f} тик. ({d_pct:.2f}%)")
                targets_block = (hc_line + ("\n" + "\n".join(strat_lines) if strat_lines else "")) if hc_line else ("\n".join(strat_lines) or "—")

                # размеры всех трёх STRAT-доборов (USD и лоты)
                sizes_lines = []
                next_idx = 1 # первый шаг уже занят
                for j, t in enumerate(pos.ordinary_targets[:3], start=1):
                    idx = next_idx + (j - 1)
                    if idx >= len(pos.step_margins): break
                    m = pos.step_margins[idx]
                    lots_j = margin_to_lots(symbol, m, price=t['price'], leverage=pos.leverage)
                    sizes_lines.append(f"{j}) {m:.2f} USD ≈ {lots_j:.2f} lot")
                sizes_block = "\n".join(sizes_lines) if sizes_lines else "—"
                
                # счётчик оставшихся / ушедших в EXT
                total_ord = 3
                planned_now = min(total_ord, len(pos.ordinary_targets))
                to_ext = max(0, total_ord - planned_now)

                await say(
                    f"✅ Хедж закрыт (по команде). Оставлена нога: <b>{remain_side}</b>\n"
                    f"Цена входа хеджа: <code>{fmt(entry_px)}</code> | Закрытие второй ноги: <code>{fmt(close_px)}</code>\n"
                    f"Средняя: <code>{fmt(pos.avg)}</code> | TP: <code>{fmt(pos.tp_price)}</code>\n"
                    f"ML(20%): {ml_arrow}<code>{fmt(ml_now)}</code> ({dist_now_txt} от текущей)\n"
                    f"ML после +1: {_fmt_ml(scen.get(1))} | +2: {_fmt_ml(scen.get(2))} | +3: {_fmt_ml(scen.get(3))}\n"
                    f"След. STRAT: {next_strat_line}\n"
                    f"Ближайшие STRAT цели:\n{targets_block}\n"
                    f"Размеры STRAT доборов:\n{sizes_block}\n"
                    f"(Осталось: {planned_now} из 3){' — ещё ' + str(to_ext) + ' уйдут в EXT' if to_ext>0 else ''}"
                )
                try:
                    next_price = pos.ordinary_targets[0]["price"] if pos.ordinary_targets else None
                    next_label = pos.ordinary_targets[0]["label"] if pos.ordinary_targets else None
                    await log_event_safely(with_banks({
                        "Event": "HEDGE_CLOSED", "Event_ID": f"HEDGE_CLOSE_{symbol}_{int(time.time())}",
                        "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "Pair": symbol, "Side": remain_side, "Signal_ID": pos.signal_id,
                        "Entry_Price": entry_px, "Avg_Price": pos.avg, "TP_Price": pos.tp_price,
                        "Liq_Est_Price": ml_now, "Next_DCA_Price": next_price, "Next_DCA_Label": next_label,
                        "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                        "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                    }), sheet)
                except Exception:
                    log.exception("log HEDGE_CLOSED failed")
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
                    is_add_event = reached
                    
                    now_ts = time.time()
                    allowed_now = is_open_event or (pos.last_add_ts is None) or ((now_ts - pos.last_add_ts) >= CONFIG.ADD_COOLDOWN_SEC)
                    if (is_open_event or is_add_event) and (not pos.freeze_ordinary) and (used_ord < pos.ord_levels) and allowed_now:
                        if is_add_event:
                            alloc_bank = _alloc_bank(bank, weight)
                            margin, _ = pos.add_step(px)
                            pos.rebalance_tail_margins_excluding_reserve(alloc_bank)
                            # пометим уровень как «израсходованный»
                            pos.last_filled_q = quantize_to_tick(nxt["price"], tick) if nxt else None
                            _advance_pointer(pos, tick)
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
                        
                        lots = margin_to_lots(symbol, margin, price=px, leverage=pos.leverage)
                        cum_margin = _pos_total_margin(pos)
                        cum_notional = cum_margin * pos.leverage
                        fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER

                        ml_price = ml_price_at(pos, CONFIG.ML_TARGET_PCT, bank, fees_paid_est)
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
                        avail_ord    = max(0, len(pos.step_margins)    - used_ord_now)
                        avail_tgts   = max(0, len(pos.ordinary_targets) - base_off)
                        avail_k      = min(3, avail_ord, avail_tgts)
                        k_list       = tuple(range(1, avail_k + 1)) if avail_k > 0 else ()
                        scen = _ml_multi_scenarios(pos, bank, fees_paid_est, k_list=k_list)
                        def _fmt_ml(v): 
                            return "N/A" if (v is None or np.isnan(v)) else fmt(v)
                        ml_scen_line = f"ML после +1: {_fmt_ml(scen.get(1))} | +2: {_fmt_ml(scen.get(2))} | +3: {_fmt_ml(scen.get(3))}"
                        # ближайшие точки усреднения (до 3 штук) с расстоянием
                        upcoming_lines = []
                        # если позиция получена «после хеджа» — вставим точку закрытия хеджа первой строкой
                        if getattr(pos, "hedge_close_px", None) is not None:
                            _d_ticks = abs((pos.hedge_close_px - px) / max(tick, 1e-12))
                            _d_pct   = abs((pos.hedge_close_px / max(px, 1e-12) - 1.0) * 100.0)
                            upcoming_lines.append(
                                f"HC) <code>{fmt(pos.hedge_close_px)}</code> (HEDGE CLOSE) — Δ≈ {_d_ticks:.0f} тик. ({_d_pct:.2f}%)"
                            )
                        start_idx = getattr(pos, "ordinary_offset", 0)
                        for i, t in enumerate(pos.ordinary_targets[start_idx:start_idx+3], start=1):
                            d_ticks = abs((t['price'] - px) / max(tick, 1e-12))
                            d_pct   = abs((t['price'] / max(px, 1e-12) - 1.0) * 100.0)
                            upcoming_lines.append(
                                f"{i}) <code>{fmt(t['price'])}</code> ({t['label']}) — Δ≈ {d_ticks:.0f} тик. ({d_pct:.2f}%)"
                            )
                        targets_block = "\n".join(upcoming_lines) if upcoming_lines else "—"
                        nxt2_txt = "N/A" if nxt2 is None else f"{fmt(nxt2['price'])} ({nxt2['label']})"
                        
                        dir_tag = "LONG 🟢" if pos.side == "LONG" else "SHORT 🛑"
                        if is_open_event:
                            header_tag = dir_tag
                        else:
                            header_tag = (nxt and nxt["label"]) or ""

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
                            f"ML(20%): {ml_arrow}<code>{fmt(ml_price)}</code> — запас ≈ {dist_ml_ticks_txt} ({dist_txt})\n"
                            f"{ml_scen_line}\n"
                            f"{brk_line}\n"
                            f"След. усреднение: <code>{nxt2_txt}</code>\n"
                            f"Ближайшие цели:\n{targets_block}\n"
                            f"Плановый добор: <b>{nxt2_dep_txt}</b> (осталось: {remaining} из {total_ord})\n"
                            f"{margin_line(pos, bank, px, fees_paid_est)}"
                        )
                        await log_event_safely(with_banks({
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
                        }), sheet)
                        if is_open_event:
                            b["fsm_state"] = int(FSM.MANAGING) # Переключаем после первого сообщения

                    # Трейл-стоп, TP/SL — теперь тоже по хвостам 1m
                    # Во время пробы пробоя (break_probe) трейлинг не двигаем
                    if pos and b.get("fsm_state") == int(FSM.MANAGING) and pos.steps_filled > 0 and not b.get("break_probe"):
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
                            new_sl_q   = quantize_to_tick(new_sl, t)
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
                                    await log_event_safely(with_banks({
                                        "Event_ID": f"TRAIL_SET_{pos.signal_id}_{int(now)}", "Signal_ID": pos.signal_id,
                                        "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                        "Pair": symbol, "Side": pos.side, "Event": "TRAIL_SET",
                                        "SL_Price": pos.sl_price, "Avg_Price": pos.avg, "Trail_Stage": stage_idx + 1,
                                        "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                                        "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                                    }), sheet)
                        
                        tp_hit, sl_hit = _tp_sl_hit(pos.side, pos.tp_price, pos.sl_price, m1_lo, m1_hi, tick, CONFIG.WICK_HYST_TICKS)
                        if tp_hit or sl_hit:
                            reason = "TP_HIT" if tp_hit else "SL_HIT"
                            exit_p = pos.tp_price if tp_hit else (pos.sl_price or px)
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
                            await log_event_safely(with_banks({
                                "Event_ID": f"{reason}_{pos.signal_id}", "Signal_ID": pos.signal_id,
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol, "Side": pos.side, "Event": reason,
                                "PNL_Realized_USDT": net_usd, "PNL_Realized_Pct": net_pct,
                                "Time_In_Trade_min": time_min, "ATR_5m": atr_now,
                                "Chat_ID": b.get("chat_id") or "", "Owner_Key": b.get("owner_key") or "",
                                "FA_Risk": b.get("fa_risk") or "", "FA_Bias": b.get("fa_bias") or "",
                            }), sheet)
                            # После SL/TP — включаем ручной режим (по конфигу) и просим /open
                            if (reason == "SL_HIT" and CONFIG.REQUIRE_MANUAL_REOPEN_ON.get("sl_hit", True)) \
                               or (reason == "TP_HIT" and CONFIG.REQUIRE_MANUAL_REOPEN_ON.get("tp_hit", True)):
                                b["user_manual_mode"] = True
                                await _remind_manual_open()
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

# ---------------------------------------------------------------------------
# STRAT commands bridge (/strat show | /strat set p1 p2 p3 | /strat reset)
# Предполагается, что внешний Telegram-слой выставляет в box:
#    box[ns_key]["cmd_strat_show"] = True
#    box[ns_key]["cmd_strat_set"]  = [p1, p2, p3]   (строки/числа)
#    box[ns_key]["cmd_strat_reset"]= True
# Ниже добавляем обработку внутри основного цикла — рядом с управлением позицией.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Task Manager: поключично запускаем/останавливаем сканеры по парам
# ---------------------------------------------------------------------------
def _ns_key(symbol: str, chat_id: Optional[int]) -> str:
    return f"{_norm_symbol(symbol)}|{chat_id or 'default'}"

def is_scanner_running(app: Application, symbol: str, chat_id: Optional[int]) -> bool:
    tasks = app.bot_data.get(TASKS_KEY) or {}
    t = tasks.get(_ns_key(symbol, chat_id))
    return bool(t and not t.done())

async def start_scanner_for_pair(
    app: Application,
    broadcast,
    *,
    symbol: str,
    chat_id: Optional[int],
    botbox: Optional[dict] = None,
) -> str:
    """
    Стартует отдельный сканер для пары/чата.
    Не стартует дубликат, возвращает человекочитаемый статус.
    """
    symbol = _norm_symbol(symbol)
    ns_key = _ns_key(symbol, chat_id)
    tasks: dict = app.bot_data.setdefault(TASKS_KEY, {})

    # уже бежит?
    if ns_key in tasks and not tasks[ns_key].done():
        return f"⏳ Сканер для {ns_key} уже запущен."

    # поднимаем флаг и стартуем задачу
    box = botbox if botbox is not None else app.bot_data
    slot = box.setdefault(ns_key, {})
    slot["bot_on"] = True

    bc = _wrap_broadcast(broadcast, chat_id)

    async def _runner():
        try:
            await scanner_main_loop(
                app,
                bc,
                symbol_override=symbol,
                target_chat_id=chat_id,
                botbox=botbox,
            )
        except asyncio.CancelledError:
            # мягкое завершение при отмене
            pass
        except Exception:
            log.exception(f"[runner:{ns_key}] crashed")
        finally:
            # убираем задачу из реестра
            reg = app.bot_data.get(TASKS_KEY) or {}
            if reg.get(ns_key) is asyncio.current_task():
                reg.pop(ns_key, None)

    task = asyncio.create_task(_runner(), name=f"scan[{ns_key}]")
    tasks[ns_key] = task
    return f"✅ Сканер для {ns_key} запущен."

async def stop_scanner_for_pair(
    app: Application,
    *,
    symbol: str,
    chat_id: Optional[int],
    hard: bool = False,
    join_timeout: float = 6.0,
) -> str:
    """
    Останавливает отдельный сканер.
    hard=True дополнительно обнуляет позицию/состояние в box.
    """
    symbol = _norm_symbol(symbol)
    ns_key = _ns_key(symbol, chat_id)
    tasks: dict = app.bot_data.get(TASKS_KEY) or {}
    t: asyncio.Task | None = tasks.get(ns_key)

    # погасим флаг цикла
    slot = app.bot_data.setdefault(ns_key, {})
    slot["bot_on"] = False

    if hard:
        slot.update(position=None, fsm_state=int(FSM.IDLE), intro_done=False)

    # если есть реальная задача — подождём мягкое завершение и при необходимости отменим
    if t:
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(t), timeout=join_timeout)
        if not t.done():
            t.cancel()
            with suppress(asyncio.CancelledError):
                await t
        # убираем из реестра
        tasks.pop(ns_key, None)
        return f"🛑 Сканер для {ns_key} остановлен{' (hard)' if hard else ''}."
    else:
        return f"ℹ️ Сканер для {ns_key} не найден (возможно, уже остановлен)."

async def stop_all_scanners(app: Application, *, hard: bool = False) -> list[str]:
    """Останавливает все сканеры; возвращает список строк-результатов."""
    tasks: dict = app.bot_data.get(TASKS_KEY) or {}
    out = []
    for k in list(tasks.keys()):
        sym, chat = (k.split("|", 1) + ["default"])[:2]
        chat_id = None if chat == "default" else int(chat) if chat.isdigit() else None
        out.append(await stop_scanner_for_pair(app, symbol=sym, chat_id=chat_id, hard=hard))
    return out
