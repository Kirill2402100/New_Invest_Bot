from __future__ import annotations
import asyncio, time, logging, json, os, inspect, numbers
# ИСПРАВЛЕНО: Возвращены недостающие импорты
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt
import ccxt as ccxt_sync
from telegram.ext import Application
import gspread

import trade_executor

log = logging.getLogger("bmr_dca_engine")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
class CONFIG:
    SYMBOL = "EURC/USDT:USDT"
    TF_ENTRY = "5m"
    TF_RANGE = "1h"
    STRATEGIC_LOOKBACK_DAYS = 60
    TACTICAL_LOOKBACK_DAYS = 3
    FETCH_TIMEOUT = 15
    BASE_STEP_MARGIN = 10.0
    DCA_LEVELS = 7
    DCA_GROWTH = 2.0
    LEVERAGE = 50
    # ИЗМЕНЕНО: Комиссии по умолчанию
    FEE_MAKER = 0.0002
    FEE_TAKER = 0.0002
    Q_LOWER = 0.025
    Q_UPPER = 0.975
    RANGE_MIN_ATR_MULT = 1.5
    RSI_LEN = 14
    ADX_LEN = 14
    VOL_WIN = 50
    WEIGHTS = {
        "border": 0.45, "rsi": 0.15, "ema_dev": 0.20,
        "supertrend": 0.10, "vol": 0.10
    }
    SCORE_THR = 0.55
    ATR_MULTS = [0, 1, 2, 3, 4, 5, 6]
    STRATEGIC_PCTS = [0.10, 0.45, 0.90]
    TACTICAL_PCTS = [0.40, 0.80]
    TP_PCT = 0.010
    TRAILING_STAGES = [(0.35, 0.25), (0.60, 0.50), (0.85, 0.75)]
    SCAN_INTERVAL_SEC = 3
    REBUILD_RANGE_EVERY_MIN = 15
    REBUILD_TACTICAL_EVERY_MIN = 5
    SAFETY_BANK_USDT = 1500.0
    CUM_DEPOSIT_FRAC_AT_FULL = 2/3
    AUTO_LEVERAGE = False
    MIN_LEVERAGE = 2
    MAX_LEVERAGE = 50
    BREAK_EPS = 0.0025
    REENTRY_BAND = 0.003
    MAINT_MMR = 0.004
    LIQ_FEE_BUFFER = 1.0
    SL_NOTIFY_MIN_TICK_STEP = 1
    AUTO_ALLOC = {
        "thin_tac_vs_strat": 0.35,
        "low_vol_z": 0.5,
        "growth_A": 1.6,
        "growth_B": 2.2,
    }

# ---------------------------------------------------------------------------
# Helper Functions
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
    "Next_DCA_Label", "Triggered_Label"
}

def _clean(v):
    if v is None:
        return ""
    if isinstance(v, (np.generic,)):
        v = v.item()
    if isinstance(v, numbers.Real):
        if not np.isfinite(v):
            return ""
        return float(v)
    return v

def _clean_payload(d: dict) -> dict:
    return {k: _clean(v) for k, v in d.items() if k in SAFE_LOG_KEYS}

async def log_event_safely(payload: dict):
    try:
        await maybe_await(trade_executor.bmr_log_event, _clean_payload(payload))
    except Exception:
        log.exception("[SHEETS] log_event_safely failed")

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

def approx_liq_price_cross(avg: float, side: str, qty: float, equity: float, mmr: float, fees_paid: float = 0.0) -> float:
    if qty <= 0 or equity <= 0:
        return float('nan')
    eq = max(0.0, equity - fees_paid)
    if side == "LONG":
        denom = max(qty * (1.0 - mmr), 1e-12)
        return (avg * qty - eq) / denom
    else:
        denom = max(qty * (1.0 + mmr), 1e-12)
        return (avg * qty + eq) / denom

def liq_distance_pct(side: str, px: float, liq: float) -> float:
    if px is None or liq is None or px <= 0 or np.isnan(liq):
        return float('nan')
    return (liq / px - 1.0) * 100 if side == "SHORT" else (1.0 - liq / px) * 100

def trend_reversal_confirmed(side: str, ind: dict) -> bool:
    return (side=="SHORT" and ind["supertrend"] in ("up_to_down_near","down")) or \
           (side=="LONG"  and ind["supertrend"] in ("down_to_up_near","up"))

async def fetch_ohlcv_safe(exchange, symbol, timeframe, limit, retries=3, timeout=None):
    for attempt in range(retries):
        try:
            return await asyncio.wait_for(
                exchange.fetch_ohlcv(symbol, timeframe, limit=limit),
                timeout or CONFIG.FETCH_TIMEOUT
            )
        except (asyncio.TimeoutError, ccxt_sync.RequestTimeout, ccxt_sync.NetworkError,
                ccxt_sync.DDoSProtection, ccxt_sync.ExchangeNotAvailable) as e:
            wait = 1.5 ** attempt
            log.warning(f"OHLCV timeout {symbol} {timeframe} lim={limit} "
                        f"try {attempt+1}/{retries}: {e}; retry in {wait:.1f}s")
            await asyncio.sleep(wait)
    small = max(240, min(500, (limit or 500)//2))
    try:
        log.warning(f"Retries failed for limit={limit}. Falling back to limit={small}.")
        return await asyncio.wait_for(
            exchange.fetch_ohlcv(symbol, timeframe, limit=small),
            (timeout or CONFIG.FETCH_TIMEOUT) * 2
        )
    except Exception as e:
        log.error(f"OHLCV final fail {symbol} {timeframe}: {e}")
        return None

def chandelier_stop(side: str, price: float, atr: float, mult: float = 3.0):
    if side == "LONG": return price - mult*atr
    else: return price + mult*atr

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

def sl_moved_enough(prev: float|None, new: float, side: str, tick: float, min_steps: int) -> bool:
    if prev is None:
        return True
    step = max(tick * max(1, min_steps), 1e-12)
    return (new > prev + step) if side == "LONG" else (new < prev - step)

def quantize_to_tick(x: float | None, tick: float) -> float | None:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return x
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
        if q is not None and (not out or (side == "SHORT" and q > out[-1] + tick) or (side == "LONG" and q < out[-1] - tick)):
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
        if not dedup:
            dedup.append(t)
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
    if not getattr(pos, "ordinary_targets", None):
        return None
    return pos.ordinary_targets[idx] if 0 <= idx < len(pos.ordinary_targets) else None

def choose_growth(ind: dict, rng_strat: dict, rng_tac: dict) -> float:
    try:
        width_ratio = (rng_tac["width"] / max(rng_strat["width"], 1e-9))
    except Exception:
        width_ratio = 1.0
    thin = width_ratio <= CONFIG.AUTO_ALLOC["thin_tac_vs_strat"]
    low_vol = abs(ind.get("vol_z", 0.0)) <= CONFIG.AUTO_ALLOC["low_vol_z"]
    return CONFIG.AUTO_ALLOC["growth_A"] if (thin and low_vol) else CONFIG.AUTO_ALLOC["growth_B"]

# ДОБАВЛЕНО: Хелпер для расчета чистого PnL
def compute_net_pnl(pos, exit_p: float, fee_entry: float, fee_exit: float) -> tuple[float, float]:
    sum_margin = sum(pos.step_margins[:pos.steps_filled]) or 1e-9
    raw_pnl = (exit_p / pos.avg - 1.0) * (1 if pos.side == "LONG" else -1)
    gross_usd = sum_margin * (raw_pnl * pos.leverage)
    entry_notional = sum_margin * pos.leverage
    exit_notional  = exit_p * pos.qty
    fee_entry_usd = entry_notional * fee_entry
    fee_exit_usd  = exit_notional  * fee_exit
    net_usd = gross_usd - fee_entry_usd - fee_exit_usd
    net_pct = (net_usd / sum_margin) * 100.0
    return net_usd, net_pct

# ---------------------------------------------------------------------------
# Core Logic Functions
# ---------------------------------------------------------------------------
async def build_range_for_days(exchange, symbol: str, lookback_days: int):
    limit_h = min(int(lookback_days * 24), 1500)
    ohlc = await fetch_ohlcv_safe(exchange, symbol, CONFIG.TF_RANGE, limit_h)
    if not ohlc: return None
    df = pd.DataFrame(ohlc, columns=["ts","open","high","low","close","volume"])
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

async def build_ranges(exchange, symbol: str):
    strat = await build_range_for_days(exchange, symbol, CONFIG.STRATEGIC_LOOKBACK_DAYS)
    tac   = await build_range_for_days(exchange, symbol, CONFIG.TACTICAL_LOOKBACK_DAYS)
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
# Position State Manager
# ---------------------------------------------------------------------------
class Position:
    def __init__(self, side: str, signal_id: str, leverage: int | None=None):
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
        self.reserved_one = False
        self.last_sl_notified_price = None
        self.ordinary_targets: list[dict] = []
        self.trail_stage: int = -1

    def plan_margins(self, bank: float, growth: float):
        total_target = bank * CONFIG.CUM_DEPOSIT_FRAC_AT_FULL
        self.step_margins = plan_margins_bank_first(total_target, CONFIG.DCA_LEVELS, growth)

    def add_step(self, price: float):
        margin = self.step_margins[self.steps_filled]
        notional = margin * self.leverage
        new_qty = notional / max(price,1e-9)
        self.avg = (self.avg*self.qty + price*new_qty) / max(self.qty+new_qty,1e-9) if self.qty>0 else price
        self.qty += new_qty
        self.steps_filled += 1
        self.tp_price = self.avg*(1+self.tp_pct) if self.side=="LONG" else self.avg*(1-self.tp_pct)
        return margin, notional

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
async def scanner_main_loop(app: Application, broadcast):
    log.info("BMR-DCA loop starting…")
    app.bot_data.setdefault("position", None)
    
    try:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS")
        sheet_key  = os.environ.get("SHEET_ID")
        if creds_json and sheet_key:
            gc = gspread.service_account_from_dict(json.loads(creds_json))
            sheet = gc.open_by_key(sheet_key)
            await maybe_await(trade_executor.ensure_bmr_log_sheet, sheet, title="BMR_DCA_Log")
    except Exception as e:
        log.error(f"Sheets init error: {e}", exc_info=True)

    exchange = ccxt.mexc({
        'options': {'defaultType': 'swap'},
        'enableRateLimit': True,
        'rateLimit': 150,
        'timeout': 20000,
    })
    await exchange.load_markets(True)
    
    candidates = [CONFIG.SYMBOL, "EUR/USDT:USDT", "EUR/USDT", "EURC/USDT", "EURUSDT"]
    symbol = None
    for s in candidates:
        if s in exchange.markets:
            symbol = s
            if s != CONFIG.SYMBOL:
                log.warning(f"Requested {CONFIG.SYMBOL}, but using available symbol {s} on the exchange.")
            else:
                log.info(f"Successfully found primary symbol {s} on the exchange.")
            break

    if not symbol:
        log.critical(f"None of the candidate symbols were found on the exchange: {candidates}")
        await exchange.close()
        return

    market = exchange.markets[symbol]
    tick = None
    p_prec = market.get("precision", {}).get("price")
    if isinstance(p_prec, (int, float)):
        if 0 < float(p_prec) < 1:
            tick = float(p_prec)
        elif int(p_prec) >= 0:
            tick = 10 ** (-int(p_prec))
    if not tick:
        tick = market.get("limits", {}).get("price", {}).get("min")
    if not tick or tick <= 0:
        tick = 1e-4
    app.bot_data["price_tick"] = float(tick)

    rng_strat, rng_tac = None, None
    last_flush = 0
    last_build_strat = 0.0
    last_build_tac = 0.0

    while app.bot_data.get("bot_on", False):
        try:
            bank = float(app.bot_data.get("safety_bank_usdt", CONFIG.SAFETY_BANK_USDT))
            fee_maker = float(app.bot_data.get("fee_maker", CONFIG.FEE_MAKER))
            fee_taker = float(app.bot_data.get("fee_taker", CONFIG.FEE_TAKER))
            
            now = time.time()
            manage_only = app.bot_data.get("scan_paused", False)
            pos: Position | None = app.bot_data.get("position")

            need_build_strat = (rng_strat is None) or ((now - last_build_strat > CONFIG.REBUILD_RANGE_EVERY_MIN*60) and (pos is None))
            need_build_tac   = (rng_tac is None) or ((now - last_build_tac > CONFIG.REBUILD_TACTICAL_EVERY_MIN*60) and (pos is None))
            if need_build_strat or need_build_tac:
                s, t = await build_ranges(exchange, symbol)
                if need_build_strat and s:
                    rng_strat = s
                    last_build_strat = now
                    app.bot_data["intro_done"] = False
                    log.info(f"[RANGE-STRAT] lower={fmt(rng_strat['lower'])} upper={fmt(rng_strat['upper'])} width={fmt(rng_strat['width'])}")
                if need_build_tac and t:
                    rng_tac = t
                    last_build_tac = now
                    app.bot_data["intro_done"] = False
                    log.info(f"[RANGE-TAC]   lower={fmt(rng_tac['lower'])} upper={fmt(rng_tac['upper'])} width={fmt(rng_tac['width'])}")
            
            if not (rng_strat and rng_tac):
                log.error("Range is not available. Cannot proceed.")
                await asyncio.sleep(10)
                continue

            ohlc5 = await fetch_ohlcv_safe(exchange, symbol, CONFIG.TF_ENTRY,
                                           limit=max(60, CONFIG.VOL_WIN+CONFIG.ADX_LEN+20))
            if not ohlc5:
                log.warning("Could not fetch 5m OHLCV data. Skipping this cycle.")
                await asyncio.sleep(2)
                continue
            
            df5 = pd.DataFrame(ohlc5, columns=["ts","open","high","low","close","volume"])
            try:
                ind = compute_indicators_5m(df5)
            except ValueError as e:
                log.warning(f"Indicator calculation failed: {e}. Skipping cycle.")
                await asyncio.sleep(2)
                continue

            px = float(df5["close"].iloc[-1])

            if (not app.bot_data.get("intro_done")) and (pos is None):
                p30_t = rng_tac["lower"] + 0.30 * rng_tac["width"]
                p70_t = rng_tac["lower"] + 0.70 * rng_tac["width"]
                d_to_long  = max(0.0, px - p30_t)
                d_to_short = max(0.0, p70_t - px)
                pct_to_long  = (d_to_long  / max(px, 1e-9)) * 100
                pct_to_short = (d_to_short / max(px, 1e-9)) * 100
                brk_up, brk_dn = break_levels(rng_strat)
                width_ratio = (rng_tac["width"] / max(rng_strat["width"], 1e-9)) * 100.0
                if broadcast:
                    msg = (
                        f"🎯 Пороги входа (<b>TAC 30/70</b>): LONG ≤ <code>{fmt(p30_t)}</code>, SHORT ≥ <code>{fmt(p70_t)}</code>\n"
                        f"📏 Диапазоны:\n"
                        f"• STRAT: [{fmt(rng_strat['lower'])} … {fmt(rng_strat['upper'])}] w={fmt(rng_strat['width'])}\n"
                        f"• TAC (3d): [{fmt(rng_tac['lower'])} … {fmt(rng_tac['upper'])}] w={fmt(rng_tac['width'])} (≈{width_ratio:.0f}% от STRAT)\n"
                        f"🔓 Пробой STRAT: ↑{fmt(brk_up)} | ↓{fmt(brk_dn)}\n"
                        f"Текущая: {fmt(px)}. До LONG: {fmt(d_to_long)} ({pct_to_long:.2f}%), "
                        f"до SHORT: {fmt(d_to_short)} ({pct_to_short:.2f}%)."
                    )
                    await broadcast(app, msg)
                app.bot_data["intro_done"] = True

            pos = app.bot_data.get("position")
            
            if pos and app.bot_data.get("force_close"):
                exit_p = px
                time_min = (time.time()-pos.open_ts)/60.0
                net_usd, net_pct = compute_net_pnl(pos, exit_p, fee_taker, fee_maker)

                if broadcast:
                    await broadcast(app, f"🧰 <b>MANUAL_CLOSE</b>\n"
                                          f"Цена выхода: <code>{fmt(exit_p)}</code>\n"
                                          f"P&L (net)≈ {net_usd:+.2f} USDT ({net_pct:+.2f}%)\n"
                                          f"Время в сделке: {time_min:.1f} мин")
                await log_event_safely({
                    "Event_ID": f"MANUAL_CLOSE_{pos.signal_id}", "Signal_ID": pos.signal_id,
                    "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "Pair": symbol, "Side": pos.side, "Event": "MANUAL_CLOSE",
                    "PNL_Realized_USDT": net_usd, "PNL_Realized_Pct": net_pct,
                    "Time_In_Trade_min": time_min
                })
                app.bot_data["force_close"] = False
                pos.last_sl_notified_price = None
                app.bot_data["position"] = None
                continue

            if pos:
                brk_up, brk_dn = break_levels(rng_strat)
                if px >= brk_up or px <= brk_dn:
                    if not pos.reserved_one:
                        pos.max_steps = min(pos.steps_filled + 1, CONFIG.DCA_LEVELS)
                        pos.reserved_one = True
                        if broadcast:
                            await broadcast(app, "📌 Пробой коридора — обычные усреднения заморожены. Оставлен 1 резерв на ретест.")

            if not pos and (app.bot_data.get("manual_open") is not None or not manage_only):
                manual = app.bot_data.pop("manual_open", None)
                if manual:
                    side_cand = manual.get("side")
                else:
                    pos_in = max(0.0, min(1.0, (px - rng_tac["lower"]) / max(rng_tac["width"], 1e-9)))
                    side_cand = "LONG" if pos_in <= 0.30 else ("SHORT" if pos_in >= 0.70 else None)
                
                if side_cand:
                    pos = Position(side_cand, signal_id=f"{symbol.split('/')[0]}_{int(now)}")
                    
                    if manual and manual.get("leverage") is not None:
                        pos.leverage = max(CONFIG.MIN_LEVERAGE, min(CONFIG.MAX_LEVERAGE, int(manual["leverage"])))
                    else:
                        pos.leverage = CONFIG.LEVERAGE

                    growth = choose_growth(ind, rng_strat, rng_tac)
                    pos.plan_margins(bank, growth)

                    if manual and manual.get("max_steps") is not None:
                        pos.max_steps = max(1, min(CONFIG.DCA_LEVELS, int(manual["max_steps"])))
                    else:
                        pos.max_steps = min(6, CONFIG.DCA_LEVELS)
                    
                    pos.ordinary_targets = compute_mixed_targets(entry=px, side=pos.side, rng_strat=rng_strat, rng_tac=rng_tac, tick=tick)
                    pos.reserved_one = False

                    margin, _ = pos.add_step(px)
                    app.bot_data["position"] = pos
                    
                    cum_margin = sum(pos.step_margins[:pos.steps_filled])
                    cum_notional = cum_margin * pos.leverage
                    fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER
                    liq = approx_liq_price_cross(
                        avg=pos.avg, side=pos.side, qty=pos.qty,
                        equity=bank, mmr=CONFIG.MAINT_MMR, fees_paid=fees_paid_est
                    )
                    if not np.isfinite(liq) or liq <= 0: liq = None
                    dist_to_liq_pct = liq_distance_pct(pos.side, px, liq)
                    dist_txt = "N/A" if np.isnan(dist_to_liq_pct) else f"{dist_to_liq_pct:.2f}%"
                    liq_arrow = "↓" if pos.side == "LONG" else "↑"

                    nxt = next_pct_target(pos)
                    nxt_txt = "N/A" if nxt is None else f"{fmt(nxt['price'])} ({nxt['label']})"
                    
                    ord_total = len(pos.ordinary_targets)
                    remaining = min(pos.max_steps - pos.steps_filled, ord_total - (pos.steps_filled - 1))
                    remaining = max(0, remaining)
                    
                    nxt_margin = pos.step_margins[pos.steps_filled] if pos.steps_filled < pos.max_steps else None
                    nxt_dep_txt = f"{nxt_margin:.2f} USDT" if nxt_margin is not None else "N/A"

                    brk_up, brk_dn = break_levels(rng_strat)
                    brk_up_pct, brk_dn_pct = break_distance_pcts(px, brk_up, brk_dn)
                    brk_line = (f"Пробой: ↑<code>{fmt(brk_up)}</code> ({brk_up_pct:.2f}%) | "
                                f"↓<code>{fmt(brk_dn)}</code> ({brk_dn_pct:.2f}%)")

                    hdr = f"BMR-DCA {pos.side} ({symbol.split('/')[0]})" + (" [MANUAL]" if manual else "")
                    if broadcast:
                        await broadcast(app,
                            f"⚡ <b>{hdr}</b>\n"
                            f"Вход: <code>{fmt(px)}</code>\n"
                            f"Депозит (старт): <b>{cum_margin:.2f} USDT</b> | Плечо: <b>{pos.leverage}x</b>\n"
                            f"TP: <code>{fmt(pos.tp_price)}</code> (+{CONFIG.TP_PCT*100:.2f}%)\n"
                            f"Ликвидация: {liq_arrow}<code>{fmt(liq)}</code> (до лик.: {dist_txt})\n"
                            f"{brk_line}\n"
                            f"След. усреднение: <code>{nxt_txt}</code> | Плановый добор: <b>{nxt_dep_txt}</b> (осталось: {remaining} из {ord_total})"
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
                        "Fee_Est_USDT": fees_paid_est / CONFIG.LIQ_FEE_BUFFER, "ATR_5m": ind["atr5m"], "ATR_1h": rng_strat["atr1h"],
                        "RSI_5m": ind["rsi"], "ADX_5m": ind["adx"], "Supertrend": ind["supertrend"], "Vol_z": ind["vol_z"],
                        "Range_Lower": rng_strat["lower"], "Range_Upper": rng_strat["upper"], "Range_Width": rng_strat["width"]
                    })

            pos = app.bot_data.get("position")
            if pos:
                if not manage_only:
                    if pos.reserved_one:
                        need_retest = (pos.side=="SHORT" and px <= rng_strat["upper"] * (1 - CONFIG.REENTRY_BAND)) or \
                                      (pos.side=="LONG"  and px >= rng_strat["lower"] * (1 + CONFIG.REENTRY_BAND))
                        can_add = pos.steps_filled < pos.max_steps
                        if need_retest and can_add and trend_reversal_confirmed(pos.side, ind):
                            margin, _ = pos.add_step(px)
                            pos.max_steps = pos.steps_filled
                            cum_margin = sum(pos.step_margins[:pos.steps_filled])
                            cum_notional = cum_margin * pos.leverage
                            fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER
                            liq = approx_liq_price_cross(
                                avg=pos.avg, side=pos.side, qty=pos.qty,
                                equity=bank, mmr=CONFIG.MAINT_MMR, fees_paid=fees_paid_est
                            )
                            if not np.isfinite(liq) or liq <= 0: liq = None
                            dist_to_liq_pct = liq_distance_pct(pos.side, px, liq)
                            dist_txt = "N/A" if np.isnan(dist_to_liq_pct) else f"{dist_to_liq_pct:.2f}%"
                            liq_arrow = "↓" if pos.side == "LONG" else "↑"
                            
                            brk_up, brk_dn = break_levels(rng_strat)
                            brk_up_pct, brk_dn_pct = break_distance_pcts(px, brk_up, brk_dn)
                            brk_line = (f"Пробой: ↑<code>{fmt(brk_up)}</code> ({brk_up_pct:.2f}%) | "
                                        f"↓<code>{fmt(brk_dn)}</code> ({brk_dn_pct:.2f}%)")

                            if broadcast:
                                await broadcast(app,
                                    f"↩️ Ретест — резервный добор\n"
                                    f"Цена: <code>{fmt(px)}</code>\n"
                                    f"Добор (резерв): <b>{margin:.2f} USDT</b> | Депозит (текущий): <b>{cum_margin:.2f} USDT</b>\n"
                                    f"Средняя: <code>{fmt(pos.avg)}</code> | TP: <code>{fmt(pos.tp_price)}</code>\n"
                                    f"Ликвидация: {liq_arrow}<code>{fmt(liq)}</code> (до лик.: {dist_txt})\n"
                                    f"{brk_line}")
                            await log_event_safely({
                                "Event_ID": f"RETEST_ADD_{pos.signal_id}_{pos.steps_filled}", "Signal_ID": pos.signal_id,
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol, "Side": pos.side, "Event": "RETEST_ADD",
                                "Step_No": pos.steps_filled, "Step_Margin_USDT": margin,
                                "Entry_Price": px, "Avg_Price": pos.avg
                            })
                    else:
                        nxt = next_pct_target(pos)
                        trigger = (nxt is not None) and ((pos.side=="LONG" and px <= nxt["price"]) or (pos.side=="SHORT" and px >= nxt["price"]))
                        if trigger and pos.steps_filled < pos.max_steps:
                            margin, _ = pos.add_step(px)
                            cum_margin = sum(pos.step_margins[:pos.steps_filled])
                            cum_notional = cum_margin * pos.leverage
                            fees_paid_est = cum_notional * fee_taker * CONFIG.LIQ_FEE_BUFFER
                            liq = approx_liq_price_cross(
                                avg=pos.avg, side=pos.side, qty=pos.qty,
                                equity=bank, mmr=CONFIG.MAINT_MMR, fees_paid=fees_paid_est
                            )
                            if not np.isfinite(liq) or liq <= 0: liq = None
                            dist_to_liq_pct = liq_distance_pct(pos.side, px, liq)
                            dist_txt = "N/A" if np.isnan(dist_to_liq_pct) else f"{dist_to_liq_pct:.2f}%"
                            liq_arrow = "↓" if pos.side == "LONG" else "↑"
                            
                            nxt2 = next_pct_target(pos)
                            nxt2_txt = "N/A" if nxt2 is None else f"{fmt(nxt2['price'])} ({nxt2['label']})"
                            
                            ord_total = len(pos.ordinary_targets)
                            remaining = min(pos.max_steps - pos.steps_filled, ord_total - (pos.steps_filled - 1))
                            remaining = max(0, remaining)
                            
                            curr_label = pos.ordinary_targets[pos.steps_filled-2]["label"] if pos.steps_filled >= 2 else ""
                            
                            nxt2_margin = pos.step_margins[pos.steps_filled] if pos.steps_filled < pos.max_steps else None
                            nxt2_dep_txt = "N/A" if nxt2_margin is None else f"{nxt2_margin:.2f} USDT"

                            brk_up, brk_dn = break_levels(rng_strat)
                            brk_up_pct, brk_dn_pct = break_distance_pcts(px, brk_up, brk_dn)
                            brk_line = (f"Пробой: ↑<code>{fmt(brk_up)}</code> ({brk_up_pct:.2f}%) | "
                                        f"↓<code>{fmt(brk_dn)}</code> ({brk_dn_pct:.2f}%)")

                            if broadcast:
                                await broadcast(app,
                                    f"➕ Усреднение #{pos.steps_filled-1} [{curr_label}]\n"
                                    f"Цена: <code>{fmt(px)}</code>\n"
                                    f"Добор: <b>{margin:.2f} USDT</b> | Депозит (текущий): <b>{cum_margin:.2f} USDT</b>\n"
                                    f"Средняя: <code>{fmt(pos.avg)}</code> | TP: <code>{fmt(pos.tp_price)}</code>\n"
                                    f"Ликвидация: {liq_arrow}<code>{fmt(liq)}</code> (до лик.: {dist_txt})\n"
                                    f"{brk_line}\n"
                                    f"След. усреднение: <code>{nxt2_txt}</code> | Плановый добор: <b>{nxt2_dep_txt}</b> (осталось: {remaining} из {ord_total})")
                            await log_event_safely({
                                "Event_ID": f"ADD_{pos.signal_id}_{pos.steps_filled}", "Signal_ID": pos.signal_id,
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol, "Side": pos.side, "Event": "ADD",
                                "Step_No": pos.steps_filled, "Step_Margin_USDT": margin,
                                "Cum_Margin_USDT": cum_margin, "Entry_Price": px, "Avg_Price": pos.avg,
                                "TP_Price": pos.tp_price, "SL_Price": pos.sl_price or "",
                                "Liq_Est_Price": liq, "Next_DCA_Price": (nxt2 and nxt2["price"]) or "", "Next_DCA_Label": (nxt2 and nxt2["label"]) or "", "Triggered_Label": curr_label,
                                "Fee_Rate_Maker": fee_maker, "Fee_Rate_Taker": fee_taker,
                                "Fee_Est_USDT": fees_paid_est / CONFIG.LIQ_FEE_BUFFER, "ATR_5m": ind["atr5m"], "ATR_1h": rng_strat["atr1h"],
                                "RSI_5m": ind["rsi"], "ADX_5m": ind["adx"], "Supertrend": ind["supertrend"], "Vol_z": ind["vol_z"],
                                "Range_Lower": rng_strat["lower"], "Range_Upper": rng_strat["upper"], "Range_Width": rng_strat["width"]
                            })

                if pos.side == "LONG": gain_to_tp = max(0.0, (px / max(pos.avg,1e-9) - 1.0) / CONFIG.TP_PCT)
                else: gain_to_tp = max(0.0, (pos.avg / max(px,1e-9) - 1.0) / CONFIG.TP_PCT)

                for stage_idx, (arm, lock) in enumerate(CONFIG.TRAILING_STAGES):
                    if pos.trail_stage >= stage_idx:
                        continue
                    if gain_to_tp < arm:
                        break
                    lock_pct = lock * CONFIG.TP_PCT
                    locked = pos.avg*(1+lock_pct) if pos.side=="LONG" else pos.avg*(1-lock_pct)
                    chand = chandelier_stop(pos.side, px, ind["atr5m"])
                    new_sl = max(locked, chand) if pos.side=="LONG" else min(locked, chand)
                    
                    tick = app.bot_data.get("price_tick", 1e-4)
                    new_sl_q  = quantize_to_tick(new_sl, tick)
                    curr_sl_q = quantize_to_tick(pos.sl_price, tick)
                    last_notif_q = quantize_to_tick(pos.last_sl_notified_price, tick)
                    improves = (curr_sl_q is None) or \
                               (pos.side == "LONG"  and new_sl_q > curr_sl_q) or \
                               (pos.side == "SHORT" and new_sl_q < curr_sl_q)
                    if improves:
                        pos.sl_price = new_sl_q
                        pos.trail_stage = stage_idx
                        if sl_moved_enough(last_notif_q, pos.sl_price, pos.side, tick, CONFIG.SL_NOTIFY_MIN_TICK_STEP):
                            if broadcast:
                                await broadcast(app, f"🛡️ Трейлинг-SL (стадия {stage_idx+1}) → <code>{fmt(pos.sl_price)}</code>")
                            pos.last_sl_notified_price = pos.sl_price
                            await log_event_safely({
                                "Event_ID": f"TRAIL_SET_{pos.signal_id}_{int(now)}", "Signal_ID": pos.signal_id,
                                "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "Pair": symbol, "Side": pos.side, "Event": "TRAIL_SET",
                                "SL_Price": pos.sl_price, "Avg_Price": pos.avg, "Trail_Stage": stage_idx+1
                            })

                tp_hit = (pos.side=="LONG" and px>=pos.tp_price) or (pos.side=="SHORT" and px<=pos.tp_price)
                sl_hit = pos.sl_price and ((pos.side=="LONG" and px<=pos.sl_price) or (pos.side=="SHORT" and px>=pos.sl_price))

                if tp_hit or sl_hit:
                    reason = "TP_HIT" if tp_hit else "SL_HIT"
                    exit_p = pos.tp_price if tp_hit else pos.sl_price
                    time_min = (time.time()-pos.open_ts)/60.0
                    net_usd, net_pct = compute_net_pnl(pos, exit_p, fee_taker, fee_maker)
                    atr_now = ind["atr5m"]
                    if broadcast:
                        await broadcast(app, f"{'✅' if net_usd > 0 else '❌'} <b>{reason}</b>\n"
                                      f"Цена выхода: <code>{fmt(exit_p)}</code>\n"
                                      f"P&L (net)≈ {net_usd:+.2f} USDT ({net_pct:+.2f}%)\n"
                                      f"ATR(5m): {atr_now:.6f}\n"
                                      f"Время в сделке: {time_min:.1f} мин")
                    await log_event_safely({
                        "Event_ID": f"{reason}_{pos.signal_id}", "Signal_ID": pos.signal_id,
                        "Timestamp_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "Pair": symbol, "Side": pos.side, "Event": reason,
                        "PNL_Realized_USDT": net_usd, "PNL_Realized_Pct": net_pct,
                        "Time_In_Trade_min": time_min,
                        "ATR_5m": atr_now
                    })
                    pos.last_sl_notified_price = None
                    app.bot_data["position"] = None

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

    await exchange.close()
    log.info("BMR-DCA loop gracefully stopped.")
