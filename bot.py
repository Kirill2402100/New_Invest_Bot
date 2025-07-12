#!/usr/bin/env python3
# ============================================================================
# Market Chameleon v5.3 • 12 Jul 2025
# ============================================================================
# • АДАПТИВНАЯ СТРАТЕГИЯ: Автоматическое переключение Тренд/Флэт
# • УЛУЧШЕНИЯ v5.3:
#   - Более точное исполнение TP/SL (проверка по High/Low свечи)
#   - Подтверждение цены входа с задержкой в 2 сек. для защиты от "шипов"
#   - Разделены Gross и Net P&L в логах
#   - Фильтр шорт-сигналов по дневному тренду/RSI
#   - Динамический порог ADX
#   - "Динамический депозит"
# ============================================================================

import os
import json
import logging
import re
import uuid
import asyncio
from datetime import datetime, timezone, time, timedelta

import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import pandas_ta as ta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Bot, Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes
)

# ── ENV: КОНФИГУРАЦИЯ БОТА ───────────────────────────────────────────────────
# --- Основные ---
BOT_TOKEN     = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW  = os.getenv("CHAT_IDS", "")
SHEET_ID      = os.getenv("SHEET_ID")
PAIR_RAW      = os.getenv("PAIR", "BTC/USDT")
TIMEFRAME     = os.getenv("TIMEFRAME", "5m")
STRAT_VERSION = "v5_3_chameleon_pro"

# --- Параметры P&L и отчётности ---
LEVERAGE             = float(os.getenv("LEVERAGE", "500"))
FEE_PCT              = float(os.getenv("FEE_PCT", "0.0004")) # 0.04% комиссия (вход + выход)
REPORT_TIME_UTC      = os.getenv("REPORT_TIME_UTC", "21:00")

# --- Параметры динамического депозита ---
INITIAL_DEPOSIT      = float(os.getenv("INITIAL_DEPOSIT", "50.0"))
REDUCED_DEPOSIT      = float(os.getenv("REDUCED_DEPOSIT", "25.0"))
DRAWDOWN_THRESHOLD_PCT = float(os.getenv("DRAWDOWN_THRESHOLD_PCT", "20.0"))

# --- Общие параметры стратегий ---
MIN_VOLUME_BTC = float(os.getenv("MIN_VOLUME_BTC", "1"))

# --- Параметры ТРЕНДОВОЙ стратегии ---
TREND_RR_RATIO = float(os.getenv("TREND_RR_RATIO", "1.5"))

# --- Параметры ФЛЭТОВОЙ стратегии ---
FLAT_RR_RATIO       = float(os.getenv("FLAT_RR_RATIO", "1.0"))
FLAT_RSI_OVERSOLD   = float(os.getenv("FLAT_RSI_OVERSOLD", "35"))
FLAT_RSI_OVERBOUGHT = float(os.getenv("FLAT_RSI_OVERBOUGHT", "65"))

# --- Настройки ATR для определения размера SL ---
ATR_LOW_USD   = float(os.getenv("ATR_LOW_USD",  "80"))
ATR_HIGH_USD  = float(os.getenv("ATR_HIGH_USD", "120"))
SL_PCT_LOW    = float(os.getenv("SL_PCT_LOW",   "0.08"))
SL_PCT_MID    = float(os.getenv("SL_PCT_MID",   "0.10"))
SL_PCT_HIGH   = float(os.getenv("SL_PCT_HIGH",  "0.12"))

# ── НАСТРОЙКА ЛОГИРОВАНИЯ ────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(STRAT_VERSION)

if not BOT_TOKEN:
    log.critical("ENV BOT_TOKEN не установлен"); raise SystemExit
if not re.fullmatch(r"\d+[mhdM]", TIMEFRAME):
    log.critical("Неверный формат TIMEFRAME '%s'", TIMEFRAME); raise SystemExit

CHAT_IDS = {int(cid) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

# ── GOOGLE SHEETS ────────────────────────────────────────────────────────────
TRADE_LOG_WS = None
def setup_google_sheets() -> None:
    global TRADE_LOG_WS
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(os.getenv("GOOGLE_CREDENTIALS")),
            ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
        )
        gs = gspread.authorize(creds)
        ss = gs.open_by_key(SHEET_ID)

        headers = [
            "Signal_ID", "Version", "Strategy_Used", "Status", "Side",
            "Entry_Time_UTC", "Exit_Time_UTC",
            "Entry_Price", "Exit_Price", "SL_Price", "TP_Price",
            "Gross_P&L_USD", "Fee_USD", "Net_P&L_USD", "Entry_Deposit_USD", "MFE_Price", "MAE_Price",
            "Entry_RSI", "Entry_ADX", "Dynamic_ADX_Threshold", "Entry_ATR",
            "Entry_Volume", "Entry_BB_Position"
        ]

        name = f"SniperLog_{PAIR_RAW.replace('/','_')}_{TIMEFRAME}_{STRAT_VERSION}"
        try:
            ws = ss.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(name, rows="1000", cols=len(headers))

        if ws.row_values(1) != headers:
            ws.clear(); ws.update("A1", [headers])
            ws.format(f"A1:{chr(ord('A')+len(headers)-1)}1",
                      {"textFormat": {"bold": True}})

        TRADE_LOG_WS = ws
        log.info("Логирование в Google Sheet включено ➜ %s", name)
    except Exception as e:
        log.error("Ошибка инициализации Google Sheets: %s", e)

setup_google_sheets()

# ── УПРАВЛЕНИЕ СОСТОЯНИЕМ ───────────────────────────────────────────────────
STATE_FILE = f"state_{STRAT_VERSION}_{PAIR_RAW.replace('/','_')}.json"
state = {
    "monitoring": False,
    "active_trade": None,
    "daily_report_data": [],
    "entry_usd_current": INITIAL_DEPOSIT,
    "equity_curve": [],
    "equity_peak": 0.0,
    "dynamic_adx_threshold": 25.0, # Начальное значение
    "last_adx_recalc_time": None
}

if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, 'r') as f:
            loaded_state = json.load(f)
            for key, default_value in state.items():
                if key not in loaded_state:
                    loaded_state[key] = default_value
            state.update(loaded_state)
    except (json.JSONDecodeError, TypeError):
        log.error("Не удалось прочитать файл состояния, будет создан новый.")

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ── ИНДИКАТОРЫ И СТРАТЕГИЯ ───────────────────────────────────────────────────
exchange = ccxt.mexc()
PAIR = PAIR_RAW.upper()

RSI_LEN = 14
EMA_FAST, EMA_SLOW = 9, 21
ATR_LEN, ADX_LEN = 14, 14
BBANDS_LEN = 20

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.ta.ema(length=EMA_FAST, append=True, col_names=(f"EMA_{EMA_FAST}",))
    df.ta.ema(length=EMA_SLOW, append=True, col_names=(f"EMA_{EMA_SLOW}",))
    df.ta.rsi(length=RSI_LEN, append=True, col_names=(f"RSI_{RSI_LEN}",))
    df.ta.atr(length=ATR_LEN, append=True, col_names=(f"ATR_{ATR_LEN}",))
    df.ta.adx(length=ADX_LEN, append=True,
              col_names=(f"ADX_{ADX_LEN}", f"DMP_{ADX_LEN}", f"DMN_{ADX_LEN}"))
    df.ta.bbands(length=BBANDS_LEN, std=2, append=True,
                col_names=(f"BBL_{BBANDS_LEN}_2.0", f"BBM_{BBANDS_LEN}_2.0",
                           f"BBU_{BBANDS_LEN}_2.0", f"BBB_{BBANDS_LEN}_2.0",
                           f"BBP_{BBANDS_LEN}_2.0"))
    return df.dropna()

def get_sl_pct_by_atr(atr: float) -> float:
    if atr <= ATR_LOW_USD:  return SL_PCT_LOW
    if atr >= ATR_HIGH_USD: return SL_PCT_HIGH
    return SL_PCT_MID

# ── УВЕДОМЛЕНИЯ И ЛОГИРОВАНИЕ ───────────────────────────────────────────────
async def notify(bot: Bot, text: str):
    for cid in CHAT_IDS:
        try:
            await bot.send_message(cid, text, parse_mode="HTML")
        except Exception as e:
            log.error("TG fail -> %s: %s", cid, e)

async def log_trade(tr: dict):
    if not TRADE_LOG_WS: return
    row = [
        tr["id"], STRAT_VERSION, tr.get("strategy_name"), tr["status"], tr["side"],
        tr["entry_time_utc"], datetime.now(timezone.utc).isoformat(),
        tr["entry_price"], tr["exit_price"],
        tr["sl_price"], tr["tp_price"],
        tr.get("gross_pnl_usd"), tr.get("fee_usd"), tr.get("net_pnl_usd"),
        tr.get("entry_deposit_usd"), tr["mfe_price"], tr["mae_price"],
        tr["entry_rsi"], tr["entry_adx"], tr.get("dynamic_adx_threshold"), tr["entry_atr"],
        tr["entry_volume"], tr["entry_bb_pos"]
    ]
    await asyncio.to_thread(
        TRADE_LOG_WS.append_row, row, value_input_option="USER_ENTERED"
    )

# ── ЛОГИКА СТРАТЕГИИ И ФИЛЬТРОВ ───────────────────────────────────────────────
async def recalculate_adx_threshold():
    try:
        log.info("Пересчет динамического порога ADX...")
        ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=2000)
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
        df.ta.adx(length=ADX_LEN, append=True, col_names=(f"ADX_{ADX_LEN}", "DMP", "DMN"))
        df.dropna(inplace=True)

        if not df.empty:
            adx_values = df[f"ADX_{ADX_LEN}"]
            p20 = np.percentile(adx_values, 20)
            p30 = np.percentile(adx_values, 30)
            new_threshold = (p20 + p30) / 2
            state["dynamic_adx_threshold"] = new_threshold
            state["last_adx_recalc_time"] = datetime.now(timezone.utc).isoformat()
            save_state()
            log.info(f"Новый порог ADX: {new_threshold:.2f} (p20={p20:.2f}, p30={p30:.2f})")
    except Exception as e:
        log.error(f"Ошибка при пересчете порога ADX: {e}")

async def is_short_allowed() -> bool:
    try:
        ohlcv_1d = await exchange.fetch_ohlcv(PAIR, timeframe='1d', limit=201)
        df_1d = pd.DataFrame(ohlcv_1d, columns=["ts","open","high","low","close","volume"])
        df_1d.ta.ema(length=200, append=True, col_names=("EMA_200",))
        df_1d.ta.rsi(length=14, append=True, col_names=("RSI_14",))
        df_1d.dropna(inplace=True)

        if df_1d.empty:
            log.warning("Не удалось получить данные 1d для фильтра, шорт разрешен по умолчанию.")
            return True

        last_daily = df_1d.iloc[-1]
        price_close_1d = last_daily['close']
        ema200_1d = last_daily['EMA_200']
        rsi14_1d = last_daily['RSI_14']

        if price_close_1d < ema200_1d or rsi14_1d > 60:
            return True
        else:
            log.info(f"SHORT blocked by trend filter. Price_1d={price_close_1d:.2f}, EMA200_1d={ema200_1d:.2f}, RSI14_1d={rsi14_1d:.2f}")
            return False
    except Exception as e:
        log.error(f"Ошибка в фильтре дневного тренда: {e}")
        return True

def update_dynamic_deposit(net_pnl_usd: float):
    state["equity_curve"].append(net_pnl_usd)
    current_equity = sum(state["equity_curve"])
    new_peak = max(state.get("equity_peak", 0.0), current_equity)

    if current_equity >= new_peak:
        state["entry_usd_current"] = INITIAL_DEPOSIT
        log.info(f"Кривая эквити достигла нового максимума! Депозит возвращен к {INITIAL_DEPOSIT}$")
    else:
        if new_peak > 0:
            drawdown = (new_peak - current_equity) / new_peak
            if drawdown * 100 >= DRAWDOWN_THRESHOLD_PCT:
                if state["entry_usd_current"] != REDUCED_DEPOSIT:
                    log.warning(f"Просадка {drawdown*100:.2f}% >= {DRAWDOWN_THRESHOLD_PCT}%. Депозит снижен до {REDUCED_DEPOSIT}$")
                    state["entry_usd_current"] = REDUCED_DEPOSIT
    
    state["equity_peak"] = new_peak
    save_state()

def get_market_state(last_candle: pd.Series) -> str:
    adx = last_candle[f"ADX_{ADX_LEN}"]
    if adx > state["dynamic_adx_threshold"]:
        return "TREND"
    else:
        return "FLAT"

def run_trend_strategy(last_candle: pd.Series, prev_candle: pd.Series):
    price = last_candle["close"]
    bull_now = last_candle[f"EMA_{EMA_FAST}"] > last_candle[f"EMA_{EMA_SLOW}"]
    bull_prev = prev_candle[f"EMA_{EMA_FAST}"] > prev_candle[f"EMA_{EMA_SLOW}"]

    long_cond  = last_candle[f"RSI_{RSI_LEN}"] > 52 and price > last_candle[f"EMA_{FAST}"]
    short_cond = last_candle[f"RSI_{RSI_LEN}"] < 48 and price < last_candle[f"EMA_{FAST}"]

    side = None
    if bull_now and not bull_prev and long_cond:
        side = "LONG"
    elif not bull_now and bull_prev and short_cond:
        side = "SHORT"
    
    if side:
        return {"side": side, "rr_ratio": TREND_RR_RATIO, "strategy_name": "Trend_EMA_Cross"}
    return None

def run_flat_strategy(last_candle: pd.Series):
    price = last_candle["close"]
    rsi = last_candle[f"RSI_{RSI_LEN}"]
    bb_lower = last_candle[f"BBL_{BBANDS_LEN}_2.0"]
    bb_upper = last_candle[f"BBU_{BBANDS_LEN}_2.0"]

    side = None
    if price <= bb_lower and rsi < FLAT_RSI_OVERSOLD:
        side = "LONG"
    elif price >= bb_upper and rsi > FLAT_RSI_OVERBOUGHT:
        side = "SHORT"

    if side:
        return {"side": side, "rr_ratio": FLAT_RR_RATIO, "strategy_name": "Flat_BB_Fade"}
    return None

# ── ОСНОВНОЙ ЦИКЛ БОТА ───────────────────────────────────────────────────────
async def monitor(app: Application):
    log.info("🚀 Основной цикл запущен: %s %s (%s)", PAIR, TIMEFRAME, STRAT_VERSION)
    while state["monitoring"]:
        try:
            now_utc = datetime.now(timezone.utc)
            last_recalc_str = state.get("last_adx_recalc_time")
            if last_recalc_str:
                if now_utc - datetime.fromisoformat(last_recalc_str) > timedelta(minutes=60):
                    await recalculate_adx_threshold()
            else:
                await recalculate_adx_threshold()

            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
            df = add_indicators(pd.DataFrame(
                ohlcv, columns=["ts","open","high","low","close","volume"]
            ))
            if len(df) < 2:
                await asyncio.sleep(30); continue

            last, prev = df.iloc[-1], df.iloc[-2]
            
            # ---------------- 1. Управление активной сделкой ----------------
            if trade := state.get("active_trade"):
                side, sl, tp = trade["side"], trade["sl_price"], trade["tp_price"]
                candle_high, candle_low = last["high"], last["low"]

                if side == "LONG":
                    trade["mfe_price"] = max(trade["mfe_price"], candle_high)
                    trade["mae_price"] = min(trade["mae_price"], candle_low)
                else:
                    trade["mfe_price"] = min(trade["mfe_price"], candle_low)
                    trade["mae_price"] = max(trade["mae_price"], candle_high)

                done = status = exit_price = None
                if side == "LONG":
                    if candle_high >= tp: done, status, exit_price = "TP_HIT", "WIN", tp
                    elif candle_low <= sl: done, status, exit_price = "SL_HIT", "LOSS", sl
                elif side == "SHORT":
                    if candle_low <= tp: done, status, exit_price = "TP_HIT", "WIN", tp
                    elif candle_high >= sl: done, status, exit_price = "SL_HIT", "LOSS", sl

                if done:
                    entry_price = trade["entry_price"]
                    current_deposit = trade["entry_deposit_usd"]
                    
                    pnl_pct = (exit_price / entry_price - 1) if side == "LONG" else (entry_price / exit_price - 1)
                    
                    gross_pnl_usd = pnl_pct * current_deposit * LEVERAGE
                    fee_usd = current_deposit * LEVERAGE * FEE_PCT
                    net_pnl_usd = gross_pnl_usd - fee_usd

                    trade.update({
                        "gross_pnl_usd": round(gross_pnl_usd, 2),
                        "fee_usd": round(fee_usd, 2),
                        "net_pnl_usd": round(net_pnl_usd, 2),
                        "status": status,
                        "exit_price": exit_price
                    })
                    
                    state["daily_report_data"].append({
                        "pnl_usd": net_pnl_usd, "entry_usd": current_deposit
                    })
                    
                    update_dynamic_deposit(net_pnl_usd)

                    pnl_text = f"💰 <b>Net P&L: {trade['net_pnl_usd']:.2f}$</b> (Fee: {trade['fee_usd']:.2f}$)"
                    msg_icon = "✅" if status == "WIN" else "❌"
                    
                    await notify(app.bot,
                        f"{msg_icon} <b>СДЕЛКА ЗАКРЫТА: {status}</b> {msg_icon}\n\n"
                        f"<b>Стратегия:</b> {trade['strategy_name']}\n"
                        f"<b>Тип:</b> {side}\n<b>ID:</b> {trade['id']}\n\n"
                        f"<b>Вход:</b> {entry_price:.2f}\n<b>Выход:</b> {exit_price:.2f}\n"
                        f"{pnl_text}"
                    )
                    await log_trade(trade)
                    state["active_trade"] = None
                    save_state()

            # ---------------- 2. Поиск нового сигнала -----------------------
            else:
                if not last["volume"] >= MIN_VOLUME_BTC:
                    await asyncio.sleep(60); continue

                market_state = get_market_state(last)
                signal_data = None
                
                if market_state == "TREND":
                    signal_data = run_trend_strategy(last, prev)
                elif market_state == "FLAT":
                    signal_data = run_flat_strategy(last)

                if signal_data:
                    side = signal_data["side"]
                    
                    if side == "SHORT" and not await is_short_allowed():
                        continue

                    # --- Подтверждение цены входа ---
                    log.info(f"Сигнал {side} получен. Ожидание 2 сек для подтверждения цены...")
                    await asyncio.sleep(2)
                    try:
                        ticker = await exchange.fetch_ticker(PAIR)
                        entry = ticker['last']
                        log.info(f"Цена входа подтверждена: {entry}")
                    except Exception as e:
                        log.error(f"Не удалось подтвердить цену входа: {e}")
                        continue
                    # --------------------------------

                    rr_ratio = signal_data["rr_ratio"]
                    strategy_name = signal_data["strategy_name"]

                    atr = last[f"ATR_{ATR_LEN}"]
                    sl_pct = get_sl_pct_by_atr(atr)
                    tp_pct = sl_pct * rr_ratio

                    sl = entry * (1 - sl_pct/100) if side=="LONG" else entry * (1 + sl_pct/100)
                    tp = entry * (1 + tp_pct/100) if side=="LONG" else entry * (1 - tp_pct/100)

                    bb_up, bb_lo = last[f"BBU_{BBANDS_LEN}_2.0"], last[f"BBL_{BBANDS_LEN}_2.0"]
                    bb_pos = "Внутри"
                    if entry > bb_up:   bb_pos = "Выше верхней"
                    elif entry < bb_lo: bb_pos = "Ниже нижней"

                    current_deposit = state['entry_usd_current']

                    trade = dict(
                        id=uuid.uuid4().hex[:8], side=side, strategy_name=strategy_name,
                        entry_time_utc=datetime.now(timezone.utc).isoformat(),
                        entry_price=entry, tp_price=tp, sl_price=sl,
                        entry_deposit_usd=current_deposit,
                        mfe_price=entry, mae_price=entry,
                        entry_rsi=round(last[f"RSI_{RSI_LEN}"],2),
                        entry_adx=round(last[f"ADX_{ADX_LEN}"],2),
                        dynamic_adx_threshold=round(state["dynamic_adx_threshold"], 2),
                        entry_atr=round(atr,2),
                        entry_volume=last["volume"], entry_bb_pos=bb_pos
                    )
                    state["active_trade"] = trade
                    save_state()

                    await notify(
                        app.bot,
                        f"🔔 <b>НОВЫЙ СИГНАЛ ({strategy_name})</b> 🔔\n\n"
                        f"<b>Тип:</b> {side} (v: {STRAT_VERSION})\n"
                        f"<b>ID:</b> {trade['id']}\n\n"
                        f"<b>Цена входа:</b> {entry:.2f}\n"
                        f"<b>Take Profit:</b> {tp:.2f} ({tp_pct:.2f}%)\n"
                        f"<b>Stop Loss:</b> {sl:.2f} ({sl_pct:.2f}%)\n\n"
                        f"<b>Размер депозита:</b> {current_deposit:.2f}$\n"
                        f"<i>Параметры: ADX={trade['entry_adx']} (T: {trade['dynamic_adx_threshold']}), RSI={trade['entry_rsi']}</i>"
                    )

        except ccxt.NetworkError as e:
            log.warning("CCXT ошибка сети: %s", e); await asyncio.sleep(60)
        except Exception as e:
            log.exception("Сбой в основном цикле:"); await asyncio.sleep(30)

        await asyncio.sleep(60)
    log.info("⛔️ Основной цикл остановлен.")

# ── ЕЖЕДНЕВНЫЙ ОТЧЁТ ────────────────────────────────────────────────────────
async def daily_reporter(app: Application):
    log.info("📈 Сервис ежедневных отчётов запущен.")
    while True:
        now_utc = datetime.now(timezone.utc)
        try:
            report_h, report_m = map(int, REPORT_TIME_UTC.split(':'))
            report_time = now_utc.replace(hour=report_h, minute=report_m, second=0, microsecond=0)
        except ValueError:
            log.error("Неверный формат REPORT_TIME_UTC. Используйте HH:MM. Отчеты отключены.")
            return

        if now_utc > report_time:
            report_time += timedelta(days=1)

        wait_seconds = (report_time - now_utc).total_seconds()
        log.info(f"Следующий суточный отчёт будет отправлен в {REPORT_TIME_UTC} UTC (через {wait_seconds/3600:.2f} ч).")
        await asyncio.sleep(wait_seconds)

        report_data = state.get("daily_report_data", [])
        if not report_data:
            await notify(app.bot, f"📊 <b>Суточный отчёт ({STRAT_VERSION})</b> 📊\n\nЗа последние 24 часа сделок не было.")
            await asyncio.sleep(60)
            continue

        total_pnl_usd = sum(item['pnl_usd'] for item in report_data)
        total_trades = len(report_data)
        wins = sum(1 for item in report_data if item['pnl_usd'] > 0)
        losses = total_trades - wins
        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
        
        total_investment = sum(item['entry_usd'] for item in report_data)
        pnl_percent = (total_pnl_usd / total_investment) * 100 if total_investment > 0 else 0
        
        report_msg = (
            f"📊 <b>Суточный отчёт по стратегии {STRAT_VERSION}</b> 📊\n\n"
            f"<b>Период:</b> последние 24 часа\n"
            f"<b>Всего сделок:</b> {total_trades} (📈{wins} / 📉{losses})\n"
            f"<b>Винрейт:</b> {win_rate:.2f}%\n\n"
            f"<b>Финансовый результат:</b>\n"
            f"💵 <b>Net P&L ($): {total_pnl_usd:+.2f}$</b>\n"
            f"💹 <b>ROI (%): {pnl_percent:+.2f}%</b>"
        )
        await notify(app.bot, report_msg)

        state["daily_report_data"] = []
        save_state()
        await asyncio.sleep(60)

# ── КОМАНДЫ TELEGRAM И ЗАПУСК ────────────────────────────────────────────────
async def _start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    CHAT_IDS.add(update.effective_chat.id)
    if not state.get("monitoring"):
        state["monitoring"] = True
        save_state()
        await update.message.reply_text(f"✅ Бот <b>{STRAT_VERSION}</b> запущен.", parse_mode="HTML")
        asyncio.create_task(monitor(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Бот уже работает.")

async def _stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False
    save_state()
    await update.message.reply_text("❌ Бот остановлен.")

async def _status(update: Update, _):
    msg = f"<b>Статус:</b> {'АКТИВЕН' if state.get('monitoring') else 'ОСТАНОВЛЕН'}\n"
    msg += f"<b>Текущий депозит:</b> {state['entry_usd_current']:.2f}$\n"
    msg += f"<b>Пик эквити:</b> {state['equity_peak']:.2f}$\n"
    msg += f"<b>Текущий порог ADX:</b> {state['dynamic_adx_threshold']:.2f}\n"

    if tr := state.get("active_trade"):
        msg += (f"\n<b>Активная сделка: {tr['side']}</b> ({tr['strategy_name']})\n"
                f"ID: {tr['id']}\n"
                f"Вход: {tr['entry_price']:.2f} | TP: {tr['tp_price']:.2f} | SL: {tr['sl_price']:.2f}")
    else:
        msg += "\n<i>Нет активных сделок.</i>"
    await update.message.reply_text(msg, parse_mode="HTML")

async def _post_init(app: Application):
    if os.path.exists(STATE_FILE):
        log.info("Файл состояния найден, загружаю.")
    if state.get("monitoring"):
        asyncio.create_task(monitor(app))
    
    asyncio.create_task(daily_reporter(app))

if __name__ == "__main__":
    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .post_init(_post_init)
           .build())
    app.add_handler(CommandHandler("start", _start))
    app.add_handler(CommandHandler("stop", _stop))
    app.add_handler(CommandHandler("status", _status))
    app.run_polling()
