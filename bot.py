#!/usr/bin/env python3
# ============================================================================
# Trend Rider v3.0 • 10 Jul 2025
# ============================================================================
# • СТРАТЕГИЯ      : Трендовая (EMA Cross + ADX Filter)
# • ФИЛЬТР ТРЕНДА  : ADX > MIN_ADX (default 25)
# • RISK/REWARD    : 1 / 1.5 (настраиваемый)
# • P&L РАСЧЁТ     : Автоматический ($/%)
# • ОТЧЁТНОСТЬ     : Ежедневный отчёт по P&L
# ============================================================================

import os
import json
import logging
import re
import uuid
import asyncio
from datetime import datetime, timezone, time

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
STRAT_VERSION = "v3_0_trend_rider"

# --- Параметры стратегии ---
MIN_VOLUME_BTC = float(os.getenv("MIN_VOLUME_BTC", "1"))
MIN_ADX        = float(os.getenv("MIN_ADX",        "25")) # Теперь MIN вместо MAX
RR_RATIO       = float(os.getenv("RR_RATIO",       "1.5")) # Соотношение TP к SL

# --- Параметры P&L и отчётности ---
ENTRY_USD      = float(os.getenv("ENTRY_USD", "50"))
LEVERAGE       = float(os.getenv("LEVERAGE",  "500"))
REPORT_TIME_UTC= os.getenv("REPORT_TIME_UTC", "21:00") # Время для ежедневного отчёта в UTC

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

        # !!! ДОБАВЛЕН НОВЫЙ ЗАГОЛОВОК "P&L_USD" !!!
        headers = [
            "Signal_ID","Version","Status","Side",
            "Entry_Time_UTC","Exit_Time_UTC",
            "Entry_Price","Exit_Price","SL_Price","TP_Price",
            "P&L_USD", # <-- НОВОЕ ПОЛЕ
            "MFE_Price","MAE_Price",
            "Entry_RSI","Entry_ADX","Entry_ATR",
            "Entry_Volume","Entry_BB_Position"
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
STATE_FILE = "btc_sniper_state.json"
state = {"monitoring": False, "active_trade": None, "daily_report_data": []}
if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, 'r') as f:
            state.update(json.load(f))
        if "daily_report_data" not in state: # Для совместимости со старым файлом
            state["daily_report_data"] = []
    except json.JSONDecodeError:
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
RSI_LONG_ENTRY, RSI_SHORT_ENTRY = 52, 48

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.ta.ema(length=EMA_FAST , append=True, col_names=(f"EMA_{EMA_FAST}",))
    df.ta.ema(length=EMA_SLOW , append=True, col_names=(f"EMA_{EMA_SLOW}",))
    df.ta.rsi(length=RSI_LEN  , append=True, col_names=(f"RSI_{RSI_LEN}",))
    df.ta.atr(length=ATR_LEN  , append=True, col_names=(f"ATR_{ATR_LEN}",))
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
        tr["id"], STRAT_VERSION, tr["status"], tr["side"],
        tr["entry_time_utc"], datetime.now(timezone.utc).isoformat(),
        tr["entry_price"], tr["exit_price"],
        tr["sl_price"], tr["tp_price"],
        tr.get("pnl_usd"), # <-- НОВОЕ ПОЛЕ
        tr["mfe_price"], tr["mae_price"],
        tr["entry_rsi"], tr["entry_adx"], tr["entry_atr"],
        tr["entry_volume"], tr["entry_bb_pos"]
    ]
    await asyncio.to_thread(
        TRADE_LOG_WS.append_row, row, value_input_option="USER_ENTERED"
    )

# ── ОСНОВНОЙ ЦИКЛ БОТА ───────────────────────────────────────────────────────
async def monitor(app: Application):
    log.info("🚀 Основной цикл запущен: %s %s (%s)", PAIR, TIMEFRAME, STRAT_VERSION)
    while state["monitoring"]:
        try:
            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
            df = add_indicators(pd.DataFrame(
                ohlcv, columns=["ts","open","high","low","close","volume"]
            ))

            last, prev = df.iloc[-1], df.iloc[-2]
            price = last["close"]

            # ---------------- 1. Управление активной сделкой ----------------
            if trade := state.get("active_trade"):
                side, sl, tp = trade["side"], trade["sl_price"], trade["tp_price"]

                # Обновление MFE/MAE
                if side == "LONG":
                    trade["mfe_price"] = max(trade["mfe_price"], price)
                    trade["mae_price"] = min(trade["mae_price"], price)
                else:
                    trade["mfe_price"] = min(trade["mfe_price"], price)
                    trade["mae_price"] = max(trade["mae_price"], price)

                done = status = None
                if side == "LONG"  and price >= tp: done, status = "TP_HIT", "WIN"
                elif side == "LONG" and price <= sl: done, status = "SL_HIT", "LOSS"
                elif side == "SHORT" and price <= tp: done, status = "TP_HIT", "WIN"
                elif side == "SHORT" and price >= sl: done, status = "SL_HIT", "LOSS"

                if done:
                    # --- РАСЧЁТ P&L ---
                    entry_price = trade["entry_price"]
                    pnl_pct = 0
                    if side == "LONG":
                        pnl_pct = (price / entry_price - 1)
                    else: # SHORT
                        pnl_pct = (entry_price / price - 1)
                    
                    pnl_usd = pnl_pct * ENTRY_USD * LEVERAGE
                    trade["pnl_usd"] = round(pnl_usd, 2)
                    
                    # --- Обновление данных для отчёта ---
                    state["daily_report_data"].append({
                        "pnl_usd": trade["pnl_usd"],
                        "entry_usd": ENTRY_USD
                    })

                    trade["status"] = status
                    trade["exit_price"] = price

                    pnl_text = f"💰 <b>P&L: {trade['pnl_usd']:.2f}$</b>"
                    msg_icon = "✅" if status == "WIN" else "❌"
                    
                    await notify(app.bot,
                        f"{msg_icon} <b>СДЕЛКА ЗАКРЫТА: {status}</b> {msg_icon}\n\n"
                        f"<b>Тип:</b> {side} (v: {STRAT_VERSION})\n"
                        f"<b>ID:</b> {trade['id']}\n\n"
                        f"<b>Вход:</b> {entry_price:.2f}\n"
                        f"<b>Выход:</b> {price:.2f}\n"
                        f"{pnl_text}"
                    )
                    await log_trade(trade)
                    state["active_trade"] = None
                    save_state()

            # ---------------- 2. Поиск нового сигнала -----------------------
            else:
                vol_ok = last["volume"] >= MIN_VOLUME_BTC
                adx_ok = last[f"ADX_{ADX_LEN}"] > MIN_ADX # <-- ИЗМЕНЕНА ЛОГИКА

                if not (vol_ok and adx_ok):
                    await asyncio.sleep(60); continue

                bull_now = last[f"EMA_{EMA_FAST}"] > last[f"EMA_{EMA_SLOW}"]
                bull_prev= prev[f"EMA_{EMA_FAST}"] > prev[f"EMA_{EMA_SLOW}"]

                long_cond  = last[f"RSI_{RSI_LEN}"] > RSI_LONG_ENTRY and price > last[f"EMA_{EMA_FAST}"]
                short_cond = last[f"RSI_{RSI_LEN}"] < RSI_SHORT_ENTRY and price < last[f"EMA_{EMA_FAST}"]

                side = None
                if   bull_now and not bull_prev and long_cond:  side = "LONG"
                elif not bull_now and bull_prev and short_cond: side = "SHORT"

                if side:
                    atr = last[f"ATR_{ATR_LEN}"]
                    sl_pct = get_sl_pct_by_atr(atr)
                    tp_pct = sl_pct * RR_RATIO # <-- НОВЫЙ РАСЧЕТ TP

                    entry = price
                    sl = entry * (1 - sl_pct/100) if side=="LONG" else entry * (1 + sl_pct/100)
                    tp = entry * (1 + tp_pct/100) if side=="LONG" else entry * (1 - tp_pct/100)

                    bb_up, bb_lo = last[f"BBU_{BBANDS_LEN}_2.0"], last[f"BBL_{BBANDS_LEN}_2.0"]
                    bb_pos = "Внутри"
                    if entry > bb_up:   bb_pos = "Выше верхней"
                    elif entry < bb_lo: bb_pos = "Ниже нижней"

                    trade = dict(
                        id=uuid.uuid4().hex[:8], side=side,
                        entry_time_utc=datetime.now(timezone.utc).isoformat(),
                        entry_price=entry, tp_price=tp, sl_price=sl,
                        mfe_price=entry, mae_price=entry,
                        entry_rsi=round(last[f"RSI_{RSI_LEN}"],2),
                        entry_adx=round(last[f"ADX_{ADX_LEN}"],2),
                        entry_atr=round(atr,2),
                        entry_volume=last["volume"], entry_bb_pos=bb_pos
                    )
                    state["active_trade"] = trade
                    save_state()

                    await notify(
                        app.bot,
                        f"🔔 <b>НОВЫЙ СИГНАЛ: {side}</b> 🔔\n\n"
                        f"<b>Версия:</b> {STRAT_VERSION}\n"
                        f"<b>ID:</b> {trade['id']}\n\n"
                        f"<b>Цена входа:</b> {entry:.2f}\n"
                        f"<b>Take Profit:</b> {tp:.2f} ({tp_pct:.2f}%)\n"
                        f"<b>Stop Loss:</b> {sl:.2f} ({sl_pct:.2f}%)\n\n"
                        f"<i>Параметры: ADX={trade['entry_adx']}, RSI={trade['entry_rsi']}, ATR={trade['entry_atr']}</i>"
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
        report_h, report_m = map(int, REPORT_TIME_UTC.split(':'))
        report_time = now_utc.replace(hour=report_h, minute=report_m, second=0, microsecond=0)
        
        if now_utc > report_time:
            # Если время уже прошло, ждём до завтра
            report_time = report_time.replace(day=now_utc.day + 1)

        wait_seconds = (report_time - now_utc).total_seconds()
        log.info(f"Следующий суточный отчёт будет отправлен в {REPORT_TIME_UTC} UTC (через {wait_seconds/3600:.2f} ч).")
        await asyncio.sleep(wait_seconds)

        # Время пришло, генерируем отчёт
        report_data = state.get("daily_report_data", [])
        if not report_data:
            await notify(app.bot, "📊 <b>Суточный отчёт</b> 📊\n\nЗа последние 24 часа сделок не было.")
            continue

        total_pnl_usd = sum(item['pnl_usd'] for item in report_data)
        total_trades = len(report_data)
        wins = sum(1 for item in report_data if item['pnl_usd'] > 0)
        losses = total_trades - wins
        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
        
        # PnL в % от общего вложенного капитала (неплечевого)
        total_investment = sum(item['entry_usd'] for item in report_data)
        pnl_percent = (total_pnl_usd / total_investment) * 100 if total_investment > 0 else 0
        
        report_msg = (
            f"📊 <b>Суточный отчёт по стратегии {STRAT_VERSION}</b> 📊\n\n"
            f"<b>Период:</b> последние 24 часа\n"
            f"<b>Всего сделок:</b> {total_trades} (📈{wins} / 📉{losses})\n"
            f"<b>Винрейт:</b> {win_rate:.2f}%\n\n"
            f"<b>Финансовый результат:</b>\n"
            f"💵 <b>P&L ($): {total_pnl_usd:+.2f}$</b>\n"
            f"💹 <b>P&L (%): {pnl_percent:+.2f}%</b>"
        )
        await notify(app.bot, report_msg)

        # Очищаем данные для следующего дня
        state["daily_report_data"] = []
        save_state()

# ── КОМАНДЫ TELEGRAM И ЗАПУСК ────────────────────────────────────────────────
async def _start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    CHAT_IDS.add(update.effective_chat.id)
    if not state["monitoring"]:
        state["monitoring"] = True; save_state()
        await update.message.reply_text(f"✅ Бот <b>{STRAT_VERSION}</b> запущен. Начинаю мониторинг.", parse_mode="HTML")
        asyncio.create_task(monitor(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Бот уже работает.")

async def _stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False; save_state()
    await update.message.reply_text("❌ Бот остановлен.")

async def _status(update: Update, _):
    msg = f"<b>Статус:</b> {'АКТИВЕН' if state['monitoring'] else 'ОСТАНОВЛЕН'}\n"
    if tr := state["active_trade"]:
        msg += (f"<b>Активная сделка: {tr['side']}</b> (ID: {tr['id']})\n"
                f"Вход: {tr['entry_price']:.2f} | TP: {tr['tp_price']:.2f} | SL: {tr['sl_price']:.2f}")
    else:
        msg += "<i>Нет активных сделок.</i>"
    await update.message.reply_text(msg, parse_mode="HTML")

async def _post_init(app: Application):
    if os.path.exists(STATE_FILE):
        log.info("Файл состояния найден, загружаю.")
    if state["monitoring"]:
        asyncio.create_task(monitor(app))
    
    # Запускаем фоновую задачу для отчётов
    asyncio.create_task(daily_reporter(app))

if __name__ == "__main__":
    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .post_init(_post_init)
           .build())
    app.add_handler(CommandHandler("start",  _start))
    app.add_handler(CommandHandler("stop",   _stop))
    app.add_handler(CommandHandler("status", _status))
    app.run_polling()
