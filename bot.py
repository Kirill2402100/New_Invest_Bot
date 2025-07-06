#!/usr/bin/env python3
# ============================================================================
# v5.0 - EMA Crossover Signal Monitor (вместо SuperTrend)
# ============================================================================

import os
import asyncio
import json
import logging
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# === ENV / Logging ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_IDS = {int(cid) for cid in os.getenv("CHAT_IDS", "0").split(",") if cid}
PAIR_RAW = os.getenv("PAIR", "BTC/USDT")
SHEET_ID = os.getenv("SHEET_ID")
TIMEFRAME = os.getenv("TIMEFRAME", "1h")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

# === GOOGLE SHEETS ===
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gs = gspread.authorize(creds)
    LOGS_WS = gs.open_by_key(SHEET_ID).worksheet("LP_Logs")
    HEADERS = ["Дата-время", "Инструмент", "Направление", "Депозит", "Вход", "Stop Loss", "Take Profit", "RR", "P&L сделки (USDT)", "Прибыль к депозиту (%)"]
    if LOGS_WS.row_values(1) != HEADERS:
        LOGS_WS.resize(rows=1); LOGS_WS.update('A1', [HEADERS])
except Exception as e:
    log.error("Google Sheets init failed: %s", e)
    LOGS_WS = None

# === STATE MANAGEMENT ===
STATE_FILE = "advanced_signal_state.json"
state = {
    "monitoring": False,
    "active_signal": None,
    "manual_position": None,
    "signal_status": {
        "rsi": False,
        "ema_position": False,
        "ema_cross": False,
        "side": None
    }
}

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            log.info("State loaded: %s", state)

# === EXCHANGE ===
exchange = ccxt.mexc()
PAIR = PAIR_RAW.upper()

# === STRATEGY PARAMS ===
RSI_LEN = 14
EMA_FAST_LEN, EMA_SLOW_LEN = 9, 21
RSI_LONG_T, RSI_SHORT_T = 52, 48
PRICE_CHANGE_STEP_PCT = 0.1

# === INDICATORS ===
def _ta_rsi(series: pd.Series, length=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=length, min_periods=length).mean()
    loss = (-delta.clip(upper=0)).rolling(window=length, min_periods=length).mean()
    if loss.empty or loss.iloc[-1] == 0: return pd.Series(100, index=series.index)
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_indicators(df: pd.DataFrame):
    df['ema_fast'] = df['close'].ewm(span=EMA_FAST_LEN, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW_LEN, adjust=False).mean()
    df['rsi'] = _ta_rsi(df['close'], RSI_LEN)
    df['ema_cross'] = df['ema_fast'] > df['ema_slow']
    return df.dropna()

# === MAIN MONITORING LOOP ===
async def monitor_loop(app):
    while state['monitoring']:
        try:
            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = calculate_indicators(df.copy())
            last = df.iloc[-1]
            prev = df.iloc[-2]
            current_price = last['close']

            side = None
            rsi_passed = last['rsi'] > RSI_LONG_T
            ema_position_passed = last['close'] > last['ema_fast'] and last['close'] > last['ema_slow']
            ema_cross_passed = prev['ema_fast'] < prev['ema_slow'] and last['ema_fast'] > last['ema_slow']

            # Логика сигнала LONG
            if rsi_passed and ema_position_passed and ema_cross_passed:
                if not state['active_signal']:
                    await broadcast_message(app, f"✅ СИГНАЛ LONG! Цена: {current_price:.2f}")
                    state['active_signal'] = {
                        "side": "LONG",
                        "price": current_price,
                        "next_target_pct": PRICE_CHANGE_STEP_PCT
                    }
                state['signal_status'] = {"rsi": True, "ema_position": True, "ema_cross": True, "side": "LONG"}

            # Проверка на отмену или возврат
            elif state['active_signal']:
                # Условия всё ещё соблюдаются?
                still_rsi = last['rsi'] > RSI_LONG_T
                still_ema_pos = last['close'] > last['ema_fast'] and last['close'] > last['ema_slow']
                still_cross = last['ema_fast'] > last['ema_slow']

                missing = []
                if not still_rsi: missing.append("RSI")
                if not still_ema_pos: missing.append("EMA положение")
                if not still_cross: missing.append("EMA пересечение")

                if missing:
                    await broadcast_message(app, f"⚠️ ОТМЕНА сигнала LONG. Нарушены условия: {', '.join(missing)}")
                    state['active_signal'] = None
                    state['signal_status'] = {"rsi": False, "ema_position": False, "ema_cross": False, "side": None}
                else:
                    # Проверка на движение цены
                    signal_data = state['active_signal']
                    price_change_pct = ((current_price - signal_data['price']) / signal_data['price']) * 100
                    if price_change_pct >= signal_data['next_target_pct']:
                        await broadcast_message(app, f"🎯 ЦЕЛЬ +{signal_data['next_target_pct']:.1f}% ДОСТИГНУТА. Текущая цена: {current_price:.2f}")
                        state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                        save_state()
            else:
                partials = []
                if rsi_passed: partials.append("RSI")
                if ema_position_passed: partials.append("EMA положение")
                if ema_cross_passed: partials.append("EMA пересечение")
                if partials:
                    await broadcast_message(app, f"ℹ️ Выполнено: {', '.join(partials)}. Ждём остальные условия...")

            save_state()
            await asyncio.sleep(30)

        except Exception as e:
            log.error("Error in monitor loop: %s", e)
            await asyncio.sleep(30)

# === COMMANDS ===
async def broadcast_message(app, text):
    for chat_id in CHAT_IDS:
        await app.bot.send_message(chat_id=chat_id, text=text)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    CHAT_IDS.add(chat_id)
    if not state['monitoring']:
        state['monitoring'] = True
        save_state()
        await update.message.reply_text("✅ Мониторинг запущен.")
        asyncio.create_task(monitor_loop(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Мониторинг уже активен.")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state['monitoring'] = False
    save_state()
    await update.message.reply_text("🛑 Мониторинг остановлен.")

if __name__ == "__main__":
    load_state()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    log.info("Bot started...")
    app.run_polling()
