#!/usr/bin/env python3
# =========================================================================
# v4.1 - Условия запоминаются поэтапно, логируется прогресс, сигнал при полном совпадении
# =========================================================================

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
import pandas_ta as ta

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

# === STATE ===
STATE_FILE = "advanced_signal_state.json"
state = {
    "monitoring": False,
    "active_signal": None,
    "manual_position": None,
    "condition_flags": {}
}

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)

# === EXCHANGE ===
exchange = ccxt.mexc()
PAIR = PAIR_RAW.upper()

# === STRATEGY PARAMS ===
RSI_LEN, ATR_LEN = 14, 14
EMA_FAST_LEN, EMA_SLOW_LEN = 20, 50
ST_ATR_LEN, ST_FACTOR = 10, 3
RSI_LONG_T, RSI_SHORT_T = 52, 48
PRICE_CHANGE_STEP_PCT = 0.1

# === INDICATORS ===
def _ta_rsi(series: pd.Series, length=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=length).mean()
    loss = -delta.clip(upper=0).rolling(window=length).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_indicators(df):
    df['ema_fast'] = df['close'].ewm(span=EMA_FAST_LEN).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW_LEN).mean()
    df['rsi'] = _ta_rsi(df['close'], RSI_LEN)
    st = ta.supertrend(df['high'], df['low'], df['close'], length=ST_ATR_LEN, multiplier=ST_FACTOR)
    df = pd.concat([df, st], axis=1)
    df['st_dir'] = df['SUPERTd_10_3.0'].map({True: 1, False: -1})
    return df.dropna()

# === MAIN LOOP ===
async def monitor_loop(app):
    while state['monitoring']:
        try:
            current_price = (await exchange.fetch_ticker(PAIR))['last']
            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = calculate_indicators(df)
            last, prev = df.iloc[-1], df.iloc[-2]

            flags = state.get("condition_flags", {"rsi": False, "ema": False, "st": False})

            if last['rsi'] > RSI_LONG_T:
                if not flags['rsi']:
                    await broadcast_message(app, "🟡 RSI условие выполнено. Жду EMA и Supertrend...")
                    flags['rsi'] = True
            else:
                flags['rsi'] = False

            if last['close'] > last['ema_fast'] and last['close'] > last['ema_slow']:
                if not flags['ema']:
                    await broadcast_message(app, "🟡 EMA условие выполнено. Жду RSI и Supertrend...")
                    flags['ema'] = True
            else:
                flags['ema'] = False

            if last['st_dir'] == 1 and prev['st_dir'] == -1:
                if not flags['st']:
                    await broadcast_message(app, "🟡 Supertrend условие выполнено. Жду RSI и EMA...")
                    flags['st'] = True
            else:
                flags['st'] = False

            if all(flags.values()) and not state.get('active_signal'):
                state['active_signal'] = {"side": "LONG", "price": current_price, "next_target_pct": PRICE_CHANGE_STEP_PCT}
                await broadcast_message(app, f"✅ Все условия выполнены. Сигнал LONG @ {current_price:.2f}")
                flags = {"rsi": False, "ema": False, "st": False}

            state['condition_flags'] = flags
            save_state()
        except Exception as e:
            log.error("Ошибка мониторинга: %s", e)
        await asyncio.sleep(30)

# === BROADCAST ===
async def broadcast_message(app, text):
    for cid in CHAT_IDS:
        await app.bot.send_message(chat_id=cid, text=text)

# === COMMANDS ===
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    CHAT_IDS.add(chat_id)
    if not state['monitoring']:
        state['monitoring'] = True
        save_state()
        await update.message.reply_text("✅ Мониторинг запущен.")
        asyncio.create_task(monitor_loop(ctx.application))
    else:
        await update.message.reply_text("Уже запущен.")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state['monitoring'] = False
    save_state()
    await update.message.reply_text("⛔️ Мониторинг остановлен.")

# === RUN ===
if __name__ == "__main__":
    load_state()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    log.info("Bot started...")
    app.run_polling()
