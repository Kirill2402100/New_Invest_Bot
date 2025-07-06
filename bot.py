#!/usr/bin/env python3
# ============================================================================
# v4.0 - Advanced Signal Monitor (SuperTrend via pandas-ta, TIMEFRAME via ENV)
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
import pandas_ta as ta  # ← Используем только pandas-ta

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
        LOGS_WS.resize(rows=1)
        LOGS_WS.update('A1', [HEADERS])
except Exception as e:
    log.error("Google Sheets init failed: %s", e)
    LOGS_WS = None

# === STATE ===
STATE_FILE = "advanced_signal_state.json"
state = {"monitoring": False, "active_signal": None, "manual_position": None}

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
    loss = (-delta.clip(upper=0)).rolling(window=length).mean()
    if loss.empty or loss.iloc[-1] == 0:
        return pd.Series(100, index=series.index)
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calc_atr(df: pd.DataFrame, length=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=length).mean()

def calculate_indicators(df: pd.DataFrame):
    df['ema_fast'] = df['close'].ewm(span=EMA_FAST_LEN, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW_LEN, adjust=False).mean()
    df['rsi'] = _ta_rsi(df['close'], RSI_LEN)
    df['atr'] = calc_atr(df, ATR_LEN)

    st = ta.supertrend(high=df['high'], low=df['low'], close=df['close'], length=ST_ATR_LEN, multiplier=ST_FACTOR)
    df = pd.concat([df, st], axis=1)
    df['st_dir'] = df['SUPERTd_10_3.0'].map({True: 1, False: -1})

    return df.dropna()

# === MAIN LOOP ===
async def monitor_loop(app):
    while state['monitoring']:
        try:
            current_price = (await exchange.fetch_ticker(PAIR))['last']
            
            if state.get('active_signal'):
                signal_data = state['active_signal']
                signal_price = signal_data['price']
                next_target = signal_data.get('next_target_pct', PRICE_CHANGE_STEP_PCT)

                ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df = calculate_indicators(df)
                last, prev = df.iloc[-1], df.iloc[-2]

                reversal = None
                if signal_data['side'] == 'LONG' and last['st_dir'] == -1 and prev['st_dir'] == 1:
                    reversal = 'SHORT'
                elif signal_data['side'] == 'SHORT' and last['st_dir'] == 1 and prev['st_dir'] == -1:
                    reversal = 'LONG'

                if reversal:
                    await broadcast_message(app, f"🔄 СМЕНА ТРЕНДА! {signal_data['side']} отменен. Новый сигнал: {reversal} @ {current_price:.2f}")
                    state['active_signal'] = {"side": reversal, "price": current_price, "next_target_pct": PRICE_CHANGE_STEP_PCT}
                    save_state()
                    await asyncio.sleep(30)
                    continue

                price_change_pct = ((current_price - signal_price) / signal_price) * 100
                if signal_data['side'] == 'LONG' and price_change_pct >= next_target:
                    await broadcast_message(app, f"🎯 ЦЕЛЬ +{next_target:.1f}% ДОСТИГНУТА (LONG от {signal_price:.2f} → {current_price:.2f})")
                    state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                    save_state()
                elif signal_data['side'] == 'SHORT' and price_change_pct <= -next_target:
                    await broadcast_message(app, f"🎯 ЦЕЛЬ -{next_target:.1f}% ДОСТИГНУТА (SHORT от {signal_price:.2f} → {current_price:.2f})")
                    state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                    save_state()

            else:
                ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df = calculate_indicators(df)

                if len(df) < 2:
                    continue

                last, prev = df.iloc[-1], df.iloc[-2]
                price = last['close']
                is_long = last['st_dir'] == 1 and prev['st_dir'] == -1
                is_short = last['st_dir'] == -1 and prev['st_dir'] == 1

                side = None
                if is_long and price > last['ema_fast'] and price > last['ema_slow'] and last['rsi'] > RSI_LONG_T:
                    side = 'LONG'
                elif is_short and price < last['ema_fast'] and price < last['ema_slow'] and last['rsi'] < RSI_SHORT_T:
                    side = 'SHORT'

                if side:
                    await broadcast_message(app, f"🔔 НОВЫЙ СИГНАЛ: {side} @ {price:.2f}")
                    state['active_signal'] = {"side": side, "price": price, "next_target_pct": PRICE_CHANGE_STEP_PCT}
                    save_state()

        except Exception as e:
            log.error("Ошибка мониторинга: %s", e)

        await asyncio.sleep(30)

# === COMMANDS ===
async def broadcast_message(app, text):
    for chat_id in app.chat_ids:
        await app.bot.send_message(chat_id=chat_id, text=text)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global state
    chat_id = update.effective_chat.id
    if not hasattr(ctx.application, 'chat_ids'):
        ctx.application.chat_ids = set()
    ctx.application.chat_ids.add(chat_id)

    if not state['monitoring']:
        state['monitoring'] = True
        save_state()
        await update.message.reply_text("✅ Мониторинг запущен.")
        asyncio.create_task(monitor_loop(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Уже запущен.")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global state
    state['monitoring'] = False
    save_state()
    await update.message.reply_text("🛑 Мониторинг остановлен.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    signal = state.get("active_signal")
    pos = state.get("manual_position")
    text = "📊 **Статус:**\n\n"
    if signal:
        text += f"- Сигнал: {signal['side']}\n- Цена: {signal['price']:.2f}\n- След. цель: {signal.get('next_target_pct', 0):.1f}%\n\n"
    else:
        text += "- Нет активного сигнала.\n\n"
    if pos:
        text += f"- Позиция вручную: {pos['direction']} @ {pos['entry_price']}"
    else:
        text += "- Позиция вручную не задана."
    await update.message.reply_text(text, parse_mode="Markdown")

# === MAIN ===
if __name__ == "__main__":
    load_state()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))
    log.info("Bot started...")
    app.run_polling()
