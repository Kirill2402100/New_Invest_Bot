#!/usr/bin/env python3
# ============================================================================
# v4.0 - Advanced Signal Monitor (SuperTrend via `ta`, TIMEFRAME from ENV)
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
from ta.trend import SuperTrend  # <-- NEW

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
state = {"monitoring": False, "active_signal": None, "manual_position": None}

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            log.info("State loaded. Active Signal: %s, Manual Position: %s", state.get("active_signal"), state.get("manual_position"))

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
    gain = delta.clip(lower=0).rolling(window=length, min_periods=length).mean()
    loss = (-delta.clip(upper=0)).rolling(window=length, min_periods=length).mean()
    if loss.empty or loss.iloc[-1] == 0: return pd.Series(100, index=series.index)
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calc_atr(df: pd.DataFrame, length=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=length, min_periods=length).mean()

import pandas_ta as ta

def calculate_indicators(df: pd.DataFrame):
    df['ema_fast'] = df['close'].ewm(span=EMA_FAST_LEN, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW_LEN, adjust=False).mean()
    df['rsi'] = _ta_rsi(df['close'], RSI_LEN)
    df['atr'] = calc_atr(df, ATR_LEN)

    # Supertrend через pandas-ta
    st = ta.supertrend(high=df['high'], low=df['low'], close=df['close'], length=ST_ATR_LEN, multiplier=ST_FACTOR)
    df = pd.concat([df, st], axis=1)
    df['st_dir'] = df['SUPERTd_10_3.0'].map({True: 1, False: -1})

    return df.dropna()
    
# === MAIN MONITORING LOOP ===
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
                df = calculate_indicators(df.copy())
                last_candle = df.iloc[-1]
                prev_candle = df.iloc[-2]

                reversal_signal = None
                if signal_data['side'] == 'LONG' and last_candle['st_dir'] == -1 and prev_candle['st_dir'] == 1:
                    reversal_signal = 'SHORT'
                elif signal_data['side'] == 'SHORT' and last_candle['st_dir'] == 1 and prev_candle['st_dir'] == -1:
                    reversal_signal = 'LONG'

                if reversal_signal:
                    await broadcast_message(app, f"🔄 СМЕНА ТРЕНДА! Предыдущий сигнал {signal_data['side']} отменен. Новый сигнал: {reversal_signal} @ {current_price:.2f}")
                    state['active_signal'] = {"side": reversal_signal, "price": current_price, "next_target_pct": PRICE_CHANGE_STEP_PCT}
                    save_state()
                    await asyncio.sleep(30)
                    continue

                price_change_pct = ((current_price - signal_price) / signal_price) * 100
                if signal_data['side'] == 'LONG' and price_change_pct >= next_target:
                    await broadcast_message(app, f"🎯 ЦЕЛЬ +{next_target:.1f}% ДОСТИГНУТА. Сигнал LONG от {signal_price:.2f}. Текущая цена: {current_price:.2f}")
                    state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                    save_state()
                elif signal_data['side'] == 'SHORT' and price_change_pct <= -next_target:
                    await broadcast_message(app, f"🎯 ЦЕЛЬ -{next_target:.1f}% ДОСТИГНУТА. Сигнал SHORT от {signal_price:.2f}. Текущая цена: {current_price:.2f}")
                    state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                    save_state()

            elif not state.get('active_signal'):
                ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df = calculate_indicators(df.copy())
                
                if len(df) < 2: continue

                last_candle = df.iloc[-1]
                prev_candle = df.iloc[-2]
                price = last_candle['close']
                
                is_long_signal = last_candle['st_dir'] == 1 and prev_candle['st_dir'] == -1
                is_short_signal = last_candle['st_dir'] == -1 and prev_candle['st_dir'] == 1
                
                side = None
                if is_long_signal and last_candle['close'] > last_candle['ema_fast'] and last_candle['close'] > last_candle['ema_slow'] and last_candle['rsi'] > RSI_LONG_T:
                    side = 'LONG'
                elif is_short_signal and last_candle['close'] < last_candle['ema_fast'] and last_candle['close'] < last_candle['ema_slow'] and last_candle['rsi'] < RSI_SHORT_T:
                    side = 'SHORT'
                
                if side:
                    await broadcast_message(app, f"🔔 НОВЫЙ СИГНАЛ: {side} @ {price:.2f}\nНачинаю слежение за ценой...")
                    state['active_signal'] = {"side": side, "price": price, "next_target_pct": PRICE_CHANGE_STEP_PCT}
                    save_state()

        except Exception as e:
            log.error("Error in monitor loop: %s", e)
        
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
        await update.message.reply_text("✅ Мониторинг запущен (v4.0 Adv. Monitor).")
        asyncio.create_task(monitor_loop(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Мониторинг уже запущен.")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global state
    state['monitoring'] = False
    save_state()
    await update.message.reply_text("❌ Мониторинг остановлен.")

async def cmd_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global state
    try:
        deposit, entry, sl, tp = map(float, ctx.args[:4])
        rr = abs((tp - entry) / (sl - entry))
        state["manual_position"] = {
            "entry_time": datetime.now(timezone.utc).isoformat(), "entry_deposit": deposit,
            "entry_price": entry, "sl": sl, "tp": tp, "rr": rr,
            "direction": state.get("active_signal", {}).get("side", "N/A")
        }
        save_state()
        await update.message.reply_text(f"✅ Вход вручную зафиксирован: {state['manual_position']['direction']} @ {entry}\nSL: {sl} | TP: {tp}")
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Использование: /entry <депозит> <цена_входа> <стоп_лосс> <тейк_профит>")

async def cmd_exit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global state
    pos = state.get("manual_position")
    if not pos:
        await update.message.reply_text("⚠️ Нет открытой ручной позиции для закрытия."); return
    try:
        exit_price = float(ctx.args[0])
        pnl = (exit_price - pos['entry_price']) * (pos['entry_deposit'] / pos['entry_price'])
        if pos['direction'] == 'SHORT': pnl = -pnl
        pct_change = (pnl / pos['entry_deposit']) * 100
        
        if LOGS_WS:
            row = [
                datetime.fromisoformat(pos['entry_time']).strftime('%Y-%m-%d %H:%M:%S'), PAIR, pos['direction'],
                pos['entry_deposit'], pos['entry_price'], pos['sl'], pos['tp'], 
                round(pos['rr'], 2), round(pnl, 2), round(pct_change, 2)
            ]
            await asyncio.to_thread(LOGS_WS.append_row, row, value_input_option='USER_ENTERED')
        
        await update.message.reply_text(f"✅ Сделка закрыта и записана.\nP&L: {pnl:.2f} USDT ({pct_change:.2f}%)")
        state["manual_position"] = None
        save_state()
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Использование: /exit <цена_выхода>")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    signal = state.get('active_signal')
    pos = state.get('manual_position')
    text = "📊 **Текущий статус**\n\n"
    if signal:
        text += f"**Мониторинг сигнала:**\n- Направление: {signal['side']}\n- Цена сигнала: {signal['price']:.2f}\n- Следующая цель: {signal.get('next_target_pct', 0):.1f}%\n\n"
    else:
        text += "**Мониторинг сигнала:**\n- Нет активного сигнала, идет поиск.\n\n"
    if pos:
        text += f"**Ручная позиция:**\n- Направление: {pos['direction']}\n- Вход: {pos['entry_price']}"
    else:
        text += "**Ручная позиция:**\n- Не открыта."
    await update.message.reply_text(text, parse_mode='Markdown')

# === START ===
if __name__ == "__main__":
    load_state()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("entry", cmd_entry))
    app.add_handler(CommandHandler("exit", cmd_exit))
    app.add_handler(CommandHandler("status", cmd_status))

    log.info("Bot started...")
    app.run_polling()
