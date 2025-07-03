#!/usr/bin/env python3
# ============================================================================
# v2.1 - Supertrend Signals
# • Сигнал на вход теперь генерируется сменой направления Supertrend.
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
LEVERAGE = int(os.getenv("LEVERAGE", 10))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

# === GOOGLE SHEETS ===
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gs = gspread.authorize(creds)
    LOGS_WS = gs.open_by_key(SHEET_ID).worksheet("LP_Logs")
    HEADERS = ["Дата-время", "Инструмент", "Направление", "Депозит", "Вход", "Stop Loss", "Take Profit", "RR", "P&L сделки (USDT)", "APR сделки (%)"]
    if LOGS_WS.row_values(1) != HEADERS:
        LOGS_WS.resize(rows=1); LOGS_WS.update('A1', [HEADERS])
except Exception as e:
    log.error("Google Sheets init failed: %s", e)
    LOGS_WS = None

# === STATE MANAGEMENT ===
STATE_FILE = "bot_state.json"
state = {"monitoring": False, "position": None}

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            log.info("State loaded from file. Current position: %s", state.get("position"))

# === EXCHANGE ===
exchange = ccxt.mexc({"apiKey": os.getenv("MEXC_API_KEY"), "secret": os.getenv("MEXC_SECRET")})
PAIR = PAIR_RAW.upper()

# === STRATEGY PARAMS ===
RSI_LEN, ATR_LEN = 14, 14
EMA_FAST_LEN, EMA_SLOW_LEN = 20, 50
ST_ATR_LEN, ST_FACTOR = 10, 3  # Параметры для Supertrend
RSI_LONG_T, RSI_SHORT_T = 52, 48
TP_ATR_MUL, SL_ATR_MUL = 3.0, 1.5

# === INDICATORS ===
def _ta_rsi(series:pd.Series, length=14):
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

def calculate_indicators(df: pd.DataFrame):
    df['ema_fast'] = df['close'].ewm(span=EMA_FAST_LEN, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW_LEN, adjust=False).mean()
    df['rsi'] = _ta_rsi(df['close'], RSI_LEN)
    df['atr'] = calc_atr(df, ATR_LEN)
    
    # Расчет Supertrend
    st_atr = calc_atr(df, ST_ATR_LEN)
    hl2 = (df['high'] + df['low']) / 2
    upper_band = hl2 + (ST_FACTOR * st_atr)
    lower_band = hl2 - (ST_FACTOR * st_atr)
    
    st_direction = pd.Series(1, index=df.index)
    for i in range(1, len(df)):
        if df['close'].iloc[i-1] <= upper_band.iloc[i-1]:
             st_direction.iloc[i] = 1 if upper_band.iloc[i] < upper_band.iloc[i-1] else -1
        else:
             st_direction.iloc[i] = -1 if lower_band.iloc[i] > lower_band.iloc[i-1] else 1
    
    df['st_dir'] = st_direction
    return df.dropna()

# === TRADING ACTIONS ===
async def open_position(side: str, price: float, atr: float, app):
    global state
    try:
        balance_info = await exchange.fetch_balance()
        usdt_balance = balance_info['USDT']['free']
        if usdt_balance < 10:
            await broadcast_message(app, "⚠️ Недостаточно средств для открытия позиции (< 10 USDT).")
            return

        amount_in_usdt = usdt_balance * 0.95 
        amount = amount_in_usdt / price
        
        log.info(f"Attempting to open {side} position of {amount:.4f} {PAIR} for {amount_in_usdt:.2f} USDT")
        order = await exchange.create_market_order(PAIR, 'buy' if side == 'LONG' else 'sell', amount)

        entry_price = order['average'] if order.get('average') else price
        sl_price = entry_price - atr * SL_ATR_MUL if side == 'LONG' else entry_price + atr * SL_ATR_MUL
        tp_price = entry_price + atr * TP_ATR_MUL if side == 'LONG' else entry_price - atr * TP_ATR_MUL
        
        state['position'] = {
            "side": side, "entry_price": entry_price, "amount": order['filled'],
            "sl": sl_price, "tp": tp_price, "entry_time": datetime.now(timezone.utc).isoformat()
        }
        save_state()
        await broadcast_message(app, f"✅ Открыта {side} позиция @ {entry_price:.4f}\nSL: {sl_price:.4f} | TP: {tp_price:.4f}")

    except Exception as e:
        log.error("Failed to open position: %s", e)
        await broadcast_message(app, f"❌ Ошибка открытия позиции: {e}")

async def close_position(price: float, reason: str, app):
    global state
    pos = state['position']
    if not pos: return

    try:
        order = await exchange.create_market_order(PAIR, 'sell' if pos['side'] == 'LONG' else 'buy', pos['amount'])
        exit_price = order['average'] if order.get('average') else price
        
        pnl = (exit_price - pos['entry_price']) * pos['amount']
        if pos['side'] == 'SHORT': pnl = -pnl
        
        await broadcast_message(app, f"⛔️ Позиция {pos['side']} закрыта по {reason} @ {exit_price:.4f}\nP&L: {pnl:.2f} USDT")
        
        if LOGS_WS:
            entry_time = datetime.fromisoformat(pos['entry_time'])
            rr = abs((pos['tp'] - pos['entry_price']) / (pos['sl'] - pos['entry_price']))
            duration_days = (datetime.now(timezone.utc) - entry_time).total_seconds() / 86400
            apr = (pnl / (pos['entry_price'] * pos['amount'])) * (365 / duration_days) * 100 if duration_days > 0 else 0
            row = [
                entry_time.strftime('%Y-%m-%d %H:%M:%S'), PAIR, pos['side'], 
                pos['entry_price'] * pos['amount'], pos['entry_price'], pos['sl'], pos['tp'], 
                round(rr, 2), round(pnl, 2), round(apr, 1)
            ]
            await asyncio.to_thread(LOGS_WS.append_row, row, value_input_option='USER_ENTERED')

    except Exception as e:
        log.error("Failed to close position: %s", e)
        await broadcast_message(app, f"❌ Ошибка закрытия позиции: {e}")
    finally:
        state['position'] = None
        save_state()

# === MAIN MONITORING LOOP ===
async def monitor_loop(app):
    while state['monitoring']:
        try:
            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe='15m', limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = calculate_indicators(df.copy())
            
            if len(df) < 2: # Убедимся, что есть хотя бы 2 строки для сравнения
                await asyncio.sleep(30)
                continue

            last_candle = df.iloc[-1]
            prev_candle = df.iloc[-2]
            price = last_candle['close']
            
            if state.get('position'):
                pos = state['position']
                if pos['side'] == 'LONG' and (price <= pos['sl'] or price >= pos['tp']):
                    await close_position(price, "SL" if price <= pos['sl'] else "TP", app)
                elif pos['side'] == 'SHORT' and (price >= pos['sl'] or price <= pos['tp']):
                    await close_position(price, "SL" if price >= pos['sl'] else "TP", app)
            
            else: # Логика входа в новую позицию
                # --- НОВАЯ ЛОГИКА СИГНАЛА ---
                long_signal = last_candle['st_dir'] == 1 and prev_candle['st_dir'] == -1
                short_signal = last_candle['st_dir'] == -1 and prev_candle['st_dir'] == 1
                
                if long_signal:
                    # Проверяем фильтры
                    if last_candle['close'] > last_candle['ema_fast'] and last_candle['ema_fast'] > last_candle['ema_slow'] and last_candle['rsi'] > RSI_LONG_T:
                        await broadcast_message(app, f"🔍 Обнаружен сигнал LONG (Supertrend) @ {price:.4f}")
                        await open_position('LONG', price, last_candle['atr'], app)
                
                elif short_signal:
                    # Проверяем фильтры
                    if last_candle['close'] < last_candle['ema_fast'] and last_candle['ema_fast'] < last_candle['ema_slow'] and last_candle['rsi'] < RSI_SHORT_T:
                        await broadcast_message(app, f"🔍 Обнаружен сигнал SHORT (Supertrend) @ {price:.4f}")
                        await open_position('SHORT', price, last_candle['atr'], app)

        except Exception as e:
            log.error("Error in monitor loop: %s", e)
        
        await asyncio.sleep(30)

# === COMMANDS and RUN ===
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
        await update.message.reply_text("✅ Мониторинг запущен (v2.1 Supertrend).")
        asyncio.create_task(monitor_loop(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Мониторинг уже запущен.")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global state
    state['monitoring'] = False
    save_state()
    await update.message.reply_text("❌ Мониторинг остановлен.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    pos = state.get('position')
    if pos:
        await update.message.reply_text(
            f"🔍 Открыта позиция: {pos['side']} @ {pos['entry_price']:.4f}\n"
            f"Количество: {pos['amount']}\n"
            f"SL: {pos['sl']:.4f} | TP: {pos['tp']:.4f}"
        )
    else:
        await update.message.reply_text("❌ Позиция не открыта.")

if __name__ == "__main__":
    load_state() 
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))

    log.info("Bot started...")
    app.run_polling()
