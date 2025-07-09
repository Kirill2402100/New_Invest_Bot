#!/usr/bin/env python3
# ============================================================================
# v11.1 - Sniper Strategy v2.0 (Autonomous)
# • CRITICAL FIX: Corrected an error in the ADX indicator calculation
#   that was causing the main loop to crash.
# • The `calculate_indicators` function now correctly handles the multiple
#   output columns from the ADX indicator.
# ============================================================================

import os
import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
import pandas_ta as ta

# === ENV / Logging ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW = os.getenv("CHAT_IDS", "")
SHEET_ID = os.getenv("SHEET_ID")
PAIR_RAW = os.getenv("PAIR", "BTC/USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "5m")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("httpcore").setLevel(logging.WARNING)

if not BOT_TOKEN:
    log.critical("Переменная окружения BOT_TOKEN не найдена!")
    exit()

if not re.match(r'^\d+[mhdM]$', TIMEFRAME):
    log.critical(f"Неверный формат таймфрейма: '{TIMEFRAME}'. Пример: 1h, 15m, 1d.")
    exit()

CHAT_IDS = {int(cid.strip()) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

# === GOOGLE SHEETS ===
TRADE_LOG_WS = None
def setup_google_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gs = gspread.authorize(creds)
        spreadsheet = gs.open_by_key(SHEET_ID)
        
        headers = [
            "Signal_ID", "Status", "Side", "Entry_Time_UTC", "Exit_Time_UTC",
            "Entry_Price", "Exit_Price", "SL_Price", "TP_Price",
            "MFE_Price", "MAE_Price",
            "Entry_RSI", "Entry_ADX", "Entry_ATR", "Entry_Volume", "Entry_BB_Position"
        ]
        
        sheet_name = f"SniperLog_{PAIR_RAW.replace('/', '_')}_{TIMEFRAME}"
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols=len(headers))
        
        if worksheet.row_values(1) != headers:
            worksheet.clear()
            worksheet.update('A1', [headers])
            worksheet.format(f'A1:{chr(ord("A")+len(headers)-1)}1', {'textFormat': {'bold': True}})
        
        log.info(f"Google Sheets setup complete. Logging to '{sheet_name}'.")
        return worksheet
    except Exception as e:
        log.error("Google Sheets init failed: %s", e)
        return None
TRADE_LOG_WS = setup_google_sheets()

# === STATE MANAGEMENT ===
STATE_FILE = "btc_sniper_state.json"
state = {"monitoring": False, "active_trade": None, "preliminary_signal": None}

def save_state():
    with open(STATE_FILE, 'w') as f: json.dump(state, f, indent=2)
def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f: state = json.load(f)
    if 'monitoring' not in state:
        state.update({"monitoring": False, "active_trade": None, "preliminary_signal": None})
    log.info(f"State loaded: {state}")

# === EXCHANGE & STRATEGY PARAMS ===
exchange = ccxt.mexc()
PAIR = PAIR_RAW.upper()

RSI_LEN = 14
EMA_FAST_LEN, EMA_SLOW_LEN = 9, 21
ATR_LEN = 14
ADX_LEN = 14
BBANDS_LEN = 20

RSI_LONG_ENTRY, RSI_SHORT_ENTRY = 52, 48
PROFIT_TARGET_PCT = 0.1 # Наша цель в 0.1%
STOP_LOSS_PCT = 0.1   # Наш стоп в 0.1%

# === INDICATORS ===
def calculate_indicators(df: pd.DataFrame):
    df.ta.ema(length=EMA_FAST_LEN, append=True, col_names=(f"EMA_{EMA_FAST_LEN}",))
    df.ta.ema(length=EMA_SLOW_LEN, append=True, col_names=(f"EMA_{EMA_SLOW_LEN}",))
    df.ta.rsi(length=RSI_LEN, append=True, col_names=(f"RSI_{RSI_LEN}",))
    df.ta.atr(length=ATR_LEN, append=True, col_names=(f"ATR_{ATR_LEN}",))
    # --- ИСПРАВЛЕНИЕ ОШИБКИ ADX ---
    # Индикатор ADX возвращает 3 столбца, поэтому мы должны указать 3 имени
    df.ta.adx(length=ADX_LEN, append=True, col_names=(f"ADX_{ADX_LEN}", f"DMP_{ADX_LEN}", f"DMN_{ADX_LEN}"))
    df.ta.bbands(length=BBANDS_LEN, std=2, append=True, col_names=(f"BBL_{BBANDS_LEN}_2.0", f"BBM_{BBANDS_LEN}_2.0", f"BBU_{BBANDS_LEN}_2.0", f"BBB_{BBANDS_LEN}_2.0", f"BBP_{BBANDS_LEN}_2.0"))
    return df.dropna()

# === CORE FUNCTIONS ===
async def log_trade_to_gs(trade_data: dict):
    if not TRADE_LOG_WS: return
    try:
        row = [
            trade_data.get('id'), trade_data.get('status'), trade_data.get('side'),
            trade_data.get('entry_time_utc'), datetime.now(timezone.utc).isoformat(),
            trade_data.get('entry_price'), trade_data.get('exit_price'),
            trade_data.get('sl_price'), trade_data.get('tp_price'),
            trade_data.get('mfe_price'), trade_data.get('mae_price'),
            trade_data.get('entry_rsi'), trade_data.get('entry_adx'),
            trade_data.get('entry_atr'), trade_data.get('entry_volume'),
            trade_data.get('entry_bb_pos')
        ]
        await asyncio.to_thread(TRADE_LOG_WS.append_row, row, value_input_option='USER_ENTERED')
        log.info(f"Trade {trade_data.get('id')} logged to Google Sheets.")
    except Exception as e:
        log.error(f"Failed to write trade to Google Sheets: {e}", exc_info=True)

async def broadcast_message(bot: Bot, text: str):
    for chat_id in CHAT_IDS:
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        except Exception as e:
            log.error(f"Failed to send message to {chat_id}: {e}")

async def monitor_loop(app: Application):
    log.info(f"Цикл мониторинга запущен для {PAIR} на таймфрейме {TIMEFRAME}.")
    while state.get('monitoring', False):
        try:
            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = calculate_indicators(df.copy())
            
            if len(df) < 2:
                await asyncio.sleep(30)
                continue

            last, prev = df.iloc[-1], df.iloc[-2]
            price = last['close']

            is_bull_cross = last[f'EMA_{EMA_FAST_LEN}'] > last[f'EMA_{EMA_SLOW_LEN}']
            was_bull_cross = prev[f'EMA_{EMA_FAST_LEN}'] > prev[f'EMA_{EMA_SLOW_LEN}']

            # --- 1. УПРАВЛЕНИЕ АКТИВНОЙ СДЕЛКОЙ ---
            if active_trade := state.get('active_trade'):
                side, sl, tp = active_trade['side'], active_trade['sl_price'], active_trade['tp_price']
                
                # Обновляем MFE/MAE
                if side == 'LONG':
                    if price > active_trade['mfe_price']: active_trade['mfe_price'] = price
                    if price < active_trade['mae_price']: active_trade['mae_price'] = price
                elif side == 'SHORT':
                    if price < active_trade['mfe_price']: active_trade['mfe_price'] = price
                    if price > active_trade['mae_price']: active_trade['mae_price'] = price

                outcome, status = None, None
                # Проверка TP/SL
                if side == 'LONG' and price >= tp: outcome, status = "TP_HIT", "WIN"
                elif side == 'LONG' and price <= sl: outcome, status = "SL_HIT", "LOSS"
                elif side == 'SHORT' and price <= tp: outcome, status = "TP_HIT", "WIN"
                elif side == 'SHORT' and price >= sl: outcome, status = "SL_HIT", "LOSS"
                # Проверка отмены по условиям
                elif side == "LONG" and (last[f'RSI_{RSI_LEN}'] < RSI_SHORT_ENTRY or price < last[f'EMA_{EMA_SLOW_LEN}']):
                    outcome, status = "CANCELED", "LOSS"
                elif side == "SHORT" and (last[f'RSI_{RSI_LEN}'] > RSI_LONG_ENTRY or price > last[f'EMA_{EMA_SLOW_LEN}']):
                    outcome, status = "CANCELED", "LOSS"

                if outcome:
                    active_trade['status'] = status
                    active_trade['exit_price'] = price
                    emoji = "✅" if status == "WIN" else "❌"
                    msg = f"{emoji} <b>СДЕЛКА ЗАКРЫТА ({outcome})</b> {emoji}\n\n<b>ID:</b> {active_trade['id']}\n<b>Цена выхода:</b> {price:.4f}"
                    await broadcast_message(app.bot, msg)
                    await log_trade_to_gs(active_trade)
                    state['active_trade'] = None
                    save_state()
            
            # --- 2. ПОИСК НОВОГО СИГНАЛА ---
            else:
                long_cond = last[f'RSI_{RSI_LEN}'] > RSI_LONG_ENTRY and price > last[f'EMA_{EMA_FAST_LEN}']
                short_cond = last[f'RSI_{RSI_LEN}'] < RSI_SHORT_ENTRY and price < last[f'EMA_{EMA_FAST_LEN}']
                
                side_to_confirm = None
                if is_bull_cross and not was_bull_cross and long_cond:
                    side_to_confirm = "LONG"
                elif not is_bull_cross and was_bull_cross and short_cond:
                    side_to_confirm = "SHORT"

                if side_to_confirm:
                    entry_price = price
                    tp_price = entry_price * (1 + PROFIT_TARGET_PCT / 100) if side_to_confirm == 'LONG' else entry_price * (1 - PROFIT_TARGET_PCT / 100)
                    sl_price = entry_price * (1 - STOP_LOSS_PCT / 100) if side_to_confirm == 'LONG' else entry_price * (1 + STOP_LOSS_PCT / 100)
                    
                    # Определяем положение цены относительно Полос Боллинджера
                    bb_upper, bb_lower = last.get(f'BBU_{BBANDS_LEN}_2.0'), last.get(f'BBL_{BBANDS_LEN}_2.0')
                    bb_pos = "Inside"
                    if entry_price > bb_upper: bb_pos = "Above_Upper"
                    elif entry_price < bb_lower: bb_pos = "Below_Lower"

                    state['active_trade'] = {
                        "id": str(uuid.uuid4())[:8],
                        "side": side_to_confirm,
                        "entry_time_utc": datetime.now(timezone.utc).isoformat(),
                        "entry_price": entry_price,
                        "tp_price": tp_price,
                        "sl_price": sl_price,
                        "mfe_price": entry_price,
                        "mae_price": entry_price,
                        "entry_rsi": round(last.get(f'RSI_{RSI_LEN}'), 2),
                        "entry_adx": round(last.get(f'ADX_{ADX_LEN}'), 2),
                        "entry_atr": round(last.get(f'ATR_{ATR_LEN}'), 5),
                        "entry_volume": last.get('volume'),
                        "entry_bb_pos": bb_pos
                    }
                    save_state()
                    msg = f"🔔 <b>НОВЫЙ СИГНАЛ ({side_to_confirm})</b> 🔔\n\n<b>ID:</b> {state['active_trade']['id']}\n<b>Цена входа:</b> {entry_price:.4f}\n<b>TP:</b> {tp_price:.4f}, <b>SL:</b> {sl_price:.4f}"
                    await broadcast_message(app.bot, msg)

        except ccxt.NetworkError as e:
            log.warning(f"Ошибка сети CCXT: {e}. Повторная попытка через 60 секунд.")
            await asyncio.sleep(60)
        except Exception as e:
            log.error(f"Критическая ошибка в цикле мониторинга: {e}", exc_info=True)
            await asyncio.sleep(30)
        
        await asyncio.sleep(60) # Проверяем раз в минуту
    log.info("Цикл мониторинга остановлен.")

# === COMMANDS & LIFECYCLE ===
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in CHAT_IDS: CHAT_IDS.add(chat_id)
    if not state.get('monitoring'):
        state['monitoring'] = True
        save_state()
        await update.message.reply_text("✅ BTC-бот (Снайпер v2.0) запущен. Начинаю мониторинг.")
        asyncio.create_task(monitor_loop(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Бот уже запущен.")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if state.get('monitoring'):
        state['monitoring'] = False
        save_state()
        await update.message.reply_text("❌ Бот остановлен.")
    else:
        await update.message.reply_text("ℹ️ Бот уже был остановлен.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = f"<b>Статус бота:</b> {'АКТИВЕН' if state.get('monitoring') else 'ОСТАНОВЛЕН'}\n\n"
    if active_trade := state.get('active_trade'):
        msg += "<b><u>Активная сделка:</u></b>\n"
        msg += (f"  - <b>{active_trade['side']}</b> (ID: {active_trade['id']})\n"
                f"    Вход: {active_trade['entry_price']:.4f}\n"
                f"    TP: {active_trade['tp_price']:.4f}, SL: {active_trade['sl_price']:.4f}\n")
    else:
        msg += "<i>Нет активных сделок. Идёт поиск сигнала.</i>"
    await update.message.reply_text(msg, parse_mode="HTML")

async def post_init(app: Application):
    load_state()
    if state.get('monitoring'):
        log.info("Обнаружен активный статус мониторинга. Возобновление работы...")
        asyncio.create_task(monitor_loop(app))

if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.run_polling()
