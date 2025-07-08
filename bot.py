#!/usr/bin/env python3
# ============================================================================
# v10.0 - Детальное логирование сделок в Google Sheets для анализа
# ============================================================================

import os
import asyncio
import json
import logging
import re
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

# === ENV / Logging ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW = os.getenv("CHAT_IDS", "")
SHEET_ID = os.getenv("SHEET_ID")
PAIR_RAW = os.getenv("PAIR", "BTC/USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "1h")

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
LOGS_WS = None
# Новые заголовки для детального логирования
GSHEET_HEADERS = [
    "Signal_ID", "Timestamp_UTC", "Event_Type", "Pair", "Side",
    "Entry_Price", "Event_Price", "RSI", "EMA_Fast", "EMA_Slow",
    "ATR_14", "Volume", "Comment"
]
try:
    creds_json_string = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json_string and SHEET_ID:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = json.loads(creds_json_string)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gs = gspread.authorize(creds)
        worksheet_name = f"TradeLog_{PAIR_RAW.replace('/', '_')}_{TIMEFRAME}"
        try:
            LOGS_WS = gs.open_by_key(SHEET_ID).worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            LOGS_WS = gs.open_by_key(SHEET_ID).add_worksheet(title=worksheet_name, rows="1", cols=len(GSHEET_HEADERS))
        
        # Проверяем и устанавливаем заголовки
        if LOGS_WS.row_values(1) != GSHEET_HEADERS:
            LOGS_WS.update('A1', [GSHEET_HEADERS])
            LOGS_WS.format('A1:M1', {'textFormat': {'bold': True}})

        log.info(f"Успешное подключение к Google Sheets. Логи будут писаться в лист '{worksheet_name}'.")
    else:
        log.warning("Переменные для Google Sheets не найдены. Логирование в таблицу отключено.")
except Exception as e:
    log.error(f"Ошибка инициализации Google Sheets: {e}", exc_info=True)
    LOGS_WS = None


# === STATE MANAGEMENT ===
STATE_FILE = "advanced_signal_state_v6.json"
state = {"monitoring": False, "active_signal": None, "preliminary_signal": None}

def save_state():
    # ... (без изменений)
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log.error(f"Не удалось сохранить состояние: {e}")

def load_state():
    # ... (без изменений)
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                loaded_state = json.load(f)
                state.update(loaded_state)
            log.info(f"Состояние успешно загружено: {state}")
        except Exception as e:
            log.error(f"Не удалось загрузить состояние: {e}")

# === EXCHANGE & STRATEGY PARAMS ===
exchange = ccxt.mexc()
PAIR = PAIR_RAW.upper()

RSI_LEN = 14
EMA_FAST_LEN, EMA_SLOW_LEN = 9, 21
ATR_LEN = 14
RSI_LONG_ENTRY, RSI_SHORT_ENTRY = 52, 48
PRICE_CHANGE_STEP_PCT = 0.1
ANTI_TARGET_STEP_PCT = 0.05

# === INDICATORS ===
def _ta_rsi(series: pd.Series, length=14):
    # ... (без изменений)
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=length, min_periods=length).mean()
    loss = (-delta.clip(upper=0)).rolling(window=length, min_periods=length).mean()
    if loss.empty or (loss_val := loss.iloc[-1]) == 0: return pd.Series(100, index=series.index)
    rs = gain.iloc[-1] / loss_val
    return 100 - (100 / (1 + rs))

def _ta_atr(high: pd.Series, low: pd.Series, close: pd.Series, length=14):
    tr1 = pd.DataFrame(high - low)
    tr2 = pd.DataFrame(abs(high - close.shift(1)))
    tr3 = pd.DataFrame(abs(low - close.shift(1)))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/length, adjust=False).mean()
    return atr

def calculate_indicators(df: pd.DataFrame):
    df['ema_fast'] = df['close'].ewm(span=EMA_FAST_LEN, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW_LEN, adjust=False).mean()
    df['rsi'] = df['close'].rolling(window=RSI_LEN + 1).apply(lambda x: _ta_rsi(pd.Series(x)), raw=False)
    # Добавляем расчет ATR
    df['atr'] = _ta_atr(df['high'], df['low'], df['close'], length=ATR_LEN)
    return df.dropna()

# === CORE FUNCTIONS ===
async def log_trade_event(event_type: str, comment: str, signal_data: dict, current_data: dict):
    if not LOGS_WS:
        return
    
    try:
        row_data = [
            signal_data.get('id'),
            datetime.now(timezone.utc).isoformat(),
            event_type,
            PAIR,
            signal_data.get('side'),
            signal_data.get('price'),
            current_data.get('price'),
            round(current_data.get('rsi', 0), 2),
            round(current_data.get('ema_fast', 0), 4),
            round(current_data.get('ema_slow', 0), 4),
            round(current_data.get('atr', 0), 5),
            current_data.get('volume'),
            comment
        ]
        LOGS_WS.append_row(row_data, value_input_option='USER_ENTERED')
        log.info(f"Событие '{event_type}' для сигнала {signal_data.get('id')} записано в Google Sheet.")
    except Exception as e:
        log.error(f"Не удалось записать событие в Google Sheet: {e}", exc_info=True)


async def broadcast_message(bot: Bot, text: str):
    # ... (без изменений)
    if not CHAT_IDS:
        log.warning("Список CHAT_IDS пуст. Сообщение не отправлено.")
        return
    log.info(f"ОТПРАВКА СООБЩЕНИЯ -> {text}")
    for chat_id in CHAT_IDS:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            log.error(f"Не удалось отправить сообщение в чат {chat_id}: {e}")

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

            last = df.iloc[-1]
            prev = df.iloc[-2]
            
            current_market_data = {
                "price": last['close'], "rsi": last['rsi'], "ema_fast": last['ema_fast'],
                "ema_slow": last['ema_slow'], "atr": last['atr'], "volume": last['volume']
            }

            log.info(f"[ОТЛАДКА] Цена: {current_market_data['price']:.4f}, RSI: {current_market_data['rsi']:.2f}, ATR: {current_market_data['atr']:.5f}")

            is_bull_cross = last['ema_fast'] > last['ema_slow']
            was_bull_cross = prev['ema_fast'] > prev['ema_slow']

            long_conditions = {"rsi": last['rsi'] > RSI_LONG_ENTRY, "price_pos": last['close'] > last['ema_fast'] and last['close'] > last['ema_slow']}
            short_conditions = {"rsi": last['rsi'] < RSI_SHORT_ENTRY, "price_pos": last['close'] < last['ema_fast'] and last['close'] < last['ema_slow']}

            if active_signal := state.get('active_signal'):
                side = active_signal['side']
                
                cancel = False
                if side == "LONG" and (last['rsi'] < RSI_SHORT_ENTRY or last['close'] < last['ema_slow']):
                    cancel = True
                elif side == "SHORT" and (last['rsi'] > RSI_LONG_ENTRY or last['close'] > last['ema_slow']):
                    cancel = True
                
                if cancel:
                    msg = f"⚠️ ОТМЕНА сигнала {side} по {PAIR}."
                    await broadcast_message(app.bot, msg)
                    await log_trade_event("CANCELLATION", msg, active_signal, current_market_data)
                    state['active_signal'] = None
                    save_state()
                    continue

                price_change_pct = ((current_market_data['price'] - active_signal['price']) / active_signal['price']) * 100
                
                if side == "LONG":
                    if price_change_pct >= active_signal['next_target_pct']:
                        target_pct = active_signal['next_target_pct']
                        msg = f"🎯 ЦЕЛЬ +{target_pct:.2f}% по {PAIR} ({side}) ДОСТИГНУТА. Цена: {current_market_data['price']:.4f}"
                        await broadcast_message(app.bot, msg)
                        await log_trade_event("TARGET", msg, active_signal, current_market_data)
                        state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                        save_state()
                    elif price_change_pct <= active_signal['next_anti_target_pct']:
                        anti_target_pct = active_signal['next_anti_target_pct']
                        msg = f"📉 АНТИ-ЦЕЛЬ {anti_target_pct:.2f}% по {PAIR} ({side}). ВНИМАНИЕ! Цена: {current_market_data['price']:.4f}"
                        await broadcast_message(app.bot, msg)
                        await log_trade_event("ANTI_TARGET", msg, active_signal, current_market_data)
                        state['active_signal']['next_anti_target_pct'] -= ANTI_TARGET_STEP_PCT
                        save_state()
                
                elif side == "SHORT":
                    if price_change_pct <= -active_signal['next_target_pct']:
                        target_pct = active_signal['next_target_pct']
                        msg = f"🎯 ЦЕЛЬ +{target_pct:.2f}% по {PAIR} ({side}) ДОСТИГНУТА. Цена: {current_market_data['price']:.4f}"
                        await broadcast_message(app.bot, msg)
                        await log_trade_event("TARGET", msg, active_signal, current_market_data)
                        state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                        save_state()
                    elif price_change_pct >= -active_signal['next_anti_target_pct']:
                        anti_target_pct = active_signal['next_anti_target_pct']
                        msg = f"📈 АНТИ-ЦЕЛЬ {anti_target_pct:.2f}% по {PAIR} ({side}). ВНИМАНИЕ! Цена: {current_market_data['price']:.4f}"
                        await broadcast_message(app.bot, msg)
                        await log_trade_event("ANTI_TARGET", msg, active_signal, current_market_data)
                        state['active_signal']['next_anti_target_pct'] -= ANTI_TARGET_STEP_PCT
                        save_state()
            
            elif preliminary_signal := state.get('preliminary_signal'):
                side = preliminary_signal['side']
                
                def confirm_signal(side_to_confirm):
                    signal_id = f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{side_to_confirm}"
                    state['active_signal'] = {
                        "id": signal_id, "side": side_to_confirm, "price": current_market_data['price'],
                        "next_target_pct": PRICE_CHANGE_STEP_PCT, "next_anti_target_pct": -ANTI_TARGET_STEP_PCT
                    }
                    state['preliminary_signal'] = None
                    msg = f"✅ ПОДТВЕРЖДЕНИЕ сигнала {side_to_confirm} по {PAIR}! Цена: {current_market_data['price']:.4f}"
                    asyncio.create_task(broadcast_message(app.bot, msg))
                    asyncio.create_task(log_trade_event("CONFIRMATION", msg, state['active_signal'], current_market_data))
                    save_state()

                if side == "LONG" and long_conditions["rsi"] and long_conditions["price_pos"]:
                    confirm_signal("LONG")
                elif side == "SHORT" and short_conditions["rsi"] and short_conditions["price_pos"]:
                    confirm_signal("SHORT")
                elif (side == "LONG" and not is_bull_cross) or (side == "SHORT" and is_bull_cross):
                    await broadcast_message(app.bot, f"🚫 Предварительный сигнал {side} по {PAIR} отменён.")
                    state['preliminary_signal'] = None
                    save_state()

            else:
                if is_bull_cross and not was_bull_cross:
                    state['preliminary_signal'] = {"side": "LONG"}
                    await broadcast_message(app.bot, f"⏳ Предварительный сигнал LONG по {PAIR}. Ждём подтверждения.")
                    save_state()
                elif not is_bull_cross and was_bull_cross:
                    state['preliminary_signal'] = {"side": "SHORT"}
                    await broadcast_message(app.bot, f"⏳ Предварительный сигнал SHORT по {PAIR}. Ждём подтверждения.")
                    save_state()

        except ccxt.NetworkError as e:
            log.warning(f"Ошибка сети CCXT: {e}. Повторная попытка через 60 секунд.")
            await asyncio.sleep(60)
        except Exception as e:
            log.error(f"Критическая ошибка в цикле мониторинга: {e}", exc_info=True)
            await asyncio.sleep(30)
        
        await asyncio.sleep(30)
    log.info("Цикл мониторинга остановлен.")

# === COMMANDS & LIFECYCLE ===
# ... (код без изменений)
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in CHAT_IDS:
        CHAT_IDS.add(chat_id)
        log.info(f"Временный CHAT_ID добавлен: {chat_id}.")
    if not state.get('monitoring'):
        state['monitoring'] = True
        save_state()
        await update.message.reply_text(f"✅ Мониторинг по паре {PAIR} ({TIMEFRAME}) запущен.")
        asyncio.create_task(monitor_loop(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Мониторинг уже активен.")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if state.get('monitoring'):
        state['monitoring'] = False
        save_state()
        await update.message.reply_text("🛑 Мониторинг остановлен.")
    else:
        await update.message.reply_text("ℹ️ Мониторинг не был запущен.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not state['monitoring']:
        await update.message.reply_text(f"Мониторинг по {PAIR} неактивен.")
        return
    
    msg = f"Статус по {PAIR} ({TIMEFRAME}):\n"
    if active_signal := state.get('active_signal'):
        side = active_signal['side']
        price = active_signal['price']
        next_target = active_signal['next_target_pct']
        next_anti = active_signal['next_anti_target_pct']
        msg += f"\n- Активный сигнал: {side} ({active_signal.get('id')})\n- Цена входа: {price:.4f}\n- Следующая цель: +{next_target:.2f}%\n- Следующая анти-цель: {next_anti:.2f}%"
    elif preliminary_signal := state.get('preliminary_signal'):
        side = preliminary_signal['side']
        msg += f"\n- Предварительный сигнал: {side}\n- Ожидается подтверждение от RSI и положения цены."
    else:
        msg += "\n- Активных сигналов нет. Идёт поиск пересечения EMA."
        
    await update.message.reply_text(msg)

async def post_init(app: Application):
    load_state()
    if state.get('monitoring'):
        log.info("Обнаружен активный статус мониторинга. Возобновление работы...")
        asyncio.create_task(monitor_loop(app))

async def on_shutdown(app: Application):
    log.info("Бот завершает работу. Сохранение состояния...")
    save_state()
    await exchange.close()
    log.info("Сессия с биржей закрыта.")

def main():
    log.info("Запуск бота...")
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).post_shutdown(on_shutdown).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.run_polling()

if __name__ == "__main__":
    main()
