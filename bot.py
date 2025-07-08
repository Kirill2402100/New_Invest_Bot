#!/usr/bin/env python3
# ============================================================================
# v6.0 - Двусторонний мониторинг (LONG/SHORT) с предварительными сигналами
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
from telegram import Update, Bot
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

# === ENV / Logging ===
# Убедитесь, что переменные окружения установлены
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW = os.getenv("CHAT_IDS", "")
SHEET_ID = os.getenv("SHEET_ID")
# Теперь торговая пара задается через переменную окружения PAIR
PAIR_RAW = os.getenv("PAIR", "BTC/USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "1h")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

if not BOT_TOKEN:
    log.critical("Переменная окружения BOT_TOKEN не найдена! Бот не может быть запущен.")
    exit()

CHAT_IDS = {int(cid.strip()) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}
if not CHAT_IDS:
    log.warning("Переменная CHAT_IDS не установлена. Уведомления будут приходить только тем, кто напишет /start.")

# === GOOGLE SHEETS (опционально) ===
LOGS_WS = None
try:
    creds_json_string = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json_string and SHEET_ID:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = json.loads(creds_json_string)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gs = gspread.authorize(creds)
        LOGS_WS = gs.open_by_key(SHEET_ID).worksheet("LP_Logs")
        HEADERS = ["Дата-время", "Инструмент", "Направление", "Депозит", "Вход", "Stop Loss", "Take Profit", "RR", "P&L сделки (USDT)", "Прибыль к депозиту (%)"]
        if LOGS_WS.row_values(1) != HEADERS:
            LOGS_WS.resize(rows=1); LOGS_WS.update('A1', [HEADERS])
        log.info("Успешное подключение к Google Sheets.")
    else:
        log.warning("Переменные для Google Sheets не найдены. Логирование в таблицу отключено.")
except Exception as e:
    log.error(f"Ошибка инициализации Google Sheets: {e}")
    LOGS_WS = None

# === STATE MANAGEMENT ===
STATE_FILE = "advanced_signal_state_v2.json"
state = {
    "monitoring": False,
    "active_signal": None,      # Для подтвержденного сигнала {side, price, next_target_pct, next_anti_target_pct}
    "preliminary_signal": None  # Для предварительного сигнала {side}
}

def save_state():
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log.error(f"Не удалось сохранить состояние: {e}")

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                loaded_state = json.load(f)
                # Убедимся, что все ключи есть
                state.update(loaded_state)
            log.info(f"Состояние успешно загружено: {state}")
        except Exception as e:
            log.error(f"Не удалось загрузить состояние: {e}")

# === EXCHANGE & STRATEGY PARAMS ===
exchange = ccxt.mexc()
PAIR = PAIR_RAW.upper()

# Параметры стратегии
RSI_LEN = 14
EMA_FAST_LEN, EMA_SLOW_LEN = 9, 21
RSI_LONG_ENTRY, RSI_SHORT_ENTRY = 52, 48
PRICE_CHANGE_STEP_PCT = 0.1
ANTI_TARGET_STEP_PCT = 0.05 # Шаг для анти-целей

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
    return df.dropna()

# === CORE FUNCTIONS ===
async def broadcast_message(bot: Bot, text: str):
    if not CHAT_IDS:
        log.warning("Список CHAT_IDS пуст. Сообщение не отправлено.")
        return
    log.info(f"Отправка сообщения в {len(CHAT_IDS)} чатов: \"{text}\"")
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
            price = last['close']

            # --- Определяем условия для LONG и SHORT ---
            long_conditions = {
                "rsi": last['rsi'] > RSI_LONG_ENTRY,
                "price_pos": price > last['ema_fast'] and price > last['ema_slow'],
                "cross": prev['ema_fast'] < prev['ema_slow'] and last['ema_fast'] > last['ema_slow']
            }
            short_conditions = {
                "rsi": last['rsi'] < RSI_SHORT_ENTRY,
                "price_pos": price < last['ema_fast'] and price < last['ema_slow'],
                "cross": prev['ema_fast'] > prev['ema_slow'] and last['ema_fast'] < last['ema_slow']
            }

            # --- 1. ЛОГИКА АКТИВНОГО СИГНАЛА ---
            if active_signal := state.get('active_signal'):
                side = active_signal['side']
                entry_price = active_signal['price']
                
                # Проверка отмены сигнала
                cancel = False
                if side == "LONG" and (last['rsi'] < RSI_SHORT_ENTRY or price < last['ema_slow']):
                    cancel = True
                elif side == "SHORT" and (last['rsi'] > RSI_LONG_ENTRY or price > last['ema_slow']):
                    cancel = True
                
                if cancel:
                    await broadcast_message(app.bot, f"⚠️ ОТМЕНА сигнала {side} по {PAIR}.")
                    state['active_signal'] = None
                    save_state()
                    continue

                # Проверка целей и анти-целей
                price_change_pct = ((price - entry_price) / entry_price) * 100
                
                # Для LONG
                if side == "LONG":
                    if price_change_pct >= active_signal['next_target_pct']:
                        await broadcast_message(app.bot, f"🎯 ЦЕЛЬ +{active_signal['next_target_pct']:.2f}% по {PAIR} ({side}) ДОСТИГНУТА. Цена: {price:.4f}")
                        state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                        save_state()
                    elif price_change_pct <= active_signal['next_anti_target_pct']:
                        await broadcast_message(app.bot, f"📉 АНТИ-ЦЕЛЬ {active_signal['next_anti_target_pct']:.2f}% по {PAIR} ({side}). ВНИМАНИЕ! Цена: {price:.4f}")
                        state['active_signal']['next_anti_target_pct'] -= ANTI_TARGET_STEP_PCT
                        save_state()
                # Для SHORT
                elif side == "SHORT":
                    if price_change_pct <= -active_signal['next_target_pct']:
                        await broadcast_message(app.bot, f"🎯 ЦЕЛЬ +{active_signal['next_target_pct']:.2f}% по {PAIR} ({side}) ДОСТИГНУТА. Цена: {price:.4f}")
                        state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                        save_state()
                    elif price_change_pct >= -active_signal['next_anti_target_pct']:
                         await broadcast_message(app.bot, f"📈 АНТИ-ЦЕЛЬ {active_signal['next_anti_target_pct']:.2f}% по {PAIR} ({side}). ВНИМАНИЕ! Цена: {price:.4f}")
                         state['active_signal']['next_anti_target_pct'] -= ANTI_TARGET_STEP_PCT
                         save_state()

            # --- 2. ЛОГИКА ПОИСКА НОВОГО СИГНАЛА ---
            else:
                # Проверяем пересечения
                if long_conditions["cross"]:
                    if long_conditions["rsi"] and long_conditions["price_pos"]:
                        # Все условия совпали -> АКТИВНЫЙ СИГНАЛ
                        state['active_signal'] = {"side": "LONG", "price": price, "next_target_pct": PRICE_CHANGE_STEP_PCT, "next_anti_target_pct": -ANTI_TARGET_STEP_PCT}
                        state['preliminary_signal'] = None
                        await broadcast_message(app.bot, f"✅ СИГНАЛ LONG по {PAIR}! Цена: {price:.4f}")
                    else:
                        # Только пересечение -> ПРЕДВАРИТЕЛЬНЫЙ СИГНАЛ
                        state['preliminary_signal'] = {"side": "LONG"}
                        await broadcast_message(app.bot, f"⏳ Предварительный сигнал LONG по {PAIR}. Ждём подтверждения RSI и положения цены.")
                    save_state()
                    continue

                if short_conditions["cross"]:
                    if short_conditions["rsi"] and short_conditions["price_pos"]:
                        state['active_signal'] = {"side": "SHORT", "price": price, "next_target_pct": PRICE_CHANGE_STEP_PCT, "next_anti_target_pct": -ANTI_TARGET_STEP_PCT}
                        state['preliminary_signal'] = None
                        await broadcast_message(app.bot, f"✅ СИГНАЛ SHORT по {PAIR}! Цена: {price:.4f}")
                    else:
                        state['preliminary_signal'] = {"side": "SHORT"}
                        await broadcast_message(app.bot, f"⏳ Предварительный сигнал SHORT по {PAIR}. Ждём подтверждения RSI и положения цены.")
                    save_state()
                    continue

                # Проверяем, не подтвердился ли предварительный сигнал
                if preliminary_signal := state.get('preliminary_signal'):
                    side = preliminary_signal['side']
                    if side == "LONG" and long_conditions["rsi"] and long_conditions["price_pos"]:
                        state['active_signal'] = {"side": "LONG", "price": price, "next_target_pct": PRICE_CHANGE_STEP_PCT, "next_anti_target_pct": -ANTI_TARGET_STEP_PCT}
                        state['preliminary_signal'] = None
                        await broadcast_message(app.bot, f"✅ ПОДТВЕРЖДЕНИЕ сигнала LONG по {PAIR}! Цена: {price:.4f}")
                        save_state()
                    elif side == "SHORT" and short_conditions["rsi"] and short_conditions["price_pos"]:
                        state['active_signal'] = {"side": "SHORT", "price": price, "next_target_pct": PRICE_CHANGE_STEP_PCT, "next_anti_target_pct": -ANTI_TARGET_STEP_PCT}
                        state['preliminary_signal'] = None
                        await broadcast_message(app.bot, f"✅ ПОДТВЕРЖДЕНИЕ сигнала SHORT по {PAIR}! Цена: {price:.4f}")
                        save_state()

        except ccxt.NetworkError as e:
            log.warning(f"Ошибка сети CCXT: {e}. Повторная попытка через 60 секунд.")
            await asyncio.sleep(60)
        except Exception as e:
            log.error(f"Критическая ошибка в цикле мониторинга: {e}", exc_info=True)
            await asyncio.sleep(30)
        
        await asyncio.sleep(30) # Пауза перед следующей итерацией
    log.info("Цикл мониторинга остановлен.")

# === COMMANDS & LIFECYCLE ===
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
        msg += f"\n- Активный сигнал: {side}\n- Цена входа: {price:.4f}\n- Следующая цель: +{next_target:.2f}%\n- Следующая анти-цель: {next_anti:.2f}%"
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
