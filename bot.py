#!/usr/bin/env python3
# ============================================================================
# v5.1 - EMA Crossover Signal Monitor (с исправлениями)
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
# Пример для CHAT_IDS: "12345678,87654321"
CHAT_IDS_RAW = os.getenv("CHAT_IDS", "")
SHEET_ID = os.getenv("SHEET_ID")
PAIR_RAW = os.getenv("PAIR", "BTC/USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "1h")

# Настройка логирования для вывода подробной информации
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# Проверка наличия токена
if not BOT_TOKEN:
    log.critical("Переменная окружения BOT_TOKEN не найдена! Бот не может быть запущен.")
    exit()

# Обработка CHAT_IDS. Игнорируем пустые значения.
CHAT_IDS = {int(cid.strip()) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}
if not CHAT_IDS:
    log.warning("Переменная окружения CHAT_IDS не установлена или пуста. Сообщения будут отправляться только тем, кто напишет /start.")

# === GOOGLE SHEETS ===
LOGS_WS = None
try:
    creds_json_string = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json_string:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = json.loads(creds_json_string)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gs = gspread.authorize(creds)
        LOGS_WS = gs.open_by_key(SHEET_ID).worksheet("LP_Logs")
        HEADERS = ["Дата-время", "Инструмент", "Направление", "Депозит", "Вход", "Stop Loss", "Take Profit", "RR", "P&L сделки (USDT)", "Прибыль к депозиту (%)"]
        if LOGS_WS.row_values(1) != HEADERS:
            LOGS_WS.resize(rows=1)
            LOGS_WS.update('A1', [HEADERS])
        log.info("Успешное подключение к Google Sheets.")
    else:
        log.warning("Переменная GOOGLE_CREDENTIALS не найдена. Работа с Google Sheets отключена.")
except Exception as e:
    log.error(f"Ошибка инициализации Google Sheets: {e}")
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
    """Сохраняет текущее состояние в JSON файл."""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log.error(f"Не удалось сохранить состояние: {e}")

def load_state():
    """Загружает состояние из JSON файла, если он существует."""
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
            log.info(f"Состояние успешно загружено: {state}")
        except Exception as e:
            log.error(f"Не удалось загрузить состояние: {e}")

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
    """Кастомный расчет RSI для избежания проблем с pandas-ta."""
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=length, min_periods=length).mean()
    loss = (-delta.clip(upper=0)).rolling(window=length, min_periods=length).mean()
    if loss.empty or loss.iloc[-1] == 0: return pd.Series(100, index=series.index)
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_indicators(df: pd.DataFrame):
    """Расчет всех необходимых индикаторов."""
    df['ema_fast'] = df['close'].ewm(span=EMA_FAST_LEN, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW_LEN, adjust=False).mean()
    df['rsi'] = _ta_rsi(df['close'], RSI_LEN)
    df['ema_cross'] = df['ema_fast'] > df['ema_slow']
    return df.dropna()

# === CORE FUNCTIONS ===
async def broadcast_message(bot: Bot, text: str):
    """Отправляет сообщение всем пользователям из списка CHAT_IDS."""
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
    """Основной цикл мониторинга сигналов."""
    log.info("Цикл мониторинга запущен.")
    while state.get('monitoring', False):
        try:
            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = calculate_indicators(df.copy())
            
            if df.empty or len(df) < 2:
                log.warning("Недостаточно данных для анализа после расчета индикаторов.")
                await asyncio.sleep(30)
                continue

            last = df.iloc[-1]
            prev = df.iloc[-2]
            current_price = last['close']

            rsi_passed = last['rsi'] > RSI_LONG_T
            ema_position_passed = last['close'] > last['ema_fast'] and last['close'] > last['ema_slow']
            ema_cross_passed = not prev['ema_cross'] and last['ema_cross'] # Более надежная проверка пересечения

            # --- Логика сигнала LONG ---
            if rsi_passed and ema_position_passed and ema_cross_passed:
                if not state.get('active_signal'):
                    state['active_signal'] = {
                        "side": "LONG",
                        "price": current_price,
                        "next_target_pct": PRICE_CHANGE_STEP_PCT
                    }
                    state['signal_status'] = {"rsi": True, "ema_position": True, "ema_cross": True, "side": "LONG"}
                    save_state()
                    await broadcast_message(app.bot, f"✅ СИГНАЛ LONG по {PAIR}! Цена: {current_price:.2f}")

            # --- Проверка на отмену или обновление активного сигнала ---
            elif state.get('active_signal'):
                signal_data = state['active_signal']
                
                # Условия для удержания LONG позиции
                still_rsi = last['rsi'] > RSI_SHORT_T # Используем порог для удержания
                still_ema_pos = last['close'] > last['ema_slow'] # Ослабленное условие
                still_cross = last['ema_fast'] > last['ema_slow']

                missing = []
                if not still_rsi: missing.append("RSI ниже порога")
                if not still_ema_pos: missing.append("Цена ниже медленной EMA")
                if not still_cross: missing.append("Обратное пересечение EMA")

                if missing:
                    await broadcast_message(app.bot, f"⚠️ ОТМЕНА сигнала LONG по {PAIR}. Нарушены условия: {', '.join(missing)}")
                    state['active_signal'] = None
                    state['signal_status'] = {"rsi": False, "ema_position": False, "ema_cross": False, "side": None}
                    save_state()
                else:
                    # Проверка на достижение цели
                    price_change_pct = ((current_price - signal_data['price']) / signal_data['price']) * 100
                    if price_change_pct >= signal_data['next_target_pct']:
                        await broadcast_message(app.bot, f"🎯 ЦЕЛЬ +{signal_data['next_target_pct']:.1f}% по {PAIR} ДОСТИГНУТА. Цена: {current_price:.2f}")
                        state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                        save_state()
            
            # --- Сообщение о частичных условиях (если нет активного сигнала) ---
            else:
                partials = []
                if rsi_passed: partials.append("RSI")
                if ema_position_passed: partials.append("EMA положение")
                if ema_cross_passed: partials.append("EMA пересечение")
                
                # Отправляем сообщение только если есть хотя бы одно, но не все условия
                if 0 < len(partials) < 3:
                    # Чтобы не спамить, можно добавить проверку на изменение статуса
                    pass # пока отключим, чтобы не было спама
            
            await asyncio.sleep(30)

        except ccxt.NetworkError as e:
            log.warning(f"Ошибка сети CCXT: {e}. Повторная попытка через 60 секунд.")
            await asyncio.sleep(60)
        except Exception as e:
            log.error(f"Критическая ошибка в цикле мониторинга: {e}", exc_info=True)
            await asyncio.sleep(30)
    log.info("Цикл мониторинга остановлен.")


# === COMMANDS ===
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Запускает мониторинг."""
    chat_id = update.effective_chat.id
    if chat_id not in CHAT_IDS:
        CHAT_IDS.add(chat_id)
        log.info(f"Временный CHAT_ID добавлен: {chat_id}. Для постоянной работы добавьте его в переменные окружения.")

    if not state.get('monitoring'):
        state['monitoring'] = True
        save_state()
        await update.message.reply_text(f"✅ Мониторинг по паре {PAIR} ({TIMEFRAME}) запущен.")
        # Запускаем цикл мониторинга как фоновую задачу
        asyncio.create_task(monitor_loop(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Мониторинг уже активен.")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Останавливает мониторинг."""
    if state.get('monitoring'):
        state['monitoring'] = False
        save_state()
        await update.message.reply_text("🛑 Мониторинг остановлен.")
    else:
        await update.message.reply_text("ℹ️ Мониторинг не был запущен.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Показывает текущий статус."""
    if not state['monitoring']:
        await update.message.reply_text("Мониторинг неактивен.")
        return

    if signal := state.get('active_signal'):
        side = signal['side']
        price = signal['price']
        next_target = signal['next_target_pct']
        await update.message.reply_text(f"Активный сигнал: {side} от цены {price:.2f}. Следующая цель: +{next_target:.1f}%")
    else:
        status = state['signal_status']
        conditions = [
            f"RSI > {RSI_LONG_T}: {'Да' if status['rsi'] else 'Нет'}",
            f"Цена > EMA: {'Да' if status['ema_position'] else 'Нет'}",
            f"Пересечение EMA: {'Да' if status['ema_cross'] else 'Нет'}"
        ]
        await update.message.reply_text("Активного сигнала нет. Ожидание условий:\n" + "\n".join(conditions))

# === APPLICATION LIFECYCLE ===
async def post_init(app: Application):
    """Выполняется после инициализации приложения."""
    load_state()
    if state.get('monitoring'):
        log.info("Обнаружен активный статус мониторинга. Возобновление работы...")
        # Запускаем цикл мониторинга, так как он был активен до перезапуска
        asyncio.create_task(monitor_loop(app))

async def on_shutdown(app: Application):
    """Выполняется при завершении работы приложения."""
    log.info("Бот завершает работу. Сохранение состояния...")
    save_state()
    # Корректно закрываем сессию с биржей
    await exchange.close()
    log.info("Сессия с биржей закрыта.")

def main():
    """Основная функция для запуска бота."""
    log.info("Запуск бота...")
    
    app = ApplicationBuilder()\
        .token(BOT_TOKEN)\
        .post_init(post_init)\
        .post_shutdown(on_shutdown)\
        .build()

    # Добавляем обработчики команд
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))

    # Запускаем бота
    app.run_polling()

if __name__ == "__main__":
    main()
