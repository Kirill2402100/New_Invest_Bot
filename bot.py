#!/usr/bin/env python3
# ============================================================================
# v8.0 - Улучшенная логика пересечений и детальное логирование для отладки
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

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)
# Устанавливаем уровень логирования для библиотеки httpcore, чтобы убрать лишний спам в логах
logging.getLogger("httpcore").setLevel(logging.WARNING)


if not BOT_TOKEN:
    log.critical("Переменная окружения BOT_TOKEN не найдена!")
    exit()

if not re.match(r'^\d+[mhdM]$', TIMEFRAME):
    log.critical(f"Неверный формат таймфрейма: '{TIMEFRAME}'. Пример: 1h, 15m, 1d.")
    exit()

CHAT_IDS = {int(cid.strip()) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}
if not CHAT_IDS:
    log.warning("CHAT_IDS не установлен. Уведомления придут только тем, кто напишет /start.")

# === STATE MANAGEMENT ===
STATE_FILE = "advanced_signal_state_v4.json"
state = {
    "monitoring": False,
    "active_signal": None,
    "preliminary_signal": None
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
                state.update(loaded_state)
            log.info(f"Состояние успешно загружено: {state}")
        except Exception as e:
            log.error(f"Не удалось загрузить состояние: {e}")

# === EXCHANGE & STRATEGY PARAMS ===
exchange = ccxt.mexc()
PAIR = PAIR_RAW.upper()

RSI_LEN = 14
EMA_FAST_LEN, EMA_SLOW_LEN = 9, 21
RSI_LONG_ENTRY, RSI_SHORT_ENTRY = 52, 48
PRICE_CHANGE_STEP_PCT = 0.1
ANTI_TARGET_STEP_PCT = 0.05

# === INDICATORS ===
def _ta_rsi(series: pd.Series, length=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=length, min_periods=length).mean()
    loss = (-delta.clip(upper=0)).rolling(window=length, min_periods=length).mean()
    if loss.empty or (loss_val := loss.iloc[-1]) == 0: return pd.Series(100, index=series.index)
    rs = gain.iloc[-1] / loss_val
    return 100 - (100 / (1 + rs))

def calculate_indicators(df: pd.DataFrame):
    df['ema_fast'] = df['close'].ewm(span=EMA_FAST_LEN, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=EMA_SLOW_LEN, adjust=False).mean()
    # Применяем расчет RSI ко всему столбцу, чтобы избежать ошибок с одиночными значениями
    df['rsi'] = df['close'].rolling(window=RSI_LEN + 1).apply(lambda x: _ta_rsi(x, RSI_LEN), raw=False)
    return df.dropna()

# === CORE FUNCTIONS ===
async def broadcast_message(bot: Bot, text: str):
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
            price = last['close']

            # --- НОВИНКА: Детальное логирование для отладки ---
            log.info(f"[ОТЛАДКА] Цена: {price:.4f}, RSI: {last['rsi']:.2f}, EMA_fast: {last['ema_fast']:.4f}, EMA_slow: {last['ema_slow']:.4f}")

            # Определяем состояние пересечения
            is_bull_cross = last['ema_fast'] > last['ema_slow']
            was_bull_cross = prev['ema_fast'] > prev['ema_slow']

            long_conditions = {
                "rsi": last['rsi'] > RSI_LONG_ENTRY,
                "price_pos": price > last['ema_fast'] and price > last['ema_slow']
            }
            short_conditions = {
                "rsi": last['rsi'] < RSI_SHORT_ENTRY,
                "price_pos": price < last['ema_fast'] and price < last['ema_slow']
            }

            # --- 1. УПРАВЛЕНИЕ АКТИВНЫМ СИГНАЛОМ ---
            if active_signal := state.get('active_signal'):
                # ... (этот блок без изменений)
                side = active_signal['side']
                entry_price = active_signal['price']
                
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

                price_change_pct = ((price - entry_price) / entry_price) * 100
                
                if side == "LONG":
                    if price_change_pct >= active_signal['next_target_pct']:
                        await broadcast_message(app.bot, f"🎯 ЦЕЛЬ +{active_signal['next_target_pct']:.2f}% по {PAIR} ({side}) ДОСТИГНУТА. Цена: {price:.4f}")
                        state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                        save_state()
                    elif price_change_pct <= active_signal['next_anti_target_pct']:
                        await broadcast_message(app.bot, f"📉 АНТИ-ЦЕЛЬ {active_signal['next_anti_target_pct']:.2f}% по {PAIR} ({side}). ВНИМАНИЕ! Цена: {price:.4f}")
                        state['active_signal']['next_anti_target_pct'] -= ANTI_TARGET_STEP_PCT
                        save_state()
                elif side == "SHORT":
                    if price_change_pct <= -active_signal['next_target_pct']:
                        await broadcast_message(app.bot, f"🎯 ЦЕЛЬ +{active_signal['next_target_pct']:.2f}% по {PAIR} ({side}) ДОСТИГНУТА. Цена: {price:.4f}")
                        state['active_signal']['next_target_pct'] += PRICE_CHANGE_STEP_PCT
                        save_state()
                    elif price_change_pct >= -active_signal['next_anti_target_pct']:
                         await broadcast_message(app.bot, f"📈 АНТИ-ЦЕЛЬ {active_signal['next_anti_target_pct']:.2f}% по {PAIR} ({side}). ВНИМАНИЕ! Цена: {price:.4f}")
                         state['active_signal']['next_anti_target_pct'] -= ANTI_TARGET_STEP_PCT
                         save_state()
            
            # --- 2. УПРАВЛЕНИЕ ПРЕДВАРИТЕЛЬНЫМ СИГНАЛОМ ---
            elif preliminary_signal := state.get('preliminary_signal'):
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
                
                # Отмена, если пересечение ушло в другую сторону
                elif (side == "LONG" and not is_bull_cross) or (side == "SHORT" and is_bull_cross):
                    await broadcast_message(app.bot, f"🚫 Предварительный сигнал {side} по {PAIR} отменён из-за обратного пересечения.")
                    state['preliminary_signal'] = None
                    save_state()

            # --- 3. ПОИСК НОВОГО СИГНАЛА ---
            else:
                # --- ИСПРАВЛЕННАЯ ЛОГИКА ---
                # Ищем изменение состояния пересечения
                if is_bull_cross and not was_bull_cross: # Произошел "золотой крест"
                    state['preliminary_signal'] = {"side": "LONG"}
                    await broadcast_message(app.bot, f"⏳ Предварительный сигнал LONG по {PAIR}. Ждём подтверждения.")
                    save_state()
                elif not is_bull_cross and was_bull_cross: # Произошел "крест смерти"
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
# ... (этот блок без изменений)
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
