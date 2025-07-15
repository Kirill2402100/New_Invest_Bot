#!/usr/bin/env python3
# ============================================================================
# Flat-Liner v6.1 • 15 Jul 2025
# ============================================================================
# • СТРАТЕГИЯ: Только флэтовая стратегия 'Flat_BB_Fade'
# • БИРЖА: OKX (Production)
# • АВТОТРЕЙДИНГ: Полная интеграция с API для размещения ордеров
# • УПРАВЛЕНИЕ: Команды для настройки депозита, плеча и тестовой торговли
# • ИСПРАВЛЕНИЕ v6.1: Корректная установка плеча (posSide) для OKX
# ============================================================================

import os
import json
import logging
import re
import uuid
import asyncio
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import pandas_ta as ta
from telegram import Bot, Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes
)

# ── ENV: КОНФИГУРАЦИЯ БОТА ───────────────────────────────────────────────────
# --- Telegram & Pair ---
BOT_TOKEN     = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW  = os.getenv("CHAT_IDS", "")
PAIR_SYMBOL   = os.getenv("PAIR_SYMBOL", "BTC-USDT-SWAP") # Формат OKX
TIMEFRAME     = os.getenv("TIMEFRAME", "5m")
STRAT_VERSION = "v6_1_flatliner_okx"

# --- OKX API ---
OKX_API_KEY      = os.getenv("OKX_API_KEY")
OKX_API_SECRET   = os.getenv("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")
# Для демо-торговли установите '1', для реальной - '0'
OKX_DEMO_MODE    = os.getenv("OKX_DEMO_MODE", "1") 

# --- Параметры стратегии ---
DEFAULT_DEPOSIT_USD = float(os.getenv("DEFAULT_DEPOSIT_USD", "50.0"))
DEFAULT_LEVERAGE    = float(os.getenv("DEFAULT_LEVERAGE", "100.0"))
FLAT_RR_RATIO       = float(os.getenv("FLAT_RR_RATIO", "1.0"))
FLAT_RSI_OVERSOLD   = float(os.getenv("FLAT_RSI_OVERSOLD", "35"))
FLAT_RSI_OVERBOUGHT = float(os.getenv("FLAT_RSI_OVERBOUGHT", "65"))

# --- Настройки ATR для определения размера SL ---
ATR_LOW_USD   = float(os.getenv("ATR_LOW_USD",  "80"))
ATR_HIGH_USD  = float(os.getenv("ATR_HIGH_USD", "120"))
SL_PCT_LOW    = float(os.getenv("SL_PCT_LOW",   "0.08"))
SL_PCT_MID    = float(os.getenv("SL_PCT_MID",   "0.10"))
SL_PCT_HIGH   = float(os.getenv("SL_PCT_HIGH",  "0.12"))

# ── НАСТРОЙКА ЛОГИРОВАНИЯ ────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(STRAT_VERSION)

# --- Проверка критических переменных окружения ---
if not all([BOT_TOKEN, CHAT_IDS_RAW, OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE]):
    log.critical("Одна или несколько переменных окружения (BOT_TOKEN, CHAT_IDS, OKX API) не установлены!"); raise SystemExit

CHAT_IDS = {int(cid) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

# ── УПРАВЛЕНИЕ СОСТОЯНИЕМ ───────────────────────────────────────────────────
STATE_FILE = f"state_{STRAT_VERSION}_{PAIR_SYMBOL.replace('-','')}.json"
state = {
    "monitoring": False,
    "active_trade_id": None,
    "leverage": DEFAULT_LEVERAGE,
    "deposit_usd": DEFAULT_DEPOSIT_USD
}

def save_state():
    with open(STATE_FILE, 'w') as f: json.dump(state, f, indent=2)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                loaded_state = json.load(f)
                for key, default_value in state.items():
                    if key not in loaded_state:
                        loaded_state[key] = default_value
                state.update(loaded_state)
            log.info("Файл состояния успешно загружен.")
        except (json.JSONDecodeError, TypeError):
            log.error("Не удалось прочитать файл состояния, будет создан новый.")
            save_state()
    else:
        save_state()
        log.info("Файл состояния не найден, создан новый.")

# ── ИНДИКАТОРЫ ───────────────────────────────────────────────────
RSI_LEN = 14
BBANDS_LEN = 20
ATR_LEN = 14

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.ta.rsi(length=RSI_LEN, append=True, col_names=(f"RSI_{RSI_LEN}",))
    df.ta.atr(length=ATR_LEN, append=True, col_names=(f"ATR_{ATR_LEN}",))
    df.ta.bbands(length=BBANDS_LEN, std=2, append=True,
                col_names=(f"BBL_{BBANDS_LEN}_2.0", f"BBM_{BBANDS_LEN}_2.0",
                           f"BBU_{BBANDS_LEN}_2.0", f"BBB_{BBANDS_LEN}_2.0",
                           f"BBP_{BBANDS_LEN}_2.0"))
    return df.dropna()

def get_sl_pct_by_atr(atr: float) -> float:
    if atr <= ATR_LOW_USD:  return SL_PCT_LOW
    if atr >= ATR_HIGH_USD: return SL_PCT_HIGH
    return SL_PCT_MID

# ── ВЗАИМОДЕЙСТВИЕ С БИРЖЕЙ (OKX) ──────────────────────────────────────────
async def initialize_exchange():
    """Инициализирует и настраивает объект биржи OKX."""
    try:
        exchange = ccxt.okx({
            'apiKey': OKX_API_KEY,
            'secret': OKX_API_SECRET,
            'password': OKX_API_PASSPHRASE,
            'options': {
                'defaultType': 'swap',
            },
        })
        exchange.set_sandbox_mode(OKX_DEMO_MODE == '1')
        log.info(f"Биржа OKX инициализирована. Демо-режим: {'ВКЛЮЧЕН' if OKX_DEMO_MODE == '1' else 'ВЫКЛЮЧЕН'}.")
        return exchange
    except Exception as e:
        log.critical(f"Критическая ошибка инициализации биржи: {e}")
        return None

async def set_leverage_on_exchange(exchange, symbol, leverage):
    """Устанавливает кредитное плечо для long и short позиций."""
    try:
        # Установка плеча для LONG
        await exchange.set_leverage(leverage, symbol, {'mgnMode': 'isolated', 'posSide': 'long'})
        # Установка плеча для SHORT
        await exchange.set_leverage(leverage, symbol, {'mgnMode': 'isolated', 'posSide': 'short'})
        log.info(f"На бирже установлено плечо {leverage}x для {symbol} (long/short)")
        return True
    except Exception as e:
        log.error(f"Ошибка установки плеча на бирже: {e}")
        return False

async def execute_trade(exchange, signal: dict):
    """Размещает ордер на бирже OKX с прикрепленными TP/SL."""
    side = signal['side']
    entry_price = signal['entry_price']
    sl_price = signal['sl_price']
    tp_price = signal['tp_price']
    deposit = signal['deposit_usd']
    leverage = signal['leverage']

    try:
        markets = await exchange.load_markets()
        market = markets[PAIR_SYMBOL]
        contract_val = float(market['contractVal'])
        
        position_value_usd = deposit * leverage
        amount_in_base_currency = position_value_usd / entry_price
        order_size_contracts = round(amount_in_base_currency / contract_val)

        if order_size_contracts == 0:
            log.warning("Рассчитанный размер ордера равен 0. Сделка отменена.")
            return None

        params = {
            'tdMode': 'isolated',
            'posSide': 'long' if side == 'LONG' else 'short',
            'attachAlgoOrds': [
                {'slTriggerPx': str(sl_price), 'slOrdPx': '-1'},
                {'tpTriggerPx': str(tp_price), 'tpOrdPx': '-1'}
            ]
        }
        
        log.info(f"Попытка разместить ордер: {side} {order_size_contracts} контрактов {PAIR_SYMBOL} по рынку. SL={sl_price}, TP={tp_price}")
        
        order = await exchange.create_order(
            symbol=PAIR_SYMBOL, type='market',
            side='buy' if side == 'LONG' else 'sell',
            amount=order_size_contracts, params=params
        )
        
        log.info(f"Ордер успешно размещен! ID: {order['id']}")
        return order['id']

    except Exception as e:
        log.error(f"Ошибка размещения ордера: {e}")
        await notify_all(f"🔴 ОШИБКА РАЗМЕЩЕНИЯ ОРДЕРА\n\nИнструмент: {PAIR_SYMBOL}\nТип: {side}\nОшибка: {e}")
        return None

# ── УВЕДОМЛЕНИЯ ───────────────────────────────────────────────
async def notify_all(text: str, bot: Bot = None):
    temp_bot = bot if bot else Bot(token=BOT_TOKEN)
    for cid in CHAT_IDS:
        try:
            await temp_bot.send_message(cid, text, parse_mode="HTML")
        except Exception as e:
            log.error(f"TG fail -> {cid}: {e}")

# ── ОСНОВНОЙ ЦИКЛ БОТА ───────────────────────────────────────────────────────
async def monitor(app: Application):
    exchange = await initialize_exchange()
    if not exchange:
        await notify_all("Не удалось инициализировать биржу. Бот остановлен.", app.bot)
        return

    if not await set_leverage_on_exchange(exchange, PAIR_SYMBOL, state['leverage']):
        await notify_all("Не удалось установить плечо. Бот остановлен.", app.bot)
        await exchange.close()
        return

    log.info("🚀 Основной цикл запущен: %s %s (%s)", PAIR_SYMBOL, TIMEFRAME, STRAT_VERSION)
    
    while state.get("monitoring", False):
        try:
            positions = await exchange.fetch_positions([PAIR_SYMBOL])
            active_position = next((p for p in positions if float(p.get('notionalUsd', 0)) > 0), None)

            if active_position:
                if not state.get("active_trade_id"):
                    log.info(f"Обнаружена активная позиция, не отслеживаемая ботом. ID: {active_position['id']}. Пропускаем цикл.")
            else:
                if state.get("active_trade_id"):
                    log.info(f"Отслеживаемая позиция {state['active_trade_id']} была закрыта.")
                    await notify_all(f"✅ Позиция {state['active_trade_id']} была закрыта.", app.bot)
                    state["active_trade_id"] = None
                    save_state()

                ohlcv = await exchange.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=100)
                df = add_indicators(pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]))
                if len(df) < 2:
                    await asyncio.sleep(30); continue

                last = df.iloc[-1]
                price = last["close"]
                rsi = last[f"RSI_{RSI_LEN}"]
                bb_lower = last[f"BBL_{BBANDS_LEN}_2.0"]
                bb_upper = last[f"BBU_{BBANDS_LEN}_2.0"]

                side = None
                if price <= bb_lower and rsi < FLAT_RSI_OVERSOLD: side = "LONG"
                elif price >= bb_upper and rsi > FLAT_RSI_OVERBOUGHT: side = "SHORT"

                if side:
                    atr = last[f"ATR_{ATR_LEN}"]
                    sl_pct = get_sl_pct_by_atr(atr)
                    tp_pct = sl_pct * FLAT_RR_RATIO
                    
                    sl_price = price * (1 - sl_pct / 100) if side == "LONG" else price * (1 + sl_pct / 100)
                    tp_price = price * (1 + tp_pct / 100) if side == "LONG" else price * (1 - tp_pct / 100)
                    
                    signal = {
                        "side": side, "entry_price": price, "sl_price": sl_price,
                        "tp_price": tp_price, "deposit_usd": state['deposit_usd'], "leverage": state['leverage']
                    }
                    
                    await notify_all(f"🔔 <b>ПОЛУЧЕН СИГНАЛ: {side}</b>\n\n"
                                     f"<b>Инструмент:</b> {PAIR_SYMBOL}\n<b>Цена:</b> {price:.2f}\n"
                                     f"<b>TP:</b> {tp_price:.2f} | <b>SL:</b> {sl_price:.2f}\n"
                                     f"<b>Депозит:</b> {state['deposit_usd']}$ | <b>Плечо:</b> {state['leverage']}x\n\n"
                                     f"Отправка ордера на биржу...", app.bot)

                    order_id = await execute_trade(exchange, signal)
                    if order_id:
                        state["active_trade_id"] = order_id
                        save_state()
                        await notify_all(f"✅ <b>ОРДЕР УСПЕШНО РАЗМЕЩЕН</b>\n\n<b>ID ордера:</b> {order_id}", app.bot)
        
        except ccxt.NetworkError as e:
            log.warning("CCXT ошибка сети: %s", e)
        except Exception as e:
            log.exception("Сбой в основном цикле:")
        
        await asyncio.sleep(60)

    await exchange.close()
    log.info("⛔️ Основной цикл остановлен. Соединение с биржей закрыто.")

# ── КОМАНДЫ TELEGRAM ────────────────────────────────────────────────
async def start_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    CHAT_IDS.add(update.effective_chat.id)
    if not state.get("monitoring"):
        state["monitoring"] = True
        save_state()
        await update.message.reply_text(f"✅ Бот <b>{STRAT_VERSION}</b> запущен.", parse_mode="HTML")
        asyncio.create_task(monitor(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Бот уже работает.")

async def stop_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False
    save_state()
    await update.message.reply_text("❌ Бот остановлен. Основной цикл завершится после текущей итерации.")

async def status_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    status_msg = f"<b>СТАТУС БОТА ({STRAT_VERSION})</b>\n\n"
    status_msg += f"<b>Мониторинг:</b> {'АКТИВЕН' if state.get('monitoring') else 'ОСТАНОВЛЕН'}\n"
    status_msg += f"<b>Инструмент:</b> {PAIR_SYMBOL}\n"
    status_msg += f"<b>Депозит на сделку:</b> {state['deposit_usd']:.2f}$\n"
    status_msg += f"<b>Кредитное плечо:</b> {state['leverage']}x\n\n"
    
    if state.get("active_trade_id"):
        status_msg += f"<b>Активная сделка (ID):</b> {state['active_trade_id']}\n"
    else:
        status_msg += "<i>Нет активных сделок.</i>"
        
    await update.message.reply_text(status_msg, parse_mode="HTML")

async def set_deposit_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        deposit = float(ctx.args[0])
        if deposit <= 0:
            await update.message.reply_text("❌ Ошибка: размер депозита должен быть больше нуля.")
            return
        state['deposit_usd'] = deposit
        save_state()
        await update.message.reply_text(f"✅ Размер депозита на сделку установлен: <b>{deposit:.2f}$</b>", parse_mode="HTML")
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Неверный формат. Используйте: /set_deposit <сумма>")

async def set_leverage_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        leverage = int(ctx.args[0])
        if not 1 <= leverage <= 125:
            await update.message.reply_text("❌ Ошибка: плечо должно быть в диапазоне от 1 до 125.")
            return
        
        exchange = await initialize_exchange()
        if not exchange:
            await update.message.reply_text("🔴 Ошибка: не удалось подключиться к бирже.")
            return
            
        success = await set_leverage_on_exchange(exchange, PAIR_SYMBOL, leverage)
        await exchange.close()

        if success:
            state['leverage'] = leverage
            save_state()
            await update.message.reply_text(f"✅ Кредитное плечо установлено: <b>{leverage}x</b>", parse_mode="HTML")
        else:
            await update.message.reply_text("� Ошибка установки плеча на бирже. Проверьте логи.")

    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Неверный формат. Используйте: /set_leverage <значение>")

async def test_trade_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        args_dict = dict(arg.split('=') for arg in ctx.args)
        deposit = float(args_dict['deposit'])
        leverage = int(args_dict['leverage'])
        tp_price = float(args_dict['tp'])
        sl_price = float(args_dict['sl'])
        side = args_dict.get('side', 'LONG').upper()

        if side not in ['LONG', 'SHORT']:
            await update.message.reply_text("❌ Ошибка: 'side' должен быть LONG или SHORT.")
            return

        await update.message.reply_text(f"🛠 <b>ЗАПУСК ТЕСТОВОЙ СДЕЛКИ</b>\n\n"
                                        f"<b>Тип:</b> {side}\n<b>Депозит:</b> {deposit}$\n<b>Плечо:</b> {leverage}x\n"
                                        f"<b>TP:</b> {tp_price}\n<b>SL:</b> {sl_price}\n\n"
                                        f"Подключаюсь к бирже и отправляю ордер...", parse_mode="HTML")

        exchange = await initialize_exchange()
        if not exchange:
            await update.message.reply_text("🔴 Ошибка: не удалось подключиться к бирже.")
            return

        await set_leverage_on_exchange(exchange, PAIR_SYMBOL, leverage)
        
        ticker = await exchange.fetch_ticker(PAIR_SYMBOL)
        entry_price = ticker['last']

        signal = {
            "side": side, "entry_price": entry_price, "sl_price": sl_price,
            "tp_price": tp_price, "deposit_usd": deposit, "leverage": leverage
        }

        order_id = await execute_trade(exchange, signal)
        
        await set_leverage_on_exchange(exchange, PAIR_SYMBOL, state['leverage'])
        await exchange.close()

        if order_id:
            await update.message.reply_text(f"✅ <b>ТЕСТОВЫЙ ОРДЕР РАЗМЕЩЕН</b>\n\n"
                                            f"<b>ID ордера:</b> {order_id}\n"
                                            f"Не забудьте закрыть позицию вручную!", parse_mode="HTML")
        else:
            await update.message.reply_text("🔴 Ошибка размещения тестового ордера. Проверьте логи.", parse_mode="HTML")

    except Exception as e:
        log.error(f"Ошибка в команде test_trade: {e}")
        await update.message.reply_text(f"⚠️ Ошибка формата команды.\n"
                                        f"<b>Пример:</b> /test_trade deposit=30 leverage=80 tp=120000 sl=100000 side=LONG",
                                        parse_mode="HTML")

# ── ЗАПУСК БОТА ──────────────────────────────────────────────────────────────
async def post_init(app: Application):
    load_state()
    log.info("Бот инициализирован. Состояние загружено.")
    await notify_all(f"✅ Бот <b>{STRAT_VERSION}</b> перезапущен.", bot=app.bot)
    if state.get("monitoring"):
        log.info("Обнаружен активный статус мониторинга, запускаю основной цикл...")
        asyncio.create_task(monitor(app))

if __name__ == "__main__":
    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .post_init(post_init)
           .build())

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("set_deposit", set_deposit_command))
    app.add_handler(CommandHandler("set_leverage", set_leverage_command))
    app.add_handler(CommandHandler("test_trade", test_trade_command))
    
    log.info("Запуск бота...")
    app.run_polling()
