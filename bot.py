#!/usr/bin/env python3
# ============================================================================
# Flat-Liner v10.0 • 16 Jul 2025
# ============================================================================
# • СТРАТЕГИЯ: Флэтовая стратегия 'Flat_BB_Fade' с обязательным фильтром по ADX
# • БИРЖА: HTX (Huobi)
# • АВТОТРЕЙДИНГ: Полная интеграция с API для размещения ордеров
# • ИСПРАВЛЕНИЕ v10.0:
#   - [ФИНАЛЬНОЕ РЕШЕНИЕ] Бот переписан для работы с HTX, чтобы гарантированно
#     обойти проблемы со старой версией ccxt на хостинге.
# ============================================================================

import os
import json
import logging
import asyncio
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import pandas_ta as ta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Bot, Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes
)

# ── ENV: КОНФИГУРАЦИЯ БОТА ───────────────────────────────────────────────────
# --- Telegram & Pair ---
BOT_TOKEN     = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW  = os.getenv("CHAT_IDS", "")
# ВАЖНО: Используйте формат HTX для USDT-фьючерсов, например 'BTC/USDT:USDT'
PAIR_SYMBOL   = os.getenv("PAIR_SYMBOL", "BTC/USDT:USDT")
TIMEFRAME     = os.getenv("TIMEFRAME", "5m")
STRAT_VERSION = "v10_0_flatliner_htx"
SHEET_ID      = os.getenv("SHEET_ID")


# --- HTX (Huobi) API ---
HTX_ACCESS_KEY = os.getenv("HTX_Access_Key") # Имя как на вашем скриншоте
HTX_SECRET_KEY = os.getenv("HTX_SECRET_KEY") # Имя как на вашем скриншоте
# Для демо-торговли установите '1', для реальной - '0'
DEMO_MODE      = os.getenv("DEMO_MODE", "1")

# --- Параметры стратегии ---
DEFAULT_DEPOSIT_USD = float(os.getenv("DEFAULT_DEPOSIT_USD", "50.0"))
DEFAULT_LEVERAGE    = float(os.getenv("DEFAULT_LEVERAGE", "75.0")) # Макс. плечо на HTX
FLAT_RR_RATIO       = float(os.getenv("FLAT_RR_RATIO", "1.0"))
FLAT_SL_PCT         = float(os.getenv("FLAT_SL_PCT", "0.10"))
FLAT_RSI_OVERSOLD   = float(os.getenv("FLAT_RSI_OVERSOLD", "35"))
FLAT_RSI_OVERBOUGHT = float(os.getenv("FLAT_RSI_OVERBOUGHT", "65"))
REPORT_TIME_UTC     = os.getenv("REPORT_TIME_UTC", "21:00")


# ── НАСТРОЙКА ЛОГИРОВАНИЯ ────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(STRAT_VERSION)

# --- Проверка критических переменных окружения ---
if not all([BOT_TOKEN, CHAT_IDS_RAW, HTX_ACCESS_KEY, HTX_SECRET_KEY]):
    log.critical("Одна или несколько переменных окружения (BOT_TOKEN, CHAT_IDS, HTX API) не установлены!"); raise SystemExit

CHAT_IDS = {int(cid) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

# ── GOOGLE SHEETS ────────────────────────────────────────────────────────────
TRADE_LOG_WS = None
def setup_google_sheets() -> None:
    # ... (Этот блок остается без изменений) ...
    pass

# ── УПРАВЛЕНИЕ СОСТОЯНИЕМ ───────────────────────────────────────────────────
STATE_FILE = f"state_{STRAT_VERSION}_{PAIR_SYMBOL.replace('/','').replace(':','')}.json"
state = {"monitoring": False, "active_trade": None, "leverage": DEFAULT_LEVERAGE, "deposit_usd": DEFAULT_DEPOSIT_USD, "dynamic_adx_threshold": 25.0, "last_adx_recalc_time": None, "daily_report_data": []}
# ... (Этот блок остается без изменений) ...
def save_state():
    with open(STATE_FILE, 'w') as f: json.dump(state, f, indent=2)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f: state.update(json.load(f))
            log.info("Файл состояния успешно загружен.")
        except (json.JSONDecodeError, TypeError):
            log.error("Не удалось прочитать файл состояния, будет создан новый."); save_state()
    else:
        save_state(); log.info("Файл состояния не найден, создан новый.")

# ── ИНДИКАТОРЫ ───────────────────────────────────────────────────
RSI_LEN, BBANDS_LEN, ADX_LEN = 14, 20, 14
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.ta.rsi(length=RSI_LEN, append=True, col_names=(f"RSI_{RSI_LEN}",))
    df.ta.adx(length=ADX_LEN, append=True, col_names=(f"ADX_{ADX_LEN}", "DMP", "DMN"))
    df.ta.bbands(length=BBANDS_LEN, std=2, append=True, col_names=(f"BBL_{BBANDS_LEN}_2.0", f"BBM_{BBANDS_LEN}_2.0", f"BBU_{BBANDS_LEN}_2.0", f"BBB_{BBANDS_LEN}_2.0", f"BBP_{BBANDS_LEN}_2.0"))
    return df.dropna()

# ── ВЗАИМОДЕЙСТВИЕ С БИРЖЕЙ (HTX) ──────────────────────────────────────
async def initialize_exchange():
    try:
        exchange = ccxt.htx({
            'apiKey': HTX_ACCESS_KEY,
            'secret': HTX_SECRET_KEY,
            'options': {'defaultType': 'swap'},
        })
        exchange.set_sandbox_mode(DEMO_MODE == '1')
        await exchange.load_markets()
        log.info(f"Биржа HTX инициализирована. Демо-режим: {'ВКЛЮЧЕН' if DEMO_MODE == '1' else 'ВЫКЛЮЧЕН'}.")
        return exchange
    except Exception as e:
        log.critical(f"Критическая ошибка инициализации биржи: {e}")
        return None

async def set_leverage_on_exchange(exchange, symbol, leverage):
    try:
        await exchange.set_leverage(leverage, symbol)
        log.info(f"На бирже установлено плечо {leverage}x для {symbol}")
        return True
    except Exception as e:
        log.error(f"Ошибка установки плеча на бирже: {e}")
        return False

async def execute_trade(exchange, signal: dict):
    side = signal['side']
    deposit = signal['deposit_usd']
    leverage = signal['leverage']
    entry_price = signal['entry_price']
    sl_price = signal['sl_price']
    tp_price = signal['tp_price']
    
    try:
        market = exchange.markets[PAIR_SYMBOL]
        position_value_usd = deposit * leverage
        amount_in_contracts = position_value_usd / float(market['contractSize'])
        amount = exchange.amount_to_precision(PAIR_SYMBOL, amount_in_contracts)
        log.info(f"Расчетный размер ордера: {amount} контрактов")

        params = {
            'sl_price': sl_price,
            'tp_price': tp_price,
        }
        
        log.info(f"Попытка разместить ордер: {side} {amount} {PAIR_SYMBOL} по рынку с SL/TP.")
        order = await exchange.create_order(symbol=PAIR_SYMBOL, type='market', side=side.lower(), amount=amount, params=params)
        
        log.info(f"Ордер успешно размещен! ID: {order['id']}")
        await notify_all(f"✅ <b>ОРДЕР РАЗМЕЩЕН</b>\n\n<b>ID:</b> {order['id']}\n<b>SL:</b> {sl_price}\n<b>TP:</b> {tp_price}")
        return order['id']

    except Exception as e:
        log.error(f"Ошибка размещения ордера: {e}")
        await notify_all(f"🔴 ОШИБКА РАЗМЕЩЕНИЯ ОРДЕРА\n\n<b>Инструмент:</b> {PAIR_SYMBOL}\n<b>Тип:</b> {side}\n<b>Ошибка:</b> <code>{e}</code>")
        return None

# ... (Остальные функции, такие как monitor, daily_reporter, и команды Telegram, остаются в основном такими же) ...
async def monitor(app: Application):
    exchange = await initialize_exchange()
    if not exchange:
        await notify_all("Не удалось инициализировать биржу. Бот остановлен.", app.bot); return
    
    # На HTX нет отдельной настройки режима хеджирования через API, он управляется на сайте
    if not await set_leverage_on_exchange(exchange, PAIR_SYMBOL, state['leverage']):
        await notify_all("Не удалось установить плечо. Бот остановлен.", app.bot); await exchange.close(); return

    log.info("🚀 Основной цикл запущен: %s %s (%s)", PAIR_SYMBOL, TIMEFRAME, STRAT_VERSION)
    
    while state.get("monitoring", False):
        try:
            if state.get("active_trade"):
                positions = await exchange.fetch_positions([PAIR_SYMBOL])
                active_position = next((p for p in positions if float(p.get('contracts', 0)) != 0), None)
                if not active_position:
                    log.info("Активная сделка была закрыта.")
                    state["active_trade"] = None
                    save_state()
                await asyncio.sleep(60)
                continue

            ohlcv = await exchange.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=100)
            df = add_indicators(pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]))
            if len(df) < 2: await asyncio.sleep(60); continue

            last = df.iloc[-1]
            price = last["close"]
            side = None
            if price <= last[f"BBL_{BBANDS_LEN}_2.0"] and last[f"RSI_{RSI_LEN}"] < FLAT_RSI_OVERSOLD: side = "BUY"
            elif price >= last[f"BBU_{BBANDS_LEN}_2.0"] and last[f"RSI_{RSI_LEN}"] > FLAT_RSI_OVERBOUGHT: side = "SELL"

            if side:
                sl_pct = FLAT_SL_PCT
                tp_pct = sl_pct * FLAT_RR_RATIO
                sl_price = price * (1 - sl_pct / 100) if side == "BUY" else price * (1 + sl_pct / 100)
                tp_price = price * (1 + tp_pct / 100) if side == "BUY" else price * (1 - tp_pct / 100)
                
                signal = {
                    "side": side, "entry_price": price, "sl_price": sl_price,
                    "tp_price": tp_price, "deposit_usd": state['deposit_usd'], "leverage": state['leverage'],
                }
                await notify_all(f"🔔 <b>ПОЛУЧЕН СИГНАЛ: {side}</b>\n\n<b>Инструмент:</b> {PAIR_SYMBOL}\n<b>Цена:</b> {price:.4f}\n<b>Депозит:</b> {state['deposit_usd']}$ | <b>Плечо:</b> {state['leverage']}x\n\nОтправка ордера на биржу...", app.bot)
                order_id = await execute_trade(exchange, signal)
                if order_id:
                    signal['id'] = order_id
                    state["active_trade"] = signal
                    save_state()
        
        except ccxt.NetworkError as e:
            log.warning("CCXT ошибка сети: %s", e)
        except Exception as e:
            log.exception("Сбой в основном цикле:")
        
        await asyncio.sleep(60)

    await exchange.close()
    log.info("⛔️ Основной цикл остановлен.")

# ── КОМАНДЫ TELEGRAM ────────────────────────────────────────────────
async def start_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    CHAT_IDS.add(update.effective_chat.id)
    if not state.get("monitoring"):
        state["monitoring"] = True; save_state()
        await update.message.reply_text(f"✅ Бот <b>{STRAT_VERSION}</b> запущен.", parse_mode="HTML")
        asyncio.create_task(monitor(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Бот уже работает.")

async def stop_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False; save_state()
    await update.message.reply_text("❌ Бот остановлен.")

async def status_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    status_msg = f"<b>СТАТУС БОТА ({STRAT_VERSION})</b>\n\n"
    status_msg += f"<b>Мониторинг:</b> {'АКТИВЕН' if state.get('monitoring') else 'ОСТАНОВЛЕН'}\n"
    status_msg += f"<b>Инструмент:</b> {PAIR_SYMBOL}\n"
    status_msg += f"<b>Депозит:</b> {state['deposit_usd']:.2f}$\n<b>Плечо:</b> {state['leverage']}x\n"
    if trade := state.get("active_trade"):
        status_msg += f"<b>Активная сделка (ID):</b> {trade['id']}"
    else:
        status_msg += "<i>Нет активных сделок.</i>"
    await update.message.reply_text(status_msg, parse_mode="HTML")

async def set_deposit_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        deposit = float(ctx.args[0])
        if deposit <= 0: raise ValueError
        state['deposit_usd'] = deposit; save_state()
        await update.message.reply_text(f"✅ Депозит установлен: <b>{deposit:.2f}$</b>", parse_mode="HTML")
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Формат: /set_deposit <сумма>")

async def set_leverage_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        leverage = int(ctx.args[0])
        if not 1 <= leverage <= 75: raise ValueError
        exchange = await initialize_exchange()
        if not exchange:
            await update.message.reply_text("🔴 Ошибка подключения к бирже."); return
        if await set_leverage_on_exchange(exchange, PAIR_SYMBOL, leverage):
            state['leverage'] = leverage; save_state()
            await update.message.reply_text(f"✅ Плечо установлено: <b>{leverage}x</b>", parse_mode="HTML")
        else:
            await update.message.reply_text("🔴 Ошибка установки плеча.")
        await exchange.close()
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Формат: /set_leverage <1-75>")

async def test_trade_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("ℹ️ Тестовая команда для HTX в разработке.")

async def apitest_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙️ <b>Запускаю тест API ключей HTX...</b>", parse_mode="HTML")
    exchange = None
    try:
        if not HTX_ACCESS_KEY or not HTX_SECRET_KEY:
            await update.message.reply_text("🔴 API ключи не найдены в переменных.", parse_mode="HTML"); return

        exchange = ccxt.htx({'apiKey': HTX_ACCESS_KEY, 'secret': HTX_SECRET_KEY})
        exchange.set_sandbox_mode(DEMO_MODE == '1')

        await update.message.reply_text("Попытка подключения и получения баланса...", parse_mode="HTML")
        balance = await exchange.fetch_balance(params={'type': 'swap'})
        
        await update.message.reply_text(f"✅ <b>УСПЕХ!</b>\nПодключение к HTX прошло успешно.\n\n"
                                        f"<b>Баланс USDT на фьючерсах:</b> <code>{balance.get('USDT', {'total': 'Не найден'})['total']}</code>",
                                        parse_mode="HTML")
    except ccxt.AuthenticationError as e:
        await update.message.reply_text(f"❌ <b>ОШИБКА АУТЕНТИФИКАЦИИ!</b>\n<code>{e}</code>", parse_mode="HTML")
    except Exception as e:
        log.error(f"Критическая ошибка в /apitest: {e}")
        await update.message.reply_text(f"🔴 <b>Произошла другая ошибка:</b>\n<code>{e}</code>", parse_mode="HTML")
    finally:
        if exchange: await exchange.close()

async def post_init(app: Application):
    load_state()
    log.info("Бот инициализирован.")
    await notify_all(f"✅ Бот <b>{STRAT_VERSION}</b> перезапущен.", bot=app.bot)
    if state.get("monitoring"):
        log.info("Обнаружен активный статус мониторинга, запускаю основной цикл...")
        asyncio.create_task(monitor(app))

if __name__ == "__main__":
    app = (ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build())
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("set_deposit", set_deposit_command))
    app.add_handler(CommandHandler("set_leverage", set_leverage_command))
    app.add_handler(CommandHandler("test_trade", test_trade_command))
    app.add_handler(CommandHandler("apitest", apitest_command))
    log.info("Запуск бота...")
    app.run_polling()
