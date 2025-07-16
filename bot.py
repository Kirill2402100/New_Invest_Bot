#!/usr/bin/env python3
# ============================================================================
# Flat-Liner v10.1 • 16 Jul 2025
# ============================================================================
# • СТРАТЕГИЯ: Флэтовая стратегия 'Flat_BB_Fade' с обязательным фильтром по ADX
# • БИРЖА: HTX (Huobi)
# • АВТОТРЕЙДИНГ: Полная интеграция с API для размещения ордеров
# • ИСПРАВЛЕНИЕ v10.1:
#   - [ФИНАЛЬНЫЙ ФИКС] Восстановлены недостающие функции (notify_all,
#     daily_reporter, и т.д.) для полной работоспособности.
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
STRAT_VERSION = "v10_1_flatliner_htx"
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
    global TRADE_LOG_WS
    if not SHEET_ID or not os.getenv("GOOGLE_CREDENTIALS"):
        log.warning("ID таблицы или учетные данные Google не установлены. Логирование в Google Sheets отключено.")
        return
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(os.getenv("GOOGLE_CREDENTIALS")),
            ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        gs = gspread.authorize(creds)
        ss = gs.open_by_key(SHEET_ID)
        headers = ["Signal_ID", "Version", "Strategy_Used", "Status", "Side", "Entry_Time_UTC", "Exit_Time_UTC", "Entry_Price", "Exit_Price", "SL_Price", "TP_Price", "Gross_P&L_USD", "Fee_USD", "Net_P&L_USD", "Entry_Deposit_USD", "Entry_ADX", "ADX_Threshold"]
        ws_name = f"HTX_Trades_{PAIR_SYMBOL.replace('/','').replace(':','')}"
        try:
            ws = ss.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(ws_name, rows="1000", cols=len(headers))
        if ws.row_values(1) != headers:
            ws.clear(); ws.update("A1", [headers])
            ws.format(f"A1:{chr(ord('A')+len(headers)-1)}1", {"textFormat": {"bold": True}})
        TRADE_LOG_WS = ws
        log.info(f"Логирование в Google Sheet включено ➜ {ws_name}")
    except Exception as e:
        log.error(f"Ошибка инициализации Google Sheets: {e}")
        TRADE_LOG_WS = None

# ── УПРАВЛЕНИЕ СОСТОЯНИЕМ ───────────────────────────────────────────────────
STATE_FILE = f"state_{STRAT_VERSION}_{PAIR_SYMBOL.replace('/','').replace(':','')}.json"
state = {"monitoring": False, "active_trade": None, "leverage": DEFAULT_LEVERAGE, "deposit_usd": DEFAULT_DEPOSIT_USD, "dynamic_adx_threshold": 25.0, "last_adx_recalc_time": None, "daily_report_data": []}

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

# ── УВЕДОМЛЕНИЯ ───────────────────────────────────────────────
async def notify_all(text: str, bot: Bot = None):
    temp_bot = bot if bot else Bot(token=BOT_TOKEN)
    for cid in CHAT_IDS:
        try:
            await temp_bot.send_message(cid, text, parse_mode="HTML")
        except Exception as e:
            log.error(f"TG fail -> {cid}: {e}")

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
        await notify_all(f"🔴 Критическая ошибка инициализации биржи: {e}")
        return None

async def set_leverage_on_exchange(exchange, symbol, leverage):
    try:
        await exchange.set_leverage(leverage, symbol)
        log.info(f"На бирже установлено плечо {leverage}x для {symbol}")
        return True
    except Exception as e:
        log.error(f"Ошибка установки плеча на бирже: {e}")
        await notify_all(f"🔴 Ошибка установки плеча: {e}")
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
        # На HTX размер ордера указывается в контрактах
        amount_in_contracts = position_value_usd / float(market['contractSize'])
        amount = exchange.amount_to_precision(PAIR_SYMBOL, amount_in_contracts)
        log.info(f"Расчетный размер ордера: {amount} контрактов")

        params = {
            'sl_price': sl_price,
            'tp_price': tp_price,
        }
        
        log.info(f"Попытка разместить ордер: {side} {amount} {PAIR_SYMBOL} по рынку с SL/TP.")
        # Для HTX side должен быть 'buy' или 'sell'
        order_side = 'buy' if side == 'BUY' else 'sell'
        order = await exchange.create_order(symbol=PAIR_SYMBOL, type='market', side=order_side, amount=amount, params=params)
        
        log.info(f"Ордер успешно размещен! ID: {order['id']}")
        await notify_all(f"✅ <b>ОРДЕР РАЗМЕЩЕН</b>\n\n<b>ID:</b> {order['id']}\n<b>SL:</b> {sl_price}\n<b>TP:</b> {tp_price}")
        return order['id']

    except Exception as e:
        log.error(f"Ошибка размещения ордера: {e}")
        await notify_all(f"🔴 ОШИБКА РАЗМЕЩЕНИЯ ОРДЕРА\n\n<b>Инструмент:</b> {PAIR_SYMBOL}\n<b>Тип:</b> {side}\n<b>Ошибка:</b> <code>{e}</code>")
        return None

async def process_closed_trade(exchange, trade_details, bot):
    try:
        log.info(f"Обработка закрытой сделки. ID ордера: {trade_details['id']}")
        await notify_all(f"✅ Сделка {trade_details['id']} ({trade_details['side']}) была закрыта.", bot)
    except Exception as e:
        log.error(f"Ошибка обработки закрытой сделки {trade_details['id']}: {e}")

async def recalculate_adx_threshold():
    try:
        log.info("Пересчет динамического порога ADX...")
        exchange = await initialize_exchange()
        if not exchange: return
        ohlcv = await exchange.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=1000)
        await exchange.close()
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
        df.ta.adx(length=ADX_LEN, append=True, col_names=(f"ADX_{ADX_LEN}", "DMP", "DMN"))
        df.dropna(inplace=True)

        if not df.empty:
            adx_values = df[f"ADX_{ADX_LEN}"]
            p20 = np.percentile(adx_values, 20)
            p30 = np.percentile(adx_values, 30)
            new_threshold = (p20 + p30) / 2
            state["dynamic_adx_threshold"] = new_threshold
            state["last_adx_recalc_time"] = datetime.now(timezone.utc).isoformat()
            save_state()
            log.info(f"Новый порог ADX: {new_threshold:.2f} (p20={p20:.2f}, p30={p30:.2f})")
    except Exception as e:
        log.error(f"Ошибка при пересчете порога ADX: {e}")

async def monitor(app: Application):
    exchange = await initialize_exchange()
    if not exchange:
        await notify_all("Не удалось инициализировать биржу. Бот остановлен.", app.bot); return
    
    if not await set_leverage_on_exchange(exchange, PAIR_SYMBOL, state['leverage']):
        await notify_all("Не удалось установить плечо. Бот остановлен.", app.bot); await exchange.close(); return

    await recalculate_adx_threshold()
    log.info("🚀 Основной цикл запущен: %s %s (%s)", PAIR_SYMBOL, TIMEFRAME, STRAT_VERSION)
    
    while state.get("monitoring", False):
        try:
            now_utc = datetime.now(timezone.utc)
            last_recalc_str = state.get("last_adx_recalc_time")
            if not last_recalc_str or (now_utc - datetime.fromisoformat(last_recalc_str)).total_seconds() > 3600:
                 await recalculate_adx_threshold()

            if state.get("active_trade"):
                positions = await exchange.fetch_positions([PAIR_SYMBOL])
                active_position = next((p for p in positions if float(p.get('contracts', 0)) != 0), None)
                if not active_position:
                    log.info("Активная сделка была закрыта.")
                    asyncio.create_task(process_closed_trade(exchange, state["active_trade"], app.bot))
                    state["active_trade"] = None
                    save_state()
                await asyncio.sleep(60)
                continue

            ohlcv = await exchange.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=100)
            df = add_indicators(pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]))
            if len(df) < 2: await asyncio.sleep(60); continue

            last = df.iloc[-1]
            adx = last[f"ADX_{ADX_LEN}"]
            
            if adx >= state.get('dynamic_adx_threshold', 25):
                await asyncio.sleep(60)
                continue

            price = last["close"]
            rsi = last[f"RSI_{RSI_LEN}"]
            bb_lower = last[f"BBL_{BBANDS_LEN}_2.0"]
            bb_upper = last[f"BBU_{BBANDS_LEN}_2.0"]

            side = None
            if price <= bb_lower and rsi < FLAT_RSI_OVERSOLD: side = "BUY"
            elif price >= bb_upper and rsi > FLAT_RSI_OVERBOUGHT: side = "SELL"

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

# ── ЕЖЕДНЕВНЫЙ ОТЧЁТ ────────────────────────────────────────────────────────
async def daily_reporter(app: Application):
    log.info("📈 Сервис ежедневных отчётов запущен.")
    while True:
        now_utc = datetime.now(timezone.utc)
        try:
            report_h, report_m = map(int, REPORT_TIME_UTC.split(':'))
            report_time = now_utc.replace(hour=report_h, minute=report_m, second=0, microsecond=0)
        except ValueError:
            log.error("Неверный формат REPORT_TIME_UTC. Используйте HH:MM. Отчеты отключены.")
            return

        if now_utc > report_time:
            report_time += timedelta(days=1)

        wait_seconds = (report_time - now_utc).total_seconds()
        log.info(f"Следующий суточный отчёт будет отправлен в {REPORT_TIME_UTC} UTC (через {wait_seconds/3600:.2f} ч).")
        await asyncio.sleep(wait_seconds)

        report_data = state.get("daily_report_data", [])
        if not report_data:
            await notify_all(f"📊 <b>Суточный отчёт ({STRAT_VERSION})</b> 📊\n\nЗа последние 24 часа сделок не было.", app.bot)
            await asyncio.sleep(60)
            continue

        total_pnl_usd = sum(item.get('pnl_usd', 0) for item in report_data)
        total_trades = len(report_data)
        wins = sum(1 for item in report_data if item.get('pnl_usd', 0) > 0)
        losses = total_trades - wins
        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
        
        total_investment = sum(item.get('entry_usd', 0) for item in report_data)
        pnl_percent = (total_pnl_usd / total_investment) * 100 if total_investment > 0 else 0
        
        report_msg = (f"📊 <b>Суточный отчёт по стратегии {STRAT_VERSION}</b> 📊\n\n"
                      f"<b>Период:</b> последние 24 часа\n"
                      f"<b>Всего сделок:</b> {total_trades} (📈{wins} / 📉{losses})\n"
                      f"<b>Винрейт:</b> {win_rate:.2f}%\n\n"
                      f"<b>Финансовый результат:</b>\n"
                      f"💵 <b>Net P&L ($): {total_pnl_usd:+.2f}$</b>\n"
                      f"💹 <b>ROI (%): {pnl_percent:+.2f}%</b>")
        await notify_all(report_msg, app.bot)

        state["daily_report_data"] = []
        save_state()
        await asyncio.sleep(60)

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
    try:
        args_dict = dict(arg.split('=') for arg in ctx.args)
        deposit = float(args_dict['deposit'])
        leverage = int(args_dict['leverage'])
        tp_price = float(args_dict['tp'])
        sl_price = float(args_dict['sl'])
        side = args_dict.get('side', 'BUY').upper()

        if side not in ['BUY', 'SELL']:
            await update.message.reply_text("❌ Ошибка: 'side' должен быть BUY или SELL."); return

        await update.message.reply_text(f"🛠 <b>ЗАПУСК ТЕСТОВОЙ СДЕЛКИ</b>...", parse_mode="HTML")

        exchange = await initialize_exchange()
        if not exchange:
            await update.message.reply_text("🔴 Ошибка: не удалось подключиться к бирже."); return

        await set_leverage_on_exchange(exchange, PAIR_SYMBOL, leverage)
        
        ticker = await exchange.fetch_ticker(PAIR_SYMBOL)
        entry_price = ticker['last']

        signal = {
            "side": side, "entry_price": entry_price, "sl_price": sl_price,
            "tp_price": tp_price, "deposit_usd": deposit, "leverage": leverage,
        }

        order_id = await execute_trade(exchange, signal)
        
        await set_leverage_on_exchange(exchange, PAIR_SYMBOL, state['leverage'])
        await exchange.close()

        if order_id:
            signal['id'] = order_id
            state["active_trade"] = signal
            save_state()
        else:
            await update.message.reply_text("🔴 Ошибка размещения тестового ордера. Проверьте логи.", parse_mode="HTML")

    except Exception as e:
        log.error(f"Ошибка в команде test_trade: {e}")
        await update.message.reply_text(f"⚠️ Ошибка формата команды.\n<b>Пример:</b> /test_trade deposit=30 leverage=10 tp=65000 sl=60000 side=BUY", parse_mode="HTML")

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
    setup_google_sheets()
    log.info("Бот инициализирован.")
    await notify_all(f"✅ Бот <b>{STRAT_VERSION}</b> перезапущен.", bot=app.bot)
    if state.get("monitoring"):
        log.info("Обнаружен активный статус мониторинга, запускаю основной цикл...")
        asyncio.create_task(monitor(app))
    asyncio.create_task(daily_reporter(app))

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
