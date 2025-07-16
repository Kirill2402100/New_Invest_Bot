#!/usr/bin/env python3
# ============================================================================
# Flat-Liner v11.7 • 16 Jul 2025
# ============================================================================
# • СТРАТЕГИЯ: Флэтовая стратегия 'Flat_BB_Fade' с обязательным фильтром по ADX
# • БИРЖА: OKX (финальная версия для нового хостинга)
# • АВТОТРЕЙДИНГ: Полная интеграция с API для размещения ордеров
# • ИСПРАВЛЕНИЕ v11.7:
#   - Переработан механизм запуска/остановки с использованием asyncio.Event
#     для полного контроля над жизненным циклом и надежного грациозного
#     завершения, что решает ошибки 'Conflict' и 'RuntimeError'.
# ============================================================================

import os
import json
import logging
import asyncio
import traceback
import signal
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
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW = os.getenv("CHAT_IDS", "")
PAIR_SYMBOL = os.getenv("PAIR_SYMBOL", "BTC-USDT-SWAP") # Формат OKX
TIMEFRAME = os.getenv("TIMEFRAME", "5m")
STRAT_VERSION = "v11_7_flatliner_okx_render"
SHEET_ID = os.getenv("SHEET_ID")

# --- OKX API ---
OKX_API_KEY = os.getenv("OKX_API_KEY")
OKX_API_SECRET = os.getenv("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")
OKX_DEMO_MODE = os.getenv("OKX_DEMO_MODE", "0")

# --- Параметры стратегии ---
DEFAULT_DEPOSIT_USD = float(os.getenv("DEFAULT_DEPOSIT_USD", "50.0"))
DEFAULT_LEVERAGE = float(os.getenv("DEFAULT_LEVERAGE", "100.0"))
FLAT_RR_RATIO = float(os.getenv("FLAT_RR_RATIO", "1.0"))
FLAT_SL_PCT = float(os.getenv("FLAT_SL_PCT", "0.10"))
FLAT_RSI_OVERSOLD = float(os.getenv("FLAT_RSI_OVERSOLD", "35"))
FLAT_RSI_OVERBOUGHT = float(os.getenv("FLAT_RSI_OVERBOUGHT", "65"))
REPORT_TIME_UTC = os.getenv("REPORT_TIME_UTC", "21:00")

# ── НАСТРОЙКА ЛОГИРОВАНИЯ ────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(STRAT_VERSION)

if not all([BOT_TOKEN, CHAT_IDS_RAW, OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE]):
    log.critical("КРИТИЧЕСКАЯ ОШИБКА: Одна или несколько переменных окружения не установлены!"); raise SystemExit

CHAT_IDS = {int(cid) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

# ── GOOGLE SHEETS ────────────────────────────────────────────────────────────
TRADE_LOG_WS = None
def setup_google_sheets() -> None:
    global TRADE_LOG_WS
    if not SHEET_ID or not os.getenv("GOOGLE_CREDENTIALS"):
        log.warning("ID таблицы или учетные данные Google не установлены. Логирование в Google Sheets отключено.")
        return
    try:
        creds_json = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        gs = gspread.authorize(creds)
        ss = gs.open_by_key(SHEET_ID)
        headers = ["Signal_ID", "Version", "Strategy_Used", "Status", "Side", "Entry_Time_UTC", "Exit_Time_UTC", "Entry_Price", "Exit_Price", "SL_Price", "TP_Price", "Gross_P&L_USD", "Fee_USD", "Net_P&L_USD", "Entry_Deposit_USD", "Entry_ADX", "ADX_Threshold"]
        ws_name = f"OKX_Trades_{PAIR_SYMBOL.replace('-','')}"
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
        log.error(f"Ошибка инициализации Google Sheets: {e}"); TRADE_LOG_WS = None

# ── УПРАВЛЕНИЕ СОСТОЯНИЕМ ───────────────────────────────────────────────────
STATE_FILE = f"state_{STRAT_VERSION}_{PAIR_SYMBOL.replace('-','')}.json"
state = {"monitoring": False, "active_trade": None, "leverage": DEFAULT_LEVERAGE, "deposit_usd": DEFAULT_DEPOSIT_USD, "dynamic_adx_threshold": 25.0, "last_adx_recalc_time": None, "daily_report_data": []}
def save_state():
    with open(STATE_FILE, 'w') as f: json.dump(state, f, indent=2)
def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f: state.update(json.load(f))
        except (json.JSONDecodeError, TypeError): save_state()
    else: save_state()

# ── ИНДИКАТОРЫ ───────────────────────────────────────────────────
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.ta.rsi(length=14, append=True, col_names=("RSI",))
    df.ta.adx(length=14, append=True, col_names=("ADX", "DMP", "DMN"))
    df.ta.bbands(length=20, std=2, append=True, col_names=("BBL", "BBM", "BBU", "BBB", "BBP"))
    return df.dropna()

# ── УВЕДОМЛЕНИЯ ──────────────────────────────────────────────────────────────
async def notify_all(text: str, bot: Bot = None):
    temp_bot = bot if bot else Bot(token=BOT_TOKEN)
    for cid in CHAT_IDS:
        try: await temp_bot.send_message(cid, text, parse_mode="HTML")
        except Exception as e: log.error(f"TG fail -> {cid}: {e}")

# ── ВЗАИМОДЕЙСТВИЕ С БИРЖЕЙ (OKX) ───────────────────────────────────────────
async def initialize_exchange():
    try:
        exchange = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE, 'options': {'defaultType': 'swap'}})
        exchange.set_sandbox_mode(OKX_DEMO_MODE == '1')
        await exchange.load_markets()
        log.info(f"Биржа OKX инициализирована. Демо: {OKX_DEMO_MODE == '1'}. CCXT: {ccxt.__version__}")
        return exchange
    except Exception as e:
        log.critical(f"Критическая ошибка инициализации биржи: {e}"); return None

async def set_leverage_on_exchange(exchange, symbol, leverage):
    try:
        await exchange.set_leverage(leverage, symbol, {'mgnMode': 'isolated', 'posSide': 'long'})
        await exchange.set_leverage(leverage, symbol, {'mgnMode': 'isolated', 'posSide': 'short'})
        log.info(f"На бирже установлено плечо {leverage}x для {symbol}")
        return True
    except Exception as e:
        log.error(f"Ошибка установки плеча: {e}"); return False

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
        amount_in_base_currency = position_value_usd / entry_price
        order_size_contracts = round(amount_in_base_currency / float(market['contractSize']))

        if order_size_contracts < float(market['limits']['amount']['min']):
            await notify_all(f"🔴 ОШИБКА: Рассчитанный размер ({order_size_contracts}) меньше минимального."); return None

        params = {
            'tdMode': 'isolated',
            'posSide': 'long' if side == 'LONG' else 'short',
            'slTriggerPx': str(sl_price),
            'slOrdPx': '-1',
            'tpTriggerPx': str(tp_price),
            'tpOrdPx': '-1'
        }
        
        order = await exchange.create_order(
            symbol=PAIR_SYMBOL, type='market', side='buy' if side == 'LONG' else 'sell',
            amount=order_size_contracts, params=params
        )
        
        log.info(f"Ордер успешно размещен! ID: {order['id']}")
        await notify_all(f"✅ <b>ОРДЕР РАЗМЕЩЕН</b>\n\n<b>ID:</b> {order['id']}\n<b>Тип:</b> {side}\n<b>SL:</b> {sl_price:.2f}\n<b>TP:</b> {tp_price:.2f}")
        return order['id']
    except Exception as e:
        error_details = traceback.format_exc()
        log.error(f"Ошибка размещения ордера: {e}\n{error_details}")
        await notify_all(f"🔴 ОШИБКА РАЗМЕЩЕНИЯ ОРДЕРА: <code>{e}</code>")
        return None

async def process_closed_trade(exchange, trade_details, bot):
    try:
        log.info(f"Обработка закрытой сделки. ID ордера: {trade_details['id']}")
        order_id = trade_details['id']
        closed_order = await exchange.fetch_order(order_id, PAIR_SYMBOL)
        exit_price = float(closed_order.get('average', trade_details['sl_price']))
        fee = abs(float(closed_order.get('fee', {}).get('cost', 0)))
        realized_pnl = float(closed_order['info'].get('pnl', 0))
        net_pnl = realized_pnl - fee
        status = "WIN" if net_pnl > 0 else "LOSS"
        report = {"id": order_id, "status": status, "side": trade_details['side'], "entry_price": trade_details['entry_price'], "exit_price": exit_price, "net_pnl_usd": round(net_pnl, 2), "fee_usd": round(fee, 2)}
        state["daily_report_data"].append({"pnl_usd": report["net_pnl_usd"], "entry_usd": trade_details['deposit_usd']})
        save_state()
        await notify_all(f"{'✅' if status == 'WIN' else '❌'} <b>СДЕЛКА ЗАКРЫТА</b>\n\n<b>ID:</b> {report['id']} | <b>Тип:</b> {report['side']}\n<b>Вход:</b> {report['entry_price']:.2f} | <b>Выход:</b> {report['exit_price']:.2f}\n💰 <b>Net P&L: {report['net_pnl_usd']:.2f}$</b> (Fee: {report['fee_usd']:.2f}$)", bot)
        if TRADE_LOG_WS:
            pass
    except Exception as e:
        log.error(f"Ошибка обработки закрытой сделки {trade_details['id']}: {e}")

async def recalculate_adx_threshold():
    try:
        log.info("Пересчет динамического порога ADX...")
        exchange = await initialize_exchange()
        if not exchange: return
        ohlcv = await exchange.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=2000)
        await exchange.close()
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
        df.ta.adx(length=14, append=True, col_names=("ADX", "DMP", "DMN"))
        df.dropna(inplace=True)
        if not df.empty:
            adx_values = df["ADX"]
            new_threshold = (np.percentile(adx_values, 20) + np.percentile(adx_values, 30)) / 2
            state["dynamic_adx_threshold"] = new_threshold
            state["last_adx_recalc_time"] = datetime.now(timezone.utc).isoformat()
            save_state()
            log.info(f"Новый порог ADX: {new_threshold:.2f}")
    except Exception as e:
        log.error(f"Ошибка при пересчете порога ADX: {e}")

# ── ОСНОВНОЙ ЦИКЛ БОТА ───────────────────────────────────────────────────────
async def monitor(app: Application):
    exchange = None
    try:
        exchange = await initialize_exchange()
        if not exchange:
            await notify_all("Не удалось инициализировать биржу. Мониторинг остановлен.", app.bot)
            return
        if not await set_leverage_on_exchange(exchange, PAIR_SYMBOL, state['leverage']):
            await notify_all("Не удалось установить плечо. Мониторинг остановлен.", app.bot)
            return
        
        await recalculate_adx_threshold()
        log.info("🚀 Основной цикл запущен: %s %s", PAIR_SYMBOL, TIMEFRAME)
        
        while state.get("monitoring", False):
            now_utc = datetime.now(timezone.utc)
            last_recalc_str = state.get("last_adx_recalc_time")
            if not last_recalc_str or (now_utc - datetime.fromisoformat(last_recalc_str)).total_seconds() > 3600:
                 await recalculate_adx_threshold()

            if active_trade_details := state.get("active_trade"):
                positions = await exchange.fetch_positions([PAIR_SYMBOL])
                trade_side = 'long' if active_trade_details['side'] == 'LONG' else 'short'
                active_position = next((p for p in positions if p.get('side') == trade_side and float(p.get('contracts', 0)) > 0), None)
                if not active_position:
                    log.info(f"Позиция {active_trade_details['id']} была закрыта.")
                    asyncio.create_task(process_closed_trade(exchange, active_trade_details, app.bot))
                    state["active_trade"] = None; save_state()
                await asyncio.sleep(60); continue

            ohlcv = await exchange.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=100)
            df = add_indicators(pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]))
            if len(df) < 2: await asyncio.sleep(60); continue

            last, price = df.iloc[-1], df.iloc[-1]["close"]
            if last["ADX"] >= state.get('dynamic_adx_threshold', 25):
                await asyncio.sleep(60); continue

            side = None
            if price <= last["BBL"] and last["RSI"] < FLAT_RSI_OVERSOLD: side = "LONG"
            elif price >= last["BBU"] and last["RSI"] > FLAT_RSI_OVERBOUGHT: side = "SHORT"

            if side:
                sl_price = price * (1 - FLAT_SL_PCT / 100) if side == "LONG" else price * (1 + FLAT_SL_PCT / 100)
                tp_price = price * (1 + (FLAT_SL_PCT * FLAT_RR_RATIO) / 100) if side == "LONG" else price * (1 - (FLAT_SL_PCT * FLAT_RR_RATIO) / 100)
                signal = {"side": side, "deposit_usd": state['deposit_usd'], "leverage": state['leverage'], "entry_price": price, "sl_price": sl_price, "tp_price": tp_price}
                
                await notify_all(f"🔔 <b>СИГНАЛ: {side}</b> {PAIR_SYMBOL} @ {price:.2f}", app.bot)
                order_id = await execute_trade(exchange, signal)
                if order_id:
                    state["active_trade"] = {"id": order_id, **signal}; save_state()
            
            await asyncio.sleep(60)
    
    except asyncio.CancelledError:
        log.info("Задача мониторинга отменяется...")
    except Exception as e:
        log.exception("Сбой в основном цикле мониторинга:")
    finally:
        if exchange:
            await exchange.close()
            log.info("Соединение с биржей в monitor закрыто.")
        log.info("⛔️ Основной цикл мониторинга полностью остановлен.")

# ── ЕЖЕДНЕВНЫЙ ОТЧЁТ ────────────────────────────────────────────────────────
async def daily_reporter(app: Application):
    log.info("📈 Сервис ежедневных отчётов запущен.")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            report_h, report_m = map(int, REPORT_TIME_UTC.split(':'))
            report_time = now_utc.replace(hour=report_h, minute=report_m, second=0, microsecond=0)
            if now_utc > report_time: report_time += timedelta(days=1)
            wait_seconds = (report_time - now_utc).total_seconds()
            log.info(f"Следующий суточный отчёт будет отправлен в {REPORT_TIME_UTC} UTC (через {wait_seconds/3600:.2f} ч).")
            await asyncio.sleep(wait_seconds)

            report_data = state.get("daily_report_data", [])
            if not report_data:
                await notify_all(f"📊 <b>Суточный отчёт ({STRAT_VERSION})</b>\n\nЗа последние 24 часа сделок не было.", app.bot)
                continue

            total_pnl = sum(item['pnl_usd'] for item in report_data)
            wins = sum(1 for item in report_data if item['pnl_usd'] > 0)
            win_rate = (wins / len(report_data)) * 100
            report_msg = (f"📊 <b>Суточный отчёт {STRAT_VERSION}</b>\n\n"
                          f"<b>Всего сделок:</b> {len(report_data)} (📈{wins} / 📉{len(report_data) - wins})\n"
                          f"<b>Винрейт:</b> {win_rate:.2f}%\n"
                          f"💵 <b>Net P&L: {total_pnl:+.2f}$</b>")
            await notify_all(report_msg, app.bot)
            state["daily_report_data"] = []; save_state()
        except asyncio.CancelledError:
            log.info("Задача ежедневных отчетов отменена.")
            break
        except Exception as e:
            log.error(f"Ошибка в daily_reporter: {e}")
            await asyncio.sleep(3600)

# ── КОМАНДЫ TELEGRAM ────────────────────────────────────────────────────────
async def start_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if state.get("monitoring"):
        await update.message.reply_text(f"Бот уже запущен.", parse_mode="HTML")
        return
    state["monitoring"] = True; save_state()
    await update.message.reply_text(f"✅ Бот <b>{STRAT_VERSION}</b> запущен.", parse_mode="HTML")
    asyncio.create_task(monitor(ctx.application))
async def stop_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False; save_state()
    await update.message.reply_text("❌ Бот остановлен. Основной цикл завершится после текущей итерации.")
async def status_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    status = 'АКТИВЕН' if state.get('monitoring') else 'ОСТАНОВЛЕН'
    trade_info = f"<b>Активная сделка:</b> {state['active_trade']['id']}" if state.get('active_trade') else "<i>Нет активных сделок.</i>"
    await update.message.reply_text(f"<b>СТАТУС ({STRAT_VERSION})</b>\n\n<b>Мониторинг:</b> {status}\n<b>Инструмент:</b> {PAIR_SYMBOL}\n{trade_info}", parse_mode="HTML")
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
        if not 1 <= leverage <= 125: raise ValueError
        exchange = await initialize_exchange()
        if not exchange: await update.message.reply_text("🔴 Ошибка подключения к бирже."); return
        if await set_leverage_on_exchange(exchange, PAIR_SYMBOL, leverage):
            state['leverage'] = leverage; save_state()
            await update.message.reply_text(f"✅ Плечо установлено: <b>{leverage}x</b>", parse_mode="HTML")
        else:
            await update.message.reply_text("🔴 Ошибка установки плеча.")
        await exchange.close()
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Формат: /set_leverage <1-125>")
async def test_trade_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        args_dict = dict(arg.split('=') for arg in ctx.args)
        deposit, leverage, tp_price, sl_price, side = float(args_dict['deposit']), int(args_dict['leverage']), float(args_dict['tp']), float(args_dict['sl']), args_dict.get('side', 'LONG').upper()
        if side not in ['LONG', 'SHORT']: await update.message.reply_text("❌ 'side' должен быть LONG или SHORT."); return
        await update.message.reply_text(f"🛠 <b>ЗАПУСК ТЕСТОВОЙ СДЕЛКИ</b>...", parse_mode="HTML")
        exchange = await initialize_exchange()
        if not exchange: await update.message.reply_text("🔴 Ошибка подключения к бирже."); return
        await set_leverage_on_exchange(exchange, PAIR_SYMBOL, leverage)
        ticker = await exchange.fetch_ticker(PAIR_SYMBOL)
        signal = {"side": side, "deposit_usd": deposit, "leverage": leverage, "entry_price": ticker['last'], "sl_price": sl_price, "tp_price": tp_price}
        order_id = await execute_trade(exchange, signal)
        if order_id: state["active_trade"] = {"id": order_id, **signal}; save_state()
        await exchange.close()
    except Exception as e:
        log.error(f"Ошибка в /test_trade: {e}")
        await update.message.reply_text(f"⚠️ Ошибка формата. Пример: /test_trade deposit=20 leverage=10 tp=65000 sl=60000 side=LONG", parse_mode="HTML")

async def apitest_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙️ <b>Тест API ключей OKX...</b>", parse_mode="HTML")
    exchange = await initialize_exchange()
    if not exchange:
        await update.message.reply_text("🔴 Не удалось инициализировать биржу."); return
    try:
        await update.message.reply_text("Попытка получить баланс...", parse_mode="HTML")
        balance = await exchange.fetch_balance()
        balance_str = json.dumps(balance, indent=2, ensure_ascii=False)
        if len(balance_str) > 4000:
            balance_str = balance_str[:4000] + "\n... (ответ обрезан)"
        await update.message.reply_text(
            f"✅ <b>УСПЕХ!</b>\nПодключение к OKX прошло успешно.\n\n"
            f"<b>Полная структура баланса:</b>\n<pre>{balance_str}</pre>",
            parse_mode="HTML"
        )
    except Exception as e:
        log.error(f"Ошибка в /apitest: {e}")
        await update.message.reply_text(f"❌ <b>ОШИБКА:</b> <code>{e}</code>", parse_mode="HTML")
    finally:
        if exchange:
            await exchange.close()

async def post_init(app: Application):
    load_state()
    setup_google_sheets()
    log.info("Бот инициализирован.")
    await notify_all(f"✅ Бот <b>{STRAT_VERSION}</b> перезапущен.", bot=app.bot)
    if state.get("monitoring"):
        asyncio.create_task(monitor(app))
    asyncio.create_task(daily_reporter(app))

async def shutdown_handler(app: Application):
    """Handles graceful shutdown."""
    log.warning("Получен сигнал на остановку. Начинаю грациозное завершение...")
    await notify_all(f"⚠️ Бот <b>{STRAT_VERSION}</b> перезапускается/останавливается.", bot=app.bot)

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    log.info(f"Отменяю {len(tasks)} фоновых задач...")
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("Все фоновые задачи завершены.")

    state["monitoring"] = False
    save_state()
    log.info("Финальное состояние сохранено. Бот выключен.")

async def main() -> None:
    """Запускает бота и управляет его жизненным циклом."""
    
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Настраиваем обработку сигналов для грациозного завершения
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)

    async with app:
        log.info("Запуск бота...")
        await app.start()
        await stop
        log.info("Получен сигнал на остановку. Завершаю работу...")
        await shutdown_handler(app)
        await app.stop()
        log.info("Бот полностью остановлен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Процесс прерван пользователем или системой.")
