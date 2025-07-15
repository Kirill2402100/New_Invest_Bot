#!/usr/bin/env python3
# ============================================================================
# Flat-Liner v7.4 • 15 Jul 2025
# ============================================================================
# • СТРАТЕГИЯ: Флэтовая стратегия 'Flat_BB_Fade' с обязательным фильтром по ADX
# • БИРЖА: OKX (Production)
# • АВТОТРЕЙДИНГ: Полная интеграция с API для размещения ордеров
# • УПРАВЛЕНИЕ: Команды для настройки депозита, плеча и тестовой торговли
# • ИСПРАВЛЕНИЕ v7.4:
#   - [ГЛАВНЫЙ ФИКС] Исправлено имя метода для pre-check на camelCase
#     (privatePostTradeOrderPrecheck), как того требует ccxt.
# ============================================================================

import os
import json
import logging
import re
import uuid
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
PAIR_SYMBOL   = os.getenv("PAIR_SYMBOL", "BTC-USDT-SWAP") # Формат OKX
TIMEFRAME     = os.getenv("TIMEFRAME", "5m")
STRAT_VERSION = "v7_4_flatliner_okx"
SHEET_ID      = os.getenv("SHEET_ID")


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
FLAT_SL_PCT         = float(os.getenv("FLAT_SL_PCT", "0.10")) # Фиксированный SL 0.10%
FLAT_RSI_OVERSOLD   = float(os.getenv("FLAT_RSI_OVERSOLD", "35"))
FLAT_RSI_OVERBOUGHT = float(os.getenv("FLAT_RSI_OVERBOUGHT", "65"))
REPORT_TIME_UTC     = os.getenv("REPORT_TIME_UTC", "21:00")


# ── НАСТРОЙКА ЛОГИРОВАНИЯ ────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(STRAT_VERSION)

# --- Проверка критических переменных окружения ---
if not all([BOT_TOKEN, CHAT_IDS_RAW, OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE]):
    log.critical("Одна или несколько переменных окружения (BOT_TOKEN, CHAT_IDS, OKX API) не установлены!"); raise SystemExit

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

        headers = [
            "Signal_ID", "Version", "Strategy_Used", "Status", "Side",
            "Entry_Time_UTC", "Exit_Time_UTC",
            "Entry_Price", "Exit_Price", "SL_Price", "TP_Price",
            "Gross_P&L_USD", "Fee_USD", "Net_P&L_USD", "Entry_Deposit_USD",
            "Entry_ADX", "ADX_Threshold"
        ]

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
        log.error(f"Ошибка инициализации Google Sheets: {e}")
        TRADE_LOG_WS = None

# ── УПРАВЛЕНИЕ СОСТОЯНИЕМ ───────────────────────────────────────────────────
STATE_FILE = f"state_{STRAT_VERSION}_{PAIR_SYMBOL.replace('-','')}.json"
state = {
    "monitoring": False,
    "active_trade": None,
    "leverage": DEFAULT_LEVERAGE,
    "deposit_usd": DEFAULT_DEPOSIT_USD,
    "dynamic_adx_threshold": 25.0,
    "last_adx_recalc_time": None,
    "daily_report_data": []
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
RSI_LEN, BBANDS_LEN, ADX_LEN = 14, 20, 14

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.ta.rsi(length=RSI_LEN, append=True, col_names=(f"RSI_{RSI_LEN}",))
    df.ta.adx(length=ADX_LEN, append=True, col_names=(f"ADX_{ADX_LEN}", "DMP", "DMN"))
    df.ta.bbands(length=BBANDS_LEN, std=2, append=True,
                col_names=(f"BBL_{BBANDS_LEN}_2.0", f"BBM_{BBANDS_LEN}_2.0",
                           f"BBU_{BBANDS_LEN}_2.0", f"BBB_{BBANDS_LEN}_2.0",
                           f"BBP_{BBANDS_LEN}_2.0"))
    return df.dropna()

# ── ВЗАИМОДЕЙСТВИЕ С БИРЖЕЙ (OKX) ──────────────────────────────────────────
async def initialize_exchange():
    try:
        exchange = ccxt.okx({
            'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE,
            'options': {'defaultType': 'swap'},
        })
        exchange.set_sandbox_mode(OKX_DEMO_MODE == '1')
        await exchange.load_markets() # Загружаем данные о рынках
        log.info(f"Биржа OKX инициализирована. Демо-режим: {'ВКЛЮЧЕН' if OKX_DEMO_MODE == '1' else 'ВЫКЛЮЧЕН'}.")
        return exchange
    except Exception as e:
        log.critical(f"Критическая ошибка инициализации биржи: {e}")
        return None

async def set_position_mode(exchange):
    try:
        await exchange.set_position_mode(hedged=True, symbol=PAIR_SYMBOL)
        log.info("Режим позиции на бирже успешно установлен: 'long_short_mode' (хеджирование).")
        return True
    except Exception as e:
        if '51033' in str(e):
             log.info("Режим позиции уже был установлен в 'long_short_mode'.")
             return True
        log.error(f"Ошибка установки режима позиции: {e}")
        return False

async def set_leverage_on_exchange(exchange, symbol, leverage):
    try:
        await exchange.set_leverage(leverage, symbol, {'mgnMode': 'isolated', 'posSide': 'long'})
        await exchange.set_leverage(leverage, symbol, {'mgnMode': 'isolated', 'posSide': 'short'})
        log.info(f"На бирже установлено плечо {leverage}x для {symbol} (long/short)")
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
        contract_val = float(market['contractSize'])
        min_order_size = float(market['limits']['amount']['min'])
        
        position_value_usd = deposit * leverage
        amount_in_base_currency = position_value_usd / entry_price
        order_size_contracts = round(amount_in_base_currency / contract_val)

        if order_size_contracts < min_order_size:
            await notify_all(f"🔴 ОШИБКА РАСЧЕТА\n\nРассчитанный размер ({order_size_contracts}) меньше минимального ({min_order_size}).")
            return None

        pre_check_params = {
            'instId': PAIR_SYMBOL, 'tdMode': 'isolated', 'side': 'buy' if side == 'LONG' else 'sell',
            'posSide': 'long' if side == 'LONG' else 'short', 'ordType': 'market', 'sz': str(order_size_contracts)
        }
        log.info(f"Выполнение предварительной проверки ордера: {pre_check_params}")

        # [ГЛАВНЫЙ ФИКС] Исправлено имя метода на camelCase, как того требует ccxt
        pre_check_result = await exchange.privatePostTradeOrderPrecheck([pre_check_params])
        
        if pre_check_result.get('code') != '0' or (pre_check_result.get('data') and pre_check_result['data'][0].get('sCode') != '0'):
             error_msg = pre_check_result['data'][0]['sMsg'] if pre_check_result.get('data') and pre_check_result['data'][0].get('sMsg') else pre_check_result.get('msg', 'Неизвестная ошибка pre-check')
             log.error(f"Предварительная проверка не пройдена: {error_msg}")
             await notify_all(f"🔴 <b>ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА НЕ ПРОЙДЕНА</b>\n\n<b>Причина:</b> {error_msg}")
             return None
        log.info("Предварительная проверка пройдена успешно.")

        params = {
            'tdMode': 'isolated', 'posSide': 'long' if side == 'LONG' else 'short',
            'attachAlgoOrds': [{'slTriggerPx': str(sl_price), 'slOrdPx': '-1'}, {'tpTriggerPx': str(tp_price), 'tpOrdPx': '-1'}]
        }
        
        log.info(f"Попытка разместить ордер: {side} {order_size_contracts} контрактов {PAIR_SYMBOL} по рынку.")
        order = await exchange.create_order(
            symbol=PAIR_SYMBOL, type='market', side='buy' if side == 'LONG' else 'sell',
            amount=order_size_contracts, params=params
        )
        
        log.info(f"Ордер успешно размещен! ID: {order['id']}")
        return order['id']

    except Exception as e:
        log.error(f"Ошибка размещения ордера: {e}")
        await notify_all(f"🔴 ОШИБКА РАЗМЕЩЕНИЯ ОРДЕРА\n\n<b>Инструмент:</b> {PAIR_SYMBOL}\n<b>Тип:</b> {side}\n<b>Ошибка:</b> <code>{e}</code>")
        return None

async def process_closed_trade(exchange, trade_details, bot):
    try:
        log.info(f"Обработка закрытой сделки. ID ордера: {trade_details['id']}")
        order_id = trade_details['id']
        
        trades = await exchange.fetch_my_trades(symbol=PAIR_SYMBOL, params={'ordId': order_id})
        if not trades:
            log.warning(f"Не удалось получить данные о сделке для ордера {order_id}. Использую данные из сигнала.")
            closed_order = await exchange.fetch_order(order_id, PAIR_SYMBOL)
            exit_price = float(closed_order.get('average', trade_details['sl_price']))
            fee = abs(float(closed_order.get('fee', {}).get('cost', 0)))
            realized_pnl = float(closed_order['info'].get('pnl', 0))
        else:
            trade = trades[-1]
            exit_price = float(trade['price'])
            fee = abs(float(trade.get('fee', {}).get('cost', 0)))
            realized_pnl = float(trade['info'].get('pnl', 0))

        gross_pnl = realized_pnl
        net_pnl = gross_pnl - fee
        status = "WIN" if net_pnl > 0 else "LOSS"

        report = {
            "id": order_id, "strategy_name": "Flat_BB_Fade", "status": status,
            "side": trade_details['side'], "entry_time_utc": trade_details['entry_time_utc'],
            "entry_price": trade_details['entry_price'], "exit_price": exit_price,
            "sl_price": trade_details['sl_price'], "tp_price": trade_details['tp_price'],
            "gross_pnl_usd": round(gross_pnl, 2), "fee_usd": round(fee, 2),
            "net_pnl_usd": round(net_pnl, 2), "entry_deposit_usd": trade_details['deposit_usd'],
            "entry_adx": trade_details['entry_adx'], "adx_threshold": trade_details['adx_threshold']
        }

        state["daily_report_data"].append({"pnl_usd": report["net_pnl_usd"], "entry_usd": report["entry_deposit_usd"]})
        save_state()

        pnl_text = f"💰 <b>Net P&L: {report['net_pnl_usd']:.2f}$</b> (Fee: {report['fee_usd']:.2f}$)"
        msg_icon = "✅" if status == "WIN" else "❌"
        
        await notify_all(
            f"{msg_icon} <b>СДЕЛКА ЗАКРЫТА</b> {msg_icon}\n\n"
            f"<b>ID:</b> {report['id']}\n<b>Тип:</b> {report['side']}\n"
            f"<b>Вход:</b> {report['entry_price']:.2f}\n<b>Выход:</b> {report['exit_price']:.2f}\n"
            f"{pnl_text}", bot
        )
        
        if TRADE_LOG_WS:
            row = [
                report["id"], STRAT_VERSION, report["strategy_name"], report["status"], report["side"],
                report["entry_time_utc"], datetime.now(timezone.utc).isoformat(),
                report["entry_price"], report["exit_price"], report["sl_price"], report["tp_price"],
                report["gross_pnl_usd"], report["fee_usd"], report["net_pnl_usd"], report["entry_deposit_usd"],
                report["entry_adx"], report["adx_threshold"]
            ]
            await asyncio.to_thread(TRADE_LOG_WS.append_row, row, value_input_option="USER_ENTERED")
            log.info(f"Сделка {report['id']} залогирована в Google Sheets.")

    except Exception as e:
        log.error(f"Ошибка обработки закрытой сделки {trade_details['id']}: {e}")
        await notify_all(f"� Ошибка обработки закрытой сделки {trade_details['id']}. Проверьте логи.", bot)

async def recalculate_adx_threshold():
    try:
        log.info("Пересчет динамического порога ADX...")
        exchange = await initialize_exchange()
        if not exchange: return
        ohlcv = await exchange.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=2000)
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
        await notify_all("Не удалось инициализировать биржу. Бот остановлен.", app.bot); return
    
    if not await set_position_mode(exchange):
        await notify_all("Не удалось установить режим позиции. Бот остановлен.", app.bot); await exchange.close(); return

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

            if active_trade_details := state.get("active_trade"):
                positions = await exchange.fetch_positions([PAIR_SYMBOL])
                trade_side = 'long' if active_trade_details['side'] == 'LONG' else 'short'
                active_position_on_exchange = next((p for p in positions if p['symbol'] == PAIR_SYMBOL and p.get('side') == trade_side and float(p.get('contracts', 0)) > 0), None)

                if not active_position_on_exchange:
                    log.info(f"Отслеживаемая позиция {active_trade_details['id']} была закрыта. Запускаю обработку...")
                    state["active_trade"] = None
                    save_state()
                    asyncio.create_task(process_closed_trade(exchange, active_trade_details, app.bot))
            
            else:
                ohlcv = await exchange.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=100)
                df = add_indicators(pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]))
                if len(df) < 2: await asyncio.sleep(60); continue

                last = df.iloc[-1]
                adx = last[f"ADX_{ADX_LEN}"]
                
                if adx >= state['dynamic_adx_threshold']:
                    log.info(f"Рынок в тренде (ADX={adx:.2f} >= {state['dynamic_adx_threshold']:.2f}). Пропускаем поиск сигнала.")
                    await asyncio.sleep(60)
                    continue

                price = last["close"]
                rsi = last[f"RSI_{RSI_LEN}"]
                bb_lower = last[f"BBL_{BBANDS_LEN}_2.0"]
                bb_upper = last[f"BBU_{BBANDS_LEN}_2.0"]

                side = None
                if price <= bb_lower and rsi < FLAT_RSI_OVERSOLD: side = "LONG"
                elif price >= bb_upper and rsi > FLAT_RSI_OVERBOUGHT: side = "SHORT"

                if side:
                    sl_pct = FLAT_SL_PCT
                    tp_pct = sl_pct * FLAT_RR_RATIO
                    
                    sl_price = price * (1 - sl_pct / 100) if side == "LONG" else price * (1 + sl_pct / 100)
                    tp_price = price * (1 + tp_pct / 100) if side == "LONG" else price * (1 - tp_pct / 100)
                    
                    signal = {
                        "side": side, "entry_price": price, "sl_price": sl_price,
                        "tp_price": tp_price, "deposit_usd": state['deposit_usd'], "leverage": state['leverage'],
                        "entry_time_utc": datetime.now(timezone.utc).isoformat(),
                        "entry_adx": round(adx, 2), "adx_threshold": round(state['dynamic_adx_threshold'], 2)
                    }
                    
                    await notify_all(f"🔔 <b>ПОЛУЧЕН СИГНАЛ: {side}</b>\n\n"
                                     f"<b>Инструмент:</b> {PAIR_SYMBOL}\n<b>Цена:</b> {price:.2f}\n"
                                     f"<b>TP:</b> {tp_price:.2f} | <b>SL:</b> {sl_price:.2f}\n"
                                     f"<b>Депозит:</b> {state['deposit_usd']}$ | <b>Плечо:</b> {state['leverage']}x\n"
                                     f"<i>ADX: {signal['entry_adx']} (Порог: {signal['adx_threshold']})</i>\n\n"
                                     f"Отправка ордера на биржу...", app.bot)

                    order_id = await execute_trade(exchange, signal)
                    if order_id:
                        signal['id'] = order_id
                        state["active_trade"] = signal
                        save_state()
                        await notify_all(f"✅ <b>ОРДЕР УСПЕШНО РАЗМЕЩЕН</b>\n\n<b>ID ордера:</b> {order_id}", app.bot)
        
        except ccxt.NetworkError as e:
            log.warning("CCXT ошибка сети: %s", e)
        except Exception as e:
            log.exception("Сбой в основном цикле:")
        
        await asyncio.sleep(60)

    await exchange.close()
    log.info("⛔️ Основной цикл остановлен. Соединение с биржей закрыто.")

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

        total_pnl_usd = sum(item['pnl_usd'] for item in report_data)
        total_trades = len(report_data)
        wins = sum(1 for item in report_data if item['pnl_usd'] > 0)
        losses = total_trades - wins
        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
        
        total_investment = sum(item['entry_usd'] for item in report_data)
        pnl_percent = (total_pnl_usd / total_investment) * 100 if total_investment > 0 else 0
        
        report_msg = (
            f"📊 <b>Суточный отчёт по стратегии {STRAT_VERSION}</b> 📊\n\n"
            f"<b>Период:</b> последние 24 часа\n"
            f"<b>Всего сделок:</b> {total_trades} (📈{wins} / 📉{losses})\n"
            f"<b>Винрейт:</b> {win_rate:.2f}%\n\n"
            f"<b>Финансовый результат:</b>\n"
            f"💵 <b>Net P&L ($): {total_pnl_usd:+.2f}$</b>\n"
            f"💹 <b>ROI (%): {pnl_percent:+.2f}%</b>"
        )
        await notify_all(report_msg, app.bot)

        state["daily_report_data"] = []
        save_state()
        await asyncio.sleep(60)

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
    status_msg += f"<b>Кредитное плечо:</b> {state['leverage']}x\n"
    status_msg += f"<b>Порог ADX:</b> {state['dynamic_adx_threshold']:.2f}\n\n"

    if trade := state.get("active_trade"):
        status_msg += f"<b>Активная сделка (ID):</b> {trade['id']}\n"
        status_msg += f"<b>Вход:</b> {trade['entry_price']:.2f}"
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
            await update.message.reply_text("❌ Ошибка: для этого инструмента плечо должно быть в диапазоне от 1 до 125.")
            return
        
        exchange = await initialize_exchange()
        if not exchange:
            await update.message.reply_text("🔴 Ошибка: не удалось подключиться к бирже."); return
            
        success = await set_leverage_on_exchange(exchange, PAIR_SYMBOL, leverage)
        await exchange.close()

        if success:
            state['leverage'] = leverage
            save_state()
            await update.message.reply_text(f"✅ Кредитное плечо установлено: <b>{leverage}x</b>", parse_mode="HTML")
        else:
            await update.message.reply_text("🔴 Ошибка установки плеча на бирже. Проверьте логи.")

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
            await update.message.reply_text("❌ Ошибка: 'side' должен быть LONG или SHORT."); return

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
            "entry_time_utc": datetime.now(timezone.utc).isoformat(),
            "entry_adx": 0, "adx_threshold": 0
        }

        order_id = await execute_trade(exchange, signal)
        
        await set_leverage_on_exchange(exchange, PAIR_SYMBOL, state['leverage'])
        await exchange.close()

        if order_id:
            signal['id'] = order_id
            state["active_trade"] = signal
            save_state()
            await update.message.reply_text(f"✅ <b>ТЕСТОВЫЙ ОРДЕР РАЗМЕЩЕН</b>\n\n<b>ID ордера:</b> {order_id}\nНе забудьте закрыть позицию вручную!", parse_mode="HTML")
        else:
            await update.message.reply_text("🔴 Ошибка размещения тестового ордера. Проверьте логи.", parse_mode="HTML")

    except Exception as e:
        log.error(f"Ошибка в команде test_trade: {e}")
        await update.message.reply_text(f"⚠️ Ошибка формата команды.\n<b>Пример:</b> /test_trade deposit=30 leverage=80 tp=120000 sl=100000 side=LONG",
                                        parse_mode="HTML")

# ── ЗАПУСК БОТА ──────────────────────────────────────────────────────────────
async def post_init(app: Application):
    load_state()
    setup_google_sheets()
    log.info("Бот инициализирован. Состояние загружено.")
    await notify_all(f"✅ Бот <b>{STRAT_VERSION}</b> перезапущен.", bot=app.bot)
    if state.get("monitoring"):
        log.info("Обнаружен активный статус мониторинга, запускаю основной цикл...")
        asyncio.create_task(monitor(app))
    
    asyncio.create_task(daily_reporter(app))

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
