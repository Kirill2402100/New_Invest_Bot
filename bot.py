#!/usr/bin/env python3 
# ============================================================================
#  Flat‑Liner • MEXC Edition — 18 Jul 2025
#  Стратегия  : Flat_BB_Fade  +  динамический ADX‑фильтр  +  RR 1:1
#  Биржа      : MEXC (USDT-M Futures)
#  Команды    : /start /stop /status /set_deposit /set_leverage /test_trade
#  Автор      : Kirill2402100  |  MIT Licence
# ============================================================================

"""
• TP/SL устанавливаются вместе с рыночным ордером.
• Один активный трейд.  При закрытии:
    ▸ P&L сообщение + запись в daily_pnls (для суточного отчёта)
    ▸ строка OPEN / CLOSE в Google-Sheets.
• Переменные окружения:
    BOT_TOKEN, CHAT_IDS
    MEXC_API_KEY / SECRET, MEXC_DEMO_MODE
    VIRTUAL_TRADING_MODE (1 - для виртуальной торговли)
    SHEET_ID, GOOGLE_CREDENTIALS
"""

import os, json, logging, asyncio, math, sys, signal
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt
import gspread
from telegram import Bot, Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

# ─────────────────── CONFIG ───────────────────────────────────────────────
BOT_TOKEN   = os.getenv("BOT_TOKEN")
CHAT_IDS    = {int(cid) for cid in os.getenv("CHAT_IDS", "").split(",") if cid}

# !!! ВАЖНО: Формат пары для MEXC Futures отличается. Пример: 'BTC/USDT:USDT'
PAIR_SYMBOL = os.getenv("PAIR_SYMBOL", "BTC/USDT:USDT")
TIMEFRAME   = os.getenv("TIMEFRAME",   "5m")

# --- Ключи для MEXC ---
MEXC_API_KEY     = os.getenv("MEXC_API_KEY")
MEXC_API_SECRET  = os.getenv("MEXC_API_SECRET")
MEXC_SANDBOX     = os.getenv("MEXC_DEMO_MODE", "0") == "1" # 1 = Демо-счет

# --- Режим торговли ---
VIRTUAL_TRADING_MODE = os.getenv("VIRTUAL_TRADING_MODE", "0") == "1" # 1 = Виртуальная торговля

DEFAULT_DEPOSIT  = float(os.getenv("DEFAULT_DEPOSIT_USD", 50))
DEFAULT_LEVERAGE = int  (os.getenv("DEFAULT_LEVERAGE",    100))

SL_PCT, RR_RATIO = 0.10, 1.0
RSI_OS, RSI_OB   = 40, 60
REPORT_UTC_HOUR  = int(os.getenv("REPORT_HOUR_UTC", 21))

# Google Sheets
SHEET_ID           = os.getenv("SHEET_ID")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

STATE_FILE = Path("state_flatliner_mexc.json") # Изменено имя файла состояния

# ─────────────────── LOGGING ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("flatliner")

def _handle_sigterm(*_):
    log.info("SIGTERM received, cancelling tasks…")
    for t in asyncio.all_tasks():
        t.cancel()
    asyncio.get_event_loop().run_until_complete(asyncio.sleep(0))
    sys.exit(0)
signal.signal(signal.SIGTERM, _handle_sigterm)

# ─────────────────── STATE ───────────────────────────────────────────────
state = {
    "monitoring": True,
    "active_trade": None,
    "deposit":  DEFAULT_DEPOSIT,
    "leverage": DEFAULT_LEVERAGE,
    "adx_threshold": 25.0,
    "last_adx_recalc": None,
    "daily_pnls": [],
    "last_adx_value": 25.0
}

def save_state(): STATE_FILE.write_text(json.dumps(state, indent=2))
def load_state():
    if STATE_FILE.exists():
        try: state.update(json.loads(STATE_FILE.read_text()))
        except: log.warning("STATE‑файл повреждён → создаю новый")
    save_state()

# ─────────────────── HELPERS ─────────────────────────────────────────────
async def notify(text: str, bot: Optional[Bot] = None, parse_mode="HTML"):
    bot = bot or Bot(BOT_TOKEN)
    for cid in CHAT_IDS:
        try:
            await bot.send_message(cid, text, parse_mode=parse_mode, disable_web_page_preview=True)
        except Exception as e:
            log.error("TG-fail → %s : %s", cid, e)

# ─────────────────── Google Sheets ───────────────────────────────────────
try:
    if SHEET_ID:
        if GOOGLE_CREDENTIALS:
            _gc = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS))
        else:
            _gc = gspread.service_account(filename="gcreds.json")
        _sheet = _gc.open_by_key(SHEET_ID).sheet1
    else:
        _sheet = None
except Exception as e:
    log.error("Google Sheets init fail: %s", e); _sheet = None

def sheet_log(row:list):
    if not _sheet: return
    try: _sheet.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e: log.error("GSHEET append error: %s", e)

# ─────────────────── INDICATORS ──────────────────────────────────────────
ADX_COL, BBL_COL, BBU_COL, RSI_COL = "ADX_14", "BBL_20_2.0", "BBU_20_2.0", "RSI_14"

def df_from_ohlcv(ohlcv):
    return pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])

def add_indicators(df):
    df.ta.rsi(length=14, append=True)
    df.ta.adx(length=14, append=True)
    df.ta.bbands(length=20, std=2, append=True)
    return df.dropna()

# ─────────────────── EXCHANGE (MEXC) ────────────────────────────────────
async def create_exchange():
    """Инициализация подключения к бирже MEXC."""
    ex = ccxt.mexc({
        "apiKey":   MEXC_API_KEY,
        "secret":   MEXC_API_SECRET,
        "options":  {
            "defaultType": "swap", # Указываем, что работаем с фьючерсами
        },
    })
    ex.set_sandbox_mode(MEXC_SANDBOX)
    await ex.load_markets()
    return ex

async def set_leverage(ex, lev):
    """Установка плеча для торговой пары на MEXC."""
    await ex.set_leverage(lev, PAIR_SYMBOL, {"marginMode": "isolated"})

# ─────────────────── CORE ────────────────────────────────────────────────
async def recalc_adx_threshold():
    ex = await create_exchange()
    try:
        ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=2000)
        adx = add_indicators(df_from_ohlcv(ohlcv))[ADX_COL]
        thresh = (np.percentile(adx, 20) + np.percentile(adx, 30)) / 2
        state["adx_threshold"] = float(thresh)
        state["last_adx_recalc"] = datetime.now(timezone.utc).isoformat()
        save_state(); log.info("ADX-threshold → %.2f", thresh)
    finally:
        await ex.close()

def calc_size(market, price, deposit, leverage):
    step = float(market["limits"]["amount"]["min"])
    prec = market["precision"]["amount"]
    raw  = (deposit * leverage) / price
    size = math.floor(raw / step) * step
    return round(size, int(prec)), step

# ───── execute_trade ───────────────────────────────────────────────────
async def execute_trade(ex, side: str, price: float, entry_adx: float, bot: Bot):
    """Создать реальный или виртуальный ордер с TP/SL, записать журнал."""
    market = ex.market(PAIR_SYMBOL)
    size, step = calc_size(market, price, state["deposit"], state["leverage"])
    if size < step:
        await notify(f"🔴 Минимальный объём — {step}. Увеличьте депозит/плечо.", bot)
        return None

    sl_price = price * (1 - SL_PCT/100) if side == "LONG" else price * (1 + SL_PCT/100)
    tp_price = price * (1 + SL_PCT*RR_RATIO/100) if side == "LONG" else price * (1 - SL_PCT*RR_RATIO/100)
    
    order_id = None
    fill_px = price # Для виртуальной торговли цена входа равна текущей цене

    if not VIRTUAL_TRADING_MODE:
        # --- РЕАЛЬНАЯ ТОРГОВЛЯ ---
        order_side = "buy" if side == "LONG" else "sell"
        params = {
            'takeProfitPrice': tp_price,
            'stopLossPrice': sl_price,
            'positionMode': 'isolated'
        }
        order = await ex.create_order(PAIR_SYMBOL, "market", order_side, size, params=params)
        order_id = order["id"]
        fill_px = float(order.get("average") or order["price"])
    else:
        # --- ВИРТУАЛЬНАЯ ТОРГОВЛЯ ---
        order_id = f"virtual_{int(datetime.now().timestamp())}"
        log.info(f"Симуляция открытия ордера: {order_id}")

    trade_prefix = "⚪️ ВИРТУАЛЬНО " if VIRTUAL_TRADING_MODE else "🟢"
    await notify(
        f"{trade_prefix} ОТКРЫТ <b>{side}</b> • size {size} • entry {fill_px}\n"
        f"SL {sl_price:.4f}  |  TP {tp_price:.4f}",
        bot, parse_mode="HTML"
    )

    sheet_log([
        order_id, "Flat-Liner MEXC", "Flat_BB_Fade", "OPEN",
        side, datetime.utcnow().isoformat(), "", fill_px, "", sl_price, tp_price, "", "", "",
        state["deposit"], entry_adx, state["adx_threshold"]
    ])

    return {
        "id":          order_id,
        "side":        side,
        "entry_price": fill_px,
        "size":        size,
        "entry_time":  datetime.utcnow().isoformat(),
        "sl_price":    sl_price,
        "tp_price":    tp_price,
        "entry_adx":   entry_adx,
        "deposit":     state["deposit"]
    }

# ─────────────────── MONITOR (авто-сделки) ───────────────────────────────
async def monitor(app: Application):
    ex = await create_exchange()
    await set_leverage(ex, state["leverage"])
    await recalc_adx_threshold()
    mode_text = "Виртуальный режим" if VIRTUAL_TRADING_MODE else "Реальный режим"
    log.info(f"🚀 Мониторинг запущен (Биржа: MEXC, Режим: {mode_text})")

    prev_regime: Optional[str] = None

    try:
        while state["monitoring"]:
            try:
                ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=100)
                if not ohlcv:
                    log.warning("Получен пустой список OHLCV. Пропускаю.")
                    await asyncio.sleep(60)
                    continue
                df = add_indicators(df_from_ohlcv(ohlcv))
                if df.empty:
                    log.warning("DataFrame пуст после добавления индикаторов. Пропускаю.")
                    await asyncio.sleep(60)
                    continue
                last = df.iloc[-1]
                state["last_adx_value"] = float(last[ADX_COL])
            except Exception as e:
                log.error("Ошибка получения OHLCV в цикле monitor: %s", e)
                await asyncio.sleep(60)
                continue

            # ... (остальная логика monitor без изменений до контроля позиции) ...
            regime = "FLAT" if last[ADX_COL] < state["adx_threshold"] else "TREND"
            if prev_regime is None: prev_regime = regime
            elif regime != prev_regime:
                prev_regime = regime
                await notify(f"🔄 Режим изменён на <b>{regime}</b>...", app.bot)
                log.info("Regime change → %s", regime)

            last_recalc = state["last_adx_recalc"]
            if (not last_recalc or (datetime.now(timezone.utc) - datetime.fromisoformat(last_recalc)).total_seconds() > 3600):
                await recalc_adx_threshold()

            # --- Контроль активной позиции ---
            if (tr := state.get("active_trade")):
                if VIRTUAL_TRADING_MODE:
                    # --- ВИРТУАЛЬНЫЙ РЕЖИМ: Проверяем пересечение SL/TP ---
                    price = last["close"]
                    closed = False
                    exit_reason = ""
                    if tr['side'] == 'LONG':
                        if price >= tr['tp_price']: closed, exit_reason = True, "TP"
                        elif price <= tr['sl_price']: closed, exit_reason = True, "SL"
                    elif tr['side'] == 'SHORT':
                        if price <= tr['tp_price']: closed, exit_reason = True, "TP"
                        elif price >= tr['sl_price']: closed, exit_reason = True, "SL"
                    
                    if closed:
                        log.info(f"Виртуальная позиция {tr['id']} закрыта по {exit_reason}.")
                        await notify(f"⚪️ ВИРТУАЛЬНАЯ ПОЗИЦИЯ ЗАКРЫТА по {exit_reason}", app.bot)
                        state["active_trade"] = None
                        save_state()
                    else:
                        await asyncio.sleep(60) # Если не закрыта, ждем следующей свечи
                    continue
                else:
                    # --- РЕАЛЬНЫЙ РЕЖИМ: Проверяем реальные позиции ---
                    positions = await ex.fetch_positions([PAIR_SYMBOL])
                    if not positions or all(p.get('contracts', 0) == 0 for p in positions):
                        log.info(f"Позиция {tr['id']} была закрыта. Сбрасываю active_trade.")
                        state["active_trade"] = None
                        save_state()
                    else:
                        await asyncio.sleep(60)
                    continue

            # ... (логика поиска новой точки входа без изменений) ...
            if last[ADX_COL] >= state["adx_threshold"]:
                await asyncio.sleep(60)
                continue

            price = last["close"]
            side = ("LONG" if price <= last[BBL_COL] and last[RSI_COL] < RSI_OS else
                    "SHORT" if price >= last[BBU_COL] and last[RSI_COL] > RSI_OB else None)

            if not side:
                await asyncio.sleep(60)
                continue

            tr_info = await execute_trade(ex, side, price, last[ADX_COL], app.bot)
            if tr_info:
                state["active_trade"] = tr_info
                save_state()

            await asyncio.sleep(60)
    except asyncio.CancelledError:
        pass
    finally:
        try: await ex.close()
        except: pass
        log.info("Мониторинг остановлен")

# ─────────────────── REPORTER (суточный отчёт) ───────────────────────────
async def reporter(app: Application):
    while True:
        now = datetime.now(timezone.utc)
        tgt = now.replace(hour=REPORT_UTC_HOUR, minute=0, second=0, microsecond=0)
        if now > tgt: tgt += timedelta(days=1)
        try: await asyncio.sleep((tgt-now).total_seconds())
        except asyncio.CancelledError: break
        data = state.pop("daily_pnls", []); state["daily_pnls"] = []; save_state()
        if not data:
            await notify("📊 За сутки сделок не было", app.bot); continue
        pnl  = sum(d["pnl_usd"] for d in data)
        wins = sum(d["pnl_usd"] > 0 for d in data)
        wr   = wins/len(data)*100
        await notify(f"📊 24-ч отчёт: {len(data)} сделок • win-rate {wr:.1f}% • P&L {pnl:+.2f}$", app.bot)

# ─────────────────── ГЕНЕРАЦИЯ СТАТУСА ────────────────────────────────────
async def get_status_text() -> str:
    ex = await create_exchange()
    try:
        last_ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=100)
        if not last_ohlcv: return "Не удалось получить данные о свечах с биржи."
        try:
            df = df_from_ohlcv(last_ohlcv)
            df_with_indicators = add_indicators(df)
            if df_with_indicators.empty: return "Недостаточно данных для расчета индикаторов."
            last = df_with_indicators.iloc[-1]
        except Exception as e:
            log.error("Ошибка при расчете индикаторов: %s", e)
            return f"Ошибка при расчете индикаторов: {e}"
    finally:
        await ex.close()

    adx_now = last[ADX_COL]
    adx_dyn = state["adx_threshold"]
    is_flat = adx_now < adx_dyn
    boll_pos = ("⬇︎ ниже BBL" if last["close"] <= last[BBL_COL] else
                "⬆︎ выше BBU" if last["close"] >= last[BBU_COL] else "⋯ внутри канала")
    status = '🟢 ON' if state["monitoring"] else '🔴 OFF'
    trade = (f"\nАктивная позиция ID <code>{state['active_trade']['id']}</code>"
             if state["active_trade"] else "")
    
    trade_mode_str = "<b>Виртуальный режим</b>" if VIRTUAL_TRADING_MODE else "Реальный режим"

    return (f"<b>Flat-Liner status (MEXC)</b>\n"
            f"Режим торговли: {trade_mode_str}\n"
            f"Мониторинг: {status}{trade}\n"
            f"Плечо: <b>{state['leverage']}x</b>    Депозит: <b>{state['deposit']}$</b>\n\n"
            f"ADX текущий: <b>{adx_now:.2f}</b>\n"
            f"ADX порог : <b>{adx_dyn:.2f}</b>\n"
            f"Режим: <b>{'FLAT' if is_flat else 'TREND'}</b>\n"
            f"RSI-14: <b>{last[RSI_COL]:.2f}</b>\n"
            f"Цена vs BB: {boll_pos}")

# ─────────── TICKER (авто-статусы) ───────────────────────────────
async def ticker(app: Application):
    while state.get("last_adx_value") == 25.0: await asyncio.sleep(2)
    last_mode = None
    while True:
        try:
            status_text = await get_status_text()
            await notify(status_text, app.bot)
            adx = state["last_adx_value"]
            mode = "FLAT" if adx < state["adx_threshold"] else "TREND"
            if last_mode and mode != last_mode:
                arrow = "🟢" if mode == "FLAT" else "🔴"
                await notify(f"{arrow} Режим изменился: <b>{mode}</b>", app.bot)
            last_mode = mode
            await asyncio.sleep(1800 if mode == "FLAT" else 3600)
        except asyncio.CancelledError: break
        except Exception as e:
            log.error("Ticker error: %s", e)
            await notify(f"‼️ Ошибка в ticker: {e}", app.bot)
            await asyncio.sleep(60)

# ─────────────────── TELEGRAM COMMANDS ───────────────────────────────────
async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    mode_text = " (Виртуальный режим)" if VIRTUAL_TRADING_MODE else ""
    await notify(f"🚀 Flat-Liner (MEXC) запущен{mode_text}. Используйте /status.", c.bot)

async def cmd_status(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        status_text = await get_status_text()
        await u.message.reply_text(status_text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        log.error("Ошибка в cmd_status: %s", e)
        await u.message.reply_text(f"Ошибка получения статуса: {e}")

async def cmd_stop(u: Update, c: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False; save_state()
    await u.message.reply_text("⛔️ Мониторинг будет остановлен.")

async def cmd_set_dep(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        state["deposit"] = float(c.args[0]); save_state()
        await u.message.reply_text(f"Депозит = {state['deposit']}$")
    except: await u.message.reply_text("Формат: /set_deposit 25")

async def cmd_set_lev(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        lev = int(c.args[0]); assert 1 <= lev <= 200
        if not VIRTUAL_TRADING_MODE:
             ex = await create_exchange(); await set_leverage(ex, lev); await ex.close()
        state["leverage"] = lev; save_state()
        await u.message.reply_text(f"Плечо = {lev}x")
    except: await u.message.reply_text("Формат: /set_leverage 50")

async def cmd_test_trade(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text("❌ Команда /test_trade временно отключена.")

# ─────────────────── MAIN ────────────────────────────────────────────────
async def post_init_tasks(app: Application):
    mode_text = " (Виртуальный режим)" if VIRTUAL_TRADING_MODE else ""
    await notify(f"♻️ <b>Бот (MEXC) перезапущен{mode_text}</b>\nНачинаю мониторинг...", app.bot)
    if not state["monitoring"]:
        state["monitoring"] = True; save_state()
    asyncio.create_task(monitor(app))
    asyncio.create_task(reporter(app))
    asyncio.create_task(ticker(app))

def main() -> None:
    load_state()
    app = (ApplicationBuilder().token(BOT_TOKEN)
           .post_init(post_init_tasks)
           .build())
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("set_deposit", cmd_set_dep))
    app.add_handler(CommandHandler("set_leverage", cmd_set_lev))
    app.add_handler(CommandHandler("test_trade", cmd_test_trade))
    app.run_polling()
    log.info("Бот остановлен.")

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        log.info("Процесс прерван пользователем.")
