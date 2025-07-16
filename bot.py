#!/usr/bin/env python3
# ============================================================================
#  Flat‑Liner • Heroku edition — 16 Jul 2025 (fixed & updated)
#  Стратегия  : Flat_BB_Fade  +  динамический ADX‑фильтр
#  Биржа      : OKX (USDT‑Swap)
#  Команды    : /start /stop /status /set_deposit /set_leverage /test_trade
#  Автор      : Kirill2402100  |  MIT Licence
# ============================================================================

import os, json, logging, asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt
from telegram import Bot, Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes
)

# ─────────────────── CONFIG ───────────────────────────────────────────────
BOT_TOKEN   = os.getenv("BOT_TOKEN")
CHAT_IDS    = {int(cid) for cid in os.getenv("CHAT_IDS", "").split(",") if cid}

PAIR_SYMBOL = os.getenv("PAIR_SYMBOL", "BTC-USDT-SWAP")
TIMEFRAME   = os.getenv("TIMEFRAME",  "5m")

OKX_API_KEY        = os.getenv("OKX_API_KEY")
OKX_API_SECRET     = os.getenv("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")
OKX_SANDBOX        = os.getenv("OKX_DEMO_MODE", "0") == "1"

DEFAULT_DEPOSIT  = float(os.getenv("DEFAULT_DEPOSIT_USD", 50))
DEFAULT_LEVERAGE = int  (os.getenv("DEFAULT_LEVERAGE",    100))

SL_PCT, RR_RATIO = 0.10, 1.0           # стоп‑лосс 0.10 %,  соотношение 1:1
RSI_OS, RSI_OB   = 35, 65              # oversold / overbought
REPORT_UTC_HOUR  = int(os.getenv("REPORT_HOUR_UTC", 21))

STATE_FILE = Path("state_flatliner_okx.json")

# ─────────────────── LOGGING ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("flatliner")

# ─────────────────── STATE ───────────────────────────────────────────────
state = {
    "monitoring": True,
    "active_trade": None,
    "deposit":  DEFAULT_DEPOSIT,
    "leverage": DEFAULT_LEVERAGE,
    "adx_threshold": 25.0,
    "last_adx_recalc": None,
    "daily_pnls": []
}

def save_state(): STATE_FILE.write_text(json.dumps(state, indent=2))

def load_state():
    if STATE_FILE.exists():
        try:
            state.update(json.loads(STATE_FILE.read_text()))
        except Exception:
            log.warning("STATE‑файл повреждён → создаю новый")
    save_state()

# ─────────────────── HELPERS ─────────────────────────────────────────────
async def notify(text: str, bot: Optional[Bot] = None):
    bot = bot or Bot(BOT_TOKEN)
    for cid in CHAT_IDS:
        try:
            await bot.send_message(cid, text, parse_mode="HTML")
        except Exception as e:
            log.error("TG‑fail → %s : %s", cid, e)

# ─────────────────── INDICATORS ──────────────────────────────────────────
ADX_COL, BBL_COL, BBU_COL, RSI_COL = "ADX_14", "BBL_20_2.0", "BBU_20_2.0", "RSI_14"

def df_from_ohlcv(ohlcv):
    return pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])

def add_indicators(df):
    df.ta.rsi(length=14, append=True)
    df.ta.adx(length=14, append=True)
    df.ta.bbands(length=20, std=2, append=True)
    return df.dropna()

# ─────────────────── EXCHANGE ────────────────────────────────────────────
async def create_exchange():
    ex = ccxt.okx({
        "apiKey":   OKX_API_KEY,
        "secret":   OKX_API_SECRET,
        "password": OKX_API_PASSPHRASE,
        "options":  {"defaultType": "swap"},
    })
    ex.set_sandbox_mode(OKX_SANDBOX)
    await ex.load_markets()
    return ex

async def set_leverage(ex, lev):
    for side in ("long", "short"):
        await ex.set_leverage(lev, PAIR_SYMBOL, {"mgnMode":"isolated", "posSide":side})

# ─────────────────── CORE ────────────────────────────────────────────────
async def recalc_adx_threshold():
    ex = await create_exchange()
    try:
        ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=2000)
        df = df_from_ohlcv(ohlcv)
        df.ta.adx(length=14, append=True)
        adx = df[ADX_COL].dropna()
        if not adx.empty:
            thresh = (np.percentile(adx,20)+np.percentile(adx,30))/2
            state["adx_threshold"] = thresh
            state["last_adx_recalc"] = datetime.now(timezone.utc).isoformat()
            save_state(); log.info("ADX‑threshold → %.2f", thresh)
    finally:
        await ex.close()

async def execute_trade(ex, side, price):
    m = ex.markets[PAIR_SYMBOL]
    size = round((state["deposit"]*state["leverage"])/price/float(m["contractSize"]))
    if size < float(m["limits"]["amount"]["min"]):
        await notify("🔴 Размер сделки меньше минимального"); return None

    sl = price*(1-SL_PCT/100) if side=="LONG" else price*(1+SL_PCT/100)
    tp = price*(1+SL_PCT*RR_RATIO/100) if side=="LONG" else price*(1-SL_PCT*RR_RATIO/100)
    params = {"tdMode":"isolated","posSide":"long" if side=="LONG" else "short",
              "slTriggerPx":str(sl),"slOrdPx":"-1","tpTriggerPx":str(tp),"tpOrdPx":"-1"}
    order = await ex.create_order(PAIR_SYMBOL,"market","buy" if side=="LONG" else "sell",size,params=params)
    await notify(f"✅ Открыта позиция {side}  ID <code>{order['id']}</code>")
    return order["id"]

# ─────────────────── MONITOR ─────────────────────────────────────────────
async def monitor(app: Application):
    ex = await create_exchange(); await set_leverage(ex, state["leverage"])
    await recalc_adx_threshold()
    log.info("🚀 Мониторинг запущен")

    try:
        while state["monitoring"]:
            last = state["last_adx_recalc"]
            if not last or (datetime.now(timezone.utc)-datetime.fromisoformat(last)).total_seconds()>3600:
                await recalc_adx_threshold()

            if (tr:=state.get("active_trade")):
                poss = await ex.fetch_positions([PAIR_SYMBOL])
                side = "long" if tr["side"]=="LONG" else "short"
                open_now = any(p["side"]==side and float(p.get("contracts",0))>0 for p in poss)
                if not open_now:
                    state["active_trade"] = None; save_state(); await notify("ℹ️ Позиция закрыта")
                await asyncio.sleep(60); continue

            ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=100)
            df = add_indicators(df_from_ohlcv(ohlcv)); last = df.iloc[-1]
            price = last["close"]
            if last[ADX_COL] >= state["adx_threshold"]:
                await asyncio.sleep(60); continue
            side = "LONG"  if price<=last[BBL_COL] and last[RSI_COL]<RSI_OS else \
                   "SHORT" if price>=last[BBU_COL] and last[RSI_COL]>RSI_OB else None
            if not side:
                await asyncio.sleep(60); continue
            oid = await execute_trade(ex, side, price)
            if oid:
                state["active_trade"] = {"id":oid,"side":side,"entry_price":price}; save_state()
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        pass
    finally:
        try: await ex.close()
        except Exception: pass
        log.info("Мониторинг остановлен")

# ─────────────────── REPORTER ────────────────────────────────────────────
async def reporter(app: Application):
    while True:
        now = datetime.now(timezone.utc)
        tgt = now.replace(hour=REPORT_UTC_HOUR,minute=0,second=0,microsecond=0)
        if now>tgt: tgt += timedelta(days=1)
        try: await asyncio.sleep((tgt-now).total_seconds())
        except asyncio.CancelledError: break
        data = state.pop("daily_pnls",[]); state["daily_pnls"]=[]; save_state()
        if not data: await notify("📊 За сутки сделок не было"); continue
        pnl = sum(d["pnl_usd"] for d in data); wins = sum(d["pnl_usd"]>0 for d in data)
        wr = wins/len(data)*100
        await notify(f"📊 24‑ч отчёт: {len(data)} сделок • win‑rate {wr:.1f}% • P&L {pnl:+.2f}$")

# ─────────────────── COMMANDS ───────────────────────────────────────────
async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Отправляет приветствие и статус при команде /start"""
    await notify("🚀 Flat-Liner запущен. Используйте /status для проверки состояния.", c.bot)
    await cmd_status(u, c)

async def cmd_status(u: Update, c: ContextTypes.DEFAULT_TYPE):
    status = '🟢' if state["monitoring"] else '🔴'
    trade  = f"\nАктивная позиция ID {state['active_trade']['id']}" if state["active_trade"] else ""
    txt = (f"<b>Flat-Liner status</b>\n\nМониторинг: {status}"
           f"\nПлечо: {state['leverage']}x  |  Депозит: {state['deposit']}$"
           f"{trade}")
    await u.message.reply_text(txt, parse_mode="HTML")

async def cmd_test_trade(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Открывает тестовую позицию с параметрами: deposit, leverage, sl, tp, side"""
    try:
        args = {k.lower(): v for k, v in (arg.split('=', 1) for arg in c.args)}
        side = args.get('side', '').upper()
        sl_price = float(args.get('sl'))
        tp_price = float(args.get('tp'))
        if side not in ['LONG', 'SHORT']: raise ValueError("Параметр 'side' обязателен (LONG или SHORT).")
        deposit = float(args.get('deposit', state['deposit']))
        leverage = int(args.get('leverage', state['leverage']))
    except (ValueError, TypeError, KeyError):
        await u.message.reply_text(
            "❌ **Ошибка в параметрах.**\n\n"
            "Убедитесь, что все значения указаны верно. Обязательные параметры: `side`, `sl`, `tp`.\n\n"
            "<i>Пример: /test_trade deposit=30 leverage=10 sl=65000 tp=68000 side=LONG</i>",
            parse_mode="HTML"
        )
        return

    await u.message.reply_text(f"🛠️ Открываю тестовую позицию {side} с вашими параметрами...")
    ex = None
    try:
        ex = await create_exchange()
        await set_leverage(ex, leverage)
        market = ex.markets[PAIR_SYMBOL]
        ticker = await ex.fetch_ticker(PAIR_SYMBOL)
        price = ticker['last']
        size = round((deposit * leverage) / price / float(market["contractSize"]))
        if size < float(market["limits"]["amount"]["min"]):
            await u.message.reply_text(f"🔴 Размер сделки ({size}) меньше минимального ({market['limits']['amount']['min']}).")
            return
        params = {"tdMode": "isolated", "posSide": "long" if side == "LONG" else "short",
                  "slTriggerPx": str(sl_price), "slOrdPx": "-1",
                  "tpTriggerPx": str(tp_price), "tpOrdPx": "-1"}
        order = await ex.create_order(PAIR_SYMBOL, "market", "buy" if side == "LONG" else "sell", size, params=params)
        await u.message.reply_text(f"✅ Тестовый ордер <code>{order['id']}</code> успешно создан.", parse_mode="HTML")
    except Exception as e:
        log.error("Ошибка в cmd_test_trade: %s", e)
        await u.message.reply_text(f"🔥 **Произошла ошибка:**\n<code>{e}</code>", parse_mode="HTML")
    finally:
        if ex: await ex.close()

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
        lev = int(c.args[0]); assert 1<=lev<=125
        ex = await create_exchange(); await set_leverage(ex, lev); await ex.close()
        state["leverage"] = lev; save_state()
        await u.message.reply_text(f"Плечо = {lev}x")
    except: await u.message.reply_text("Формат: /set_leverage 50")

# ─────────────────── MAIN ──────────────────────────────────────────────────
async def post_init_tasks(app: Application):
    """Запускает фоновые задачи после инициализации бота."""
    await notify("♻️ Бот перезапущен.", app.bot)
    if not state["monitoring"]:
        state["monitoring"] = True
        save_state()
    asyncio.create_task(monitor(app))
    asyncio.create_task(reporter(app))

def main() -> None:
    """Основная функция запуска бота."""
    load_state()
    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .post_init(post_init_tasks)
           .build())

    # Регистрация обработчиков команд
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("stop",         cmd_stop))
    app.add_handler(CommandHandler("set_deposit",  cmd_set_dep))
    app.add_handler(CommandHandler("set_leverage", cmd_set_lev))
    app.add_handler(CommandHandler("test_trade",   cmd_test_trade))

    app.run_polling()
    log.info("Бот остановлен.")

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        log.info("Процесс прерван пользователем.")
