#!/usr/bin/env python3
# ============================================================================
#   Flat-Liner • Render edition – 16 Jul 2025
#   Автор: Kirill2402100  |  Telegram: @…
# ============================================================================
# Стратегия  : Flat_BB_Fade  +  ADX-filter
# Биржа      : OKX (USDT-Swap); демо-режим включается переменной OKX_DEMO_MODE
# Управление : /status /set_deposit /set_leverage /test_trade /stop
# ============================================================================

import os, json, logging, asyncio, signal, traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt

from telegram import Bot, Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes
)

# ──────────────────── CONFIG ────────────────────────────────────────────────
BOT_TOKEN          = os.getenv("BOT_TOKEN")
CHAT_IDS           = {int(cid) for cid in os.getenv("CHAT_IDS", "").split(",") if cid}
PAIR_SYMBOL        = os.getenv("PAIR_SYMBOL", "BTC-USDT-SWAP")
TIMEFRAME          = os.getenv("TIMEFRAME", "5m")

OKX_API_KEY        = os.getenv("OKX_API_KEY")
OKX_API_SECRET     = os.getenv("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")
OKX_DEMO_MODE      = os.getenv("OKX_DEMO_MODE", "0") == "1"

DEFAULT_DEPOSIT    = float(os.getenv("DEFAULT_DEPOSIT_USD" , 50))
DEFAULT_LEVERAGE   = int  (os.getenv("DEFAULT_LEVERAGE"    , 100))
SL_PCT             = float(os.getenv("FLAT_SL_PCT"         , 0.10))      # 0 - 100
RR_RATIO           = float(os.getenv("FLAT_RR_RATIO"       , 1.0))
RSI_OS, RSI_OB     = 35, 65
REPORT_UTC_HOUR    = int(os.getenv("REPORT_HOUR_UTC", 21))

STATE_FILE = Path("state_flatliner_okx.json")

# ──────────────────── LOGGING ───────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger("flatliner")

# ──────────────────── STATE ─────────────────────────────────────────────────
state = {
    "monitoring": False,
    "active_trade": None,
    "deposit": DEFAULT_DEPOSIT,
    "leverage": DEFAULT_LEVERAGE,
    "adx_threshold": 25.0,
    "last_adx_recalc": None,
    "daily_pnls": []          # [{pnl_usd, entry_usd}, …]
}
def save_state() -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))
def load_state() -> None:
    if STATE_FILE.exists():
        try:
            state.update(json.loads(STATE_FILE.read_text()))
        except Exception:
            log.warning("STATE файл повреждён - перезаписываю.")
            save_state()

# ──────────────────── HELPERS ───────────────────────────────────────────────
async def notify(text: str, bot: Bot | None = None) -> None:
    bot = bot or Bot(BOT_TOKEN)
    for cid in CHAT_IDS:
        try:
            await bot.send_message(cid, text, parse_mode="HTML")
        except Exception as e:
            log.error("Telegram-fail → %s : %s", cid, e)

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.ta.rsi(length=14, append=True, col_names=("RSI",))
    df.ta.adx(length=14, append=True, col_names=("ADX", "DMP", "DMN"))
    df.ta.bbands(length=20, std=2, append=True,
                 col_names=("BBL", "BBM", "BBU", "BBB", "BBP"))
    return df.dropna()

# ──────────────────── EXCHANGE ──────────────────────────────────────────────
async def okx() -> ccxt.okx:
    ex = ccxt.okx({
        "apiKey": OKX_API_KEY,
        "secret": OKX_API_SECRET,
        "password": OKX_API_PASSPHRASE,
        "options": {"defaultType": "swap"},
    })
    ex.set_sandbox_mode(OKX_DEMO_MODE)
    await ex.load_markets()
    return ex

async def set_leverage(ex, lev: int) -> None:
    await ex.set_leverage(lev, PAIR_SYMBOL, {"mgnMode": "isolated", "posSide": "long"})
    await ex.set_leverage(lev, PAIR_SYMBOL, {"mgnMode": "isolated", "posSide": "short"})

# ──────────────────── CORE LOGIC ────────────────────────────────────────────
async def recalc_adx_threshold() -> None:
    ex = await okx()
    ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=2000)
    await ex.close()
    df = pd.DataFrame(ohlcv, columns=["ts","o","h","l","c","v"])
    df.ta.adx(length=14, append=True, col_names=("ADX", "DMP", "DMN"))
    adx = df["ADX"].dropna()
    t = (np.percentile(adx, 20) + np.percentile(adx, 30)) / 2
    state["adx_threshold"] = t
    state["last_adx_recalc"] = datetime.now(timezone.utc).isoformat()
    save_state()
    log.info("ADX-threshold обновлён: %.2f", t)

async def execute_trade(ex, side: str, price: float) -> str | None:
    market = ex.markets[PAIR_SYMBOL]
    size = round((state["deposit"] * state["leverage"]) / price / market["contractSize"])
    if size < market["limits"]["amount"]["min"]:
        await notify("🔴 Размер сделки меньше минимального — торговля отменена."); return None

    sl = price * (1 - SL_PCT/100) if side=="LONG" else price * (1 + SL_PCT/100)
    tp = price * (1 + SL_PCT*RR_RATIO/100) if side=="LONG" else price * (1 - SL_PCT*RR_RATIO/100)

    params = {"tdMode":"isolated","posSide":"long" if side=="LONG" else "short",
              "slTriggerPx":str(sl),"slOrdPx":"-1",
              "tpTriggerPx":str(tp),"tpOrdPx":"-1"}
    order = await ex.create_order(PAIR_SYMBOL, "market",
                                  "buy" if side=="LONG" else "sell",
                                  size, params=params)
    await notify(f"✅ Открыта позиция {side} • ID <code>{order['id']}</code>")
    return order["id"]

# ──────────────────── MONITOR LOOP ──────────────────────────────────────────
async def monitor(app: Application):
    ex = await okx()
    await set_leverage(ex, state["leverage"])
    await recalc_adx_threshold()
    log.info("🚀 Мониторинг запущен.")

    try:
        while state["monitoring"]:
            # пересчёт ADX раз в час
            last = state["last_adx_recalc"]
            if not last or (datetime.now(timezone.utc)
                            - datetime.fromisoformat(last)).seconds > 3600:
                await recalc_adx_threshold()

            # если позиция есть — проверяем, осталась ли она на бирже
            if tr := state.get("active_trade"):
                poss = await ex.fetch_positions([PAIR_SYMBOL])
                side = "long" if tr["side"]=="LONG" else "short"
                still_open = any(p["side"]==side and float(p["contracts"] or 0)>0
                                 for p in poss)
                if not still_open:
                    state["active_trade"] = None
                    save_state()
                    await notify("ℹ️ Позиция закрыта на бирже (факт).")
                await asyncio.sleep(60); continue

            # нет открытой позиции → ищем сигнал
            ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, timeframe=TIMEFRAME, limit=100)
            df = add_indicators(pd.DataFrame(ohlcv, columns=["ts","o","h","l","c","v"]))
            last = df.iloc[-1]
            price = last["c"]

            if last["ADX"] >= state["adx_threshold"]:
                await asyncio.sleep(60); continue

            side = None
            if price <= last["BBL"] and last["RSI"] < RSI_OS:   side = "LONG"
            if price >= last["BBU"] and last["RSI"] > RSI_OB:   side = "SHORT"
            if not side:
                await asyncio.sleep(60); continue

            order_id = await execute_trade(ex, side, price)
            if order_id:
                state["active_trade"] = {"id": order_id, "side": side,
                                         "entry_price": price}
                save_state()

            await asyncio.sleep(60)

    except asyncio.CancelledError:
        pass
    finally:
        await ex.close()
        log.info("Мониторинг остановлен.")

# ──────────────────── DAILY REPORT ──────────────────────────────────────────
async def reporter(app: Application):
    while True:
        now = datetime.now(timezone.utc)
        target = now.replace(hour=REPORT_UTC_HOUR, minute=0, second=0, microsecond=0)
        if now > target: target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())

        data = state["daily_pnls"]; state["daily_pnls"] = []; save_state()
        if not data:
            await notify("📊 За сутки сделок не было."); continue

        pnl   = sum(x["pnl_usd"]   for x in data)
        win   = sum(1 for x in data if x["pnl_usd"]>0)
        total = len(data)
        wr    = win/total*100
        await notify(f"📊 Отчёт 24 ч → сделок {total}  |  win-rate {wr:.1f}%  |  P&L {pnl:+.2f}$")

# ──────────────────── BOT COMMANDS ──────────────────────────────────────────
async def cmd_status(u: Update, c: ContextTypes.DEFAULT_TYPE):
    text = ("<b>Flat-Liner status</b>\n\n"
            f"Мониторинг: {'🟢' if state['monitoring'] else '🔴'}\n"
            f"Плечо: {state['leverage']}x   |   Депозит: {state['deposit']}$")
    await u.message.reply_text(text, parse_mode="HTML")

async def cmd_stop(u: Update, c: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False; save_state()
    await u.message.reply_text("⛔️ Мониторинг будет остановлен.")
async def cmd_set_dep(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        state["deposit"] = float(c.args[0]); save_state()
        await u.message.reply_text(f"OK, депозит = {state['deposit']}$")
    except: await u.message.reply_text("Формат: /set_deposit 25")
async def cmd_set_lev(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        lev = int(c.args[0]); assert 1<=lev<=125
        ex = await okx(); await set_leverage(ex, lev); await ex.close()
        state["leverage"] = lev; save_state()
        await u.message.reply_text(f"OK, плечо = {lev}x")
    except: await u.message.reply_text("Формат: /set_leverage 50")

# ──────────────────── MAIN ──────────────────────────────────────────────────
async def run() -> None:
    load_state()

    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .post_init(lambda a: notify("♻️ Бот перезапущен.", a.bot))
           .build())

    for cmd, h in {
        "status": cmd_status,
        "stop":   cmd_stop,
        "set_deposit":  cmd_set_dep,
        "set_leverage": cmd_set_lev,
    }.items():
        app.add_handler(CommandHandler(cmd, h))

    # авто-запуск мониторинга при холодном старте
    if not state["monitoring"]:
        state["monitoring"] = True; save_state()

    monitor_task  = asyncio.create_task(monitor(app))
    report_task   = asyncio.create_task(reporter(app))

    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
    loop.add_signal_handler(signal.SIGINT , stop.set_result, None)

    async with app:
        await app.start()
        await stop          # ждём сигнала от Render
        monitor_task.cancel(); report_task.cancel()
        await asyncio.gather(monitor_task, report_task, return_exceptions=True)
        await app.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except (KeyboardInterrupt, SystemExit):
        pass
