#!/usr/bin/env python3
# ============================================================================
#  Flat‑Liner • Heroku edition — 16 Jul 2025  (debug build)
#  Стратегия  : Flat_BB_Fade  +  динамический ADX‑фильтр
#  Биржа      : OKX (USDT‑Swap)
#  Команды    : /start /stop /status /set_deposit /set_leverage /test_trade
#  Автор      : Kirill2402100  |  MIT Licence
# ============================================================================

"""
В этой версии добавлены строки логирования вида
    log.info("ALGOREQ %s", payload)
перед каждым вызовом `/trade/order-algo`.
Их вывод ищём в Heroku‑логах, чтобы увидеть фактический JSON‑payload.
"""

import os, json, logging, asyncio, math
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

# graceful shutdown — чтобы Heroku не ловил R12
import signal, sys

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

async def calc_size(market, price, deposit, leverage):
    step   = float(market["limits"]["amount"]["min"])  # 0.01 для BTC‑SWAP
    raw    = (deposit * leverage) / price / float(market["contractSize"])
    size   = math.floor(raw / step) * step
    return size, step

async def execute_trade(ex, side:str, price:float):
    m        = ex.market(PAIR_SYMBOL)
    size,step = await calc_size(m, price, state["deposit"], state["leverage"])
    if size < step:
        await notify(f"🔴 Минимальный объём — {step}. Увеличьте депозит/плечо.");
        return None

    # ── 1. открываем позицию ────────────────────────────────────────────
    pos_side   = "long" if side == "LONG" else "short"
    order_side = "buy"  if side == "LONG" else "sell"
    order = await ex.create_order(PAIR_SYMBOL, "market", order_side, size,
                                  params={"tdMode":"isolated", "posSide":pos_side})
    await notify(f"✅ Открыта позиция {side}  ID <code>{order['id']}</code>. Устанавливаю SL/TP…", parse_mode="HTML")

    # ── 2. рассчитываем TP / SL ─────────────────────────────────────────
    sl_price = price * (1 - SL_PCT/100) if side=="LONG" else price * (1 + SL_PCT/100)
    tp_price = price * (1 + SL_PCT*RR_RATIO/100) if side=="LONG" else price * (1 - SL_PCT*RR_RATIO/100)
    side_close = "sell" if side=="LONG" else "buy"

    payload = {
        "instId":   PAIR_SYMBOL,
        "tdMode":   "isolated",
        "side":     side_close,
        "posSide":  pos_side,
        "sz":       str(size),
        "ordType":  "conditional",
        "tpTriggerPx": str(tp_price),
        "tpOrdPx":     "-1",
        "slTriggerPx": str(sl_price),
        "slOrdPx":     "-1",
    }
    log.info("ALGOREQ %s", payload)   # ⬅️ debug‑print

    await ex.private_post_trade_order_algo(payload)
    await notify(f"✅ SL/TP для ордера <code>{order['id']}</code> успешно установлены.", parse_mode="HTML")
    return order["id"]
    
# ─────────────────── MONITOR ─────────────────────────────────────────────
async def monitor(app: Application):
    ex = await create_exchange(); await set_leverage(ex, state["leverage"])
    await recalc_adx_threshold()
    log.info("🚀 Мониторинг запущен")

    try:
        while state["monitoring"]:
            # пересчёт ADX‑порога раз в час
            last = state["last_adx_recalc"]
            if not last or (datetime.now(timezone.utc)-datetime.fromisoformat(last)).total_seconds()>3600:
                await recalc_adx_threshold()

            # контроль открытой позиции
            if (tr := state.get("active_trade")):
                poss = await ex.fetch_positions([PAIR_SYMBOL])
                side = "long" if tr["side"]=="LONG" else "short"
                open_now = any(p["side"]==side and float(p.get("contracts",0))>0 for p in poss)
                if not open_now:
                    state["active_trade"] = None; save_state(); await notify("ℹ️ Позиция закрыта")
                await asyncio.sleep(60); continue

            # поиск новой точки входа
            ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=100)
            df    = add_indicators(df_from_ohlcv(ohlcv)); last = df.iloc[-1]; price = last["close"]

            if last[ADX_COL] >= state["adx_threshold"]:   # флэт‑фильтр не прошёл
                await asyncio.sleep(60); continue

            side = "LONG" if price<=last[BBL_COL] and last[RSI_COL]<RSI_OS else \
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
        try: await ex.close();
        except Exception: pass
        log.info("Мониторинг остановлен")

# ─────────────────── REPORTER ────────────────────────────────────────────
async def reporter(app: Application):
    while True:
        now = datetime.now(timezone.utc)
        tgt = now.replace(hour=REPORT_UTC_HOUR,minute=0,second=0,microsecond=0)
        if now>tgt: tgt += timedelta(days=1)
        try:
            await asyncio.sleep((tgt-now).total_seconds())
        except asyncio.CancelledError:
            break
        data = state.pop("daily_pnls",[]); state["daily_pnls"]=[]; save_state()
        if not data:
            await notify("📊 За сутки сделок не было"); continue
        pnl  = sum(d["pnl_usd"] for d in data); wins = sum(d["pnl_usd"]>0 for d in data)
        wr   = wins/len(data)*100
        await notify(f"📊 24‑ч отчёт: {len(data)} сделок • win‑rate {wr:.1f}% • P&L {pnl:+.2f}$")

# ─────────────────── COMMANDS ───────────────────────────────────────────
async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
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
        if side not in ['LONG', 'SHORT']:
            raise ValueError("Параметр 'side' обязателен (LONG или SHORT).")
        deposit  = float(args.get('deposit', state['deposit']))
        leverage = int  (args.get('leverage', state['leverage']))
    except Exception:
        await u.message.reply_text(
            "❌ **Ошибка в параметрах.**\n\n"
            "Обязательные: side, sl, tp. Пример: /test_trade side=LONG sl=119000 tp=120000",
            parse_mode="HTML")
        return

    await u.message.reply_text(f"🛠️ Открываю тестовую позицию {side}…")
    ex = None
    try:
        ex = await create_exchange()
        await set_leverage(ex, leverage)
        market = ex.market(PAIR_SYMBOL)
        ticker = await ex.fetch_ticker(PAIR_SYMBOL)
        price  = ticker['last']
        step   = float(market['limits']['amount']['min'])
        raw_sz = (deposit*leverage) / price / float(market['contractSize'])
        size   = math.floor(raw_sz/step)*step
        if size < step:
            await u.message.reply_text(f"🔴 Размер сделки ({size}) меньше минимального ({step}).")
            return

        pos_side   = "long" if side == "LONG" else "short"
        order_side = "buy"  if side == "LONG" else "sell"
        order = await ex.create_order(PAIR_SYMBOL, "market", order_side, size,
                                      params={"tdMode":"isolated", "posSide":pos_side})
        await u.message.reply_text(f"✅ Ордер <code>{order['id']}</code> создан. Устанавливаю SL/TP…", parse_mode="HTML")

        # Conditional TP/SL
        await place_tp_sl(ex, size, side, pos_side, price)
        await u.message.reply_text(f"✅ SL/TP для ордера <code>{order['id']}</code> успешно установлены.", parse_mode="HTML")

    except Exception as e:
        log.error("Ошибка в cmd_test_trade: %s", e)
        await u.message.reply_text(f"🔥 **Произошла ошибка:**\n<code>{e}</code>", parse_mode="HTML")
    finally:
        if ex:
            await ex.close()

async def cmd_stop(u: Update, c: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False; save_state()
    await u.message.reply_text("⛔️ Мониторинг будет остановлен.")

async def cmd_set_dep(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        state['deposit'] = float(c.args[0]); save_state()
        await u.message.reply_text(f"Депозит = {state['deposit']}$")
    except Exception:
        await u.message.reply_text("Формат: /set_deposit 25")

async def cmd_set_lev(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        lev = int(c.args[0]); assert 1<=lev<=125
        ex = await create_exchange(); await set_leverage(ex, lev); await ex.close()
        state['leverage'] = lev; save_state()
        await u.message.reply_text(f"Плечо = {lev}x")
    except Exception:
        await u.message.reply_text("Формат: /set_leverage 50")

# ─────────────────── MAIN ──────────────────────────────────────────────────
async def post_init_tasks(app: Application):
    await notify("♻️ Бот перезапущен.", app.bot)
    if not state["monitoring"]:
        state["monitoring"] = True; save_state()
    asyncio.create_task(monitor(app))
    asyncio.create_task(reporter(app))


def main() -> None:
    load_state()
    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .post_init(post_init_tasks)
           .build())
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
