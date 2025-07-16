#!/usr/bin/env python3
# ============================================================================
#  Flat‑Liner • Heroku edition — 16 Jul 2025  (debug build, full logging)
#  Стратегия  : Flat_BB_Fade  +  динамический ADX‑фильтр  +  RR 1:1
#  Биржа      : OKX (USDT‑Swap)
#  Команды    : /start /stop /status /set_deposit /set_leverage /test_trade
#  Автор      : Kirill2402100  |  MIT Licence
# ============================================================================

"""
• TP/SL — ordType="conditional", RR 1:1 (SL_PCT = 0.10 %, TP = SL).
• Один активный трейд.  При закрытии:
    ▸ P&L сообщение + запись в daily_pnls (для суточного отчёта)
    ▸ строка OPEN / CLOSE в Google-Sheets (sheet1 первой вкладки).
• Переменные окружения:
    BOT_TOKEN, CHAT_IDS
    OKX_API_KEY / SECRET / PASSPHRASE, OKX_DEMO_MODE
    SHEET_ID, GOOGLE_CREDENTIALS     (JSON service-account → строкой)
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
BOT_TOKEN  = os.getenv("BOT_TOKEN")
CHAT_IDS   = {int(cid) for cid in os.getenv("CHAT_IDS", "").split(",") if cid}

PAIR_SYMBOL = os.getenv("PAIR_SYMBOL", "BTC-USDT-SWAP")     # биржевой id
TIMEFRAME   = os.getenv("TIMEFRAME",  "5m")

OKX_API_KEY        = os.getenv("OKX_API_KEY")
OKX_API_SECRET     = os.getenv("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")
OKX_SANDBOX        = os.getenv("OKX_DEMO_MODE", "0") == "1"

DEFAULT_DEPOSIT  = float(os.getenv("DEFAULT_DEPOSIT_USD", 50))
DEFAULT_LEVERAGE = int  (os.getenv("DEFAULT_LEVERAGE",    100))

SL_PCT, RR_RATIO = 0.10, 1.0         # SL = 0.10 %;   TP = SL  (RR 1 : 1)
RSI_OS, RSI_OB   = 35, 65
REPORT_UTC_HOUR  = int(os.getenv("REPORT_HOUR_UTC", 21))

# Google Sheets
SHEET_ID           = os.getenv("SHEET_ID")            # обязательна
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")  # JSON-строка service account

STATE_FILE = Path("state_flatliner_okx.json")

# ─────────────────── LOGGING ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("flatliner")

# graceful shutdown — чтобы dyno не ловил R12
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
    "active_trade": None,      # {id, side, entry_price, size}
    "deposit":  DEFAULT_DEPOSIT,
    "leverage": DEFAULT_LEVERAGE,
    "adx_threshold": 25.0,
    "last_adx_recalc": None,
    "daily_pnls": []           # [{ts, pnl_usd}]
}

def save_state(): STATE_FILE.write_text(json.dumps(state, indent=2))
def load_state():
    if STATE_FILE.exists():
        try: state.update(json.loads(STATE_FILE.read_text()))
        except: log.warning("STATE‑файл повреждён → создаю новый")
    save_state()

# ─────────────────── HELPERS ─────────────────────────────────────────────
async def notify(text: str, bot: Optional[Bot] = None):
    """Отправка сообщения во все CHAT_IDS (async-safe)."""
    bot = bot or Bot(BOT_TOKEN)
    for cid in CHAT_IDS:
        try:
            await bot.send_message(cid, text, parse_mode="HTML")
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

# ─────────────────── EXCHANGE ────────────────────────────────────────────
async def create_exchange():
    ex = ccxt.okx({
        "apiKey":   OKX_API_KEY,
        "secret":   OKX_API_SECRET,
        "password": OKX_API_PASSPHRASE,
        "options":  {"defaultType": "swap"},
    })
    ex.set_sandbox_mode(OKX_SANDBOX)
    await ex.load_markets(); return ex

async def set_leverage(ex, lev):
    for side in ("long", "short"):
        await ex.set_leverage(lev, PAIR_SYMBOL, {"mgnMode":"isolated", "posSide":side})

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
    """
    рассчитываем размер позиции с учётом:
      • минимального шага (step)
      • точности поля amount (precision)
    """
    step = float(market["limits"]["amount"]["min"])
    prec = market["precision"]["amount"]          # обычно 0 или 2 знака
    raw  = (deposit * leverage) / price / float(market["contractSize"])
    size = math.floor(raw / step) * step
    return round(size, prec), step
  
async def place_tp_sl(ex, size, side, pos_side, entry_price):
    sl_price = entry_price * (1 - SL_PCT/100) if side == "LONG" else entry_price * (1 + SL_PCT/100)
    tp_price = entry_price * (1 + SL_PCT*RR_RATIO/100) if side == "LONG" else entry_price * (1 - SL_PCT*RR_RATIO/100)
    side_close = "sell" if side == "LONG" else "buy"
    inst_id = ex.market(PAIR_SYMBOL)["id"]

    payload = {
        "instId": inst_id, "tdMode": "isolated",
        "side": side_close, "posSide": pos_side, "sz": str(size),
        "ordType": "conditional",
        "tpTriggerPx": str(tp_price), "tpOrdPx": "-1",
        "slTriggerPx": str(sl_price), "slOrdPx": "-1",
    }
    log.info("ALGOREQ %s", payload)
    await ex.private_post_trade_order_algo(payload)

async def execute_trade(ex, side: str, price: float):
    m = ex.market(PAIR_SYMBOL)
    size, step = calc_size(m, price, state["deposit"], state["leverage"])
    if size < step:
        await notify(f"🔴 Минимальный объём — {step}. Увеличьте депозит/плечо.")
        return None

    pos_side   = "long" if side == "LONG" else "short"
    order_side = "buy"  if side == "LONG" else "sell"
    order = await ex.create_order(
        PAIR_SYMBOL, "market", order_side, size,
        params={"tdMode": "isolated", "posSide": pos_side}
    )
    await notify(f"✅ Открыта позиция {side}  ID <code>{order['id']}</code>. Устанавливаю SL/TP…", parse_mode="HTML")
    sheet_log([datetime.utcnow().isoformat(), "OPEN", side, size, price, "", ""])

    await place_tp_sl(ex, size, side, pos_side, price)
    await notify(f"✅ SL/TP для ордера <code>{order['id']}</code> успешно установлены.", parse_mode="HTML")
    return order["id"], size        # ← возвращаем id и size

# ─────────────────── MONITOR (авто-сделки) ───────────────────────────────
async def monitor(app: Application):
    ex = await create_exchange(); await set_leverage(ex, state["leverage"])
    await recalc_adx_threshold(); log.info("🚀 Мониторинг запущен")

    try:
        while state["monitoring"]:
            # пересчёт ADX-порога
            last = state["last_adx_recalc"]
            if not last or (datetime.now(timezone.utc)-datetime.fromisoformat(last)).total_seconds() > 3600:
                await recalc_adx_threshold()

            # контроль открытой позиции
                        # контроль открытой позиции
            if (tr := state.get("active_trade")):
                poss = await ex.fetch_positions([PAIR_SYMBOL])
                side = "long" if tr["side"] == "LONG" else "short"

                # есть ли ещё контракты в выбранную сторону?
                open_now = any(p["side"] == side and float(p.get("contracts", 0)) > 0 for p in poss)

                if not open_now:
                    # --- определяем цену выхода --------------------------------
                    if not poss:
                        # позиция полностью закрыта и биржа не вернула позицию –
                        # берём последнюю рыночную цену
                        exit_price = float((await ex.fetch_ticker(PAIR_SYMBOL))["last"])
                    else:
                        # позиция закрыта, но биржа всё ещё отдаёт «пустую» запись
                        pos   = poss[0]
                        exit_price = float(
                            pos.get("avgPx") or
                            pos.get("markPx") or
                            (await ex.fetch_ticker(PAIR_SYMBOL))["last"]
                        )
                    # ----------------------------------------------------------

                    size  = tr["size"]
                    entry = tr["entry_price"]
                    pnl   = (exit_price - entry) * size if tr["side"] == "LONG" else (entry - exit_price) * size

                    state["daily_pnls"].append({"ts": datetime.utcnow().isoformat(),
                                                "pnl_usd": pnl})
                    save_state()

                    await notify(f"ℹ️ Позиция закрыта  P&L {pnl:+.2f}$")
                    sheet_log([
                        datetime.utcnow().isoformat(), "CLOSE",
                        tr["side"], size, entry, exit_price, pnl
                    ])

                    state["active_trade"] = None
                    save_state()

                await asyncio.sleep(60)
                continue
              
            # поиск точки входа
            ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=100)
            df    = add_indicators(df_from_ohlcv(ohlcv)); last = df.iloc[-1]; price = last["close"]

            if last[ADX_COL] >= state["adx_threshold"]:
                await asyncio.sleep(60); continue

            side = "LONG" if price<=last[BBL_COL] and last[RSI_COL]<RSI_OS else \
                   "SHORT" if price>=last[BBU_COL] and last[RSI_COL]>RSI_OB else None
            if not side:
                await asyncio.sleep(60); continue

            res = await execute_trade(ex, side, price)
            if res:
                oid, size = res                     # ← распаковка!
                state["active_trade"] = {"id":oid,"side":side,"entry_price":price,"size":size}
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
        if now > tgt:
            tgt += timedelta(days=1)
        try:
            await asyncio.sleep((tgt-now).total_seconds())
        except asyncio.CancelledError:
            break
        data = state.pop("daily_pnls", []); state["daily_pnls"] = []; save_state()
        if not data:
            await notify("📊 За сутки сделок не было"); continue
        pnl  = sum(d["pnl_usd"] for d in data)
        wins = sum(d["pnl_usd"] > 0 for d in data)
        wr   = wins/len(data)*100
        await notify(f"📊 24-ч отчёт: {len(data)} сделок • win-rate {wr:.1f}% • P&L {pnl:+.2f}$")
      
# ─────────────────── TELEGRAM COMMANDS ───────────────────────────────────
async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await notify("🚀 Flat-Liner запущен. Используйте /status.", c.bot)
    await cmd_status(u, c)

async def cmd_status(u: Update, c: ContextTypes.DEFAULT_TYPE):
    status = "🟢" if state["monitoring"] else "🔴"
    trade  = (f"\nАктивная позиция ID {state['active_trade']['id']}"
              if state["active_trade"] else "")
    txt = (f"<b>Flat-Liner status</b>\n\nМониторинг: {status}"
           f"\nПлечо: {state['leverage']}x  |  Депозит: {state['deposit']}$"
           f"{trade}")
    await u.message.reply_text(txt, parse_mode="HTML")

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
        lev = int(c.args[0]); assert 1 <= lev <= 125
        ex = await create_exchange(); await set_leverage(ex, lev); await ex.close()
        state["leverage"] = lev; save_state()
        await u.message.reply_text(f"Плечо = {lev}x")
    except: await u.message.reply_text("Формат: /set_leverage 50")

async def cmd_test_trade(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /test_trade side=LONG deposit=50 leverage=20
    SL/TP в процентах — как в основной логике.
    """
    try:
        args = {k.lower(): v for k, v in (arg.split('=', 1) for arg in c.args)}
        side = args.get('side', '').upper()
        if side not in ['LONG', 'SHORT']:
            raise ValueError
        deposit  = float(args.get('deposit', state['deposit']))
        leverage = int  (args.get('leverage', state['leverage']))
    except Exception:
        await u.message.reply_text("❌ Параметры: side=LONG|SHORT deposit=50 leverage=20")
        return

    await u.message.reply_text(f"🛠️ Открываю тестовую позицию {side}…")
    ex = None
    try:
        ex = await create_exchange()
        await set_leverage(ex, leverage)
        market = ex.market(PAIR_SYMBOL)
        price  = (await ex.fetch_ticker(PAIR_SYMBOL))['last']
        size, step = calc_size(market, price, deposit, leverage)
        if size < step:
            await u.message.reply_text(f"🔴 Size ({size}) < min ({step})."); return

        pos_side   = "long" if side=="LONG" else "short"
        order_side = "buy"  if side=="LONG" else "sell"
        order = await ex.create_order(
            PAIR_SYMBOL, "market", order_side, size,
            params={"tdMode":"isolated", "posSide":pos_side})
        await u.message.reply_text(f"✅ Ордер <code>{order['id']}</code> создан. Ставлю SL/TP…", parse_mode="HTML")

        await place_tp_sl(ex, size, side, pos_side, price)
        await u.message.reply_text("✅ SL/TP выставлены.", parse_mode="HTML")

    except Exception as e:
        log.error("cmd_test_trade error: %s", e)
        await u.message.reply_text(f"🔥 Ошибка: <code>{e}</code>", parse_mode="HTML")
    finally:
        if ex: await ex.close()
          
# ─────────────────── MAIN ────────────────────────────────────────────────
async def post_init_tasks(app: Application):
    await notify("♻️ Бот перезапущен.", app.bot)
    if not state["monitoring"]:
        state["monitoring"] = True; save_state()
    asyncio.create_task(monitor(app)); asyncio.create_task(reporter(app))

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
