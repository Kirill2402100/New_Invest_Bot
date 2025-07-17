#!/usr/bin/env python3
# ============================================================================
#  Flat‑Liner • Heroku edition — 17 Jul 2025  (debug build, full logging)
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
    SHEET_ID, GOOGLE_CREDENTIALS      (JSON service-account → строкой)
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

PAIR_SYMBOL = os.getenv("PAIR_SYMBOL", "BTC-USDT-SWAP")      # биржевой id
TIMEFRAME   = os.getenv("TIMEFRAME",   "5m")

OKX_API_KEY        = os.getenv("OKX_API_KEY")
OKX_API_SECRET     = os.getenv("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")
OKX_SANDBOX        = os.getenv("OKX_DEMO_MODE", "0") == "1"

DEFAULT_DEPOSIT  = float(os.getenv("DEFAULT_DEPOSIT_USD", 50))
DEFAULT_LEVERAGE = int  (os.getenv("DEFAULT_LEVERAGE",    100))

SL_PCT, RR_RATIO = 0.10, 1.0        # SL = 0.10 %;    TP = SL  (RR 1 : 1)
RSI_OS, RSI_OB   = 35, 65
REPORT_UTC_HOUR  = int(os.getenv("REPORT_HOUR_UTC", 21))

# Google Sheets
SHEET_ID           = os.getenv("SHEET_ID")           # обязательна
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS") # JSON-строка service account

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
    "active_trade": None,       # {id, side, entry_price, size}
    "deposit":  DEFAULT_DEPOSIT,
    "leverage": DEFAULT_LEVERAGE,
    "adx_threshold": 25.0,
    "last_adx_recalc": None,
    "daily_pnls": [],           # [{ts, pnl_usd}]
    "last_adx_value": 25.0      # Для ticker
}

def save_state(): STATE_FILE.write_text(json.dumps(state, indent=2))
def load_state():
    if STATE_FILE.exists():
        try: state.update(json.loads(STATE_FILE.read_text()))
        except: log.warning("STATE‑файл повреждён → создаю новый")
    save_state()

# ─────────────────── HELPERS ─────────────────────────────────────────────
async def notify(text: str, bot: Optional[Bot] = None, parse_mode="HTML"):
    """Отправка сообщения во все CHAT_IDS (async-safe)."""
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
    Возвращает (size, step) c учётом:
      • минимального шага (step)
      • точности amount (precision, целое кол-во знаков)
    """
    step = float(market["limits"]["amount"]["min"])          # 0.01
    # precision["amount"] иногда возвращает float (0.01), иногда int (2).
    raw_prec = market["precision"].get("amount", 0) or 0
    prec = int(raw_prec) if isinstance(raw_prec, (int, float)) else 0

    raw  = (deposit * leverage) / price / float(market["contractSize"])
    size = math.floor(raw / step) * step                      # учитываем шаг
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
    return sl_price, tp_price

async def execute_trade(ex, side: str, price: float, entry_adx: float, bot: Bot):
    """Создать рыночный ордер, выставить SL/TP, занести строку OPEN в G-Sheets."""
    market = ex.market(PAIR_SYMBOL)
    size, step = calc_size(market, price, state["deposit"], state["leverage"])
    if size < step:
        await notify(f"🔴 Минимальный объём — {step}. Увеличьте депозит/плечо.", bot)
        return None

    pos_side   = "long" if side == "LONG" else "short"
    order_side = "buy"  if side == "LONG" else "sell"

    # --- market-ордер ------------------------------------------------------
    order = await ex.create_order(
        PAIR_SYMBOL, "market", order_side, size,
        params={"tdMode": "isolated", "posSide": pos_side}
    )
    fee_open = order.get("fee", {}).get("cost", 0.0)

    # --- SL / TP -----------------------------------------------------------
    sl_price, tp_price = await place_tp_sl(ex, size, side, pos_side, price)
    
    # --- Уведомление об открытии --------------------------------------------
    await notify(
        f"🟢 ОТКРЫТ <b>{side}</b> • size {size} • entry {price}\n"
        f"SL {sl_price:.4f}  |  TP {tp_price:.4f}",
        bot, parse_mode="HTML"
    )

    # --- JOURNAL : строка OPEN --------------------------------------------
    now_iso = datetime.utcnow().isoformat()
    sheet_log([
        order["id"], "Flat-Liner v28-2025-07-17", "Flat_BB_Fade", "OPEN",
        side, now_iso, "", price, "", sl_price, tp_price, "", "", "",
        state["deposit"], entry_adx, state["adx_threshold"]
    ])

    # --- сохраняем в state --------------------------------------------------
    return {
        "id":          order["id"],
        "side":        side,
        "entry_price": price,
        "size":        size,
        "entry_time":  now_iso,
        "fee_open":    fee_open,
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
    log.info("🚀 Мониторинг запущен")

    try:
        while state["monitoring"]:
            # C. В начале каждой итерации мониторинга записываем ADX, чтобы ticker имел актуальное значение
            try:
                ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=100)
                df    = add_indicators(df_from_ohlcv(ohlcv))
                last  = df.iloc[-1]
                state["last_adx_value"] = float(last[ADX_COL])
            except Exception as e:
                log.error("Ошибка получения OHLCV в цикле monitor: %s", e)
                await asyncio.sleep(60)
                continue

            # ── пересчёт ADX порога раз в час ────────────────────────────
            last_recalc = state["last_adx_recalc"]
            if (not last_recalc or
                (datetime.now(timezone.utc) - datetime.fromisoformat(last_recalc)).total_seconds() > 3600):
                await recalc_adx_threshold()

            # ── контроль открытой позиции ────────────────────────────────
            if (tr := state.get("active_trade")):
                poss = await ex.fetch_positions([PAIR_SYMBOL])
                side_mark = "long" if tr["side"] == "LONG" else "short"

                still_open = any(p["side"] == side_mark and float(p.get("contracts", 0)) > 0 for p in poss)
                if still_open:
                    await asyncio.sleep(60)
                    continue

                # ----- позиция закрыта -----------------------------------
                last_ticker = await ex.fetch_ticker(PAIR_SYMBOL)
                exit_price = float(last_ticker['last'])

                # P&L
                gross_pnl = (
                    (exit_price - tr["entry_price"]) * tr["size"] * float(ex.market(PAIR_SYMBOL)['contractSize'])
                    if tr["side"] == "LONG"
                    else (tr["entry_price"] - exit_price) * tr["size"] * float(ex.market(PAIR_SYMBOL)['contractSize'])
                )
                
                # комиссия
                try:
                    filled_order = await ex.fetch_order(tr["id"], PAIR_SYMBOL)
                    fee_close = filled_order.get("fee", {}).get("cost", 0.0)
                except ccxt.OrderNotFound:
                    log.warning("Ордер %s не найден для расчёта комиссии закрытия.", tr['id'])
                    fee_close = 0.0
                fee_total = tr.get("fee_open", 0.0) + fee_close
                net_pnl = gross_pnl - fee_total

                # статистика
                state["daily_pnls"].append({"ts": datetime.utcnow().isoformat(), "pnl_usd": net_pnl})
                
                # Уведомление о закрытии
                await notify(
                    f"🔴 ЗАКРЫТО  {tr['side']} • {net_pnl:+.2f}$  "
                    f"(gross {gross_pnl:+.2f}$  |  fee {fee_total:.4f})\n"
                    f"Цена выхода: {exit_price}",
                    app.bot
                )

                # журнал (Google Sheets) — строка CLOSE
                sheet_log([
                    tr["id"], "Flat-Liner v28-2025-07-17", "Flat_BB_Fade", "CLOSE",
                    tr["side"], tr["entry_time"], datetime.utcnow().isoformat(),
                    tr["entry_price"], exit_price, tr["sl_price"], tr["tp_price"],
                    gross_pnl, fee_total, net_pnl, tr["deposit"], tr["entry_adx"], state["adx_threshold"]
                ])

                state["active_trade"] = None
                save_state()
                await asyncio.sleep(60)
                continue

            # ── поиск новой точки входа ──────────────────────────────────
            price = last["close"]

            if last[ADX_COL] >= state["adx_threshold"]:
                await asyncio.sleep(60)
                continue

            side = (
                "LONG"  if price <= last[BBL_COL] and last[RSI_COL] < RSI_OS else
                "SHORT" if price >= last[BBU_COL] and last[RSI_COL] > RSI_OB else
                None
            )
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
        except Exception: pass
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
            await notify("📊 За сутки сделок не было", app.bot); continue
        pnl  = sum(d["pnl_usd"] for d in data)
        wins = sum(d["pnl_usd"] > 0 for d in data)
        wr   = wins/len(data)*100
        await notify(f"📊 24-ч отчёт: {len(data)} сделок • win-rate {wr:.1f}% • P&L {pnl:+.2f}$", app.bot)
        
# ─────────────────── ГЕНЕРАЦИЯ СТАТУСА ────────────────────────────────────
async def get_status_text() -> str:
    """Получает актуальные данные с биржи и формирует текстовое представление статуса."""
    ex = await create_exchange()
    try:
        last_ohlcv = await ex.fetch_ohlcv(PAIR_SYMBOL, TIMEFRAME, limit=100)
        last = add_indicators(df_from_ohlcv(last_ohlcv)).iloc[-1]
    finally:
        await ex.close()

    adx_now = last[ADX_COL]
    adx_dyn = state["adx_threshold"]
    is_flat = adx_now < adx_dyn
    boll_pos = (
        "⬇︎ ниже BBL" if last["close"] <= last[BBL_COL] else
        "⬆︎ выше BBU" if last["close"] >= last[BBU_COL] else
        "⋯ внутри канала"
    )

    status = '🟢 ON' if state["monitoring"] else '🔴 OFF'
    trade = (f"\nАктивная позиция ID <code>{state['active_trade']['id']}</code>"
             if state["active_trade"] else "")
    
    return (f"<b>Flat-Liner status</b>\n"
            f"Мониторинг: {status}{trade}\n"
            f"Плечо: <b>{state['leverage']}x</b>   Депозит: <b>{state['deposit']}$</b>\n\n"
            f"ADX текущий: <b>{adx_now:.2f}</b>\n"
            f"ADX порог : <b>{adx_dyn:.2f}</b>\n"
            f"Режим: <b>{'FLAT' if is_flat else 'TREND'}</b>\n"
            f"RSI-14: <b>{last[RSI_COL]:.2f}</b>\n"
            f"Цена vs BB: {boll_pos}")

# ─────────────────── TICKER (авто-статусы) ───────────────────────────────
async def ticker(app: Application):
    """B. Корутина для периодической отправки статуса."""
    while True:
        try:
            status_text = await get_status_text()
            await notify(status_text, app.bot)

            adx_now = state.get("last_adx_value", state["adx_threshold"])
            interval = 1800 if adx_now < state["adx_threshold"] else 3600
            await asyncio.sleep(interval)
            
        except asyncio.CancelledError:
            log.info("Ticker-задача отменена.")
            break
        except Exception as e:
            log.error("Ошибка в ticker: %s", e)
            await notify(f"‼️ Ошибка в ticker: {e}", app.bot)
            await asyncio.sleep(60)

# ─────────────────── TELEGRAM COMMANDS ───────────────────────────────────
async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await notify("🚀 Flat-Liner запущен. Используйте /status.", c.bot)
    await cmd_status(u, c)

async def cmd_status(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """A. Новый обработчик cmd_status"""
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
        lev = int(c.args[0]); assert 1 <= lev <= 125
        ex = await create_exchange(); await set_leverage(ex, lev); await ex.close()
        state["leverage"] = lev; save_state()
        await u.message.reply_text(f"Плечо = {lev}x")
    except: await u.message.reply_text("Формат: /set_leverage 50")

async def cmd_test_trade(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """/test_trade side=LONG deposit=50 leverage=20"""
    try:
        args = {k.lower(): v for k, v in (arg.split('=', 1) for arg in c.args)}
        side = args.get('side', '').upper()
        if side not in ['LONG', 'SHORT']: raise ValueError
        deposit  = float(args.get('deposit', state['deposit']))
        leverage = int(float(args.get('leverage', state['leverage'])))
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
    """C. Задачи, запускаемые после инициализации + добавление ticker'а"""
    await notify("♻️ <b>Бот перезапущен</b>\nНачинаю мониторинг...", app.bot)
    if not state["monitoring"]:
        state["monitoring"] = True; save_state()
    asyncio.create_task(monitor(app))
    asyncio.create_task(reporter(app))
    asyncio.create_task(ticker(app)) # ← ДОБАВИЛИ

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
