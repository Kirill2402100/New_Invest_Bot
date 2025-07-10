#!/usr/bin/env python3
# ============================================================================
# Sniper Strategy v2.1-fix  •  10 Jul 2025
# ============================================================================
# • Volume filter   : 5-min volume ≥ MIN_VOLUME_BTC  (default 1 BTC)
# • Trend  filter   : ADX < MAX_ADX                 (default 25)
# • LONG bias       : shorts только при RSI > SHORT_RSI_FILTER (default 60)
# • Dynamic TP/SL   : 0.08 / 0.10 / 0.15 % по ATR-коридорам
# • Versioned sheet : SniperLog_<PAIR>_<TF>_<STRAT_VERSION>
# ============================================================================

import os, json, logging, re, uuid, asyncio
from datetime import datetime, timezone

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

# ── ENV ──────────────────────────────────────────────────────────────────────
BOT_TOKEN      = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW   = os.getenv("CHAT_IDS", "")
SHEET_ID       = os.getenv("SHEET_ID")
PAIR_RAW       = os.getenv("PAIR", "BTC/USDT")
TIMEFRAME      = os.getenv("TIMEFRAME", "5m")
STRAT_VERSION  = os.getenv("STRAT_VERSION", "v2_1")

MIN_VOLUME_BTC   = float(os.getenv("MIN_VOLUME_BTC",   "1"))
MAX_ADX          = float(os.getenv("MAX_ADX",          "25"))
SHORT_RSI_FILTER = float(os.getenv("SHORT_RSI_FILTER", "60"))

ATR_LOW_USD   = float(os.getenv("ATR_LOW_USD",  "80"))
ATR_HIGH_USD  = float(os.getenv("ATR_HIGH_USD", "120"))
TP_PCT_LOW    = float(os.getenv("TP_PCT_LOW",   "0.08"))
TP_PCT_MID    = float(os.getenv("TP_PCT_MID",   "0.10"))
TP_PCT_HIGH   = float(os.getenv("TP_PCT_HIGH",  "0.15"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger("Sniper-v2.1")

if not BOT_TOKEN:
    log.critical("ENV BOT_TOKEN not set"); raise SystemExit
if not re.fullmatch(r"\d+[mhdM]", TIMEFRAME):
    log.critical("Bad TIMEFRAME '%s'", TIMEFRAME); raise SystemExit

CHAT_IDS = {int(cid) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

# ── GOOGLE SHEETS ────────────────────────────────────────────────────────────
TRADE_LOG_WS = None
def setup_google_sheets() -> None:
    global TRADE_LOG_WS
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(os.getenv("GOOGLE_CREDENTIALS")),
            ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
        )
        gs = gspread.authorize(creds)
        ss = gs.open_by_key(SHEET_ID)

        headers = [
            "Signal_ID","Version","Status","Side",
            "Entry_Time_UTC","Exit_Time_UTC",
            "Entry_Price","Exit_Price","SL_Price","TP_Price",
            "MFE_Price","MAE_Price",
            "Entry_RSI","Entry_ADX","Entry_ATR",
            "Entry_Volume","Entry_BB_Position"
        ]

        name = f"SniperLog_{PAIR_RAW.replace('/','_')}_{TIMEFRAME}_{STRAT_VERSION}"
        try:
            ws = ss.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(name, rows="1000", cols=len(headers))

        if ws.row_values(1) != headers:
            ws.clear(); ws.update("A1", [headers])
            ws.format(f"A1:{chr(ord('A')+len(headers)-1)}1",
                      {"textFormat": {"bold": True}})

        TRADE_LOG_WS = ws
        log.info("Logging to Google Sheet tab ➜ %s", name)
    except Exception as e:
        log.error("Google Sheets init failed: %s", e)

setup_google_sheets()

# ── STATE ────────────────────────────────────────────────────────────────────
STATE_FILE = "btc_sniper_state.json"
state = {"monitoring": False, "active_trade": None}
if os.path.exists(STATE_FILE):
    state.update(json.load(open(STATE_FILE)))

def save_state():
    json.dump(state, open(STATE_FILE, "w"), indent=2)

# ── EXCHANGE & INDICATORS ────────────────────────────────────────────────────
exchange = ccxt.mexc()    # async
PAIR = PAIR_RAW.upper()

RSI_LEN = 14
EMA_FAST, EMA_SLOW = 9, 21
ATR_LEN, ADX_LEN = 14, 14
BBANDS_LEN = 20
RSI_LONG_ENTRY, RSI_SHORT_ENTRY = 52, 48

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df.ta.ema(length=EMA_FAST , append=True, col_names=(f"EMA_{EMA_FAST}",))
    df.ta.ema(length=EMA_SLOW , append=True, col_names=(f"EMA_{EMA_SLOW}",))
    df.ta.rsi(length=RSI_LEN  , append=True, col_names=(f"RSI_{RSI_LEN}",))
    df.ta.atr(length=ATR_LEN  , append=True, col_names=(f"ATR_{ATR_LEN}",))
    # ADX returns 3 series ➜ ADX, +DI, –DI
    df.ta.adx(length=ADX_LEN, append=True,
              col_names=(f"ADX_{ADX_LEN}", f"DMP_{ADX_LEN}", f"DMN_{ADX_LEN}"))
    df.ta.bbands(length=BBANDS_LEN, std=2, append=True,
                 col_names=(f"BBL_{BBANDS_LEN}_2.0", f"BBM_{BBANDS_LEN}_2.0",
                            f"BBU_{BBANDS_LEN}_2.0", f"BBB_{BBANDS_LEN}_2.0",
                            f"BBP_{BBANDS_LEN}_2.0"))
    return df.dropna()

def atr_bucket(atr: float) -> float:
    if atr <= ATR_LOW_USD:        return TP_PCT_LOW
    if atr >= ATR_HIGH_USD:       return TP_PCT_HIGH
    return TP_PCT_MID

# ── TELEGRAM ────────────────────────────────────────────────────────────────
async def notify(bot: Bot, text: str):
    for cid in CHAT_IDS:
        try:    await bot.send_message(cid, text, parse_mode="HTML")
        except Exception as e: log.error("TG fail -> %s: %s", cid, e)

# ── GOOGLE-SHEETS WRITE ─────────────────────────────────────────────────────
async def log_trade(tr: dict):
    if not TRADE_LOG_WS: return
    row = [
        tr["id"], STRAT_VERSION, tr["status"], tr["side"],
        tr["entry_time_utc"], datetime.now(timezone.utc).isoformat(),
        tr["entry_price"], tr["exit_price"],
        tr["sl_price"], tr["tp_price"],
        tr["mfe_price"], tr["mae_price"],
        tr["entry_rsi"], tr["entry_adx"], tr["entry_atr"],
        tr["entry_volume"], tr["entry_bb_pos"]
    ]
    await asyncio.to_thread(
        TRADE_LOG_WS.append_row, row, value_input_option="USER_ENTERED"
    )

# ── MAIN LOOP ───────────────────────────────────────────────────────────────
async def monitor(app: Application):
    log.info("Loop start: %s %s (%s)", PAIR, TIMEFRAME, STRAT_VERSION)
    while state["monitoring"]:
        try:
            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
            df = add_indicators(pd.DataFrame(
                ohlcv, columns=["ts","open","high","low","close","volume"]
            ))

            last, prev = df.iloc[-1], df.iloc[-2]
            price = last["close"]

            # ---------------- active trade management -----------------------
            if trade := state["active_trade"]:
                side, sl, tp = trade["side"], trade["sl_price"], trade["tp_price"]

                if side == "LONG":
                    trade["mfe_price"] = max(trade["mfe_price"], price)
                    trade["mae_price"] = min(trade["mae_price"], price)
                else:
                    trade["mfe_price"] = min(trade["mfe_price"], price)
                    trade["mae_price"] = max(trade["mae_price"], price)

                done = status = None
                if side == "LONG"  and price >= tp: done, status = "TP_HIT", "WIN"
                elif side == "LONG" and price <= sl: done, status = "SL_HIT", "LOSS"
                elif side == "SHORT" and price <= tp: done, status = "TP_HIT", "WIN"
                elif side == "SHORT" and price >= sl: done, status = "SL_HIT", "LOSS"

                if done:
                    trade["status"] = status
                    trade["exit_price"] = price
                    await notify(app.bot,
                        f"{'✅' if status=='WIN' else '❌'} <b>TRADE CLOSED {status}</b>\\n"
                        f"ID: {trade['id']}  •  Exit: {price:.2f}"
                    )
                    await log_trade(trade)
                    state["active_trade"] = None; save_state()

            # ---------------- new signal search -----------------------------
            else:
                vol_ok  = last["volume"] >= MIN_VOLUME_BTC
                adx_ok  = last[f"ADX_{ADX_LEN}"] < MAX_ADX
                if not (vol_ok and adx_ok):              # фильтры
                    await asyncio.sleep(60); continue

                bull_now = last[f"EMA_{EMA_FAST}"] > last[f"EMA_{EMA_SLOW}"]
                bull_prev= prev[f"EMA_{EMA_FAST}"] > prev[f"EMA_{EMA_SLOW}"]

                long_cond  = last[f"RSI_{RSI_LEN}"] > RSI_LONG_ENTRY and price > last[f"EMA_{EMA_FAST}"]
                short_cond = (last[f"RSI_{RSI_LEN}"] < RSI_SHORT_ENTRY and
                              last[f"RSI_{RSI_LEN}"] > SHORT_RSI_FILTER and
                              price < last[f"EMA_{EMA_FAST}"])

                side = None
                if  bull_now and not bull_prev and long_cond:  side = "LONG"
                elif not bull_now and bull_prev and short_cond: side = "SHORT"

                if side:
                    atr = last[f"ATR_{ATR_LEN}"]
                    tpct= atr_bucket(atr)

                    entry = price
                    tp = entry * (1 + tpct/100) if side=="LONG" else entry * (1 - tpct/100)
                    sl = entry * (1 - tpct/100) if side=="LONG" else entry * (1 + tpct/100)

                    bb_up, bb_lo = last[f"BBU_{BBANDS_LEN}_2.0"], last[f"BBL_{BBANDS_LEN}_2.0"]
                    bb_pos = "Inside"
                    if entry > bb_up:  bb_pos = "Above_Upper"
                    elif entry < bb_lo: bb_pos = "Below_Lower"

                    trade = dict(
                        id=uuid.uuid4().hex[:8], side=side,
                        entry_time_utc=datetime.now(timezone.utc).isoformat(),
                        entry_price=entry, tp_price=tp, sl_price=sl,
                        mfe_price=entry, mae_price=entry,
                        entry_rsi=round(last[f"RSI_{RSI_LEN}"],2),
                        entry_adx=round(last[f"ADX_{ADX_LEN}"],2),
                        entry_atr=round(atr,2),
                        entry_volume=last["volume"], entry_bb_pos=bb_pos
                    )
                    state["active_trade"] = trade; save_state()

                    await notify(
                        app.bot,
                        f"🔔 <b>NEW SIGNAL {side}</b> 🔔\\n"
                        f"ID: {trade['id']}  •  Version: {STRAT_VERSION}\\n"
                        f"Entry: {entry:.2f}  |  TP: {tp:.2f}  |  SL: {sl:.2f}\\n"
                        f"ATR bucket: {tpct}%"
                    )

        except ccxt.NetworkError as e:
            log.warning("CCXT network error: %s", e); await asyncio.sleep(60)
        except Exception as e:
            log.exception("Monitor loop crash:"); await asyncio.sleep(30)

        await asyncio.sleep(60)   # one-minute cadence

    log.info("Monitor loop stopped")

# ── TELEGRAM CMDS & APP BOOT ─────────────────────────────────────────────────
async def _start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS:
        CHAT_IDS.add(update.effective_chat.id)
    if not state["monitoring"]:
        state["monitoring"] = True; save_state()
        await update.message.reply_text("✅ Sniper bot started.")
        asyncio.create_task(monitor(ctx.application))
    else:
        await update.message.reply_text("ℹ️ Bot already running.")

async def _stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state["monitoring"] = False; save_state()
    await update.message.reply_text("❌ Bot stopped.")

async def _status(update: Update, _):
    msg = f"<b>Status:</b> {'ACTIVE' if state['monitoring'] else 'STOPPED'}\\n"
    if tr := state["active_trade"]:
        msg += (f"{tr['side']} | ID {tr['id']}\\n"
                f"Entry {tr['entry_price']:.2f}  TP {tr['tp_price']:.2f}  SL {tr['sl_price']:.2f}")
    else:
        msg += "No active trades."
    await update.message.reply_text(msg, parse_mode="HTML")

async def _post_init(app: Application):
    if state["monitoring"]:
        asyncio.create_task(monitor(app))

if __name__ == "__main__":
    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .post_init(_post_init)
           .build())
    app.add_handler(CommandHandler("start",  _start))
    app.add_handler(CommandHandler("stop",   _stop))
    app.add_handler(CommandHandler("status", _status))
    app.run_polling()
