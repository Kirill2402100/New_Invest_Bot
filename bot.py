#!/usr/bin/env python3
# ============================================================================
# v12.1 - Sniper Strategy v2.1 (Autonomous)
# RELEASE DATE: 2025-07-10
# CHANGELOG:
#   • Added volume filter (MIN_VOLUME_BTC)
#   • Added trend filter (ADX < MAX_ADX)
#   • LONG bias — shorts allowed only if RSI > SHORT_RSI_FILTER
#   • Dynamic TP/SL based on ATR (ATR_LOW_USD, ATR_HIGH_USD, TP_PCT_LOW/MID/HIGH)
#   • Versioned Google‑Sheets tab via STRAT_VERSION
# ============================================================================

import os
import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
import pandas_ta as ta

# === ENV / Logging ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_IDS_RAW = os.getenv("CHAT_IDS", "")
SHEET_ID = os.getenv("SHEET_ID")
PAIR_RAW = os.getenv("PAIR", "BTC/USDT")
TIMEFRAME = os.getenv("TIMEFRAME", "5m")
STRAT_VERSION = os.getenv("STRAT_VERSION", "v2_1")   # <-- new

# New env variables
MIN_VOLUME_BTC  = float(os.getenv("MIN_VOLUME_BTC",  "1"))   # 5‑min volume ≥ 1 BTC
MAX_ADX         = float(os.getenv("MAX_ADX",         "25"))  # filter out strong trend
SHORT_RSI_FILTER= float(os.getenv("SHORT_RSI_FILTER","60"))  # shorts only if RSI > 60

ATR_LOW_USD  = float(os.getenv("ATR_LOW_USD",  "80"))
ATR_HIGH_USD = float(os.getenv("ATR_HIGH_USD", "120"))
TP_PCT_LOW   = float(os.getenv("TP_PCT_LOW",   "0.08"))
TP_PCT_MID   = float(os.getenv("TP_PCT_MID",   "0.10"))
TP_PCT_HIGH  = float(os.getenv("TP_PCT_HIGH",  "0.15"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("httpcore").setLevel(logging.WARNING)

if not BOT_TOKEN:
    log.critical("ENV BOT_TOKEN missing!")
    raise SystemExit

if not re.match(r'^\\d+[mhdM]$', TIMEFRAME):
    log.critical(f"Bad timeframe '{TIMEFRAME}'. Ex: 1h, 15m, 1d.")
    raise SystemExit

CHAT_IDS = {int(cid.strip()) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

# === GOOGLE SHEETS ===========================================================
TRADE_LOG_WS = None
def setup_google_sheets():
    try:
        creds_json = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            creds_json,
            ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
        )
        gs = gspread.authorize(creds)
        spreadsheet = gs.open_by_key(SHEET_ID)

        headers = [
            "Signal_ID", "Version", "Status", "Side",
            "Entry_Time_UTC", "Exit_Time_UTC",
            "Entry_Price", "Exit_Price", "SL_Price", "TP_Price",
            "MFE_Price", "MAE_Price",
            "Entry_RSI", "Entry_ADX", "Entry_ATR",
            "Entry_Volume", "Entry_BB_Position"
        ]

        sheet_name = f"SniperLog_{PAIR_RAW.replace('/', '_')}_{TIMEFRAME}_{STRAT_VERSION}"
        try:
            ws = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols=len(headers))

        if ws.row_values(1) != headers:
            ws.clear()
            ws.update('A1', [headers])
            ws.format(f'A1:{chr(ord('A') + len(headers) - 1)}1',
                      {'textFormat': {'bold': True}})

        log.info(f"Google Sheets ready → {sheet_name}")
        return ws
    except Exception as exc:
        log.error("Google Sheets init failed: %s", exc)
        return None

TRADE_LOG_WS = setup_google_sheets()

# === STATE MGMT ==============================================================
STATE_FILE = "btc_sniper_state.json"
state = {"monitoring": False, "active_trade": None}

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
    if 'monitoring' not in state:
        state.update({"monitoring": False, "active_trade": None})
    log.info("State loaded: %s", state)

# === EXCHANGE & INDICATOR PARAMS =============================================
exchange = ccxt.mexc()
PAIR = PAIR_RAW.upper()

RSI_LEN = 14
EMA_FAST_LEN, EMA_SLOW_LEN = 9, 21
ATR_LEN = 14
ADX_LEN = 14
BBANDS_LEN = 20

RSI_LONG_ENTRY, RSI_SHORT_ENTRY = 52, 48

# === INDICATORS ==============================================================
def calculate_indicators(df: pd.DataFrame):
    df.ta.ema(length=EMA_FAST_LEN, append=True, col_names=(f\"EMA_{EMA_FAST_LEN}\",))
    df.ta.ema(length=EMA_SLOW_LEN, append=True, col_names=(f\"EMA_{EMA_SLOW_LEN}\",))
    df.ta.rsi(length=RSI_LEN, append=True, col_names=(f\"RSI_{RSI_LEN}\",))
    df.ta.atr(length=ATR_LEN, append=True, col_names=(f\"ATR_{ATR_LEN}\",))
    df.ta.adx(length=ADX_LEN, append=True,
              col_names=(f\"ADX_{ADX_LEN}\", f\"DMP_{ADX_LEN}\", f\"DMN_{ADX_LEN}\"))
    df.ta.bbands(length=BBANDS_LEN, std=2, append=True,
                 col_names=(f\"BBL_{BBANDS_LEN}_2.0\", f\"BBM_{BBANDS_LEN}_2.0\",
                            f\"BBU_{BBANDS_LEN}_2.0\", f\"BBB_{BBANDS_LEN}_2.0\",
                            f\"BBP_{BBANDS_LEN}_2.0\"))
    return df.dropna()

def bucket_target_pct(atr_usd: float) -> float:
    if atr_usd <= ATR_LOW_USD:
        return TP_PCT_LOW
    if atr_usd >= ATR_HIGH_USD:
        return TP_PCT_HIGH
    return TP_PCT_MID

# === GOOGLE SHEETS LOGGING ====================================================
async def log_trade_to_gs(trade: dict):
    if not TRADE_LOG_WS:
        return
    try:
        row = [
            trade['id'], STRAT_VERSION, trade['status'], trade['side'],
            trade['entry_time_utc'], datetime.now(timezone.utc).isoformat(),
            trade['entry_price'], trade['exit_price'],
            trade['sl_price'], trade['tp_price'],
            trade['mfe_price'], trade['mae_price'],
            trade['entry_rsi'], trade['entry_adx'], trade['entry_atr'],
            trade['entry_volume'], trade['entry_bb_pos']
        ]
        await asyncio.to_thread(
            TRADE_LOG_WS.append_row, row, value_input_option='USER_ENTERED'
        )
        log.info("Trade %s logged", trade['id'])
    except Exception as exc:
        log.error("Google Sheets write failed: %s", exc, exc_info=True)

# === TELEGRAM ================================================================
async def broadcast(bot: Bot, text: str):
    for cid in CHAT_IDS:
        try:
            await bot.send_message(cid, text, parse_mode=\"HTML\")
        except Exception as exc:
            log.error("Telegram send failed to %s: %s", cid, exc)

# === MAIN LOOP ===============================================================
async def monitor(app: Application):
    log.info("Loop start: %s %s (%s)", PAIR, TIMEFRAME, STRAT_VERSION)
    while state.get('monitoring', False):
        try:
            ohlcv = await exchange.fetch_ohlcv(PAIR, timeframe=TIMEFRAME, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
            df = calculate_indicators(df)

            if len(df) < 2:
                await asyncio.sleep(30); continue

            last, prev = df.iloc[-1], df.iloc[-2]
            price = last['close']

            vol_ok = last['volume'] >= MIN_VOLUME_BTC
            adx_ok = last[f\"ADX_{ADX_LEN}\"] < MAX_ADX

            is_bull = last[f\"EMA_{EMA_FAST_LEN}\"] > last[f\"EMA_{EMA_SLOW_LEN}\"]
            was_bull = prev[f\"EMA_{EMA_FAST_LEN}\"] > prev[f\"EMA_{EMA_SLOW_LEN}\"]

            # === manage open trade ===
            if trade := state.get('active_trade'):
                side, sl, tp = trade['side'], trade['sl_price'], trade['tp_price']

                if side == 'LONG':
                    trade['mfe_price'] = max(trade['mfe_price'], price)
                    trade['mae_price'] = min(trade['mae_price'], price)
                else:
                    trade['mfe_price'] = min(trade['mfe_price'], price)
                    trade['mae_price'] = max(trade['mae_price'], price)

                outcome = status = None
                if side == 'LONG' and price >= tp: outcome, status = 'TP_HIT','WIN'
                elif side == 'LONG' and price <= sl: outcome, status = 'SL_HIT','LOSS'
                elif side == 'SHORT' and price <= tp: outcome, status = 'TP_HIT','WIN'
                elif side == 'SHORT' and price >= sl: outcome, status = 'SL_HIT','LOSS'

                if outcome:
                    trade['status'] = status
                    trade['exit_price'] = price
                    emoji = '✅' if status == 'WIN' else '❌'
                    msg = (f\"{emoji} <b>TRADE CLOSED {status}</b> {emoji}\\nID: {trade['id']}\\n\"
                           f\"Exit: {price:.2f} (ATR bucket {bucket_target_pct(trade['entry_atr'])}%)\")
                    await broadcast(app.bot, msg)
                    await log_trade_to_gs(trade)
                    state['active_trade'] = None
                    save_state()

            # === look for new signal ===
            elif vol_ok and adx_ok:
                long_cond = last[f\"RSI_{RSI_LEN}\"] > RSI_LONG_ENTRY and price > last[f\"EMA_{EMA_FAST_LEN}\"]
                short_cond = (last[f\"RSI_{RSI_LEN}\"] < RSI_SHORT_ENTRY
                              and price < last[f\"EMA_{EMA_FAST_LEN}\"]
                              and last[f\"RSI_{RSI_LEN}\"] > SHORT_RSI_FILTER)

                side = None
                if is_bull and not was_bull and long_cond: side = 'LONG'
                elif not is_bull and was_bull and short_cond: side = 'SHORT'

                if side:
                    atr_val = last[f\"ATR_{ATR_LEN}\"]
                    target_pct = bucket_target_pct(atr_val)

                    entry_price = price
                    if side == 'LONG':
                        tp_price = entry_price * (1 + target_pct/100)
                        sl_price = entry_price * (1 - target_pct/100)
                    else:
                        tp_price = entry_price * (1 - target_pct/100)
                        sl_price = entry_price * (1 + target_pct/100)

                    bb_upper, bb_lower = last[f\"BBU_{BBANDS_LEN}_2.0\"], last[f\"BBL_{BBANDS_LEN}_2.0\"]
                    bb_pos = 'Inside'
                    if entry_price > bb_upper: bb_pos = 'Above_Upper'
                    elif entry_price < bb_lower: bb_pos = 'Below_Lower'

                    trade_id = uuid.uuid4().hex[:8]
                    state['active_trade'] = {
                        'id': trade_id,
                        'side': side,
                        'entry_time_utc': datetime.now(timezone.utc).isoformat(),
                        'entry_price': entry_price,
                        'tp_price': tp_price,
                        'sl_price': sl_price,
                        'mfe_price': entry_price,
                        'mae_price': entry_price,
                        'entry_rsi': round(last[f\"RSI_{RSI_LEN}\"],2),
                        'entry_adx': round(last[f\"ADX_{ADX_LEN}\"],2),
                        'entry_atr': round(atr_val,2),
                        'entry_volume': last['volume'],
                        'entry_bb_pos': bb_pos
                    }
                    save_state()
                    msg = (f\"🔔 <b>NEW SIGNAL {side}</b> 🔔\\n\"
                           f\"ID: {trade_id} | Version: {STRAT_VERSION}\\n\"
                           f\"Entry: {entry_price:.2f}\\nTP: {tp_price:.2f} | SL: {sl_price:.2f}\\n\"
                           f\"ATR bucket: {target_pct}%\")
                    await broadcast(app.bot, msg)

        except ccxt.NetworkError as net_err:
            log.warning(\"CCXT network error: %s\", net_err)
            await asyncio.sleep(60)
        except Exception as exc:
            log.error(\"Main loop error: %s\", exc, exc_info=True)
            await asyncio.sleep(30)

        await asyncio.sleep(60)

    log.info(\"Monitoring loop stopped.\")

# === COMMANDS ================================================================
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id not in CHAT_IDS:
        CHAT_IDS.add(update.effective_chat.id)
    if not state.get('monitoring'):
        state['monitoring'] = True
        save_state()
        await update.message.reply_text(\"✅ Sniper bot started.\")
        asyncio.create_task(monitor(ctx.application))
    else:
        await update.message.reply_text(\"ℹ️ Bot already running.\")

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if state.get('monitoring'):
        state['monitoring'] = False
        save_state()
        await update.message.reply_text(\"❌ Bot stopped.\")
    else:
        await update.message.reply_text(\"ℹ️ Bot is not running.\")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = f\"<b>Status:</b> {'ACTIVE' if state.get('monitoring') else 'STOPPED'}\\n\"
    if trade := state.get('active_trade'):
        msg += (f\"Current trade {trade['side']} (ID {trade['id']})\\n\"
                f\"Entry {trade['entry_price']:.2f} | TP {trade['tp_price']:.2f} | SL {trade['sl_price']:.2f}\")
    else:
        msg += \"No active trades.\"
    await update.message.reply_text(msg, parse_mode=\"HTML\")

async def post_init(app: Application):
    load_state()
    if state.get('monitoring'):
        asyncio.create_task(monitor(app))

# === ENTRYPOINT ==============================================================
if __name__ == \"__main__\":
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler(\"start\", cmd_start))
    app.add_handler(CommandHandler(\"stop\", cmd_stop))
    app.add_handler(CommandHandler(\"status\", cmd_status))
    app.run_polling()
