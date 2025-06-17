import os
import asyncio
import json
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# === ENV ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_IDS = [int(cid) for cid in os.getenv("CHAT_IDS", "0").split(",")]
PAIR = os.getenv("PAIR", "BTC/USDT")
SHEET_ID = os.getenv("SHEET_ID")

# === GOOGLE SHEETS ===
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gs = gspread.authorize(creds)
LOGS_WS = gs.open_by_key(SHEET_ID).worksheet("LP_Logs")

HEADERS = [
    "Дата-время", "Инструмент", "Депозит", "Вход", "Stop Loss", "Take Profit",
    "RR", "P&L сделки (USDT)", "APR сделки (%)"
]
if LOGS_WS.row_values(1) != HEADERS:
    LOGS_WS.resize(rows=1)
    LOGS_WS.update('A1', [HEADERS])

# === STATE ===
current_signal = None
last_cross = None
position = None
log = []
monitoring = False

# === EXCHANGE ===
exchange = ccxt.mexc()

def calculate_ssl(df):
    sma = df['close'].rolling(13).mean()
    hlv = (df['close'] > sma).astype(int)
    ssl_up, ssl_down = [], []
    for i in range(len(df)):
        if i < 12:
            ssl_up.append(None)
            ssl_down.append(None)
        else:
            if hlv.iloc[i] == 1:
                ssl_up.append(df['high'].iloc[i-12:i+1].max())
                ssl_down.append(df['low'].iloc[i-12:i+1].min())
            else:
                ssl_up.append(df['low'].iloc[i-12:i+1].min())
                ssl_down.append(df['high'].iloc[i-12:i+1].max())
    df['ssl_up'], df['ssl_down'], df['ssl_channel'] = ssl_up, ssl_down, None
    for i in range(1, len(df)):
        if pd.notna(df['ssl_up'].iloc[i]) and pd.notna(df['ssl_down'].iloc[i]):
            prev, curr = df.iloc[i - 1], df.iloc[i]
            if prev['ssl_up'] < prev['ssl_down'] and curr['ssl_up'] > curr['ssl_down']:
                df.at[df.index[i], 'ssl_channel'] = 'LONG'
            elif prev['ssl_up'] > prev['ssl_down'] and curr['ssl_up'] < curr['ssl_down']:
                df.at[df.index[i], 'ssl_channel'] = 'SHORT'
    return df

async def fetch_ssl_signal():
    ohlcv = exchange.fetch_ohlcv(PAIR, timeframe='15m', limit=100)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df = calculate_ssl(df)
    valid_signals = df['ssl_channel'].dropna()
    if len(valid_signals) < 2:
        return None, df['close'].iloc[-1]
    prev, curr = valid_signals.iloc[-2], valid_signals.iloc[-1]
    price = df['close'].iloc[-1]
    return (curr, price) if prev != curr else (None, price)

async def monitor_signal(app):
    global current_signal, last_cross
    while monitoring:
        try:
            signal, price = await fetch_ssl_signal()
            if signal and signal != current_signal:
                current_signal = signal
                last_cross = datetime.now(timezone.utc)
                for chat_id in app.chat_ids:
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=f"\U0001f4e1 Сигнал: {signal}\n\U0001f4b0 Цена: {price:.4f}\n\u23f0 Время: {last_cross.strftime('%H:%M UTC')}"
                    )
        except Exception as e:
            print("[error]", e)
        await asyncio.sleep(30)

# === COMMANDS ===
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global monitoring
    chat_id = update.effective_chat.id
    ctx.application.chat_ids.add(chat_id)
    monitoring = True
    await update.message.reply_text("✅ Мониторинг запущен.")
    asyncio.create_task(monitor_signal(ctx.application))

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global monitoring
    monitoring = False
    await update.message.reply_text("❌ Мониторинг остановлен.")

async def cmd_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global position
    try:
        deposit, entry, sl, tp = map(float, ctx.args[:4])
        rr = abs((tp - entry) / (sl - entry))
        position = {
            "entry_time": datetime.now(timezone.utc),
            "entry_deposit": deposit,
            "entry_price": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "direction": current_signal
        }
        await update.message.reply_text(
            f"✅ Вход: {current_signal} @ {entry}\nДепозит: {deposit:.2f}$ | SL: {sl} | TP: {tp} | RR: {rr:.2f}"
        )
    except:
        await update.message.reply_text("⚠️ Использование: /entry <депозит> <вход> <SL> <TP>")

async def cmd_exit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global position
    try:
        exit_deposit = float(ctx.args[0])
        exit_price = float(ctx.args[1])
        if not position:
            await update.message.reply_text("⚠️ Позиция не открыта.")
            return

        pnl = exit_deposit - position['entry_deposit']
        apr = (pnl / position['entry_deposit']) * 100
        duration = datetime.now(timezone.utc) - position['entry_time']
        minutes = int(duration.total_seconds() // 60)

        row = [
            position['entry_time'].strftime('%Y-%m-%d %H:%M:%S'),
            position['direction'],
            position['entry_deposit'],
            position['entry_price'],
            position['sl'],
            position['tp'],
            round(position['rr'], 2),
            round(pnl, 2),
            round(apr, 1)
        ]
        await asyncio.to_thread(LOGS_WS.append_row, row, value_input_option='USER_ENTERED')

        await update.message.reply_text(
            f"✅ Сделка закрыта\n📈 P&L: {pnl:.2f} USDT\n📊 APR: {apr:.2f}%\n⏰ Время в позиции: {minutes} мин"
        )
        position = None
    except:
        await update.message.reply_text("⚠️ Использование: /exit <депозит> <цена>")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if position:
        await update.message.reply_text(
            f"🔍 Позиция: {position['direction']} от {position['entry_price']}\nБаланс: {position['entry_deposit']}$"
        )
    else:
        await update.message.reply_text("❌ Позиция не открыта.")

async def cmd_log(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not log:
        await update.message.reply_text("⚠️ Сделок пока нет.")
        return
    text = "📊 История сделок:\n"
    for i, trade in enumerate(log[-5:], 1):
        text += f"{i}. {trade['direction']} | P&L: {trade['pnl']:.2f}$ | APR: {trade['apr']:.2f}% | {trade['duration_min']} мин\n"
    await update.message.reply_text(text)

async def cmd_reset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global position, log
    position = None
    log.clear()
    await update.message.reply_text("♻️ История и позиция сброшены.")

# === RUN ===
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.chat_ids = set(CHAT_IDS)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("entry", cmd_entry))
    app.add_handler(CommandHandler("exit", cmd_exit))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("log", cmd_log))
    app.add_handler(CommandHandler("reset", cmd_reset))

    app.run_polling()
