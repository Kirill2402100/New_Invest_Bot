# -*- coding: utf-8 -*-
"""
LP supervisor bot – c поддержкой Google Sheets.
Шапки и структура листов:
  LP_Logs   : Дата-время | Время start | Время stop | Минут | P&L | APR_цикла
  Daily     : День       | P&L | Ср_APR | Прогноз_APR | Циклов | LP_время(%)
  Monthly   : Месяц      | P&L | Ср_APR | Прогноз_APR | Циклов | LP_время(%)
"""
import os, json, asyncio, time
from datetime import datetime, timezone
from statistics import mean
from math import erf, sqrt

import requests, gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes
)

# ---------- ПАРАМЕТРЫ ----------
PAIR          = os.getenv("PAIR", "EURC-USDC")
GRANULARITY   = 60          # свеча 1 мин
ATR_WINDOW    = 48
OBS_INTERVAL  = 15 * 60     # 15 мин
CHAT_IDS      = [
    int(os.getenv("CHAT_ID_MAIN", "0")),
    int(os.getenv("CHAT_ID_OPERATOR", "0"))
]
BOT_TOKEN     = os.getenv("BOT_TOKEN")

# ---------- GOOGLE SHEETS ----------
SHEET_ID      = os.getenv("SHEET_ID")
scope         = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds_dict    = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
creds         = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gs            = gspread.authorize(creds)

LOGS_WS       = gs.open_by_key(SHEET_ID).worksheet("LP_Logs")

HEADERS = [
    "Дата-время",        # A
    "Время start",       # B
    "Время stop",        # C
    "Минут",             # D
    "P&L за цикл (USDC)",# E
    "APR цикла (%)",     # F
]

def ensure_headers(ws):
    first_row = ws.row_values(1)
    if first_row != HEADERS:
        ws.resize(1)               # очищаем, оставляем только 1-ю строку
        ws.append_row(HEADERS)

ensure_headers(LOGS_WS)             # один раз при старте

# ---------- СОСТОЯНИЕ ----------
lp_open        = False
lp_start_price = None
lp_start_time  = None
lp_capital_in  = 0.0

# ---------- УТИЛИТЫ ----------
def cdf_norm(x): return 0.5 * (1 + erf(x / sqrt(2)))

def exit_prob(d_pct, sigma_pct, h=6):
    if sigma_pct == 0: return 0.0
    z = d_pct / (sigma_pct * sqrt(h / 24))
    return 2 * (1 - cdf_norm(z))

def price_and_atr():
    url = f"https://api.exchange.coinbase.com/products/{PAIR}/candles"
    r   = requests.get(url, params=dict(granularity=GRANULARITY, limit=ATR_WINDOW+1))
    r.raise_for_status()
    candles = sorted(r.json(), key=lambda x: x[0])
    closes  = [c[4] for c in candles]
    atr     = mean(abs(closes[i]-closes[i-1]) for i in range(1,len(closes)))
    return closes[-1], atr / closes[-1] * 100

async def say(txt):
    bot = Bot(BOT_TOKEN)
    for cid in CHAT_IDS:
        await bot.send_message(cid, txt, parse_mode="Markdown")

# ---------- КОМАНДЫ ----------
async def cmd_capital(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    global lp_capital_in
    if not ctx.args: return
    lp_capital_in = float(ctx.args[0].replace(',','.'))
    await update.message.reply_text(f"💰 Капитал входа установлен: *{lp_capital_in:.2f} USDC*", parse_mode='Markdown')

async def cmd_set(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    """
    /set 1.13495 1.14001
    """
    global lp_open, lp_start_price, lp_start_time
    if len(ctx.args) != 2:
        await update.message.reply_text("Формат: /set <price_low> <price_high>")
        return
    low, high      = map(float, ctx.args)
    lp_start_price = (low + high) / 2
    lp_open        = True
    lp_start_time  = datetime.now(timezone.utc)
    await update.message.reply_text(
        f"📦 LP открыт\nДиапазон: `{low}` – `{high}`", parse_mode='Markdown'
    )

async def cmd_reset(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    """
    /reset 11500   — фиксируем выход, считаем P&L, заносим строку в таблицу
    """
    global lp_open
    if not lp_open:
        await update.message.reply_text("LP уже закрыт.")
        return
    if not ctx.args:
        await update.message.reply_text("Формат: /reset <Cap_out_USDC>")
        return

    cap_out   = float(ctx.args[0].replace(',','.'))
    t_stop    = datetime.now(timezone.utc)
    minutes   = round((t_stop - lp_start_time).total_seconds() / 60, 1)
    pnl       = cap_out - lp_capital_in
    apr_cycle = (pnl / lp_capital_in) * (525600 / minutes) * 100 if minutes > 0 else 0

    LOGS_WS.append_row([
        lp_start_time.strftime('%Y-%m-%d %H:%M:%S'),        # Дата-время (старт)
        lp_start_time.strftime('%H:%M'),                    # Время start
        t_stop.strftime('%H:%M'),                           # Время stop
        minutes,                                           # Минут
        round(pnl, 2),                                     # P&L
        round(apr_cycle, 1),                               # APR цикла
    ])

    lp_open = False
    await update.message.reply_text(
        f"🚪 LP закрыт. P&L: *{pnl:+.2f} USDC*, APR цикла ≈ *{apr_cycle:.1f}%*",
        parse_mode='Markdown'
    )

async def cmd_status(update:Update, _):
    status = "OPEN" if lp_open else "CLOSED"
    await update.message.reply_text(f"Статус LP: *{status}*", parse_mode='Markdown')

# ---------- ОСНОВНОЙ ЦИКЛ НАБЛЮДЕНИЯ ----------
async def watcher():
    while True:
        # … здесь можно добавить логику сигналов при выходе цены
        await asyncio.sleep(300)

# ---------- ЗАПУСК ----------
if __name__ == "__main__":
    import nest_asyncio, asyncio
    nest_asyncio.apply()

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("capital", cmd_capital))
    app.add_handler(CommandHandler("set",      cmd_set))
    app.add_handler(CommandHandler("reset",    cmd_reset))
    app.add_handler(CommandHandler("status",   cmd_status))

    loop = asyncio.get_event_loop()
    loop.create_task(watcher())
    app.run_polling()
