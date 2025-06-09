# -*- coding: utf-8 -*-
"""
LP supervisor bot – c поддержкой Google Sheets.
"""

import os, sys, json, asyncio
from datetime import datetime, timezone
from statistics import mean
from math import erf, sqrt

# ---------- ЗАЩИТА ОТ ПОВТОРНОГО ЗАПУСКА ----------
LOCK_FILE = "/tmp/bot.lock"

def ensure_single_instance():
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(fd, str(os.getpid()).encode())
    except FileExistsError:
        print("❌ Бот уже запущен в другом процессе!")
        sys.exit(1)

ensure_single_instance()

# ---------- ИМПОРТЫ ДЛЯ БОТА ----------
import requests, gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
)
# ---------- ПАРАМЕТРЫ ----------
PAIR          = os.getenv("PAIR", "EURC-USDC")
GRANULARITY   = 60
ATR_WINDOW    = 48
CHAT_IDS      = [int(cid) for cid in os.getenv("CHAT_IDS", "0").split(",")]
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
    "Дата-время", "Время start", "Время stop", "Минут",
    "P&L за цикл (USDC)", "APR цикла (%)"
]

def ensure_headers(ws):
    if ws.row_values(1) != HEADERS:
        ws.resize(1)
        ws.append_row(HEADERS)

ensure_headers(LOGS_WS)

# ---------- СОСТОЯНИЕ ----------
lp_open        = False
lp_start_price = None
lp_start_time  = None
lp_capital_in  = 0.0
lp_range_low   = None
lp_range_high  = None
last_in_lp     = True
entry_exit_log = []

# ---------- УТИЛИТЫ ----------
def cdf_norm(x): return 0.5 * (1 + erf(x / sqrt(2)))

def price_and_atr():
    url = f"https://api.exchange.coinbase.com/products/{PAIR}/candles"
    r   = requests.get(url, params=dict(granularity=GRANULARITY, limit=ATR_WINDOW+1))
    r.raise_for_status()
    candles = sorted(r.json(), key=lambda x: x[0])
    closes  = [c[4] for c in candles]
    atr     = mean(abs(closes[i] - closes[i-1]) for i in range(1, len(closes)))
    return closes[-1], atr / closes[-1] * 100

async def say(text):
    bot = Bot(BOT_TOKEN)
    for cid in CHAT_IDS:
        await bot.send_message(cid, text, parse_mode="Markdown")

# ---------- КОМАНДЫ ----------
async def cmd_capital(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global lp_capital_in
    if not ctx.args: return
    lp_capital_in = float(ctx.args[0].replace(',', '.'))
    await update.message.reply_text(
        f"💰 Капитал входа установлен: *{lp_capital_in:.2f} USDC*", parse_mode='Markdown'
    )

async def cmd_set(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global lp_open, lp_start_price, lp_start_time, lp_range_low, lp_range_high, last_in_lp, entry_exit_log
    if len(ctx.args) != 2:
        await update.message.reply_text("/set <low> <high>")
        return
    low, high = map(float, ctx.args)
    lp_start_price = (low + high) / 2
    lp_range_low, lp_range_high = low, high
    lp_open = True
    lp_start_time = datetime.now(timezone.utc)
    last_in_lp = True
    entry_exit_log = []
    await update.message.reply_text(
        f"📦 LP открыт\nДиапазон: `{low}` – `{high}`", parse_mode='Markdown'
    )

async def cmd_reset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global lp_open
    if not lp_open:
        await update.message.reply_text("LP уже закрыт.")
        return
    if not ctx.args:
        await update.message.reply_text("/reset <Cap_out_USDC>")
        return
    try:
        cap_out = float(ctx.args[0].replace(',', '.'))
        t_stop = datetime.now(timezone.utc)
        minutes = round((t_stop - lp_start_time).total_seconds() / 60, 1)
        pnl = cap_out - lp_capital_in
        apr = (pnl / lp_capital_in) * (525600 / minutes) * 100 if minutes > 0 else 0

        row = [
            lp_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            lp_start_time.strftime('%H:%M'),
            t_stop.strftime('%H:%M'),
            minutes,
            round(pnl, 2),
            round(apr, 1)
        ]
        await asyncio.to_thread(LOGS_WS.append_row, row, value_input_option='USER_ENTERED')
        lp_open = False
        await update.message.reply_text(
            f"🚪 LP закрыт. P&L: *{pnl:+.2f} USDC*, APR: *{apr:.1f}%*", parse_mode='Markdown'
        )
    except Exception as e:
        await update.message.reply_text(f"Ошибка при закрытии: {e}")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    status = "OPEN" if lp_open else "CLOSED"
    await update.message.reply_text(f"Статус LP: *{status}*", parse_mode='Markdown')

# ---------- WATCHER ----------
async def watcher():
    global lp_open, lp_range_low, lp_range_high, last_in_lp, entry_exit_log

    while True:
        await asyncio.sleep(60)

        if not lp_open or lp_range_low is None or lp_range_high is None:
            continue

        try:
            price, _ = price_and_atr()
            center = lp_start_price
            deviation = (price - center) / center * 100
            now_in_lp = lp_range_low <= price <= lp_range_high
            entry_exit_log.append(now_in_lp)
            if len(entry_exit_log) > 240:
                entry_exit_log.pop(0)

            if now_in_lp != last_in_lp:
                last_in_lp = now_in_lp
                if not now_in_lp:
                    msg = f"*LP EXIT* \u0426ена: *{price:.5f}* (\u043eт \u0446ентра: {deviation:+.3f}%)\n"
                    if abs(deviation) < 0.02:
                        msg += "→ Цена близка, LP не трогаем. Следим. 👁"
                    elif abs(deviation) < 0.05:
                        msg += "→ ⚠️ Рекомендуется продать 50% EURC → USDC.\nЖдём стабилизации."
                    else:
                        msg += "→ ❌ Рекомендуется *полный выход*. Продать EURC → USDC."
                    await say(msg)

            flips = sum(1 for i in range(1, len(entry_exit_log)) if entry_exit_log[i] != entry_exit_log[i-1])
            if flips >= 6:
                await say("🔁 *Обнаружена пила: 6+ выходов/входов за 4ч*\n→ 💡 Рекомендуется пересобрать LP диапазон ближе к текущей цене.")
                entry_exit_log = []

        except Exception as e:
            await say(f"🚨 Ошибка в watcher: {e}")

# ---------- ЗАПУСК ----------
if __name__ == "__main__":
    import nest_asyncio
    from telegram import Bot

    nest_asyncio.apply()

    async def main():
        await Bot(BOT_TOKEN).delete_webhook(drop_pending_updates=True)
        app = ApplicationBuilder().token(BOT_TOKEN).build()

        app.add_handler(CommandHandler("capital", cmd_capital))
        app.add_handler(CommandHandler("set", cmd_set))
        app.add_handler(CommandHandler("reset", cmd_reset))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND,
                lambda update, context: update.message.reply_text(f"Ваш chat_id: {update.effective_chat.id}")
            )
        )

        asyncio.get_running_loop().create_task(watcher())
        await app.run_polling()

    asyncio.run(main())
