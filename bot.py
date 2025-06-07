# lp_supervisor_bot_with_reports.py

import os
import time
import json
from datetime import datetime, timezone
from statistics import mean
from math import erf, sqrt
import requests
import asyncio
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# === Конфигурация ===
PAIR = os.getenv("PAIR", "EURC-USDC")
GRANULARITY = 60  # 1 минута
ATR_WINDOW = 48
OBSERVE_INTERVAL = 15 * 60  # 15 минут

CHAT_IDS = [
    int(os.getenv("CHAT_ID_MAIN", "0")),
    int(os.getenv("CHAT_ID_OPERATOR", "0"))
]
BOT_TOKEN = os.getenv("BOT_TOKEN")

# === Google Sheets через переменную окружения ===
import json

SHEET_ID = os.getenv("SHEET_ID")

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = "credentials.json"
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)

import gspread
sheet_client = gspread.authorize(creds)
sheet = sheet_client.open_by_key(SHEET_ID).worksheet("LP_Logs")

# === Состояние LP ===
lp_center = None
lp_lower = None
lp_upper = None
lp_state = "closed"
observe_mode = False
observe_start = None
last_exit_price = None
entry_exit_count = 0
last_report_time = 0
lp_capital = 0.0
lp_start_time = None

# === Утилиты ===
def cdf_standard_normal(x):
    return 0.5 * (1 + erf(x / sqrt(2)))

def exit_probability(d_pct, sigma_pct, horizon_h=6):
    if sigma_pct == 0:
        return 0.0
    z = d_pct / (sigma_pct * sqrt(horizon_h / 24))
    return 2 * (1 - cdf_standard_normal(z))

def fetch_price_and_atr():
    url = f"https://api.exchange.coinbase.com/products/{PAIR}/candles"
    params = {"granularity": GRANULARITY, "limit": ATR_WINDOW + 1}
    r = requests.get(url, params=params)
    r.raise_for_status()
    candles = sorted(r.json(), key=lambda x: x[0])
    closes = [c[4] for c in candles]
    tr = [abs(closes[i] - closes[i-1]) for i in range(1, len(closes))]
    atr = mean(tr)
    return closes[-1], atr / closes[-1] * 100

async def broadcast(text):
    bot = Bot(token=BOT_TOKEN)
    for cid in CHAT_IDS:
        await bot.send_message(chat_id=cid, text=text, parse_mode='Markdown')

# === Telegram Команды ===
async def set_lp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global lp_center, lp_start_time
    if context.args:
        try:
            lp_center = float(context.args[0].replace(",", "."))
            lp_start_time = datetime.now(timezone.utc)
            await update.message.reply_text(f"📍 Центр LP установлен: `{lp_center:.4f}`", parse_mode='Markdown')
        except ValueError:
            await update.message.reply_text("Введите число: /set <цена>")

async def step_lp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global lp_lower, lp_upper, lp_state, lp_center, observe_mode, entry_exit_count
    if lp_center is None:
        await update.message.reply_text("Сначала задай центр LP: /set <цена>")
        return
    if len(context.args) == 2:
        low, high = map(float, context.args)
        lp_lower = lp_center * (1 - low / 100)
        lp_upper = lp_center * (1 + high / 100)
        lp_state = "open"
        observe_mode = False
        entry_exit_count = 0
        await update.message.reply_text(
            f"📦 Диапазон LP: `{lp_lower:.4f} – {lp_upper:.4f}`\nСтатус: *LP активен*.", parse_mode='Markdown')

async def capital(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global lp_capital
    if context.args:
        try:
            lp_capital = float(context.args[0].replace(",", "."))
            await update.message.reply_text(f"💰 Капитал LP установлен: `{lp_capital:.2f} USDC`", parse_mode='Markdown')
        except ValueError:
            await update.message.reply_text("Введите сумму: /capital <сумма>")

# === Основная логика ===
async def monitor():
    global observe_mode, observe_start, last_exit_price, entry_exit_count, last_report_time, lp_start_time
    while True:
        if lp_state != "open":
            await asyncio.sleep(60)
            continue

        price, sigma = fetch_price_and_atr()
        p_exit = exit_probability(0.1, sigma)
        now = datetime.now(timezone.utc)

        in_range = lp_lower <= price <= lp_upper
        message = None

        if in_range:
            if observe_mode and (now - observe_start).total_seconds() > OBSERVE_INTERVAL:
                observe_mode = False
                entry_exit_count = 0
                message = (
                    f"✅ Цена вернулась в диапазон\n\n"
                    f"Цена: `{price:.4f}`  \nσ = `{sigma:.2f}%`\nP_exit = `{p_exit*100:.1f}%`\n\n"
                    "Диапазон стабилен.  \n📊 Рассматриваем повторное открытие LP или удержание текущей позиции."
                )
        else:
            diff_pct = abs(price - (lp_upper if price > lp_upper else lp_lower)) / lp_center * 100
            if not observe_mode:
                observe_mode = True
                observe_start = now
                entry_exit_count = 1
                if lp_start_time:
                    duration_min = (now - lp_start_time).total_seconds() / 60
                    apr = 0
                    if duration_min > 0 and lp_capital > 0:
                        apr = ((price - lp_center) / lp_capital) * (525600 / duration_min) * 100
                    sheet.append_row([
                        now.strftime('%Y-%m-%d %H:%M:%S'),
                        f"{lp_center:.4f}", f"{lp_lower:.4f} – {lp_upper:.4f}",
                        f"{lp_capital:.2f}", f"{price:.4f}", f"{price - lp_center:.4f}",
                        f"{duration_min:.1f}", f"{apr:.2f}"
                    ])
            else:
                entry_exit_count += 1

            header = "🚨 Цена вышла за диапазон"
            if diff_pct > 0.05:
                header = "🚨 Цена *резко* вышла за диапазон"

            if diff_pct <= 0.02:
                msg = "Спокойно. Не предпринимаем действий."
            elif diff_pct <= 0.05:
                msg = (
                    "📉 Умеренное движение.\n\n"
                    "💱 Рекомендую конвертировать **50%** ликвидности в **USDC** для снижения риска."
                )
            else:
                msg = (
                    "⚠️ Высокая волатильность.\n\n"
                    "💱 *Рекомендую полностью конвертировать остаток в USDC*."
                )

            message = (
                f"{header}\nТекущая: `{price:.4f}` (на {diff_pct:.2f}% {'выше' if price > lp_upper else 'ниже'} границы)\n\n"
                f"σ = `{sigma:.2f}%`\nP_exit = `{p_exit*100:.1f}%`\n\n"
                f"{msg}\n🕵️ Продолжаем наблюдение за волатильностью…"
            )

            if entry_exit_count >= 5:
                message += (
                    f"\n\n🔁 Цена вышла и вернулась в диапазон 5 раз\n\n"
                    f"Текущая: `{price:.4f}`\nДиапазон: `{lp_lower:.4f} – {lp_upper:.4f}`\n\n"
                    f"⚠️ Цена пилит границу диапазона.\n📐 Рекомендую пересобрать диапазон LP."
                )

        if message and (now.timestamp() - last_report_time) > 60:
            await broadcast(message)
            last_report_time = now.timestamp()

        await asyncio.sleep(60)

# === Запуск ===
if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()

    from telegram.ext import Application
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("set", set_lp))
    app.add_handler(CommandHandler("step", step_lp))
    app.add_handler(CommandHandler("capital", capital))

    loop = asyncio.get_event_loop()
    loop.create_task(monitor())
    app.run_polling()
