# lp_supervisor_bot.py

import os
import time
from datetime import datetime, timezone
from statistics import mean
from math import erf, sqrt
import requests
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import asyncio

# --- Config ---
PAIR = os.getenv("PAIR", "EURC-USDC")
GRANULARITY = 60  # 1 minute candles
ATR_WINDOW = 48   # 48 x 1min = last 48 mins, acceptable for now
OBSERVE_INTERVAL = 15 * 60  # seconds to stay in observe mode

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = int(os.getenv("CHAT_ID", "0"))

# --- LP State ---
lp_center = None
lp_lower = None
lp_upper = None
lp_state = "closed"
observe_mode = False
observe_start = None
last_exit_price = None
entry_exit_count = 0
entry_log = []

# --- Helpers ---
def cdf_standard_normal(x):
    return 0.5 * (1 + erf(x / sqrt(2)))

def exit_probability(d_pct, sigma_pct, horizon_h=6):
    if sigma_pct == 0:
        return 0.0
    z = d_pct / (sigma_pct * sqrt(horizon_h / 24))
    return 2 * (1 - cdf_standard_normal(z))

def expected_apy(width_pct):
    return 0.15 / (width_pct / 100)

def fetch_price_and_atr():
    url = f"https://api.exchange.coinbase.com/products/{PAIR}/candles"
    params = {"granularity": GRANULARITY, "limit": ATR_WINDOW + 1}
    r = requests.get(url, params=params)
    r.raise_for_status()
    candles = sorted(r.json(), key=lambda x: x[0])
    close_prices = [c[4] for c in candles]
    tr = [abs(close_prices[i] - close_prices[i - 1]) for i in range(1, len(close_prices))]
    atr = mean(tr)
    last_price = close_prices[-1]
    sigma_pct = atr / last_price * 100
    return last_price, sigma_pct

def format_lp_status(price, sigma_pct):
    p_exit = exit_probability(0.1, sigma_pct)
    status = f"\u2728 *LP Статус*\nЦена: `{price:.4f}`\nДиапазон: `{lp_lower:.4f} – {lp_upper:.4f}`\n\nσ = `{sigma_pct:.2f}%`\nP_exit = `{p_exit*100:.1f}%`\nСостояние: `{lp_state}`"
    return status

async def send_message(text):
    bot = Bot(token=BOT_TOKEN)
    await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode='MarkdownV2')

# --- Telegram Commands ---
async def set_lp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global lp_center, lp_state
    if context.args:
        lp_center = float(context.args[0])
        await update.message.reply_text(f"Центр LP установлен: {lp_center:.4f}")
    else:
        await update.message.reply_text("Использование: /set <цена>")

async def step_lp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global lp_lower, lp_upper, lp_state, lp_center
    if not lp_center:
        await update.message.reply_text("Сначала задай центр LP: /set <цена>")
        return
    if len(context.args) == 2:
        low_pct, high_pct = map(float, context.args)
        lp_lower = lp_center * (1 - low_pct / 100)
        lp_upper = lp_center * (1 + high_pct / 100)
        lp_state = "open"
        await update.message.reply_text(f"Диапазон LP: {lp_lower:.4f} – {lp_upper:.4f}\nLP активен.")
    else:
        await update.message.reply_text("Использование: /step <низ %> <верх %>")

async def reset_lp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global lp_center, lp_lower, lp_upper, lp_state, observe_mode, observe_start, entry_exit_count
    lp_center = lp_lower = lp_upper = None
    lp_state = "closed"
    observe_mode = False
    observe_start = None
    entry_exit_count = 0
    await update.message.reply_text("Настройки LP сброшены.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if lp_state == "closed":
        await update.message.reply_text("LP не активен.")
        return
    price, sigma = fetch_price_and_atr()
    msg = format_lp_status(price, sigma)
    await update.message.reply_text(msg)

# --- Monitoring loop ---
async def monitor():
    global lp_state, observe_mode, observe_start, last_exit_price, entry_exit_count
    while True:
        try:
            if lp_state != "open":
                await asyncio.sleep(60)
                continue

            price, sigma = fetch_price_and_atr()
            now = datetime.now(timezone.utc)

            if lp_lower <= price <= lp_upper:
                if observe_mode:
                    if (datetime.now() - observe_start).total_seconds() > OBSERVE_INTERVAL:
                        apy = expected_apy(0.10)
                        await send_message(
                            f"🚀 *Стабильность восстановлена*\nσ = `{sigma:.2f}%`\nP_exit = `{exit_probability(0.1, sigma)*100:.1f}%`\n→ Рекомендуется открыть LP: ±0.10%\nAPY: ~{apy:.0f}%"
                        )
                        observe_mode = False
                        lp_state = "open"
                await asyncio.sleep(60)
                continue

            deviation = abs(price - lp_center) / lp_center * 100
            msg = f"🔴 *Цена вышла за пределы LP*\nТекущая цена: `{price:.4f}`\nОтклонение: `{deviation:.3f}%`\n"

            if deviation < 0.02:
                msg += "→ Отклонение незначительное\. Наблюдаем\."
            elif deviation < 0.05:
                msg += "→ Рекомендуется конвертировать *50%* EURC → USDC"
            else:
                msg += "→ *Критично*\. Рекомендуется продать *всё* EURC → USDC"

            await send_message(msg)
            observe_mode = True
            observe_start = datetime.now()
            lp_state = "observe"

        except Exception as e:
            print("[error]", e)
        await asyncio.sleep(60)

# --- Main ---
async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("set", set_lp))
    app.add_handler(CommandHandler("step", step_lp))
    app.add_handler(CommandHandler("reset", reset_lp))
    app.add_handler(CommandHandler("status", status))
    asyncio.create_task(monitor())
    await app.run_polling()

if __name__ == '__main__':
    asyncio.run(main())
