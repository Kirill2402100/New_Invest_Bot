# bot.py  – LP-supervisor с Google Sheets отчётностью
# -----------------------------------------------
import os, json, asyncio, time
from datetime import datetime, timezone
from statistics import mean
from math import erf, sqrt

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder, Application,
    CommandHandler, ContextTypes
)

# ----------- ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ----------------
PAIR          = os.getenv("PAIR", "EURC-USDC")
BOT_TOKEN     = os.getenv("BOT_TOKEN")
CHAT_IDS      = [int(x) for x in os.getenv("CHAT_ID", "").split(",") if x]

SHEET_ID      = os.getenv("SHEET_ID")               # id таблицы
GOOGLE_CREDS  = json.loads(os.getenv("GOOGLE_CREDENTIALS"))

# ----------- GOOGLE SHEETS -----------------------
scope  = ["https://spreadsheets.google.com/feeds",
          "https://www.googleapis.com/auth/drive"]
creds  = ServiceAccountCredentials.from_json_keyfile_dict(GOOGLE_CREDS, scope)
sheet  = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet("LP_Logs")

# ----------- СОСТОЯНИЕ LP ------------------------
lp_lower: float | None = None     # нижняя граница
lp_upper: float | None = None     # верхняя
lp_center: float | None = None    # центр (считаем сами)
lp_state   = "closed"             # "open"/"closed"
observe_mode   = False
observe_start  = None
entry_exit_cnt = 0
last_report_ts = 0.0

cap_in   = 0.0                    # USDC, вход
cap_out  = 0.0                    # USDC, выход
lp_start = None                   # datetime, вход

# ----------- КОНСТАНТЫ РАССЧЁТА ------------------
GRANULARITY      = 60             # сек, 1-мин свеча
ATR_WINDOW       = 48             # 48 мин
OBSERVE_INTERVAL = 15 * 60        # сек

# ----------- УТИЛИТЫ -----------------------------
def cdf(x): return 0.5 * (1 + erf(x / sqrt(2)))

def exit_prob(d_pct, sigma_pct, horizon_h=6):
    if sigma_pct == 0: return 0.0
    z = d_pct / (sigma_pct * sqrt(horizon_h / 24))
    return 2 * (1 - cdf(z))

def fetch_price_atr():
    url = f"https://api.exchange.coinbase.com/products/{PAIR}/candles"
    r   = requests.get(url, params={"granularity": GRANULARITY,
                                    "limit": ATR_WINDOW+1}, timeout=10)
    r.raise_for_status()
    cs   = sorted(r.json(), key=lambda x: x[0])
    close = [c[4] for c in cs]
    tr    = [abs(close[i]-close[i-1]) for i in range(1, len(close))]
    atr   = mean(tr)
    return close[-1], atr/close[-1]*100

async def say(msg:str):
    bot = Bot(BOT_TOKEN)
    for cid in CHAT_IDS:
        await bot.send_message(cid, msg, parse_mode="Markdown")

def log_exit(now, pnl_usd, pnl_pct, apr_pct, dur_min):
    """Пишем строку EXIT в лист"""
    sheet.append_row([
        now.strftime('%Y-%m-%d %H:%M:%S'),
        f"{lp_center:.5f}" if lp_center else "",
        "EXIT",
        f"{cap_in:.2f}", f"{cap_out:.2f}",
        f"{pnl_usd:+.2f}", f"{pnl_pct:+.4f}",
        f"{dur_min:.1f}", f"{apr_pct:.2f}"
    ])

# ----------- КОМАНДЫ TG --------------------------
async def cmd_set(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    global lp_lower, lp_upper, lp_center, lp_state, observe_mode, entry_exit_cnt
    if len(ctx.args) != 2:
        await update.message.reply_text("Формат: /set LOW HIGH (цены)")
        return
    try:
        lp_lower, lp_upper = map(float, ctx.args)
        if lp_lower >= lp_upper:
            raise ValueError
        lp_center = (lp_lower + lp_upper) / 2
        lp_state  = "open"
        observe_mode = False; entry_exit_cnt = 0
        await update.message.reply_text(
            f"📦 Диапазон активирован:\n`{lp_lower:.5f} — {lp_upper:.5f}`",
            parse_mode='Markdown'
        )
    except ValueError:
        await update.message.reply_text("Неверные числа. Пример: /set 1.13495 1.14001")

async def cmd_capital(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    global cap_in, lp_start
    if not ctx.args: return
    try:
        cap_in = float(ctx.args[0].replace(",", "."))
        lp_start = datetime.now(timezone.utc)
        await update.message.reply_text(f"💰 Вход: `{cap_in:.2f} USDC`", parse_mode='Markdown')
    except ValueError:
        await update.message.reply_text("Формат: /capital 1000.00")

async def cmd_reset(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    """Фиксируем выход, пишем строку в лог и закрываем LP"""
    global cap_out, lp_state, observe_mode, entry_exit_cnt
    if not ctx.args:
        await update.message.reply_text("⚠️ /reset <сумма выхода>")
        return
    try:
        cap_out = float(ctx.args[0].replace(",", "."))
    except ValueError:
        return await update.message.reply_text("Число USDC, пример: /reset 1040")

    now   = datetime.now(timezone.utc)
    dur   = (now - lp_start).total_seconds()/60 if lp_start else 0
    pnl   = cap_out - cap_in
    pnlpc = (pnl / cap_in) if cap_in else 0
    apr   = pnlpc * (525600/dur)*100 if dur and cap_in else 0

    log_exit(now, pnl, pnlpc, apr, dur)

    lp_state = "closed"; observe_mode = False; entry_exit_cnt = 0
    await say(f"✅ *LP закрыт*\n"
              f"PnL: `{pnl:+.2f} USDC`  ({pnlpc*100:+.2f} %)\n"
              f"APR: `{apr:+.2f} %` за {dur:.1f} мин.")

async def cmd_status(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    price, _ = fetch_price_atr()
    msg = f"*Статус*: {lp_state.upper()}\n" \
          f"Цена: `{price:.5f}`\n"
    if lp_state == "open":
        msg += f"Диапазон: `{lp_lower:.5f} — {lp_upper:.5f}`\n" \
               f"Вход: `{cap_in:.2f} USDC`"
    await update.message.reply_text(msg, parse_mode='Markdown')

# ----------- МОНИТОР -----------------------------
async def monitor():
    global observe_mode, observe_start, entry_exit_cnt, last_report_ts
    while True:
        if lp_state != "open":
            await asyncio.sleep(60) ; continue

        price, sigma = fetch_price_atr()
        now  = datetime.now(timezone.utc)
        in_range = lp_lower <= price <= lp_upper
        header = None

        if in_range:
            if observe_mode and (now-observe_start).total_seconds() > OBSERVE_INTERVAL:
                observe_mode=False; entry_exit_cnt=0
                header="✅ Цена вернулась в диапазон"
        else:
            diff = abs(price - (lp_upper if price>lp_upper else lp_lower))
            diff_pct = diff/lp_center*100 if lp_center else 0
            p_exit = exit_prob(0.1, sigma)
            if not observe_mode:
                observe_mode=True; observe_start=now; entry_exit_cnt=1
            else:
                entry_exit_cnt +=1
            header="🚨 Цена вышла" + (" *резко*" if diff_pct>0.05 else "")

            advice = ("Спокойно." if diff_pct<=0.02 else
                      "📉 Конвертируйте 50 % в USDC." if diff_pct<=0.05 else
                      "⚠️ Полная конвертация в USDC!")

            message = (f"{header}\n"
                       f"Текущая: `{price:.5f}`\n"
                       f"Δ = {diff_pct:.2f}%  σ = {sigma:.2f}%\n"
                       f"P_exit ≈ {p_exit*100:.1f}%\n\n{advice}")
            if (time.time()-last_report_ts) > 60:
                await say(message); last_report_ts=time.time()

        await asyncio.sleep(60)

# ----------- СТАРТ -------------------------------
def main():
    app:Application = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("set",     cmd_set))
    app.add_handler(CommandHandler("capital", cmd_capital))
    app.add_handler(CommandHandler("reset",   cmd_reset))
    app.add_handler(CommandHandler("status",  cmd_status))

    loop = asyncio.get_event_loop()
    loop.create_task(monitor())
    app.run_polling()

if __name__ == "__main__":
    import nest_asyncio; nest_asyncio.apply()
    main()
