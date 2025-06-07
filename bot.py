# lp_supervisor_bot.py  (финальный)

import os, json, asyncio, requests
from datetime import datetime, timezone
from statistics import mean
from math import erf, sqrt

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ========== Конфиг ==========
PAIR         = os.getenv("PAIR", "EURC-USDC")
GRANULARITY  = 60        # 1-минутные свечи
ATR_WINDOW   = 48
OBSERVE_INT  = 15*60     # 15 мин
BOT_TOKEN    = os.getenv("BOT_TOKEN")

CHAT_IDS = [int(os.getenv("CHAT_ID_MAIN", "0")),
            int(os.getenv("CHAT_ID_OPERATOR", "0"))]

# Google Sheets
SHEET_ID   = os.getenv("SHEET_ID")
creds_json = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
scope   = ["https://spreadsheets.google.com/feeds",
           "https://www.googleapis.com/auth/drive"]
creds   = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
sheet   = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet("LP_Logs")

# ========== Глобальное состояние ==========
lp_center = lp_lower = lp_upper = None
lp_state  = "closed"              # closed | open
observe   = False
obs_start = None
exit_cnt  = 0
last_ts   = 0

lp_cap_in  = 0.0   # ввод
lp_cap_out = 0.0   # вывод
lp_start   = None

# ========== Вспомогательные ==========
def cdf(x): return 0.5*(1+erf(x/1.414213562))
def p_exit(d_pct, sigma_pct, h=6):
    if not sigma_pct: return 0
    z = d_pct / (sigma_pct*(h/24)**0.5)
    return 2*(1-cdf(z))

def price_and_sigma():
    url = f"https://api.exchange.coinbase.com/products/{PAIR}/candles"
    data = sorted(requests.get(url, params={"granularity":GRANULARITY,
                                            "limit":ATR_WINDOW+1}).json(),
                  key=lambda c:c[0])
    closes = [c[4] for c in data]
    atr_pct = mean(abs(closes[i]-closes[i-1]) for i in range(1,len(closes)))/closes[-1]*100
    return closes[-1], atr_pct

async def send(msg):  # широковещательно
    bot = Bot(BOT_TOKEN)
    for cid in CHAT_IDS:
        await bot.send_message(cid, msg, parse_mode="Markdown")

# ========== Команды ==========
async def cmd_set(u:Update,c:ContextTypes.DEFAULT_TYPE):
    global lp_center, lp_start
    if not c.args: return await u.message.reply_text("Пример: /set 1.13500")
    try:
        lp_center = float(c.args[0].replace(",",".")); lp_start = datetime.now(timezone.utc)
        await u.message.reply_text(f"📍 Центр: `{lp_center:.5f}`",parse_mode="Markdown")
    except ValueError: await u.message.reply_text("Введите число")

async def cmd_step(u:Update,c:ContextTypes.DEFAULT_TYPE):
    global lp_lower, lp_upper, lp_state, observe, exit_cnt
    if lp_center is None: return await u.message.reply_text("Сначала /set <центр>")
    if len(c.args)!=2: return await u.message.reply_text("Пример: /step 1.1300 1.1400")
    try:
        lp_lower, lp_upper = map(lambda x: float(x.replace(",",".")), c.args)
    except ValueError: return await u.message.reply_text("Неверные цены")
    lp_state="open"; observe=False; exit_cnt=0
    await u.message.reply_text(f"📦 Диапазон `{lp_lower:.5f} – {lp_upper:.5f}` активен",parse_mode="Markdown")

async def cmd_capital(u:Update,c:ContextTypes.DEFAULT_TYPE):
    global lp_cap_in
    if not c.args: return await u.message.reply_text("Пример: /capital 1000")
    try:
        lp_cap_in=float(c.args[0].replace(",",".")); 
        await u.message.reply_text(f"💰 Capital IN: `{lp_cap_in:.2f}` USDC",parse_mode="Markdown")
    except ValueError: await u.message.reply_text("Введите число")

async def cmd_reset(u:Update,c:ContextTypes.DEFAULT_TYPE):
    global lp_state, lp_cap_out, observe, exit_cnt, lp_center, lp_lower, lp_upper, lp_start
    if not c.args: return await u.message.reply_text("Пример: /reset 1040")
    try: lp_cap_out=float(c.args[0].replace(",",".")); 
    except ValueError: return await u.message.reply_text("Введите число")
    now=datetime.now(timezone.utc)
    dur=(now-lp_start).total_seconds()/60 if lp_start else 0
    pnl_pct=((lp_cap_out-lp_cap_in)/lp_cap_in*100) if lp_cap_in else 0
    sheet.append_row([now.strftime("%Y-%m-%d %H:%M:%S"),
                      f"{lp_center:.5f}" if lp_center else "",
                      "EXIT",
                      f"{lp_cap_in:.2f}", f"{lp_cap_out:.2f}",
                      f"{pnl_pct:+.2f}", f"{dur:.1f}"])
    lp_state="closed"; lp_center=lp_lower=lp_upper=None
    observe=False; exit_cnt=0; lp_start=None
    await u.message.reply_text(f"🏁 Закрыто. OUT `{lp_cap_out:.2f}` USDC ({pnl_pct:+.2f}% PnL)",parse_mode="Markdown")

async def cmd_status(u:Update,c:ContextTypes.DEFAULT_TYPE):
    if lp_state!="open": return await u.message.reply_text("LP закрыт.")
    dur=(datetime.now(timezone.utc)-lp_start).total_seconds()/60 if lp_start else 0
    txt=(f"*LP активен*\nЦентр `{lp_center:.5f}`\nДиапазон `{lp_lower:.5f} – {lp_upper:.5f}`\n"
         f"IN `{lp_cap_in:.2f}` USDC\nМинут: `{dur:.1f}`\nПилёж: `{exit_cnt}`")
    await u.message.reply_text(txt,parse_mode="Markdown")

# ========== Монитор ==========
async def monitor():
    global observe, obs_start, exit_cnt, last_ts
    while True:
        if lp_state!="open": await asyncio.sleep(60); continue
        price,sigma=price_and_sigma(); pex=p_exit(0.1,sigma); now=datetime.now(timezone.utc)
        in_range=lp_lower<=price<=lp_upper; msg=None
        if in_range:
            if observe and (now-obs_start).total_seconds()>OBSERVE_INT:
                observe=False; exit_cnt=0
                msg=(f"✅ Цена вернулась.\n`{price:.5f}`  σ `{sigma:.2f}%`  P_exit `{pex*100:.1f}%`")
        else:
            diff=abs(price-(lp_upper if price>lp_upper else lp_lower))/lp_center*100
            header="🚨 Цена вышла за диапазон" if diff<=0.05 else "🚨 Цена *резко* вышла за диапазон"
            rec="Спокойно." if diff<=0.02 else ("💱 ↘ 50 % в USDC." if diff<=0.05 else "💱 *Полный выход* в USDC.")
            if not observe: observe=True; obs_start=now; exit_cnt=1
            else: exit_cnt+=1
            msg=(f"{header}\nТекущая `{price:.5f}` (±{diff:.2f}%)\nσ `{sigma:.2f}%`  "
                 f"P_exit `{pex*100:.1f}%`\n{rec}")
            if exit_cnt>=5: msg+="\n🔁 Цена пилит границу ≥5 раз."
        if msg and now.timestamp()-last_ts>60: await send(msg); last_ts=now.timestamp()
        await asyncio.sleep(60)

# ========== Запуск ==========
if __name__=="__main__":
    import nest_asyncio; nest_asyncio.apply()
    app=ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("set",cmd_set))
    app.add_handler(CommandHandler("step",cmd_step))
    app.add_handler(CommandHandler("capital",cmd_capital))
    app.add_handler(CommandHandler("reset",cmd_reset))
    app.add_handler(CommandHandler("status",cmd_status))
    asyncio.get_event_loop().create_task(monitor())
    app.run_polling()
