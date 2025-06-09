# -*- coding: utf-8 -*-
"""
LP supervisor bot – c поддержкой Google Sheets.
"""
import os, json, asyncio, sys, atexit
from datetime import datetime, timezone
from statistics import mean
from math import erf, sqrt

import requests, gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, Bot
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters
)

# ---------- ANTI-DUPLICATE PROTECTION (Оставляем на всякий случай, но не полагаемся на него полностью) ----------
LOCKFILE = "lockfile.pid"

if os.path.exists(LOCKFILE):
    print("⚠️ Бот уже запущен. Завершаем второй экземпляр.")
    sys.exit(1)

with open(LOCKFILE, "w") as f:
    f.write(str(os.getpid()))

def cleanup():
    if os.path.exists(LOCKFILE):
        os.remove(LOCKFILE)
atexit.register(cleanup)


# ---------- ПАРАМЕТРЫ ----------
PAIR          = os.getenv("PAIR", "EURC-USDC")
GRANULARITY   = 60
ATR_WINDOW    = 48
# Убедитесь, что CHAT_IDS и BOT_TOKEN установлены в переменных окружения на Railway
CHAT_IDS      = [int(cid) for cid in os.getenv("CHAT_IDS", "0").split(",")]
BOT_TOKEN     = os.getenv("BOT_TOKEN")

# ---------- GOOGLE SHEETS ----------
SHEET_ID      = os.getenv("SHEET_ID")
scope         = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
# Убедитесь, что GOOGLE_CREDENTIALS установлены корректно
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
        ws.resize(rows=1) # Сначала очистим лишние строки, если есть
        ws.update('A1', [HEADERS]) # Обновляем заголовки

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

async def say(bot: Bot, text: str):
    for cid in CHAT_IDS:
        await bot.send_message(cid, text, parse_mode="Markdown")

def escape_md(text):
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    # В новой версии python-telegram-bot лучше использовать встроенный escape
    # from telegram.helpers import escape_markdown
    # return escape_markdown(text, version=2)
    # Но пока оставим ваш вариант для совместимости
    for c in escape_chars:
        text = text.replace(c, f"\\{c}")
    return text

# ---------- КОМАНДЫ ----------
async def cmd_capital(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global lp_capital_in
    if not ctx.args: return
    try:
        lp_capital_in = float(ctx.args[0].replace(',', '.'))
        await update.message.reply_text(
            f"💰 Капитал входа установлен: *{lp_capital_in:.2f} USDC*", parse_mode='Markdown'
        )
    except (ValueError, IndexError):
        await update.message.reply_text("Пожалуйста, введите корректное число. Пример: /capital 1000.50")


async def cmd_set(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global lp_open, lp_start_price, lp_start_time, lp_range_low, lp_range_high, last_in_lp, entry_exit_log
    if len(ctx.args) != 2:
        await update.message.reply_text("Неверный формат. Используйте: /set <цена_низ> <цена_верх>")
        return
    try:
        low, high = sorted(map(float, ctx.args))
        lp_start_price = (low + high) / 2
        lp_range_low, lp_range_high = low, high
        lp_open = True
        lp_start_time = datetime.now(timezone.utc)
        last_in_lp = True
        entry_exit_log = []
        await update.message.reply_text(
            f"📦 LP открыт\nДиапазон: `{low}` – `{high}`", parse_mode='Markdown'
        )
    except ValueError:
        await update.message.reply_text("Цены должны быть числами. Пример: /set 0.9995 1.0005")


async def cmd_reset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global lp_open
    if not lp_open:
        await update.message.reply_text("LP уже закрыт.")
        return
    if not ctx.args:
        await update.message.reply_text("Неверный формат. Используйте: /reset <капитал_на_выходе_USDC>")
        return
    try:
        cap_out = float(ctx.args[0].replace(',', '.'))
        t_stop = datetime.now(timezone.utc)
        minutes = round((t_stop - lp_start_time).total_seconds() / 60, 1)
        pnl = cap_out - lp_capital_in
        apr = (pnl / lp_capital_in) * (525600 / minutes) * 100 if minutes > 0 and lp_capital_in > 0 else 0

        row = [
            lp_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            lp_start_time.strftime('%H:%M'),
            t_stop.strftime('%H:%M'),
            minutes,
            round(pnl, 2),
            round(apr, 1)
        ]
        # Запуск синхронной функции gspread в отдельном потоке
        await asyncio.to_thread(LOGS_WS.append_row, row, value_input_option='USER_ENTERED')
        lp_open = False
        await update.message.reply_text(
            f"🚪 LP закрыт. P&L: *{pnl:+.2f} USDC*, APR: *{apr:.1f}%*", parse_mode='Markdown'
        )
    except ValueError:
        await update.message.reply_text("Капитал на выходе должен быть числом. Пример: /reset 1005.70")
    except Exception as e:
        print(f"Ошибка при закрытии LP: {e}")
        await update.message.reply_text(f"Произошла ошибка при закрытии LP: {e}")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    status = "✅ OPEN" if lp_open else "❌ CLOSED"
    msg = f"Статус LP: *{status}*"
    if lp_open:
        msg += f"\nДиапазон: `{lp_range_low}` - `{lp_range_high}`"
        msg += f"\nКапитал входа: `{lp_capital_in:.2f} USDC`"
        msg += f"\nВремя старта: `{lp_start_time.strftime('%Y-%m-%d %H:%M')}` UTC"
    await update.message.reply_text(msg, parse_mode='Markdown')

# ---------- WATCHER ----------
async def watcher(app: Application):
    """Фоновая задача, которая проверяет цену каждую минуту."""
    global lp_open, lp_range_low, lp_range_high, last_in_lp, entry_exit_log

    while True:
        await asyncio.sleep(60)

        if not lp_open or lp_range_low is None or lp_range_high is None:
            continue

        try:
            price, _ = price_and_atr()
            center = lp_start_price
            deviation = (price - center) / center * 100 if center != 0 else 0
            now_in_lp = lp_range_low <= price <= lp_range_high

            # Логика входа/выхода
            if now_in_lp != last_in_lp:
                last_in_lp = now_in_lp
                if not now_in_lp: # Цена вышла из диапазона
                    msg = f"PRICE *LP EXIT* | Цена: `{price:.5f}` (от центра: {deviation:+.3f}%)"
                    await say(app.bot, msg) # Используем escape_md прямо тут
                else: # Цена вернулась в диапазон
                    msg = f"PRICE *LP RE-ENTRY* | Цена: `{price:.5f}` (от центра: {deviation:+.3f}%)"
                    await say(app.bot, msg)

            # Логика "пилы"
            entry_exit_log.append(now_in_lp)
            if len(entry_exit_log) > 240: # Ограничиваем лог последними 4 часами (240 минут)
                entry_exit_log.pop(0)

            flips = sum(1 for i in range(1, len(entry_exit_log)) if entry_exit_log[i] != entry_exit_log[i-1])
            if flips >= 6:
                await say(app.bot, "🔁 *Обнаружена пила: 6+ пересечений границы диапазона за последние 4 часа.*\n→ Рекомендуется пересоздать LP с более широким диапазоном.")
                entry_exit_log = [] # Сбрасываем счетчик после оповещения

        except requests.exceptions.RequestException as e:
            print(f"Ошибка сети в watcher: {e}")
            # Не спамим в телеграм об ошибках сети, они могут быть временными
        except Exception as e:
            print(f"Критическая ошибка в watcher: {e}")
            await say(app.bot, f"🚨 Ошибка в фоновой задаче: {e}")


# ---------- ЗАПУСК БОТА (ИСПРАВЛЕННАЯ ВЕРСИЯ) ----------
def main() -> None:
    """Основная функция для запуска бота."""
    if not BOT_TOKEN:
        print("Ошибка: BOT_TOKEN не найден. Укажите его в переменных окружения.")
        sys.exit(1)

    # 1. Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()

    # 2. Добавляем обработчики команд
    application.add_handler(CommandHandler("start", cmd_status)) # Добавим status на /start
    application.add_handler(CommandHandler("capital", cmd_capital))
    application.add_handler(CommandHandler("set", cmd_set))
    application.add_handler(CommandHandler("reset", cmd_reset))
    application.add_handler(CommandHandler("status", cmd_status))
    
    # Ответ на любое текстовое сообщение, не являющееся командой
    async def show_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(f"Ваш chat_id: `{update.effective_chat.id}`", parse_mode='Markdown')
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, show_chat_id))

    # 3. Настраиваем и запускаем фоновую задачу watcher
    application.job_queue.run_once(watcher, 5, name="price_watcher")
    
    # 4. Запускаем бота
    # drop_pending_updates=True очистит обновления, которые бот пропустил, пока был оффлайн
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    # `nest_asyncio` не требуется в этой структуре
    main()
