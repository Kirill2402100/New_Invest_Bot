# main.py
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# === наш сканер ===
from scanner_bmr_dca import (
    start_scanner_for_pair,
    stop_scanner_for_pair,
    stop_all_scanners,
    is_scanner_running,
    CONFIG,
)

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("main")

# -----------------------------------------------------------------------------
# HTML sanitizer + универсальная отправка
# -----------------------------------------------------------------------------
ALLOWED_TAGS = {
    "b", "strong", "i", "em", "u", "ins", "s", "strike", "del",
    "code", "pre", "a", "tg-spoiler"
}
A_OPEN_RE = re.compile(r"&lt;a\s+href=(['\"]).*?\1\s*&gt;", re.IGNORECASE)

def html_safe(text: str) -> str:
    """
    Телеграм строго парсит HTML. Любые неразрешённые угловые скобки надо экранировать,
    а известные теги вернуть обратно. Поддерживаем <a href="...">...</a>.
    """
    if text is None:
        return ""
    # 1) экранируем всё
    t = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    # 2) вернём разрешённые теги открывающие/закрывающие
    for tag in ALLOWED_TAGS:
        t = t.replace(f"&lt;{tag}&gt;", f"<{tag}>")
        t = t.replace(f"&lt;/{tag}&gt;", f"</{tag}>")
    # 3) аккуратно починим <a href="...">...</a>
    #    (допускаем только сам атрибут href; остальные останутся экранированными)
    def _restore_a(m: re.Match) -> str:
        s = m.group(0)
        s = s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
        return s
    t = A_OPEN_RE.sub(_restore_a, t)
    t = t.replace("&lt;/a&gt;", "</a>")
    return t

async def broadcast_html(app: Application, text: str, target_chat_id: Optional[int] = None):
    if target_chat_id is None:
        log.warning("broadcast_html: target_chat_id is None; drop message")
        return
    try:
        await app.bot.send_message(
            chat_id=target_chat_id,
            text=html_safe(text),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.error(f"broadcast_html send failed: {e}", exc_info=True)


# -----------------------------------------------------------------------------
# Вспомогательные функции/ключи состояния
# -----------------------------------------------------------------------------
def _ns_key(symbol: str, chat_id: Optional[int]) -> str:
    return f"{(symbol or '').upper()}|{chat_id or 'default'}"

def _get_current_symbol(app: Application, chat_id: int) -> Optional[str]:
    return app.bot_data.get(f"current_symbol|{chat_id}")

def _set_current_symbol(app: Application, chat_id: int, symbol: str):
    app.bot_data[f"current_symbol|{chat_id}"] = (symbol or "").upper()

def _slot(app: Application, symbol: str, chat_id: int) -> dict:
    return app.bot_data.setdefault(_ns_key(symbol, chat_id), {})


# -----------------------------------------------------------------------------
# Команды бота
# -----------------------------------------------------------------------------
START_TEXT = (
    "🤖 Бот запущен.\n"
    "Команды (в этом чате):\n"
    "• /run <SYMBOL> — запустить сканер для пары (пример: /run GBPUSD)\n"
    "• /stop [hard] — остановить сканер по текущей паре (hard — с очисткой состояния)\n"
    "• /stopall — остановить все сканеры\n"
    "• /status — краткий статус по текущей паре\n"
    "• /setbank <USD> — установить банк пары (пример: /setbank 6000)\n"
    "• /open <long|short> [steps] — запрос на ручной вход c указанным направлением\n"
    "• /close — принудительно закрыть текущую позицию\n"
    "• /mychat — показать ID этого чата\n"
)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(html_safe(START_TEXT), parse_mode=ParseMode.HTML)

async def cmd_mychat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    await update.message.reply_text(
        html_safe(f"Этот чат: <code>{cid}</code>"),
        parse_mode=ParseMode.HTML,
    )

async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    cid = update.effective_chat.id
    if not context.args:
        await update.message.reply_text(
            html_safe("Укажи символ: /run <SYMBOL> (напр. /run GBPUSD)"),
            parse_mode=ParseMode.HTML,
        )
        return
    symbol = context.args[0].upper()
    _set_current_symbol(app, cid, symbol)
    msg = await start_scanner_for_pair(
        app,
        broadcast=broadcast_html,
        symbol=symbol,
        chat_id=cid,
        botbox=app.bot_data,
    )
    await broadcast_html(app, f"✅ {html_safe(msg)}", target_chat_id=cid)

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    cid = update.effective_chat.id
    symbol = _get_current_symbol(app, cid)
    if not symbol:
        await update.message.reply_text(
            html_safe("Нет выбранной пары в этом чате. Сначала запусти: /run <SYMBOL>"),
            parse_mode=ParseMode.HTML,
        )
        return
    hard = False
    if context.args and context.args[0].lower() == "hard":
        hard = True
    msg = await stop_scanner_for_pair(app, symbol=symbol, chat_id=cid, hard=hard)
    await broadcast_html(app, msg, target_chat_id=cid)

async def cmd_stopall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    msgs = await stop_all_scanners(app, hard=("hard" in [a.lower() for a in context.args] if context.args else False))
    text = ";\n".join(msgs) if msgs else "Нет активных сканеров."
    await broadcast_html(app, text, target_chat_id=update.effective_chat.id)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    cid = update.effective_chat.id
    symbol = _get_current_symbol(app, cid)
    if not symbol:
        await update.message.reply_text(
            html_safe("Сканер ещё не запускался в этом чате. Используй: /run <SYMBOL>"),
            parse_mode=ParseMode.HTML,
        )
        return
    slot = _slot(app, symbol, cid)
    line = slot.get("status_line")
    if not line:
        if is_scanner_running(app, symbol, cid):
            line = f"⏳ Сканер для <b>{symbol}</b> работает. Данные собираются…"
        else:
            line = f"ℹ️ Сканер для <b>{symbol}</b> не запущен."
    await broadcast_html(app, line, target_chat_id=cid)

async def cmd_setbank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Устанавливает банк (USD) для текущей пары в этом чате.
    Пример: /setbank 6000
    """
    app = context.application
    cid = update.effective_chat.id
    symbol = _get_current_symbol(app, cid)
    if not context.args:
        await update.message.reply_text(
            html_safe("Использование: /setbank <USD>, пример: /setbank 6000"),
            parse_mode=ParseMode.HTML,
        )
        return
    if not symbol:
        await update.message.reply_text(
            html_safe("Сначала выбери пару командой /run <SYMBOL>"),
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        amount = float(context.args[0].replace(",", "."))
        if amount <= 0:
            raise ValueError
    except Exception:
        await update.message.reply_text(
            html_safe("Некорректная сумма. Пример: /setbank 6000"),
            parse_mode=ParseMode.HTML,
        )
        return

    slot = _slot(app, symbol, cid)
    slot["safety_bank_usdt"] = float(amount)
    # Пользовательский таргет можем не трогать — его даёт FUND_BOT; но если его нет,
    # пусть таргет будет равен факту, чтобы статус выглядел консистентно.
    if "bank_target_usdt" not in slot:
        slot["bank_target_usdt"] = float(amount)

    await broadcast_html(
        app,
        f"✅ Банк для пары <b>{symbol}</b> установлен: <code>{amount:.2f}</code> USD.",
        target_chat_id=cid,
    )

async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ручной запрос на вход: /open long [steps] или /open short [steps]
    Шаги опционально. Плечо берётся из CONFIG.
    """
    app = context.application
    cid = update.effective_chat.id
    symbol = _get_current_symbol(app, cid)
    if not symbol:
        await update.message.reply_text(
            html_safe("Сначала запусти сканер: /run <SYMBOL>"),
            parse_mode=ParseMode.HTML,
        )
        return
    if not context.args:
        await update.message.reply_text(
            html_safe("Использование: /open <long|short> [steps]"),
            parse_mode=ParseMode.HTML,
        )
        return
    side = context.args[0].lower()
    if side not in ("long", "short"):
        await update.message.reply_text(
            html_safe("Направление должно быть long или short."),
            parse_mode=ParseMode.HTML,
        )
        return
    steps = None
    if len(context.args) >= 2:
        try:
            steps = int(float(context.args[1]))
        except Exception:
            steps = None

    slot = _slot(app, symbol, cid)
    slot["manual_open"] = {"side": side.upper(), "max_steps": steps}
    await broadcast_html(
        app,
        f"⏳ Запрос на ручной вход по <b>{symbol}</b>: <code>{side.upper()}</code>"
        + (f" (steps={steps})" if steps else ""),
        target_chat_id=cid,
    )

async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    cid = update.effective_chat.id
    symbol = _get_current_symbol(app, cid)
    if not symbol:
        await update.message.reply_text(
            html_safe("Сначала запусти сканер: /run <SYMBOL>"),
            parse_mode=ParseMode.HTML,
        )
        return
    slot = _slot(app, symbol, cid)
    slot["force_close"] = True
    await broadcast_html(app, f"🧰 Ручное закрытие запрошено по <b>{symbol}</b>.", target_chat_id=cid)

async def cmd_diag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    cid = update.effective_chat.id
    symbol = _get_current_symbol(app, cid)
    if not symbol:
        await update.message.reply_text(
            html_safe("Сначала /run <SYMBOL>"),
            parse_mode=ParseMode.HTML,
        )
        return
    slot = _slot(app, symbol, cid)
    slot["cmd_diag_targets"] = True
    await broadcast_html(app, "🧪 Диагностика таргетов запрошена.", target_chat_id=cid)


# -----------------------------------------------------------------------------
# boot
# -----------------------------------------------------------------------------
def build_app() -> Application:
    token = os.environ.get("TELEGRAM_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN is not set")
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("stopall", cmd_stopall))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("setbank", cmd_setbank))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("mychat", cmd_mychat))
    app.add_handler(CommandHandler("diag", cmd_diag))

    return app

if __name__ == "__main__":
    application = build_app()
    log.info("Bot starting…")
    # polling (Railway/Heroku ok). Если нужен webhook — добавь конфиг при деплое.
    application.run_polling(close_loop=False)
