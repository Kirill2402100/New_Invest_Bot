# main.py
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Optional

from telegram import Update
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes
)

# наш DCA-сканер
from scanner_bmr_dca import (
    start_scanner_for_pair, stop_scanner_for_pair, is_scanner_running,
    _norm_symbol, CONFIG
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("main")

# ------------ утилиты ------------

def _get_bot_token() -> Optional[str]:
    for key in ("TELEGRAM_TOKEN", "BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TOKEN"):
        v = os.getenv(key)
        if v and v.strip():
            return v.strip()
    return None

def _chat_id(update: Update) -> Optional[int]:
    if update.effective_chat:
        return update.effective_chat.id
    return None

def _ns_key(symbol: str, chat_id: Optional[int]) -> str:
    return f"{_norm_symbol(symbol)}|{chat_id or 'default'}"

def _parse_amount(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(" ", "").replace(",", ".")
    # поддержим суффиксы k/m (на всякий случай)
    m = re.fullmatch(r"([0-9]*\.?[0-9]+)\s*([kKmM]?)", s)
    if not m:
        return None
    x = float(m.group(1))
    suf = (m.group(2) or "").lower()
    if suf == "k": x *= 1_000
    if suf == "m": x *= 1_000_000
    return x

def _get_slot(app: Application, symbol: str, chat_id: Optional[int]) -> dict:
    ns = _ns_key(symbol, chat_id)
    return app.bot_data.setdefault(ns, {})

# общий broadcast: здесь не используем «сырые» <>
async def _broadcast(app: Application, text: str, target_chat_id: Optional[int] = None):
    cid = target_chat_id or None
    try:
        await app.bot.send_message(chat_id=cid, text=text, parse_mode="HTML")
    except Exception as e:
        log.error(f"Broadcast send failed: {e}")

# ------------ команды ------------

HELP_TEXT = (
    "Команды:\n"
    "• <code>/setbank SYMBOL USD</code> — задать банк для пары (пример: <code>/setbank GBPUSD 6000</code>)\n"
    "• <code>/setbank USD</code> — задать банк для ранее выбранной пары в этом чате\n"
    "• <code>/run SYMBOL</code> — запустить сканер пары (требуется заданный банк)\n"
    "• <code>/stop SYMBOL</code> — остановить сканер пары (доп. флаг: <code>hard</code>)\n"
    "• <code>/status</code> — краткий статус\n"
    "• <code>/open SYMBOL</code> — взвод ручного входа (направление выберет сканер)\n"
    "• <code>/close [SYMBOL]</code> — ручное закрытие позиции\n"
    "• <code>/diag [SYMBOL]</code> — диагностика (снапшот FUND_BOT)\n"
)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html(
        "Бот запущен. Сначала установите банк: "
        "<code>/setbank SYMBOL USD</code>\n\n" + HELP_TEXT
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html(HELP_TEXT)

async def cmd_setbank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []

    # формы:
    # 1) /setbank 6000             -> для уже выбранной пары (в этом чате)
    # 2) /setbank GBPUSD 6000       -> явная пара + сумма (можно до /run)
    if len(args) == 1:
        amt = _parse_amount(args[0])
        if amt is None or amt <= 0:
            return await update.message.reply_html("Некорректная сумма. Пример: <code>/setbank 6000</code>")
        # берём последнюю выбранную пару из chat_data
        sym = context.chat_data.get("current_symbol")
        if not sym:
            return await update.message.reply_html(
                "Неизвестна активная пара для этого чата. "
                "Используйте форму: <code>/setbank SYMBOL USD</code> "
                "(пример: <code>/setbank GBPUSD 6000</code>)."
            )
        slot = _get_slot(context.application, sym, chat_id)
        slot["safety_bank_usdt"] = float(amt)
        slot["safety_bank_user_set"] = True
        slot["bank_set_ts"] = time.time()
        return await update.message.reply_html(
            f"OK. Банк для <b>{_norm_symbol(sym)}</b> установлен: <b>{amt:.2f} USD</b>.\n"
            f"Теперь можете запустить: <code>/run {_norm_symbol(sym)}</code>"
        )

    elif len(args) >= 2:
        sym = _norm_symbol(args[0])
        amt = _parse_amount(args[1])
        if not sym:
            return await update.message.reply_html("Укажите символ. Пример: <code>/setbank GBPUSD 6000</code>")
        if amt is None or amt <= 0:
            return await update.message.reply_html("Некорректная сумма. Пример: <code>/setbank GBPUSD 6000</code>")

        slot = _get_slot(context.application, sym, chat_id)
        slot["safety_bank_usdt"] = float(amt)
        slot["safety_bank_user_set"] = True
        slot["bank_set_ts"] = time.time()
        # запомним «текущую» пару в чате
        context.chat_data["current_symbol"] = sym

        return await update.message.reply_html(
            f"OK. Банк для <b>{sym}</b> установлен: <b>{amt:.2f} USD</b>.\n"
            f"Запуск: <code>/run {sym}</code>"
        )

    else:
        return await update.message.reply_html(
            "Использование:\n"
            "• <code>/setbank SYMBOL USD</code> (пример: <code>/setbank GBPUSD 6000</code>)\n"
            "• <code>/setbank USD</code> — для уже выбранной пары"
        )

async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []
    if not args:
        return await update.message.reply_html("Укажите символ: <code>/run SYMBOL</code> (пример: <code>/run GBPUSD</code>)")

    sym = _norm_symbol(args[0])
    context.chat_data["current_symbol"] = sym

    slot = _get_slot(context.application, sym, chat_id)
    # требуем, чтобы банк был задан пользователем до старта
    if not slot.get("safety_bank_user_set"):
        ex = slot.get("safety_bank_usdt")
        hint = f" (сейчас задано по умолчанию: {ex:.2f} USD)" if ex else ""
        return await update.message.reply_html(
            f"Сначала задайте банк для <b>{sym}</b>: "
            f"<code>/setbank {sym} USD</code>{hint}"
        )

    if is_scanner_running(context.application, sym, chat_id):
        return await update.message.reply_html(f"Сканер для <b>{sym}</b> уже запущен.")

    msg = await start_scanner_for_pair(
        context.application,
        _broadcast,
        symbol=sym,
        chat_id=chat_id,
        botbox=None,
    )
    await update.message.reply_html(msg)

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []
    # Разрешаем: /stop, /stop SYMBOL, /stop hard, /stop SYMBOL hard
    hard = any(a.lower() == "hard" for a in args)
    # уберём 'hard' и возьмём первый оставшийся аргумент как символ (если есть)
    non_flags = [a for a in args if a.lower() != "hard"]
    if non_flags:
        sym = _norm_symbol(non_flags[0])
        context.chat_data["current_symbol"] = sym
    else:
        sym = context.chat_data.get("current_symbol")
        if not sym:
            return await update.message.reply_html("Укажите символ: <code>/stop SYMBOL</code> (или используйте <code>/run SYMBOL</code> сначала)")

    msg = await stop_scanner_for_pair(context.application, symbol=sym, chat_id=chat_id, hard=hard)
    await update.message.reply_html(msg)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    sym = context.chat_data.get("current_symbol") or CONFIG.SYMBOL
    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.get(ns) or {}

    # короткий статус из снапшота (кладёт его сам сканер)
    snap = box.get("status_snapshot") or {}
    state = snap.get("state", "N/A")
    bank_f = snap.get("bank_fact_usdt")
    bank_t = snap.get("bank_target_usdt")
    has_rng = "✅" if snap.get("has_ranges") else "❌"
    # если сканер уже сформировал готовую строку — покажем её
    text = box.get("status_line") or (
        f"<b>Статус ({_norm_symbol(sym)})</b>\n"
        f"Сканер: <b>{state}</b>\n"
        f"Банк (факт/план): {bank_f if bank_f is not None else 'N/A'} / {bank_t if bank_t is not None else 'N/A'} USD\n"
        f"Диапазоны доступны: {has_rng}"
    )
    await update.message.reply_html(text)

async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Взвод ручного входа: /open SYMBOL (направление выберет сканер)."""
    chat_id = _chat_id(update)
    args = context.args or []
    if not args:
        return await update.message.reply_html("Укажите символ: <code>/open SYMBOL</code>")
    sym = _norm_symbol(args[0])
    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})
    box["manual_open"] = {}        # авто-направление и авто-кол-во шагов
    box["user_manual_mode"] = False   # снять ручной режим после TP/SL/manual_close
    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"Готово. Взведён /open для <b>{sym}</b> (направление выберет сканер).")

async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручное закрытие текущей позиции."""
    chat_id = _chat_id(update)
    args = context.args or []
    sym = _norm_symbol(args[0]) if args else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)
    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})
    box["force_close"] = True
    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"MANUAL_CLOSE запрошен для <b>{sym}</b>.")

async def cmd_diag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Разовая диагностика: снапшот таргетов FUND_BOT в канал."""
    chat_id = _chat_id(update)
    args = context.args or []
    sym = _norm_symbol(args[0]) if args else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)
    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})
    box["cmd_diag_targets"] = True
    await update.message.reply_html("Диагностика: запросил снапшот таргетов FUND_BOT.")

# ------------ сборка приложения ------------

def build_app() -> Application:
    token = _get_bot_token()
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN/BOT_TOKEN is not set")

    application = ApplicationBuilder().token(token).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help",  cmd_help))
    application.add_handler(CommandHandler("setbank", cmd_setbank))
    application.add_handler(CommandHandler("run",   cmd_run))
    application.add_handler(CommandHandler("stop",  cmd_stop))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("open",   cmd_open))
    application.add_handler(CommandHandler("close",  cmd_close))
    application.add_handler(CommandHandler("diag",   cmd_diag))

    log.info("Bot application built.")
    return application

if __name__ == "__main__":
    app = build_app()
    app.run_polling(close_loop=False)
