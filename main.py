from __future__ import annotations
import os
import asyncio
import logging
from typing import Optional

from telegram import (
    Update, constants, BotCommand,
    BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats,
    BotCommandScopeChat, MenuButtonCommands
)
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler,
    ContextTypes, PicklePersistence
)

# --- Logging FIRST ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("bot")

# --- Engine imports ---
from scanner_bmr_dca import CONFIG, _norm_symbol
from scanner_bmr_dca import (
    start_scanner_for_pair,
    stop_scanner_for_pair,
    is_scanner_running,
)

BOT_VERSION = "BMR-DCA FX v3.0 (Per-Chat Per-Pair)"

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is not set")

# === Helpers ===

def ns_key(pair: str, chat_id: int | None) -> str:
    return f"{_norm_symbol(pair)}|{chat_id or 'default'}"

def slot_for(app: Application, pair: str, chat_id: int) -> dict:
    """Бокс состояния движка для конкретной пары в конкретном чате."""
    key = ns_key(pair, chat_id)
    return app.bot_data.setdefault(key, {"chat_id": chat_id})

async def broadcast(app: Application, txt: str, target_chat_id: int | None = None):
    """Единая точка рассылки: если задан chat_id — пишем только туда."""
    if not target_chat_id:
        log.warning("broadcast called without target_chat_id; dropping message")
        return
    try:
        await app.bot.send_message(chat_id=target_chat_id, text=txt, parse_mode=constants.ParseMode.HTML)
    except Exception as e:
        log.error(f"Send message failed for chat {target_chat_id}: {e}")

# === Lifecycle / init ===

async def post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        log.warning(f"delete_webhook failed: {e}")

    # Команды (везде одинаковые)
    CMDS = [
        BotCommand("start",   "Справка и команды"),
        BotCommand("run",     "Запуск: /run EURUSD"),
        BotCommand("stop",    "Стоп: /stop EURUSD [hard]"),
        BotCommand("status",  "Статус: /status EURUSD"),
        BotCommand("open",    "Открыть: /open EURUSD long|short [lev] [steps]"),
        BotCommand("close",   "Закрыть: /close EURUSD"),
        BotCommand("setbank", "Банк: /setbank EURUSD 1000"),
        BotCommand("mychatid","Показать chat_id"),
    ]
    for scope in (BotCommandScopeAllPrivateChats(), BotCommandScopeAllGroupChats()):
        try:
            await app.bot.set_my_commands(CMDS, scope=scope)
        except Exception as e:
            log.warning(f"set_my_commands({scope}) failed: {e}")
    try:
        await app.bot.set_my_commands(CMDS)
    except Exception as e:
        log.warning(f"set_my_commands(default) failed: {e}")

    # Кнопка меню
    try:
        await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    except Exception as e:
        log.warning(f"set_chat_menu_button(default) failed: {e}")

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error in handler", exc_info=context.error)

# === Commands ===

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ <b>Бот запущен.</b>\n"
        "Все команды работают <u>для указанной пары в текущем чате</u>.\n\n"
        "<b>Примеры</b>:\n"
        "• /run EURUSD — запустить сканер по EURUSD\n"
        "• /stop EURUSD — мягко остановить (добавь <code>hard</code> для сброса)\n"
        "• /status EURUSD — короткий статус\n"
        "• /open EURUSD long 200 5 — открыть лонг, плечо 200, макс. 5 шагов\n"
        "• /close EURUSD — закрыть текущую позицию\n"
        "• /setbank EURUSD 1200 — установить банк пары\n"
        "• /mychatid — показать ID чата",
        parse_mode=constants.ParseMode.HTML
    )

async def cmd_mychatid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Ваш chat_id: <code>{update.effective_chat.id}</code>",
        parse_mode=constants.ParseMode.HTML
    )

async def cmd_run(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Формат: /run <PAIR>\nНапример: <code>/run EURUSD</code>", parse_mode=constants.ParseMode.HTML)
        return
    pair = _norm_symbol(ctx.args[0])
    chat_id = update.effective_chat.id

    msg = await start_scanner_for_pair(
        ctx.application,
        broadcast,
        symbol=pair,
        chat_id=chat_id,
        botbox=ctx.application.bot_data,
    )
    await update.message.reply_text(msg, parse_mode=constants.ParseMode.HTML)

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Формат: /stop <PAIR> [hard]\nНапример: <code>/stop EURUSD hard</code>", parse_mode=constants.ParseMode.HTML)
        return
    pair = _norm_symbol(ctx.args[0])
    hard = any(a.lower() == "hard" for a in ctx.args[1:])
    chat_id = update.effective_chat.id

    msg = await stop_scanner_for_pair(
        ctx.application,
        symbol=pair,
        chat_id=chat_id,
        hard=hard
    )
    await update.message.reply_text(msg, parse_mode=constants.ParseMode.HTML)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Формат: /status <PAIR>\nНапример: <code>/status EURUSD</code>", parse_mode=constants.ParseMode.HTML)
        return
    pair = _norm_symbol(ctx.args[0])
    chat_id = update.effective_chat.id
    key = ns_key(pair, chat_id)
    box = ctx.application.bot_data.get(key, {})

    # Движок в каждом цикле кладёт "status_line" (см. _update_status_snapshot)
    line = box.get("status_line")
    if not line:
        running = is_scanner_running(ctx.application, pair, chat_id)
        hint = "Сканер не запущен." if not running else "Идёт инициализация… подождите немного."
        await update.message.reply_text(f"ℹ️ {hint}", parse_mode=constants.ParseMode.HTML)
        return

    await update.message.reply_text(line, parse_mode=constants.ParseMode.HTML)

async def cmd_open(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 2:
        await update.message.reply_text(
            "Формат: /open <PAIR> long|short [leverage] [steps]\n"
            "Например: <code>/open EURUSD long 200 5</code>",
            parse_mode=constants.ParseMode.HTML
        )
        return

    pair = _norm_symbol(ctx.args[0])
    side = (ctx.args[1] or "").upper()
    if side not in ("LONG", "SHORT"):
        await update.message.reply_text("Вторая позиция — сторона: long или short", parse_mode=constants.ParseMode.HTML)
        return

    lev = None
    steps = None
    if len(ctx.args) >= 3:
        try: lev = int(ctx.args[2])
        except: lev = None
    if len(ctx.args) >= 4:
        try: steps = int(ctx.args[3])
        except: steps = None

    # нормализуем по конфигу
    if lev is not None:
        lev = max(CONFIG.MIN_LEVERAGE, min(CONFIG.MAX_LEVERAGE, lev))
    if steps is not None:
        steps = max(1, min(CONFIG.DCA_LEVELS, steps))

    chat_id = update.effective_chat.id
    box = slot_for(ctx.application, pair, chat_id)

    if box.get("position"):
        await update.message.reply_text("Уже есть открытая позиция по этой паре. Сначала закройте (/close <PAIR>).")
        return

    # гарантируем запущенный сканер
    if not is_scanner_running(ctx.application, pair, chat_id):
        await start_scanner_for_pair(ctx.application, broadcast, symbol=pair, chat_id=chat_id, botbox=ctx.application.bot_data)
        await asyncio.sleep(0.3)

    box["manual_open"] = {"side": side, "leverage": lev, "max_steps": steps}
    box["bot_on"] = True

    lev_txt = f"(плечо: {lev}) " if lev else ""
    steps_txt = f"(макс. шагов: {steps})" if steps else ""
    await update.message.reply_text(
        f"Ок, открываю {side} по <b>{pair}</b> по рынку. {lev_txt}{steps_txt}",
        parse_mode=constants.ParseMode.HTML
    )

async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Формат: /close <PAIR>\nНапример: <code>/close EURUSD</code>", parse_mode=constants.ParseMode.HTML)
        return
    pair = _norm_symbol(ctx.args[0])
    chat_id = update.effective_chat.id
    box = slot_for(ctx.application, pair, chat_id)

    if not box.get("position"):
        await update.message.reply_text("ℹ️ Активной позиции по этой паре нет.")
        return

    # если сканер не бежит — поднимем, чтобы обработать закрытие
    if not is_scanner_running(ctx.application, pair, chat_id):
        await start_scanner_for_pair(ctx.application, broadcast, symbol=pair, chat_id=chat_id, botbox=ctx.application.bot_data)
        await asyncio.sleep(0.3)

    box["force_close"] = True
    await update.message.reply_text(f"🧰 Запрошено закрытие позиции по <b>{pair}</b>. Закрою в ближайшем цикле.",
                                    parse_mode=constants.ParseMode.HTML)

async def cmd_setbank(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 2:
        await update.message.reply_text("Формат: /setbank <PAIR> <AMOUNT>\nНапример: <code>/setbank EURUSD 1200</code>",
                                        parse_mode=constants.ParseMode.HTML)
        return
    pair = _norm_symbol(ctx.args[0])
    try:
        val = float(ctx.args[1])
        assert val > 0
    except Exception:
        await update.message.reply_text("Укажите корректную сумму (> 0).")
        return

    chat_id = update.effective_chat.id
    box = slot_for(ctx.application, pair, chat_id)
    box["safety_bank_usdt"] = val
    await update.message.reply_text(f"💰 Банк для <b>{pair}</b> установлен: {val:.2f} USD", parse_mode=constants.ParseMode.HTML)

# === Entrypoint ===

if __name__ == "__main__":
    persistence = PicklePersistence(filepath="bot_persistence")
    app = ApplicationBuilder() \
        .token(BOT_TOKEN) \
        .persistence(persistence) \
        .post_init(post_init) \
        .build()

    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("mychatid",cmd_mychatid))
    app.add_handler(CommandHandler("run",     cmd_run))
    app.add_handler(CommandHandler("stop",    cmd_stop))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("open",    cmd_open))
    app.add_handler(CommandHandler("close",   cmd_close))
    app.add_handler(CommandHandler("setbank", cmd_setbank))

    log.info(f"Bot {BOT_VERSION} starting...")
    app.run_polling()
