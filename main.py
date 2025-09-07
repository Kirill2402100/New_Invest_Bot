from __future__ import annotations
import os
import asyncio
import logging
from typing import Optional

from telegram import Update, constants, BotCommand
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes, PicklePersistence

import scanner_bmr_dca as scanner_engine
from scanner_bmr_dca import CONFIG, estimate_margin_metrics, fmt2

# --- Configuration ---
BOT_VERSION = "BMR-DCA FX v1.6 (Final Polished)"
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is not set")

log = logging.getLogger("bot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)

# --- Multi-Session Helpers ---
def _parse_chat_symbols_env() -> list[tuple[int, str]]:
    raw = os.getenv("CHAT_SYMBOLS", "").strip()
    pairs = []
    if not raw:
        return pairs
    for token in raw.split(","):
        token = token.strip()
        if not token or ":" not in token:
            continue
        cid_str, sym = token.split(":", 1)
        try:
            pairs.append((int(cid_str.strip()), sym.strip().upper()))
        except ValueError:
            log.warning(f"Could not parse token: {token}")
    return pairs

def _find_box_by_chat(app: Application, chat_id: int) -> tuple[Optional[dict], Optional[str]]:
    loops = app.bot_data.get("loops", {})
    for sym, rec in loops.items():
        if rec.get("chat_id") == chat_id:
            return rec.get("box"), sym
    return None, None

def _is_this_box_running(app: Application, box: dict) -> bool:
    for rec in app.bot_data.get("loops", {}).values():
        if rec.get("box") is box:
            task = rec.get("task")
            return bool(task and not task.done())
    single_task = app.bot_data.get("_main_loop_task")
    return bool(single_task and not single_task.done())

def _any_loop_running(app: Application) -> bool:
    loops = app.bot_data.get("loops", {})
    if any(rec.get("task") and not rec["task"].done() for rec in loops.values()):
        return True
    t = app.bot_data.get("_main_loop_task")
    return bool(t and not t.done())

# --- Utilities ---
async def post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        log.warning(f"delete_webhook failed: {e}")

    bd = app.bot_data
    bd.setdefault('chat_ids', set())
    env_ids = os.getenv("CHAT_IDS", "").strip()
    if env_ids:
        try:
            bd['chat_ids'].update(int(x) for x in env_ids.replace(" ", "").split(",") if x)
        except Exception:
            log.warning("CHAT_IDS env parse failed")

    bd.setdefault('safety_bank_usdt', float(os.getenv("SAFETY_BANK_USDT", CONFIG.SAFETY_BANK_USDT)))

    await app.bot.set_my_commands([
        BotCommand("start", "Запустить/перезапустить бота"),
        BotCommand("run", "Запустить сканеры по символам"),
        BotCommand("stop", "Остановить все сканеры"),
        BotCommand("status", "Показать статус для вашего символа"),
        BotCommand("mychatid", "Узнать ваш chat_id"),
        BotCommand("open", "Открыть позицию: /open long|short [lev] [steps]"),
        BotCommand("close", "Закрыть текущую позицию"),
        BotCommand("setbank", "Установить банк для вашего символа"),
    ])

async def broadcast(app: Application, txt: str, target_chat_id: int | None = None):
    chat_ids = set(app.bot_data.get('chat_ids', set()))
    if target_chat_id:
        chat_ids.add(target_chat_id)
    
    targets = [target_chat_id] if target_chat_id else list(chat_ids)
    
    for cid in targets:
        try:
            await app.bot.send_message(chat_id=cid, text=txt, parse_mode=constants.ParseMode.HTML)
        except Exception as e:
            log.error(f"Не удалось отправить сообщение в чат {cid}: {e}")
            if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                chat_ids.discard(cid)
                log.info(f"Чат {cid} удален из рассылки (бот заблокирован).")
    app.bot_data['chat_ids'] = chat_ids

async def start_symbol_loops(app: Application):
    pairs = _parse_chat_symbols_env()
    if not pairs:
        log.warning("CHAT_SYMBOLS пуст — запускается одиночный цикл.")
        box = {"bot_on": True, "scan_paused": False}
        task = asyncio.create_task(scanner_engine.scanner_main_loop(app, broadcast, botbox=box))
        app.bot_data["_main_loop_task"] = task
        app.bot_data["_main_loop_box"] = box
        return

    app.bot_data.setdefault("loops", {})
    for chat_id, symbol in pairs:
        rec = app.bot_data["loops"].get(symbol)
        if rec and rec.get("task") and not rec["task"].done():
            log.warning(f"Loop for {symbol} already running. Skipping.")
            continue

        box = {"bot_on": True, "scan_paused": False}
        task = asyncio.create_task(
            scanner_engine.scanner_main_loop(app, broadcast, symbol_override=symbol, target_chat_id=chat_id, botbox=box)
        )
        app.bot_data["loops"][symbol] = {"task": task, "box": box, "chat_id": chat_id}
        log.info(f"Started loop for {symbol} -> chat {chat_id}")

# --- Command Handlers ---

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ctx.bot_data.setdefault('chat_ids', set()).add(chat_id)
    await update.message.reply_text(
        f"✅ <b>Бот {BOT_VERSION} запущен.</b>\n"
        f"• /mychatid — покажет ID чата\n"
        f"• /run — запустить сканеры из CHAT_SYMBOLS\n"
        f"• /setbank 1000 — установить банк для этого чата/символа\n"
        f"• /status — статус по вашему символу",
        parse_mode=constants.ParseMode.HTML
    )

async def cmd_run(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    app = ctx.application
    if _any_loop_running(app):
        await update.message.reply_text("ℹ️ Сканеры уже запущены. Для остановки используйте /stop.")
        return
    await start_symbol_loops(app)
    await update.message.reply_text("🚀 <b>Запускаю сканеры по символам из CHAT_SYMBOLS...</b>", parse_mode=constants.ParseMode.HTML)

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    app = ctx.application
    if not _any_loop_running(app):
        await update.message.reply_text("ℹ️ Сканеры уже остановлены.")
        return

    loops = app.bot_data.get("loops", {})
    tasks_to_wait = []
    
    for rec in loops.values():
        rec["box"]["bot_on"] = False
        if rec.get("task"): tasks_to_wait.append(rec["task"])
    
    if "_main_loop_task" in app.bot_data:
        app.bot_data["_main_loop_box"]["bot_on"] = False
        tasks_to_wait.append(app.bot_data["_main_loop_task"])

    log.info(f"Stopping {len(tasks_to_wait)} scanner loops...")
    for t in tasks_to_wait:
        t.cancel()
    await asyncio.gather(*tasks_to_wait, return_exceptions=True)
    
    app.bot_data["loops"] = {}
    if "_main_loop_task" in app.bot_data: del app.bot_data["_main_loop_task"]
    if "_main_loop_box" in app.bot_data: del app.bot_data["_main_loop_box"]
    
    await update.message.reply_text("🛑 <b>Все сканеры остановлены.</b>", parse_mode=constants.ParseMode.HTML)

async def cmd_mychatid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Ваш chat_id: <code>{update.effective_chat.id}</code>", parse_mode=constants.ParseMode.HTML)

async def cmd_setbank(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        val = float(ctx.args[0]); assert val > 0
    except Exception:
        await update.message.reply_text("Использование: /setbank 1000"); return
    
    box, sym = _find_box_by_chat(ctx.application, update.effective_chat.id)
    
    if box is None:
        ctx.bot_data["safety_bank_usdt"] = val
        await update.message.reply_text(f"💰 Банк (общий) установлен: {val:.2f} USD")
    else:
        box["safety_bank_usdt"] = val
        await update.message.reply_text(f"💰 Банк для {sym} установлен: {val:.2f} USD")

async def cmd_open(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    app = ctx.application
    box, sym = _find_box_by_chat(app, update.effective_chat.id)
    if box is None:
        box = ctx.bot_data.get("_main_loop_box")
        sym = sym or CONFIG.SYMBOL
        if box is None:
            await update.message.reply_text("Этот чат не привязан к символу и одиночный режим не запущен. Введите /run.")
            return

    if not _is_this_box_running(app, box):
        log.info(f"Loop for {sym} is not running. Restarting it for /open command.")
        await update.message.reply_text(f"⚙️ Перезапускаю сканер для {sym}...")
        if box is app.bot_data.get("_main_loop_box"):
            task = asyncio.create_task(scanner_engine.scanner_main_loop(app, broadcast, botbox=box))
            app.bot_data["_main_loop_task"] = task
        else:
            task = asyncio.create_task(
                scanner_engine.scanner_main_loop(app, broadcast, symbol_override=sym, target_chat_id=update.effective_chat.id, botbox=box)
            )
            loops = app.bot_data.setdefault("loops", {})
            if sym in loops:
                loops[sym]["task"] = task
        await asyncio.sleep(0.5)

    if box.get("position"):
        await update.message.reply_text("Уже есть открытая позиция. Сначала закройте (/close)."); return
    if not ctx.args:
        await update.message.reply_text("Использование: /open long|short [leverage] [steps]"); return

    side = ctx.args[0].upper()
    if side not in ("LONG", "SHORT"):
        await update.message.reply_text("Укажите сторону: long или short"); return

    lev, steps = None, None
    if len(ctx.args) >= 2:
        try: lev = int(ctx.args[1])
        except Exception: lev = None
    if len(ctx.args) >= 3:
        try: steps = int(ctx.args[2])
        except Exception: steps = None

    if lev is not None:
        lev = max(CONFIG.MIN_LEVERAGE, min(CONFIG.MAX_LEVERAGE, lev))
    if steps is not None:
        steps = max(1, min(CONFIG.DCA_LEVELS, steps))

    box["manual_open"] = {"side": side, "leverage": lev, "max_steps": steps}
    box["bot_on"] = True
    await update.message.reply_text(
        f"Ок, открываю {side} для {sym} по рынку. "
        f"{'(плечо: '+str(lev)+') ' if lev else ''}"
        f"{'(макс. шагов: '+str(steps)+')' if steps else ''}"
    )

async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    box, sym = _find_box_by_chat(ctx.application, update.effective_chat.id)
    if box is None:
        box = ctx.bot_data.get("_main_loop_box")
        sym = sym or CONFIG.SYMBOL
        if box is None:
            await update.message.reply_text("Этот чат не привязан к символу и одиночный режим не запущен."); return

    if not box.get("position"):
        await update.message.reply_text("ℹ️ Активной позиции нет."); return
    box["force_close"] = True
    await update.message.reply_text(f"🧰 Запрошено закрытие позиции по {sym}. Закрою в ближайшем цикле.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    box, sym = _find_box_by_chat(ctx.application, update.effective_chat.id)
    if box is None:
        box = ctx.bot_data.get("_main_loop_box", ctx.bot_data)
        sym = CONFIG.SYMBOL
    
    is_running = _is_this_box_running(ctx.application, box)
    active_position = box.get('position', None)
    
    scanner_status = "🔌 ОСТАНОВЛЕН"
    if is_running:
        scanner_status = "⚡️ РАБОТАЕТ"

    position_status = "Нет активной позиции."
    if active_position:
        pos = active_position
        bank = box.get("safety_bank_usdt", CONFIG.SAFETY_BANK_USDT)
        px = box.get("last_px", pos.avg)
        used, eq, free, ml = estimate_margin_metrics(pos, px, bank)
        
        reserved_used = bool(getattr(pos, "reserve_used", False))
        reserved_available = bool(getattr(pos, "reserve_available", False) and not reserved_used)
        total_ord = max(0, getattr(pos, "ord_levels", 0) -1)
        used_ord = max(0, min(total_ord, pos.steps_filled - 1 - (1 if reserved_used else 0)))
        ordinary_left = max(0, total_ord - used_ord)
        reserve_left = 1 if reserved_available else 0

        position_status = (
            f"• <b>Сигнал ID:</b> {pos.signal_id}\n"
            f"• <b>Сторона:</b> {pos.side}\n"
            f"• <b>Плечо:</b> {pos.leverage}x\n"
            f"• <b>Средняя:</b> <code>{fmt2(pos.avg)}</code>\n"
            f"• <b>Ступеней:</b> {pos.steps_filled} / {pos.max_steps}\n"
            f"• <b>Осталось (об./рез.):</b> {ordinary_left} | {reserve_left}\n"
            f"• <b>Маржа:</b> used {fmt2(used)} | free {fmt2(free)}\n"
            f"• <b>Уровень маржи:</b> {fmt2(ml)}%"
        )

    bank = box.get("safety_bank_usdt", CONFIG.SAFETY_BANK_USDT)
    msg = (
        f"<b>Состояние ({sym})</b>\n\n"
        f"<b>Сканер:</b> {scanner_status}\n"
        f"<b>Банк:</b> {bank:.2f} USD\n\n"
        f"<b><u>Позиция:</u></b>\n{position_status}"
    )
    await update.message.reply_text(msg, parse_mode=constants.ParseMode.HTML)

if __name__ == "__main__":
    persistence = PicklePersistence(filepath="bot_persistence")
    app = ApplicationBuilder().token(BOT_TOKEN).persistence(persistence).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("mychatid", cmd_mychatid))
    app.add_handler(CommandHandler("setbank", cmd_setbank))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("close", cmd_close))
    
    log.info(f"Bot {BOT_VERSION} starting...")
    app.run_polling()
