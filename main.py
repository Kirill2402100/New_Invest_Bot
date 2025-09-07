from __future__ import annotations
import os
import asyncio
import logging
import inspect
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

# --- Engine import + helpers fallback ---
import scanner_bmr_dca as scanner_engine
from scanner_bmr_dca import CONFIG
try:
    from scanner_bmr_dca import estimate_margin_metrics, fmt2
except Exception:
    log.warning("Helpers from scanner_bmr_dca not found; using local fallbacks.")

    def fmt2(x: float) -> str:
        try:
            if x is None:
                return "N/A"
            if isinstance(x, float) and (x != x or x in (float("inf"), float("-inf"))):
                return "N/A"
            ax = abs(x)
            if ax < 1:
                return f"{x:.4f}"
            if ax < 1000:
                return f"{x:.2f}"
            return f"{x:.0f}"
        except Exception:
            return "N/A"

    def _pos_total_margin(pos):
        ord_used = sum(pos.step_margins[:min(pos.steps_filled, getattr(pos, 'ord_levels', pos.steps_filled))])
        res = pos.reserve_margin_usdt if getattr(pos, 'reserve_used', False) else 0.0
        return ord_used + res

    def estimate_margin_metrics(pos, px: float, bank: float):
        used = _pos_total_margin(pos)
        notional = used * max(1, int(getattr(pos, "leverage", 1) or 1))
        if getattr(pos, "qty", 0) <= 0 or getattr(pos, "avg", 0) <= 0:
            unreal = 0.0
        else:
            sgn = 1.0 if pos.side == "LONG" else -1.0
            unreal = (px / pos.avg - 1.0) * sgn * notional
        equity = bank + unreal
        free = equity - used
        ml = (equity / used) * 100.0 if used > 1e-12 else float("inf")
        return used, equity, free, ml


# --- Configuration ---
BOT_VERSION = "BMR-DCA FX v2.3 (Multi-Engine Safe)"
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is not set")


# --- Broadcaster that pins a target chat ---
def _make_bcaster(target_chat_id: Optional[int]):
    async def _bc(app: Application, txt: str, target_chat_id_override: Optional[int] = None):
        await broadcast(app, txt, target_chat_id or target_chat_id_override)
    return _bc


# --- Engine compatibility ---
def _engine_supports_multi() -> bool:
    """
    Считаем, что движок готов к многосессии, если:
    - принимает kwargs 'symbol_override' и 'target_chat_id', или
    - имеет хотя бы 3 позиционных аргумента (app, broadcast, box).
    """
    try:
        sig = inspect.signature(scanner_engine.scanner_main_loop)
        names = list(sig.parameters.keys())
        kw = set(names)
        return {"symbol_override", "target_chat_id"}.issubset(kw) or len(names) >= 3
    except Exception:
        log.warning("Could not inspect scanner signature; assuming single-mode.")
        return False


def _spawn_scanner_task(app: Application, symbol: Optional[str], chat_id: Optional[int], box: Optional[dict]):
    """
    Запускаем сканер в современном виде: (app, broadcaster, box).
    """
    fn = scanner_engine.scanner_main_loop
    bc = _make_bcaster(chat_id)

    # Пробуем стандартную сигнатуру (app, bc, box)
    try:
        log.info(f"Spawning scanner for {symbol or 'default'} with (app, bc, box).")
        return asyncio.create_task(fn(app, bc, box))
    except TypeError:
        pass

    # Фоллбек на (app, bc)
    try:
        log.warning(f"Spawning scanner for {symbol or 'default'} with fallback (app, bc).")
        return asyncio.create_task(fn(app, bc))
    except TypeError:
        pass

    # Самый простой вариант (app, broadcast)
    log.warning(f"Spawning scanner for {symbol or 'default'} with minimal (app, broadcast).")
    return asyncio.create_task(fn(app, bc))


# --- Multi-Session helpers ---
def _parse_chat_symbols_env() -> list[tuple[int, str]]:
    raw = os.getenv("CHAT_SYMBOLS", "").strip()
    pairs: list[tuple[int, str]] = []
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
            t = rec.get("task")
            return bool(t and not t.done())
    t = app.bot_data.get("_main_loop_task")
    return bool(t and not t.done())


def _any_loop_running(app: Application) -> bool:
    loops = app.bot_data.get("loops", {})
    if any(rec.get("task") and not rec["task"].done() for rec in loops.values()):
        return True
    t = app.bot_data.get("_main_loop_task")
    return bool(t and not t.done())


# --- Bot init & lifecycle ---
async def post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        log.warning(f"delete_webhook failed: {e}")

    bd = app.bot_data
    bd.setdefault("chat_ids", set())
    env_ids = os.getenv("CHAT_IDS", "").strip()
    if env_ids:
        try:
            bd["chat_ids"].update(int(x) for x in env_ids.replace(" ", "").split(",") if x)
        except Exception:
            log.warning("CHAT_IDS env parse failed")

    bd.setdefault("safety_bank_usdt", float(os.getenv("SAFETY_BANK_USDT", CONFIG.SAFETY_BANK_USDT)))

    CMDS = [
        BotCommand("start", "Запустить/перезапустить бота"),
        BotCommand("run", "Запустить сканеры из CHAT_SYMBOLS"),
        BotCommand("stop", "Остановить все сканеры"),
        BotCommand("status", "Показать статус для вашего символа"),
        BotCommand("mychatid", "Узнать ваш chat_id"),
        BotCommand("open", "Открыть позицию: /open long|short [lev] [steps]"),
        BotCommand("close", "Закрыть текущую позицию"),
        BotCommand("setbank", "Установить банк для вашего символа"),
    ]

    # Очистить команды во всех скоупах
    for scope in (BotCommandScopeAllPrivateChats(), BotCommandScopeAllGroupChats()):
        try:
            await app.bot.delete_my_commands(scope=scope)
        except Exception as e:
            log.warning(f"delete_my_commands({scope}) failed: {e}")
    try:
        await app.bot.delete_my_commands()
    except Exception as e:
        log.warning(f"delete_my_commands(default) failed: {e}")

    # Задать команды
    await app.bot.set_my_commands(CMDS, scope=BotCommandScopeAllGroupChats())
    await app.bot.set_my_commands(CMDS, scope=BotCommandScopeAllPrivateChats())
    await app.bot.set_my_commands(CMDS)

    # Включить кнопку меню
    try:
        await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    except Exception as e:
        log.warning(f"set_chat_menu_button(default) failed: {e}")

    # Персонально для чатов из CHAT_SYMBOLS
    for chat_id, _ in _parse_chat_symbols_env():
        try:
            await app.bot.delete_my_commands(scope=BotCommandScopeChat(chat_id))
            await app.bot.set_my_commands(CMDS, scope=BotCommandScopeChat(chat_id))
            await app.bot.set_chat_menu_button(chat_id=chat_id, menu_button=MenuButtonCommands())
            log.info(f"Commands & menu set for chat {chat_id}")
        except Exception as e:
            log.warning(f"Per-chat commands/menu for {chat_id} failed: {e}")


async def broadcast(app: Application, txt: str, target_chat_id: int | None = None):
    """
    Если задан target_chat_id — отправляем только туда,
    иначе — во все чаты, записанные в app.bot_data['chat_ids'].
    """
    chat_ids = set(app.bot_data.get("chat_ids", set()))
    targets = [target_chat_id] if target_chat_id else list(chat_ids)

    for cid in targets:
        try:
            await app.bot.send_message(chat_id=cid, text=txt, parse_mode=constants.ParseMode.HTML)
        except Exception as e:
            log.error(f"Не удалось отправить сообщение в чат {cid}: {e}")
            if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                chat_ids.discard(cid)
                log.info(f"Чат {cid} удален из рассылки.")
    app.bot_data["chat_ids"] = chat_ids


async def start_symbol_loops(app: Application):
    app.bot_data["bot_on"] = True
    pairs = _parse_chat_symbols_env()
    supports_multi = _engine_supports_multi()

    # Если движок без многосессии и нет CHAT_SYMBOLS — одиночный режим
    if not pairs and not supports_multi:
        box = {"bot_on": True, "scan_paused": False}
        task = _spawn_scanner_task(app, None, None, box)
        app.bot_data["_main_loop_task"] = task
        app.bot_data["_main_loop_box"] = box
        return

    # Если нет CHAT_SYMBOLS — одиночный режим
    if not pairs:
        log.warning("CHAT_SYMBOLS is empty — running single-mode.")
        box = {"bot_on": True, "scan_paused": False}
        task = _spawn_scanner_task(app, None, None, box)
        app.bot_data["_main_loop_task"] = task
        app.bot_data["_main_loop_box"] = box
        return

    # Многосессионный режим
    app.bot_data.setdefault("loops", {})
    for chat_id, symbol in pairs:
        rec = app.bot_data["loops"].get(symbol)
        if rec and rec.get("task") and not rec["task"].done():
            log.warning(f"Loop for {symbol} already running. Skipping.")
            continue

        box = {
            "bot_on": True,
            "scan_paused": False,
            "symbol": symbol,
            "chat_id": chat_id,
        }
        task = _spawn_scanner_task(app, symbol, chat_id, box)
        app.bot_data["loops"][symbol] = {"task": task, "box": box, "chat_id": chat_id}
        log.info(f"Started loop for {symbol} -> chat {chat_id}")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error in handler", exc_info=context.error)


# --- Command Handlers ---
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ctx.bot_data.setdefault("chat_ids", set()).add(chat_id)
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
    app.bot_data["bot_on"] = True
    if _any_loop_running(app):
        await update.message.reply_text("ℹ️ Сканеры уже запущены. Для остановки используйте /stop.")
        return
    await start_symbol_loops(app)
    await update.message.reply_text("🚀 <b>Запускаю сканеры по символам из CHAT_SYMBOLS...</b>", parse_mode=constants.ParseMode.HTML)


async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    app = ctx.application
    app.bot_data["bot_on"] = False
    if not _any_loop_running(app):
        await update.message.reply_text("ℹ️ Сканеры уже остановлены.")
        return

    loops = app.bot_data.get("loops", {})
    tasks_to_wait = []

    for rec in loops.values():
        rec["box"]["bot_on"] = False
        if rec.get("task"):
            tasks_to_wait.append(rec["task"])

    if "_main_loop_task" in app.bot_data:
        app.bot_data["_main_loop_box"]["bot_on"] = False
        tasks_to_wait.append(app.bot_data["_main_loop_task"])

    log.info(f"Stopping {len(tasks_to_wait)} scanner loops...")
    for t in tasks_to_wait:
        t.cancel()
    await asyncio.gather(*tasks_to_wait, return_exceptions=True)

    app.bot_data["loops"] = {}
    if "_main_loop_task" in app.bot_data:
        del app.bot_data["_main_loop_task"]
    if "_main_loop_box" in app.bot_data:
        del app.bot_data["_main_loop_box"]

    await update.message.reply_text("🛑 <b>Все сканеры остановлены.</b>", parse_mode=constants.ParseMode.HTML)


async def cmd_mychatid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Ваш chat_id: <code>{update.effective_chat.id}</code>",
        parse_mode=constants.ParseMode.HTML
    )


async def cmd_setbank(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        val = float(ctx.args[0])
        assert val > 0
    except Exception:
        await update.message.reply_text("Использование: /setbank 1000")
        return

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
        task = _spawn_scanner_task(app, sym if "symbol" in box else None, update.effective_chat.id, box)
        if "loops" in app.bot_data and sym in app.bot_data["loops"]:
            app.bot_data["loops"][sym]["task"] = task
        else:
            app.bot_data["_main_loop_task"] = task
        await asyncio.sleep(0.5)

    if box.get("position"):
        await update.message.reply_text("Уже есть открытая позиция. Сначала закройте (/close).")
        return
    if not ctx.args:
        await update.message.reply_text("Использование: /open long|short [leverage] [steps]")
        return

    side = ctx.args[0].upper()
    if side not in ("LONG", "SHORT"):
        await update.message.reply_text("Укажите сторону: long или short")
        return

    lev, steps = None, None
    if len(ctx.args) >= 2:
        try:
            lev = int(ctx.args[1])
        except Exception:
            lev = None
    if len(ctx.args) >= 3:
        try:
            steps = int(ctx.args[2])
        except Exception:
            steps = None

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
            await update.message.reply_text("Этот чат не привязан к символу и одиночный режим не запущен.")
            return

    if not box.get("position"):
        await update.message.reply_text("ℹ️ Активной позиции нет.")
        return

    box["force_close"] = True
    await update.message.reply_text(f"🧰 Запрошено закрытие позиции по {sym}. Закрою в ближайшем цикле.")


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    box, sym = _find_box_by_chat(ctx.application, update.effective_chat.id)
    if box is None:
        box = ctx.bot_data.get("_main_loop_box", ctx.bot_data)
        sym = CONFIG.SYMBOL

    def _is_running(bx: dict) -> bool:
        return _is_this_box_running(ctx.application, bx)

    is_running = _is_running(box)
    active_position = box.get("position", None)

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
        total_ord = max(0, min(getattr(pos, "ord_levels", 0) - 1, len(getattr(pos, "ordinary_targets", []))))
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


# --- Entrypoint ---
if __name__ == "__main__":
    persistence = PicklePersistence(filepath="bot_persistence")
    app = ApplicationBuilder().token(BOT_TOKEN).persistence(persistence).post_init(post_init).build()

    app.add_error_handler(on_error)

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
