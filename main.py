import os
import asyncio
import logging
from telegram import Update, constants, BotCommand
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes, PicklePersistence

import scanner_bmr_dca as scanner_engine
from scanner_bmr_dca import CONFIG
import trade_executor

# --- Конфигурация ---
BOT_VERSION = "BMR-DCA EURC v0.1"
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var is not set")

DEFAULT_BANK_USDT = 1000.0
DEFAULT_BUFFER_OVER_EDGE = 0.30

log = logging.getLogger("bot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)

# --- Утилиты ---
def is_loop_running(app: Application) -> bool:
    task = getattr(app, "_main_loop_task", None)
    return task is not None and not task.done()

async def post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        log.warning(f"delete_webhook failed: {e}")

    log.info("Бот запущен. Проверяем, нужно ли запускать основной цикл...")
    if app.bot_data.get('run_loop_on_startup', False):
        log.info("Обнаружен флаг 'run_loop_on_startup'. Запускаю основной цикл.")
        task = asyncio.create_task(scanner_engine.scanner_main_loop(app, broadcast))
        setattr(app, "_main_loop_task", task)

    await app.bot.set_my_commands([
        BotCommand("start", "Запустить/перезапустить бота"),
        BotCommand("run", "Запустить сканер"),
        BotCommand("stop", "Остановить сканер"),
        BotCommand("status", "Показать текущий статус и параметры"),
        BotCommand("pause", "Приостановить поиск новых сигналов"),
        BotCommand("resume", "Возобновить поиск новых сигналов"),
        BotCommand("close", "Закрыть текущую позицию по рынку"),
        BotCommand("open", "Открыть позицию: /open long|short [lev] [steps]"),
        BotCommand("setbank", "Установить общий банк позиции, USDT"),
        BotCommand("setbuf", "Установить буфер за границей (напр. 0.3 или 30%)"),
        BotCommand("setfees", "Установить комиссии, %: /setfees [maker] [taker]"),
        BotCommand("fees", "Показать текущие комиссии"),
    ])

async def broadcast(app: Application, txt: str):
    chat_ids = set(app.bot_data.get('chat_ids', set()))
    for cid in list(chat_ids):
        try:
            await app.bot.send_message(chat_id=cid, text=txt, parse_mode=constants.ParseMode.HTML)
        except Exception as e:
            log.error(f"Не удалось отправить сообщение в чат {cid}: {e}")
            if "bot was blocked" in str(e):
                chat_ids.discard(cid)
                log.info(f"Чат {cid} удален из списка рассылки (бот заблокирован).")
    app.bot_data['chat_ids'] = chat_ids

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    bd = ctx.bot_data
    bd.setdefault('chat_ids', set()).add(chat_id)
    bd.setdefault('run_loop_on_startup', False)
    bd.setdefault('scan_paused', False)
    log.info(f"Пользователь {chat_id} запустил бота.")
    await update.message.reply_text(
        f"✅ <b>Бот {BOT_VERSION} запущен.</b>\nИспользуйте /run для запуска сканера.",
        parse_mode=constants.ParseMode.HTML
    )

async def cmd_run(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    app = ctx.application
    if is_loop_running(app):
        await update.message.reply_text("ℹ️ Сканер уже запущен. Для остановки используйте /stop.")
        return

    app.bot_data['bot_on'] = True
    app.bot_data['run_loop_on_startup'] = True
    app.bot_data['scan_paused'] = False
    log.info("Команда /run: запускаем основной цикл.")
    task = asyncio.create_task(scanner_engine.scanner_main_loop(app, broadcast))
    setattr(app, "_main_loop_task", task)
    await update.message.reply_text("🚀 <b>Запускаю сканер...</b>", parse_mode=constants.ParseMode.HTML)

async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    app = ctx.application
    if not is_loop_running(app):
        await update.message.reply_text("ℹ️ Сканер уже остановлен.")
        return

    app.bot_data['bot_on'] = False
    task = getattr(app, "_main_loop_task", None)
    if task:
        try:
            await task
        except asyncio.CancelledError:
            pass
        setattr(app, "_main_loop_task", None)

    app.bot_data['run_loop_on_startup'] = False
    log.info("Команда /stop: основной цикл остановлен.")
    await update.message.reply_text("🛑 <b>Сканер остановлен.</b>", parse_mode=constants.ParseMode.HTML)

async def cmd_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_loop_running(ctx.application):
        await update.message.reply_text("ℹ️ Сканер не запущен, нечего ставить на паузу.")
        return
    ctx.bot_data["scan_paused"] = True
    log.info("Команда /pause: поиск новых сигналов приостановлен.")
    await update.message.reply_text("⏸️ <b>Поиск новых сигналов приостановлен.</b>\nСопровождение открытых сделок продолжается. Для возобновления используйте /resume.", parse_mode=constants.ParseMode.HTML)

async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_loop_running(ctx.application):
        await update.message.reply_text("ℹ️ Сканер не запущен.")
        return
    ctx.bot_data["scan_paused"] = False
    log.info("Команда /resume: поиск новых сигналов возобновлен.")
    await update.message.reply_text("▶️ <b>Поиск новых сигналов возобновлён.</b>", parse_mode=constants.ParseMode.HTML)

async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_loop_running(ctx.application):
        await update.message.reply_text("ℹ️ Сканер не запущен.")
        return
    if not ctx.bot_data.get("position"):
        await update.message.reply_text("ℹ️ Активной позиции нет.")
        return
    ctx.bot_data["force_close"] = True
    await update.message.reply_text("🧰 Запрошено закрытие позиции. Закрою в ближайшем цикле.")

async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    app = context.application

    if not is_loop_running(app):
        app.bot_data['bot_on'] = True
        app.bot_data['run_loop_on_startup'] = True
        app.bot_data['scan_paused'] = False
        task = asyncio.create_task(scanner_engine.scanner_main_loop(app, broadcast))
        setattr(app, "_main_loop_task", task)
        await update.message.reply_text("🔌 Сканер был выключен — запускаю его…")

    if app.bot_data.get("position"):
        await update.message.reply_text("Уже есть открытая позиция. Сначала закройте её (/close).")
        return

    if not context.args:
        await update.message.reply_text("Использование: /open long|short [leverage] [steps]")
        return

    side = context.args[0].upper()
    if side not in ("LONG", "SHORT"):
        await update.message.reply_text("Укажите сторону: long или short")
        return

    lev = None
    steps = None
    if len(context.args) >= 2:
        try: lev = int(context.args[1])
        except Exception: lev = None
    if len(context.args) >= 3:
        try: steps = int(context.args[2])
        except Exception: steps = None

    if lev is not None:
        lev = max(CONFIG.MIN_LEVERAGE, min(CONFIG.MAX_LEVERAGE, lev))
    if steps is not None:
        steps = max(1, min(CONFIG.DCA_LEVELS, steps))

    app.bot_data["manual_open"] = {"side": side, "leverage": lev, "max_steps": steps}

    await update.message.reply_text(
        f"Ок, открываю {side} по рынку текущей ценой. "
        f"{'(левередж: '+str(lev)+') ' if lev else ''}"
        f"{'(макс. шагов: '+str(steps)+')' if steps else ''}"
    )

async def cmd_setbank(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        val = float(ctx.args[0])
        if val <= 0: raise ValueError
        ctx.bot_data["safety_bank_usdt"] = val
        await update.message.reply_text(f"💰 Банк на позицию установлен: {val:.2f} USDT")
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /setbank 1000")

async def cmd_setbuf(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        raw = ctx.args[0].strip().replace('%','')
        v = float(raw)
        buf = v/100.0 if v > 1 else v
        if not (0.0 < buf < 0.9): raise ValueError
        ctx.bot_data["buffer_over_edge"] = buf
        await update.message.reply_text(f"🛡️ Буфер за границей установлен: {buf:.2%}")
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /setbuf 0.30 (или 30%)")

def _parse_fee_arg(x: str) -> float:
    s = x.strip()
    had_pct = s.endswith('%')
    if had_pct:
        s = s[:-1]
    v = float(s)
    if had_pct:
        return v / 100.0
    return v if v < 0.01 else v / 100.0

async def cmd_setfees(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        if len(ctx.args) == 1:
            f = _parse_fee_arg(ctx.args[0])
            if not (0 <= f < 0.01): raise ValueError
            ctx.bot_data["fee_maker"] = f
            ctx.bot_data["fee_taker"] = f
            await update.message.reply_text(f"✅ Комиссии установлены: maker={f*100:.4f}% taker={f*100:.4f}%")
        elif len(ctx.args) >= 2:
            fm = _parse_fee_arg(ctx.args[0])
            ft = _parse_fee_arg(ctx.args[1])
            if not (0 <= fm < 0.01 and 0 <= ft < 0.01): raise ValueError
            ctx.bot_data["fee_maker"] = fm
            ctx.bot_data["fee_taker"] = ft
            await update.message.reply_text(f"✅ Комиссии установлены: maker={fm*100:.4f}% taker={ft*100:.4f}%")
        else:
            await update.message.reply_text("Использование: /setfees 0.02 или /setfees 0.02 0.02 (в %, либо 0.0002 0.0002)")
    except Exception:
        await update.message.reply_text("⚠️ Неверные значения. Пример: /setfees 0.02 0.02 (это 0.02% на сторону)")

async def cmd_fees(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    fm = float(ctx.bot_data.get("fee_maker", getattr(CONFIG, "FEE_MAKER", 0.0002)))
    ft = float(ctx.bot_data.get("fee_taker", getattr(CONFIG, "FEE_TAKER", 0.0002)))
    await update.message.reply_text(f"Текущие комиссии: maker={fm*100:.4f}%  taker={ft*100:.4f}% (round-trip ≈ {(fm+ft)*100:.4f}%)")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bot_data = ctx.bot_data
    is_running = is_loop_running(ctx.application)
    is_paused = bot_data.get("scan_paused", False)
    
    active_position = bot_data.get('position', None)
    cfg = scanner_engine.CONFIG

    scanner_status = "🔌 ОСТАНОВЛЕН"
    if is_running:
        scanner_status = "⏸️ НА ПАУЗЕ" if is_paused else "⚡️ РАБОТАЕТ"

    position_status = "Нет активной позиции."
    if active_position:
        pos = active_position
        sl_show = f"{pos.sl_price:.6f}" if pos.sl_price is not None else "N/A"
        tp_show = f"{pos.tp_price:.6f}" if getattr(pos, "tp_price", None) else "N/A"
        avg_show = f"{pos.avg:.6f}" if getattr(pos, "avg", None) else "N/A"
        max_steps = getattr(pos, "max_steps", (len(getattr(pos, "step_margins", [])) or cfg.DCA_LEVELS))
        lev_show = getattr(pos, "leverage", getattr(cfg, "LEVERAGE", "N/A"))
        
        remaining_total = max(0, getattr(pos, "max_steps", 0) - getattr(pos, "steps_filled", 0))
        reserved = getattr(pos, "reserved_one", False)
        reserved_left = 1 if (reserved and remaining_total > 0) else 0
        ordinary_left = max(0, remaining_total - reserved_left)
        
        position_status = (
            f"• <b>Сигнал ID:</b> {pos.signal_id}\n"
            f"• <b>Сторона:</b> {pos.side}\n"
            f"• <b>Плечо:</b> {lev_show}x\n"
            f"• <b>Ступеней:</b> {pos.steps_filled} / {max_steps}\n"
            f"• <b>Средняя цена:</b> <code>{avg_show}</code>\n"
            f"• <b>TP/SL:</b> <code>{tp_show}</code> / <code>{sl_show}</code>\n"
            f"• <b>Резерв активирован:</b> {'Да' if reserved else 'Нет'}\n"
            f"• <b>Осталось (обычных | резерв):</b> {ordinary_left} | {reserved_left}"
        )
    
    bank = bot_data.get("safety_bank_usdt", getattr(cfg, "SAFETY_BANK_USDT", DEFAULT_BANK_USDT))
    buf  = bot_data.get("buffer_over_edge", getattr(cfg, "BUFFER_OVER_EDGE", DEFAULT_BUFFER_OVER_EDGE))
    fm = bot_data.get("fee_maker", getattr(cfg, "FEE_MAKER", 0.0002))
    ft = bot_data.get("fee_taker", getattr(cfg, "FEE_TAKER", 0.0002))
    
    dca_info = f"• DCA: max_steps={CONFIG.DCA_LEVELS} (резерв {'вкл' if active_position and getattr(active_position, 'reserved_one', False) else 'выкл'})\n"

    msg = (
        f"<b>Состояние бота {BOT_VERSION}</b>\n\n"
        f"<b>Статус сканера:</b> {scanner_status}\n\n"
        f"<b><u>Риск/банк:</u></b>\n"
        f"• Банк позиции: <b>{bank:.2f} USDT</b>\n"
        f"• Буфер за границей: <b>{buf:.2%}</b> (неактивно в MARGIN-режиме)\n"
        f"• Комиссии: maker <b>{fm*100:.4f}%</b> / taker <b>{ft*100:.4f}%</b> (RT≈ {(fm+ft)*100:.4f}%)\n"
        f"{dca_info}\n"
        f"<b><u>Активная позиция:</u></b>\n{position_status}"
    )

    await update.message.reply_text(msg, parse_mode=constants.ParseMode.HTML)


if __name__ == "__main__":
    # ИСПРАВЛЕНО: Убран лишний аргумент для совместимости с PTB v20+
    persistence = PicklePersistence(filepath="bot_persistence")
    app = ApplicationBuilder().token(BOT_TOKEN).persistence(persistence).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("run", cmd_run))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("setbank", cmd_setbank))
    app.add_handler(CommandHandler("setbuf", cmd_setbuf))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("open", cmd_open))
    app.add_handler(CommandHandler("setfees", cmd_setfees))
    app.add_handler(CommandHandler("fees", cmd_fees))

    log.info(f"Bot {BOT_VERSION} starting...")
    app.run_polling()
