from __future__ import annotations

import logging
import os
import re
from typing import Optional

from html import escape as h
from telegram import (
    Update, BotCommand,
    BotCommandScopeDefault, BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats, BotCommandScopeChat,
)
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters
)

# ---------------------------------------------------------------------
# Робастный импорт API сканера
# ---------------------------------------------------------------------
try:
    from scanner_bmr_dca import (
        start_scanner_for_pair,
        stop_scanner_for_pair,
        is_scanner_running,
        _norm_symbol,
        CONFIG,
        BOXES_KEY,   # гарантируем одинаковые ключи с модулем сканера
        BANKS_KEY,
    )
    _IMPORTED_OK = True
except ImportError as _e:
    # Мягкий режим: не даём процессу упасть, подставляем заглушки
    import scanner_bmr_dca as _sc
    start_scanner_for_pair = getattr(_sc, "start_scanner_for_pair", None)
    stop_scanner_for_pair = getattr(_sc, "stop_scanner_for_pair", None)
    is_scanner_running = getattr(_sc, "is_scanner_running", None)
    _norm_symbol = getattr(
        _sc,
        "_norm_symbol",
        getattr(
            _sc,
            "norm_symbol",
            lambda s: str(s or "").upper().replace(" ", "").replace("-", ""),
        ),
    )
    CONFIG = getattr(_sc, "CONFIG", type("CONFIG", (), {"SYMBOL": "BTCUSDT", "SAFETY_BANK_USDT": 1500.0}))
    BOXES_KEY = getattr(_sc, "BOXES_KEY", "scan_boxes")
    BANKS_KEY = getattr(_sc, "BANKS_KEY", "scan_banks")

    _IMPORTED_OK = all([start_scanner_for_pair, stop_scanner_for_pair, is_scanner_running])

    if not _IMPORTED_OK:
        async def _stub_start(app, *_, **__):
            return "❌ scanner_bmr_dca не экспортирует нужные функции. Проверьте модуль."

        async def _stub_stop(app, *_, **__):
            return "❌ scanner_bmr_dca не экспортирует stop_scanner_for_pair."

        def _stub_is_running(app, *_, **__):
            return False

        start_scanner_for_pair = _stub_start
        stop_scanner_for_pair = _stub_stop
        is_scanner_running = _stub_is_running

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("main")

# ---------------------------------------------------------------------
# УТИЛИТЫ
# ---------------------------------------------------------------------


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


def _get_scanner_ns_key(chat_id: int, symbol: str) -> str:
    """Ключ namespace, который ожидает сканер: f"{chat_id}:{SYMBOL}"."""
    return f"{chat_id}:{_norm_symbol(symbol)}"


def _get_scanner_box(app: Application, symbol: str, chat_id: Optional[int]) -> dict:
    """
    Получает 'коробку' (box) состояния для сканера из app.bot_data[BOXES_KEY].
    Именно в неё сканер пишет/читает состояния.
    """
    if chat_id is None:
        log.warning("chat_id is None, returning empty box")
        return {}
    ns_key = _get_scanner_ns_key(chat_id, symbol)
    boxes = app.bot_data.setdefault(BOXES_KEY, {})
    return boxes.setdefault(ns_key, {})


def _get_banks_registry(app: Application) -> dict:
    """Глобальный реестр банков (по ключу chat_id:SYMBOL)."""
    return app.bot_data.setdefault(BANKS_KEY, {})


def _hs(s: str) -> str:
    """Нормализованный и экранированный символ для безопасных HTML-ответов."""
    return h(_norm_symbol(s))


def _parse_amount(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(" ", "").replace(",", ".")
    m = re.fullmatch(r"([0-9]*\.?[0-9]+)\s*([kKmM]?)", s)
    if not m:
        return None
    x = float(m.group(1))
    suf = (m.group(2) or "").lower()
    if suf == "k":
        x *= 1_000
    if suf == "m":
        x *= 1_000_000
    return x


async def _broadcast(app: Application, text: str, target_chat_id: Optional[int] = None):
    cid = target_chat_id or None
    if cid is None:
        log.warning("Broadcast skipped: no chat_id")
        return
    try:
        await app.bot.send_message(chat_id=cid, text=text, parse_mode="HTML")
    except Exception as e:
        log.error(f"Broadcast send failed: {e}")


# ---------------------------------------------------------------------
# КОМАНДЫ / HELP
# ---------------------------------------------------------------------

HELP_TEXT = (
    "Команды:\n"
    "• <code>/restart [SYMBOL]</code> — попросить сканер перезапустить цикл по паре\n"
    "• <code>/setbank SYMBOL USD</code> — задать банк для пары (пример: <code>/setbank GBPUSD 6000</code>)\n"
    "• <code>/setbank USD</code> — задать банк для текущей пары в этом чате\n"
    "• <code>/run SYMBOL</code> — запустить сканер пары (нужен банк)\n"
    "• <code>/stop [SYMBOL] [hard]</code> — остановить сканер\n"
    "• <code>/status</code> — краткий статус\n"
    "• <code>/open SYMBOL</code> — снять ручной режим и разрешить новый цикл\n"
    "• <code>/pause [SYMBOL]</code> — включить ручной режим (входы не стартуют)\n"
    "• <code>/fees MAKER TAKER [SYMBOL]</code> — задать комиссии в долях\n"
    "• <code>/close [SYMBOL]</code> — ручное закрытие позиции\n"
    "• <code>/hedge_close PRICE [SYMBOL]</code> — подтвердить закрытие прибыльной ноги хеджа\n"
    "• <code>/strat show|set|reset [SYMBOL]</code> — работа со STRAT\n"
    "• <code>/tac set TAC1 [TAC2] [SYMBOL]</code> — задать тактику(и) между HC и STRAT#1\n"
    "• <code>/tac reset [SYMBOL]</code> — вернуть авто-TAC\n"
    "• <code>/tac2 set PRICE [SYMBOL]</code> — задать только TAC #2\n"
    "• <code>/tac2 reset [SYMBOL]</code> — сбросить только TAC #2\n"
    "• <code>/openlong [PRICE] [SYMBOL]</code> — ручной старт LONG через хедж\n"
    "• <code>/openshort [PRICE] [SYMBOL]</code> — ручной старт SHORT через хедж\n"
    "• <code>/hedge_flip LONG|SHORT [SYMBOL]</code> — перевернуть bias хеджа\n"
)

COMMANDS = [
    BotCommand("start", "Запустить/показать помощь"),
    BotCommand("help", "Показать помощь"),
    BotCommand("restart", "Перезапустить сканер по паре"),
    BotCommand("setbank", "Установить банк: SYMBOL USD"),
    BotCommand("run", "Запустить сканер SYMBOL"),
    BotCommand("stop", "Остановить сканер [SYMBOL] [hard]"),
    BotCommand("status", "Краткий статус"),
    BotCommand("open", "Снять ручной режим и разрешить новый цикл"),
    BotCommand("pause", "Включить ручной режим"),
    BotCommand("close", "Ручное закрытие позиции [SYMBOL]"),
    BotCommand("fees", "Комиссии: maker taker [SYMBOL]"),
    BotCommand("hedge_close", "Подтвердить закрытие прибыльной ноги хеджа"),
    BotCommand("strat", "STRAT: show | set | reset"),
    BotCommand("tac", "TAC: set [TAC2] | reset"),
    BotCommand("tac2", "TAC #2: set | reset"),
    BotCommand("openlong", "Ручной старт LONG через хедж"),
    BotCommand("openshort", "Ручной старт SHORT через хедж"),
    BotCommand("hedge_flip", "Перевернуть bias хеджа"),
]


async def _post_init(app: Application):
    try:
        await app.bot.set_my_commands(COMMANDS, scope=BotCommandScopeDefault())
        await app.bot.set_my_commands(COMMANDS, scope=BotCommandScopeAllPrivateChats())
        await app.bot.set_my_commands(COMMANDS, scope=BotCommandScopeAllGroupChats())
        log.info("Bot commands set for default/private/group scopes.")
    except Exception:
        log.exception("Failed to set bot commands")


async def _ensure_chat_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        cid = update.effective_chat.id if update.effective_chat else None
        if cid:
            await context.bot.set_my_commands(COMMANDS, scope=BotCommandScopeChat(cid))
            log.info("Commands set for chat %s", cid)
    except Exception:
        log.exception("Failed to set chat-scoped commands")


# ---------------------------------------------------------------------
# КОМАНДЫ
# ---------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _ensure_chat_commands(update, context)
    await update.message.reply_html("Бот запущен.\n\n" + HELP_TEXT)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _ensure_chat_commands(update, context)
    await update.message.reply_html(HELP_TEXT)


async def cmd_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Полностью останавливает сканер и УДАЛЯЕТ банк для пары в этом чате.
    Требует нового /setbank и /run.
    """
    chat_id = _chat_id(update)
    args = context.args or []

    if args:
        sym = _norm_symbol(args[0])
    else:
        sym = context.chat_data.get("current_symbol")
        if not sym:
            return await update.message.reply_html("Укажи символ: <code>/restart SYMBOL</code>")

    context.chat_data["current_symbol"] = sym
    ns_key = _get_scanner_ns_key(chat_id, sym)

    stop_msg = await stop_scanner_for_pair(context.application, symbol=sym, chat_id=chat_id, hard=True)

    banks_registry = _get_banks_registry(context.application)
    removed_bank = banks_registry.pop(ns_key, None)

    boxes = context.application.bot_data.get(BOXES_KEY, {})
    boxes.pop(ns_key, None)

    if removed_bank is not None:
        await update.message.reply_html(
            f"{stop_msg}\n"
            f"🏦 Банк для <b>{_hs(sym)}</b> (<code>{removed_bank:.2f} USD</code>) сброшен.\n"
            f"Требуется новая настройка: <code>/setbank {_hs(sym)} USD</code> и <code>/run {_hs(sym)}</code>."
        )
    else:
        await update.message.reply_html(
            f"{stop_msg}\n"
            f"Банк для <b>{_hs(sym)}</b> не найден (уже сброшен).\n"
            f"Требуется новая настройка: <code>/setbank {_hs(sym)} USD</code>."
        )


async def cmd_setbank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []
    banks_registry = _get_banks_registry(context.application)

    if len(args) == 1:
        amt = _parse_amount(args[0])
        if amt is None or amt <= 0:
            return await update.message.reply_html("Некорректная сумма. Пример: <code>/setbank 6000</code>")
        sym = context.chat_data.get("current_symbol")
        if not sym:
            return await update.message.reply_html(
                "Неизвестна активная пара в этом чате.\n"
                "Используй: <code>/setbank SYMBOL USD</code>"
            )

        ns_key = _get_scanner_ns_key(chat_id, sym)
        banks_registry[ns_key] = float(amt)

        # если сканер уже крутится — обновим коробку
        box = _get_scanner_box(context.application, sym, chat_id)
        if box:
            box["bank_usd"] = float(amt)

        return await update.message.reply_html(
            f"OK. Банк для <b>{_hs(sym)}</b> установлен: <b>{amt:.2f} USD</b>.\n"
            f"Теперь: <code>/run {_hs(sym)}</code>"
        )

    elif len(args) >= 2:
        sym = _norm_symbol(args[0])
        amt = _parse_amount(args[1])
        if not sym:
            return await update.message.reply_html("Укажи символ. Пример: <code>/setbank GBPUSD 6000</code>")
        if amt is None or amt <= 0:
            return await update.message.reply_html("Некорректная сумма. Пример: <code>/setbank GBPUSD 6000</code>")

        ns_key = _get_scanner_ns_key(chat_id, sym)
        banks_registry[ns_key] = float(amt)

        box = _get_scanner_box(context.application, sym, chat_id)
        if box:
            box["bank_usd"] = float(amt)

        context.chat_data["current_symbol"] = sym

        return await update.message.reply_html(
            f"OK. Банк для <b>{_hs(sym)}</b> установлен: <b>{amt:.2f} USD</b>.\n"
            f"Запуск: <code>/run {_hs(sym)}</code>"
        )

    else:
        return await update.message.reply_html(
            "Использование:\n"
            "• <code>/setbank SYMBOL USD</code>\n"
            "• <code>/setbank USD</code> — для уже выбранной пары"
        )


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []
    if not args:
        return await update.message.reply_html("Укажи символ: <code>/run SYMBOL</code>")

    sym = _norm_symbol(args[0])
    context.chat_data["current_symbol"] = sym

    ns_key = _get_scanner_ns_key(chat_id, sym)
    banks_registry = _get_banks_registry(context.application)

    if ns_key not in banks_registry:
        default_bank = getattr(CONFIG, "SAFETY_BANK_USDT", 1500.0)
        hint = f" (сейчас по умолчанию: {default_bank:.2f} USD)"
        return await update.message.reply_html(
            f"Сначала задай банк: <code>/setbank {_hs(sym)} USD</code>{hint}"
        )

    if is_scanner_running(context.application, sym, chat_id):
        return await update.message.reply_html(f"Сканер для <b>{_hs(sym)}</b> уже запущен.")

    msg = await start_scanner_for_pair(
        context.application,
        symbol=sym,
        chat_id=chat_id,
    )
    await update.message.reply_html(msg)


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []

    hard = any(a.lower() == "hard" for a in args)
    non_flags = [a for a in args if a.lower() != "hard"]

    if non_flags:
        sym = _norm_symbol(non_flags[0])
        context.chat_data["current_symbol"] = sym
    else:
        sym = context.chat_data.get("current_symbol")
        if not sym:
            return await update.message.reply_html("Укажи символ: <code>/stop SYMBOL</code>")

    msg = await stop_scanner_for_pair(context.application, symbol=sym, chat_id=chat_id, hard=hard)
    await update.message.reply_html(msg)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    sym = context.chat_data.get("current_symbol") or CONFIG.SYMBOL

    box = _get_scanner_box(context.application, sym, chat_id)

    snap = box.get("status_snapshot") or {}
    state = snap.get("state", "N/A")
    bank_f = snap.get("bank_fact_usdt")
    bank_t = snap.get("bank_target_usdt")
    has_rng = "✅" if snap.get("has_ranges") else "❌"

    text = box.get("status_line") or (
        f"<b>Статус ({_hs(sym)})</b>\n"
        f"Сканер: <b>{state}</b>\n"
        f"Банк (факт/план): {bank_f if bank_f is not None else 'N/A'} / {bank_t if bank_t is not None else 'N/A'} USD\n"
        f"Диапазоны доступны: {has_rng}"
    )
    await update.message.reply_html(text)


async def cmd_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []
    if not args:
        return await update.message.reply_html("Укажи символ: <code>/open SYMBOL</code>")
    sym = _norm_symbol(args[0])

    box = _get_scanner_box(context.application, sym, chat_id)
    box["user_manual_mode"] = False

    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"Готово. /open взведён для <b>{_hs(sym)}</b>.")


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []
    sym = _norm_symbol(args[0]) if args else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)

    box = _get_scanner_box(context.application, sym, chat_id)
    box["force_close"] = True

    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"MANUAL_CLOSE запрошен для <b>{_hs(sym)}</b>.")


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []
    sym = _norm_symbol(args[0]) if args else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)

    box = _get_scanner_box(context.application, sym, chat_id)
    box["user_manual_mode"] = True

    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(
        f"⏸ Ручной режим по <b>{_hs(sym)}</b>. Используй <code>/open {_hs(sym)}</code> для продолжения."
    )


async def cmd_fees(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 2:
        return await update.message.reply_html("Формат: <code>/fees 0.0000 0.0000 [SYMBOL]</code>")
    try:
        maker = float(str(args[0]).replace(",", "."))
        taker = float(str(args[1]).replace(",", "."))
    except Exception:
        return await update.message.reply_html("Не могу разобрать комиссии. Пример: <code>/fees 0.0002 0.0005</code>")
    sym = _norm_symbol(args[2]) if len(args) > 2 else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)
    chat_id = _chat_id(update)

    box = _get_scanner_box(context.application, sym, chat_id)
    box["fee_maker"] = maker
    box["fee_taker"] = taker

    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(
        f"⚙️ Комиссии для <b>{_hs(sym)}</b> заданы: maker={maker:.6f}, taker={taker:.6f}"
    )


# ---------------- TAC (1 и 2) -----------------
async def cmd_tac(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """TAC: set PRICE [PRICE2] [SYMBOL] | reset [SYMBOL]"""
    chat_id = _chat_id(update)
    args = context.args or []
    if not args:
        return await update.message.reply_html(
            "Использование:\n"
            "• <code>/tac set PRICE1 [PRICE2] [SYMBOL]</code>\n"
            "• <code>/tac reset [SYMBOL]</code>"
        )

    sub = args[0].lower()
    tail = args[1:]

    def _is_num(s: str) -> bool:
        try:
            float(str(s).replace(",", "."))
            return True
        except Exception:
            return False

    sym = None
    if tail and not all(_is_num(x) for x in tail):
        for x in reversed(tail):
            if not _is_num(x):
                sym = _norm_symbol(x)
                break
    if sym is None:
        sym = context.chat_data.get("current_symbol") or CONFIG.SYMBOL

    box = _get_scanner_box(context.application, sym, chat_id)

    if sub == "reset":
        box["cmd_tac_reset"] = True
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(f"Запросил сброс TAC до авто-плана по <b>{_hs(sym)}</b>.")

    if sub == "set":
        prices: list[float] = []
        for x in tail:
            if _is_num(x):
                prices.append(float(str(x).replace(",", ".")))
        if not prices:
            return await update.message.reply_html("Укажи цену: <code>/tac set PRICE [PRICE2] [SYMBOL]</code>")
        box["cmd_tac_set"] = prices[0]              # TAC #1
        if len(prices) > 1:
            box["cmd_tac2_set"] = prices[1]        # TAC #2
        context.chat_data["current_symbol"] = sym
        if len(prices) == 1:
            return await update.message.reply_html(
                f"Запросил установку TAC #1 по <b>{_hs(sym)}</b>: <code>{prices[0]:.6f}</code>"
            )
        else:
            return await update.message.reply_html(
                f"Запросил установку TAC #1 и TAC #2 по <b>{_hs(sym)}</b>: "
                f"<code>{prices[0]:.6f}</code>, <code>{prices[1]:.6f}</code>"
            )

    return await update.message.reply_html("Неизвестная подкоманда. Используй: <code>/tac set|reset</code>")


async def cmd_tac2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """TAC #2: set PRICE [SYMBOL] | reset [SYMBOL]"""
    chat_id = _chat_id(update)
    args = context.args or []
    if not args:
        return await update.message.reply_html(
            "Использование:\n"
            "• <code>/tac2 set PRICE [SYMBOL]</code>\n"
            "• <code>/tac2 reset [SYMBOL]</code>"
        )
    sub = args[0].lower()
    tail = args[1:]

    def _is_num(s: str) -> bool:
        try:
            float(str(s).replace(",", "."))
            return True
        except Exception:
            return False

    sym = None
    if tail and not all(_is_num(x) for x in tail):
        for x in reversed(tail):
            if not _is_num(x):
                sym = _norm_symbol(x)
                break
    if sym is None:
        sym = context.chat_data.get("current_symbol") or CONFIG.SYMBOL

    box = _get_scanner_box(context.application, sym, chat_id)

    if sub == "reset":
        box["cmd_tac2_set"] = None
        box["cmd_tac_reset"] = False
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(f"Запросил сброс только TAC #2 по <b>{_hs(sym)}</b>.")

    if sub == "set":
        price = None
        for x in tail:
            if _is_num(x):
                price = float(str(x).replace(",", "."))
                break
        if price is None:
            return await update.message.reply_html("Укажи цену: <code>/tac2 set PRICE [SYMBOL]</code>")
        box["cmd_tac2_set"] = price
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(
            f"Запросил установку TAC #2 по <b>{_hs(sym)}</b>: <code>{price:.6f}</code>"
        )

    return await update.message.reply_html("Неизвестная подкоманда. Используй: <code>/tac2 set|reset</code>")


# ---------------- STRAT -----------------
async def cmd_strat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []
    if not args:
        return await update.message.reply_html(
            "Использование:\n"
            "• <code>/strat show [SYMBOL]</code>\n"
            "• <code>/strat set P1 [P2 P3] [SYMBOL]</code>\n"
            "• <code>/strat reset [SYMBOL]</code>"
        )

    sub = args[0].lower()
    tail = args[1:]

    def _is_num(s: str) -> bool:
        try:
            float(str(s).replace(",", "."))
            return True
        except Exception:
            return False

    sym = None
    if tail and not all(_is_num(x) for x in tail):
        for x in reversed(tail):
            if not _is_num(x):
                sym = _norm_symbol(x)
                break
    if sym is None:
        sym = context.chat_data.get("current_symbol") or CONFIG.SYMBOL

    box = _get_scanner_box(context.application, sym, chat_id)

    if sub == "show":
        box["cmd_strat_show"] = True
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(f"Запросил STRAT-отчёт по <b>{_hs(sym)}</b>.")

    if sub == "reset":
        box["cmd_strat_reset"] = True
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(f"Запросил сброс STRAT по <b>{_hs(sym)}</b>.")

    if sub == "set":
        prices = []
        for x in tail:
            if _is_num(x):
                prices.append(float(str(x).replace(",", ".")))
            else:
                break
        if not prices:
            return await update.message.reply_html("Укажи 1–3 цены: <code>/strat set P1 [P2 P3] [SYMBOL]</code>")
        box["cmd_strat_set"] = prices[:3]
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(
            f"Запросил установку STRAT по <b>{_hs(sym)}</b>: "
            + ", ".join(f"<code>{p:.6f}</code>" for p in prices[:3])
        )

    return await update.message.reply_html("Неизвестная подкоманда. Используй: <code>/strat show|set|reset</code>")


async def cmd_hedge_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if not args:
        return await update.message.reply_html("Формат: <code>/hedge_close 150.123 [SYMBOL]</code>")
    price_raw = args[0]
    sym = _norm_symbol(args[1]) if len(args) > 1 else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)
    chat_id = _chat_id(update)

    box = _get_scanner_box(context.application, sym, chat_id)
    try:
        px = float(str(price_raw).replace(",", "."))
        box["pending_hc_price"] = px
        context.chat_data["current_symbol"] = sym
        await update.message.reply_html(
            f"✅ Принял цену закрытия хеджа по <b>{_hs(sym)}</b>: <code>{px:.6f}</code>. Ожидаю пересчет..."
        )
    except Exception:
        box["pending_hc_price"] = None
        await update.message.reply_html(f"⚠️ Цена не распознана — сканер возьмёт рыночную. Пара: <b>{_hs(sym)}</b>.")


async def cmd_hedge_close_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "")
    parts = text.split(maxsplit=2)
    context.args = parts[1:] if len(parts) > 1 else []
    return await cmd_hedge_close(update, context)


async def _set_manual_open(update: Update, context: ContextTypes.DEFAULT_TYPE, side: str):
    chat_id = _chat_id(update)
    args = context.args or []
    price = None
    sym = None
    if args:
        try:
            price = float(str(args[0]).replace(",", "."))
            if len(args) > 1:
                sym = _norm_symbol(args[1])
        except Exception:
            sym = _norm_symbol(args[0])
    if sym is None:
        sym = context.chat_data.get("current_symbol") or CONFIG.SYMBOL

    if not is_scanner_running(context.application, sym, chat_id):
        return await update.message.reply_html(
            f"Сканер для <b>{_hs(sym)}</b> не запущен. Сначала: <code>/run {_hs(sym)}</code>"
        )

    box = _get_scanner_box(context.application, sym, chat_id)
    box["pending_hedge_bias"] = "LONG" if side.upper() == "LONG" else "SHORT"
    box["user_manual_mode"] = False

    if price is not None:
        box["pending_hc_price"] = float(price)

    context.chat_data["current_symbol"] = sym
    extra = f" @ <code>{price:.6f}</code>" if price is not None else ""
    await update.message.reply_html(
        f"Ок. Запросил ручной bias: <b>{side.upper()}</b> по <b>{_hs(sym)}</b>{extra}.\n"
        f"Ручной режим снят. Сканер выполнит вход при первой возможности или пересчитает план."
    )


async def cmd_openlong(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _set_manual_open(update, context, "LONG")


async def cmd_openshort(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _set_manual_open(update, context, "SHORT")


async def cmd_hedge_flip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if not args or args[0].upper() not in ("LONG", "SHORT"):
        return await update.message.reply_html("Формат: <code>/hedge_flip LONG|SHORT [SYMBOL]</code>")
    side = args[0].upper()
    sym = _norm_symbol(args[1]) if len(args) > 1 else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)
    chat_id = _chat_id(update)

    box = _get_scanner_box(context.application, sym, chat_id)
    box["pending_hedge_bias"] = side

    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"Ок. Переворот bias хеджа на <b>{side}</b> по <b>{_hs(sym)}</b> запрошен. Ожидаю пересчет...")


async def cmd_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html("Неизвестная команда.\n\n" + HELP_TEXT)


# ---------------------------------------------------------------------
# СБОРКА ПРИЛОЖЕНИЯ
# ---------------------------------------------------------------------
def build_app() -> Application:
    token = _get_bot_token()
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN/BOT_TOKEN is not set")

    application = ApplicationBuilder().token(token).post_init(_post_init).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("restart", cmd_restart))

    application.add_handler(CommandHandler("setbank", cmd_setbank))
    application.add_handler(CommandHandler("run", cmd_run))
    application.add_handler(CommandHandler("stop", cmd_stop))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("open", cmd_open))
    application.add_handler(CommandHandler("pause", cmd_pause))
    application.add_handler(CommandHandler("close", cmd_close))
    application.add_handler(CommandHandler("fees", cmd_fees))

    application.add_handler(CommandHandler("hedge_close", cmd_hedge_close))
    application.add_handler(CommandHandler("strat", cmd_strat))
    application.add_handler(CommandHandler("tac", cmd_tac))
    application.add_handler(CommandHandler("tac2", cmd_tac2))

    application.add_handler(CommandHandler("openlong", cmd_openlong))
    application.add_handler(CommandHandler("openshort", cmd_openshort))
    application.add_handler(CommandHandler("hedge_flip", cmd_hedge_flip))

    # кириллический алиас:
    application.add_handler(
        MessageHandler(filters.Regex(r"^/хедж_закрытие(?:@[\w_]+)?(?:\s|$)"), cmd_hedge_close_alias)
    )

    # неизвестные — в самом конце
    application.add_handler(MessageHandler(filters.COMMAND, cmd_unknown))

    log.info("Bot application built.")
    return application


if __name__ == "__main__":
    app = build_app()
    app.run_polling(close_loop=False, drop_pending_updates=True)
