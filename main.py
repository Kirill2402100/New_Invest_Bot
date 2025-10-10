from __future__ import annotations

import logging
import os
import re
import time
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

# Робастный импорт API сканера: поддерживаем альтернативные имена.
try:
    from scanner_bmr_dca import (
        start_scanner_for_pair, stop_scanner_for_pair, is_scanner_running,
        _norm_symbol, CONFIG
    )
except ImportError as _e:
    import scanner_bmr_dca as _sc
    start_scanner_for_pair = getattr(_sc, "start_scanner_for_pair",
                                     getattr(_sc, "start_pair_scanner", None))
    stop_scanner_for_pair  = getattr(_sc, "stop_scanner_for_pair",
                                     getattr(_sc, "stop_pair_scanner", None))
    is_scanner_running     = getattr(_sc, "is_scanner_running",
                                     getattr(_sc, "is_pair_scanner_running", None))
    _norm_symbol           = getattr(
        _sc, "_norm_symbol",
        getattr(_sc, "norm_symbol",
                lambda s: str(s or "").upper().replace(" ", "").replace("-", ""))
    )
    CONFIG = getattr(_sc, "CONFIG", type("CONFIG", (), {"SYMBOL": "BTCUSDT"}))
    _missing = [n for n, v in {
        "start_scanner_for_pair": start_scanner_for_pair,
        "stop_scanner_for_pair":  stop_scanner_for_pair,
        "is_scanner_running":     is_scanner_running,
    }.items() if v is None]
    if _missing:
        raise ImportError(
            "scanner_bmr_dca не экспортирует необходимые функции: " + ", ".join(_missing)
        ) from _e

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

def _hs(s: str) -> str:
    """Нормализованный и экранированный символ для безопасных HTML-ответов."""
    return h(_norm_symbol(s))

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
    if cid is None:
        log.warning("Broadcast skipped: no chat_id")
        return
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
    "• <code>/open SYMBOL</code> — снять ручной режим и разрешить новый цикл\n"
    "• <code>/pause [SYMBOL]</code> — включить ручной режим (входы не стартуют)\n"
    "• <code>/fees MAKER TAKER [SYMBOL]</code> — задать комиссии в долях (пример: <code>/fees 0.0002 0.0005 EURUSD</code>)\n"
    "• <code>/close [SYMBOL]</code> — ручное закрытие позиции\n"
    "• <code>/hedge_close PRICE [SYMBOL]</code> / <code>/хедж_закрытие PRICE [SYMBOL]</code> — подтвердить закрытие прибыльной ноги хеджа\n"
    "• <code>/strat show [SYMBOL]</code> — показать текущий STRAT-план\n"
    "• <code>/strat set P1 [P2 P3] [SYMBOL]</code> — задать 1–3 цены STRAT\n"
    "• <code>/strat reset [SYMBOL]</code> — вернуть авто-план STRAT\n"
    "• <code>/tac set PRICE [SYMBOL]</code> — задать ручной TAC между HC и STRAT#1\n"
    "• <code>/tac reset [SYMBOL]</code> — вернуть авто-TAC\n"
    "• <code>/openlong [PRICE] [SYMBOL]</code> — немедленный ручной старт через хедж в LONG\n"
    "• <code>/openshort [PRICE] [SYMBOL]</code> — немедленный ручной старт через хедж в SHORT\n"
    "• <code>/hedge_flip LONG|SHORT [SYMBOL]</code> — перевернуть bias активного хеджа\n"
)

# --- меню команд для Telegram ---
COMMANDS = [
    BotCommand("start", "Запустить/перезапустить бота"),
    BotCommand("help", "Показать помощь"),
    BotCommand("setbank", "Установить банк: SYMBOL USD"),
    BotCommand("run", "Запустить сканер SYMBOL"),
    BotCommand("stop", "Остановить сканер [SYMBOL] [hard]"),
    BotCommand("status", "Краткий статус"),
    BotCommand("open", "Снять ручной режим и разрешить новый цикл"),
    BotCommand("pause", "Включить ручной режим (входы не стартуют)"),
    BotCommand("close", "Ручное закрытие позиции [SYMBOL]"),
    BotCommand("fees", "Комиссии: maker taker [SYMBOL]"),
    BotCommand("hedge_close", "Подтвердить закрытие прибыльной ноги хеджа"),
    BotCommand("strat", "STRAT: show | set | reset"),
    BotCommand("tac", "TAC: set | reset"),
    BotCommand("openlong", "Ручной старт LONG через хедж"),
    BotCommand("openshort", "Ручной старт SHORT через хедж"),
    BotCommand("hedge_flip", "Перевернуть bias хеджа: LONG|SHORT"),
]

async def _post_init(app: Application):
    try:
        # 1) на все чаты по умолчанию
        await app.bot.set_my_commands(COMMANDS, scope=BotCommandScopeDefault())
        # 2) на все личные чаты
        await app.bot.set_my_commands(COMMANDS, scope=BotCommandScopeAllPrivateChats())
        # 3) на все групповые чаты
        await app.bot.set_my_commands(COMMANDS, scope=BotCommandScopeAllGroupChats())
        log.info("Bot commands set for default/private/group scopes.")
    except Exception:
        log.exception("Failed to set bot commands")

async def _ensure_chat_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительно обновить список команд для конкретного чата (перебивает BotFather-списки чата)."""
    try:
        cid = update.effective_chat.id if update.effective_chat else None
        if cid:
            await context.bot.set_my_commands(COMMANDS, scope=BotCommandScopeChat(cid))
            log.info("Commands set for chat %s", cid)
    except Exception:
        log.exception("Failed to set chat-scoped commands")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _ensure_chat_commands(update, context)
    await update.message.reply_html(
        "Бот запущен. Сначала установите банк: "
        "<code>/setbank SYMBOL USD</code>\n\n" + HELP_TEXT
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # на всякий случай тоже обновим список для текущего чата
    await _ensure_chat_commands(update, context)
    await update.message.reply_html(HELP_TEXT)

async def cmd_setbank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = _chat_id(update)
    args = context.args or []

    # формы:
    # 1) /setbank 6000           -> для уже выбранной пары (в этом чате)
    # 2) /setbank GBPUSD 6000     -> явная пара + сумма (можно до /run)
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
            f"OK. Банк для <b>{_hs(sym)}</b> установлен: <b>{amt:.2f} USD</b>.\n"
            f"Теперь можете запустить: <code>/run {_hs(sym)}</code>"
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
            f"OK. Банк для <b>{_hs(sym)}</b> установлен: <b>{amt:.2f} USD</b>.\n"
            f"Запуск: <code>/run {_hs(sym)}</code>"
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
            f"Сначала задайте банк для <b>{_hs(sym)}</b>: "
            f"<code>/setbank {_hs(sym)} USD</code>{hint}"
        )

    if is_scanner_running(context.application, sym, chat_id):
        return await update.message.reply_html(f"Сканер для <b>{_hs(sym)}</b> уже запущен.")

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
        f"<b>Статус ({_hs(sym)})</b>\n"
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
    box["user_manual_mode"] = False  # снять ручной режим после TP/SL/manual_close
    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"Готово. Взведён /open для <b>{_hs(sym)}</b> (направление выберет сканер).")

async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручное закрытие текущей позиции."""
    chat_id = _chat_id(update)
    args = context.args or []
    sym = _norm_symbol(args[0]) if args else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)
    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})
    box["force_close"] = True
    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"MANUAL_CLOSE запрошен для <b>{_hs(sym)}</b>.")

async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Включить ручной режим (входы не стартуют)."""
    chat_id = _chat_id(update)
    args = context.args or []
    sym = _norm_symbol(args[0]) if args else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)
    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})
    box["user_manual_mode"] = True
    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"⏸ Включён ручной режим по <b>{_hs(sym)}</b>. Используйте <code>/open {_hs(sym)}</code> для продолжения.")

async def cmd_fees(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Установить комиссии: maker taker (в долях). /fees 0.0002 0.0005 [SYMBOL]"""
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
    box = _get_slot(context.application, sym, chat_id)
    box["fee_maker"] = maker
    box["fee_taker"] = taker
    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(
        f"⚙️ Комиссии для <b>{_hs(sym)}</b> заданы: maker={maker:.6f}, taker={taker:.6f}"
    )

async def cmd_tac(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """TAC: set PRICE [SYMBOL] | reset [SYMBOL]"""
    chat_id = _chat_id(update)
    args = context.args or []
    if not args:
        return await update.message.reply_html(
            "Использование:\n"
            "• <code>/tac set PRICE [SYMBOL]</code>\n"
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

    # SYMBOL — последний нечисловой аргумент (если есть)
    sym = None
    if tail and not all(_is_num(x) for x in tail):
        for x in reversed(tail):
            if not _is_num(x):
                sym = _norm_symbol(x); break
    if sym is None:
        sym = context.chat_data.get("current_symbol") or CONFIG.SYMBOL

    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})

    if sub == "reset":
        box["cmd_tac_reset"] = True
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(f"Запросил сброс TAC до авто-плана по <b>{_hs(sym)}</b>.")

    if sub == "set":
        # первый числовой аргумент — цена
        price = None
        for x in tail:
            if _is_num(x):
                price = float(str(x).replace(",", "."))
                break
        if price is None:
            return await update.message.reply_html("Укажите цену: <code>/tac set PRICE [SYMBOL]</code>")
        box["cmd_tac_set"] = price
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(
            f"Запросил установку TAC по <b>{_hs(sym)}</b>: <code>{price:.6f}</code>"
        )

    return await update.message.reply_html("Неизвестная подкоманда. Используйте: <code>/tac set|reset</code>")

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

    # SYMBOL — последний нечисловой аргумент (если есть)
    sym = None
    if tail:
        if not all(_is_num(x) for x in tail):
            # возьмём последний нечисловой как символ
            for x in reversed(tail):
                if not _is_num(x):
                    sym = _norm_symbol(x); break
    if sym is None:
        sym = context.chat_data.get("current_symbol") or CONFIG.SYMBOL

    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})

    if sub == "show":
        box["cmd_strat_show"] = True
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(f"Запросил STRAT-отчёт по <b>{_hs(sym)}</b>.")

    if sub == "reset":
        box["cmd_strat_reset"] = True
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(f"Запросил сброс STRAT до авто-плана по <b>{_hs(sym)}</b>.")

    if sub == "set":
        # соберём до трёх цен слева направо
        prices = []
        for x in tail:
            if _is_num(x):
                prices.append(float(str(x).replace(",", ".")))
            else:
                # дальше пошли флаги/символ — останавливаем сбор
                break
        if not prices:
            return await update.message.reply_html("Укажите 1–3 цены: <code>/strat set P1 [P2 P3] [SYMBOL]</code>")
        box["cmd_strat_set"] = prices[:3]
        context.chat_data["current_symbol"] = sym
        return await update.message.reply_html(
            f"Запросил установку STRAT точек по <b>{_hs(sym)}</b>: " +
            ", ".join(f"<code>{p:.6f}</code>" for p in prices[:3])
        )

    return await update.message.reply_html("Неизвестная подкоманда. Используйте: <code>/strat show|set|reset</code>")

async def cmd_hedge_close(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтвердить закрытие прибыльной ноги хеджа и передать фактическую цену.
    Команды: /hedge_close PRICE [SYMBOL]  и  /хедж_закрытие PRICE [SYMBOL]
    """
    args = context.args or []
    if not args:
        return await update.message.reply_html("Формат: <code>/hedge_close 150.123 [SYMBOL]</code>")
    price_raw = args[0]
    sym = _norm_symbol(args[1]) if len(args) > 1 else (context.chat_data.get("current_symbol") or CONFIG.SYMBOL)
    chat_id = _chat_id(update)
    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})
    try:
        px = float(str(price_raw).replace(",", "."))
        box["hedge_close_price"] = px
        context.chat_data["current_symbol"] = sym
        await update.message.reply_html(f"✅ Принял цену закрытия хеджа по <b>{_hs(sym)}</b>: <code>{px:.6f}</code>")
    except Exception:
        box["hedge_close_price"] = None
        await update.message.reply_html(f"⚠️ Цена не распознана — сканер возьмёт рыночную. Пара: <b>{_hs(sym)}</b>.")

# --- кириллический алиас /хедж_закрытие (CommandHandler не принимает нелатиницу) ---
async def cmd_hedge_close_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "")
    parts = text.split(maxsplit=2)  # "/хедж_закрытие 1.2345 EURUSD"
    context.args = parts[1:] if len(parts) > 1 else []
    return await cmd_hedge_close(update, context)

# --- новые команды для ручного хеджа ---
async def _set_manual_open(update: Update, context: ContextTypes.DEFAULT_TYPE, side: str):
    chat_id = _chat_id(update)
    args = context.args or []
    # допускаем: /openlong [PRICE] [SYMBOL]
    price = None
    sym = None
    if args:
        # если первый аргумент — число, это PRICE
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

    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})
    # ключ с новым явным смыслом — немедленный старт через хедж
    box["cmd_force_open_now_dir"] = "LONG" if side.upper() == "LONG" else "SHORT"
    if price is not None:
        box["cmd_force_open_now_entry_px"] = float(price)
    box["force_open_now_ts"] = time.time()
    # на всякий случай снимаем ручной режим, чтобы цикл не стопорился
    box["user_manual_mode"] = False

    context.chat_data["current_symbol"] = sym
    extra = f" @ <code>{price:.6f}</code>" if price is not None else ""
    await update.message.reply_html(
        f"Ок. Запросил <b>немедленный</b> ручной старт через хедж: "
        f"<b>{side.upper()}</b> по <b>{_hs(sym)}</b>{extra}."
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
    ns = _ns_key(sym, chat_id)
    box = context.application.bot_data.setdefault(ns, {})
    box["cmd_hedge_flip"] = side
    context.chat_data["current_symbol"] = sym
    await update.message.reply_html(f"Ок. Переворот bias хеджа на <b>{side}</b> по <b>{_hs(sym)}</b> запрошен.")

async def cmd_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html("Неизвестная команда. Вот список:\n\n" + HELP_TEXT)

# ------------ сборка приложения ------------

def build_app() -> Application:
    token = _get_bot_token()
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN/BOT_TOKEN is not set")

    application = ApplicationBuilder().token(token).post_init(_post_init).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help",  cmd_help))
    application.add_handler(CommandHandler("setbank", cmd_setbank))
    application.add_handler(CommandHandler("run",   cmd_run))
    application.add_handler(CommandHandler("stop",  cmd_stop))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("open",   cmd_open))
    application.add_handler(CommandHandler("pause",  cmd_pause))
    application.add_handler(CommandHandler("close",  cmd_close))
    application.add_handler(CommandHandler("fees",   cmd_fees))
    application.add_handler(CommandHandler("hedge_close", cmd_hedge_close))
    application.add_handler(CommandHandler("strat", cmd_strat))
    application.add_handler(CommandHandler("tac",   cmd_tac))
    application.add_handler(CommandHandler("openlong",  cmd_openlong))
    application.add_handler(CommandHandler("openshort", cmd_openshort))
    application.add_handler(CommandHandler("hedge_flip", cmd_hedge_flip))
    # кириллический алиас ловим как обычный текст:
    application.add_handler(MessageHandler(filters.Regex(r"^/хедж_закрытие(?:@[\w_]+)?(?:\s|$)"), cmd_hedge_close_alias))
    # обработчик неизвестных команд — в самом конце:
    application.add_handler(MessageHandler(filters.COMMAND, cmd_unknown))

    log.info("Bot application built.")
    return application

if __name__ == "__main__":
    app = build_app()
    app.run_polling(close_loop=False, drop_pending_updates=True)
