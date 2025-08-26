import gspread
import gspread.utils
from gspread.exceptions import APIError, GSpreadException, WorksheetNotFound
import logging
import asyncio
# ИСПРАВЛЕНО: Возвращены недостающие импорты
from datetime import datetime, timezone
from typing import Dict, List, Optional

# отдельный логгер под модуль
log = logging.getLogger("trade_executor")

TRADE_LOG_WS = None
TRADING_HEADERS_CACHE = None
PENDING_TRADES: List[List] = []
SAFE_CHAR = '⧗'

BUFFER_LOCK: asyncio.Lock | None = None

# --- Хедеры листа для BMR-DCA ---
BMR_HEADERS = [
    "Event_ID","Signal_ID","Timestamp_UTC","Pair","Side","Event",
    "Step_No","Step_Margin_USDT","Cum_Margin_USDT","Leverage",
    "Entry_Price","Avg_Price","TP_Pct","TP_Price","SL_Price","Liq_Est_Price",
    "Next_DCA_Price","Next_DCA_Label","Triggered_Label",
    "ATR_5m","ATR_1h","RSI_5m","ADX_5m","Supertrend","Vol_z",
    "Range_Lower","Range_Upper","Range_Width",
    "Fee_Rate_Maker","Fee_Rate_Taker","Fee_Est_USDT",
    "PNL_Realized_USDT","PNL_Realized_Pct","Time_In_Trade_min","Trail_Stage"
]

# --- УТИЛИТЫ/СЕРВИС ---

def get_lock() -> asyncio.Lock:
    global BUFFER_LOCK
    if BUFFER_LOCK is None:
        BUFFER_LOCK = asyncio.Lock()
    return BUFFER_LOCK

def safe_id(text: str) -> str:
    return text.replace(":", SAFE_CHAR).replace("/", SAFE_CHAR)

def clear_headers_cache():
    global TRADING_HEADERS_CACHE
    TRADING_HEADERS_CACHE = None
    log.info("Trading headers cache cleared; will be re-fetched on next access.")

def get_headers(worksheet: gspread.Worksheet):
    global TRADING_HEADERS_CACHE
    if TRADING_HEADERS_CACHE is None:
        log.info(f"Reading headers from worksheet '{worksheet.title}'...")
        try:
            all_values = worksheet.get_all_values()
            TRADING_HEADERS_CACHE = all_values[0] if all_values else []
        except GSpreadException as e:
            log.error(f"Failed to get worksheet values: {e}")
            TRADING_HEADERS_CACHE = []
    return TRADING_HEADERS_CACHE

async def _write_headers(ws: gspread.Worksheet, headers: list):
    """Пишем заголовки в строку 1 и обновляем кэш."""
    loop = asyncio.get_running_loop()
    end_a1 = gspread.utils.rowcol_to_a1(1, len(headers))
    await loop.run_in_executor(None, lambda: ws.update(f"A1:{end_a1}", [headers]))
    global TRADING_HEADERS_CACHE, TRADE_LOG_WS
    TRADE_LOG_WS = ws
    TRADING_HEADERS_CACHE = headers[:]
    log.info(f"Header row set for '{ws.title}': {len(headers)} columns.")

async def ensure_bmr_log_sheet(gfile: gspread.Spreadsheet, title: str = "BMR_DCA_Log"):
    """Гарантирует наличие листа с заголовками BMR_HEADERS и настраивает TRADE_LOG_WS."""
    loop = asyncio.get_running_loop()
    global TRADE_LOG_WS
    ws = None
    try:
        ws = await loop.run_in_executor(None, lambda: gfile.worksheet(title))
        headers = await loop.run_in_executor(None, lambda: ws.row_values(1))
        if headers != BMR_HEADERS:
            log.warning(f"Headers mismatch in '{title}'. Archiving and creating a new sheet.")
            archive_title = f"{title}_archive_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M%S')}"
            await loop.run_in_executor(None, lambda: ws.update_title(archive_title))
            ws = await loop.run_in_executor(None, lambda: gfile.add_worksheet(title=title, rows=2000, cols=len(BMR_HEADERS)))
            await loop.run_in_executor(None, lambda: ws.append_row(BMR_HEADERS, value_input_option="USER_ENTERED"))
    except WorksheetNotFound:
        log.info(f"Worksheet '{title}' not found. Creating a new one.")
        ws = await loop.run_in_executor(None, lambda: gfile.add_worksheet(title=title, rows=2000, cols=len(BMR_HEADERS)))
        await loop.run_in_executor(None, lambda: ws.append_row(BMR_HEADERS, value_input_option="USER_ENTERED"))

    TRADE_LOG_WS = ws
    clear_headers_cache()
    log.info(f"[BMR] Worksheet '{title}' is ready with correct headers.")

def _prepare_row(headers: list, data: dict) -> list:
    if 'Signal_ID' in data:
        data['Signal_ID'] = safe_id(data.get('Signal_ID', ''))
    return [data.get(h, '') for h in headers]

async def bmr_log_event(data: dict):
    if not TRADE_LOG_WS:
        log.warning("bmr_log_event called but TRADE_LOG_WS is not set.")
        return
    try:
        headers = get_headers(TRADE_LOG_WS)
        if not headers or headers != BMR_HEADERS:
            log.warning("Headers in the sheet are missing or incorrect. Attempting to fix.")
            await _write_headers(TRADE_LOG_WS, BMR_HEADERS)
            headers = BMR_HEADERS
        row = _prepare_row(headers, data)
        async with get_lock():
            PENDING_TRADES.append(row)
        log.info(f"[BMR] Buffered event {data.get('Event')} for {safe_id(data.get('Signal_ID',''))}")
    except Exception as e:
        log.error(f"[BMR] Error buffering event: {e}", exc_info=True)

async def flush_log_buffers():
    loop = asyncio.get_running_loop()
    while True:
        chunk_to_flush = []
        async with get_lock():
            if not PENDING_TRADES:
                return
            chunk_to_flush = PENDING_TRADES[:40]
            del PENDING_TRADES[:40]
        if not chunk_to_flush:
            return
        log.info(f"Flushing {len(chunk_to_flush)} row(s) to Google Sheets...")
        try:
            await loop.run_in_executor(
                None,
                lambda: TRADE_LOG_WS.append_rows(chunk_to_flush, value_input_option='USER_ENTERED')
            )
            log.info("Flush OK.")
        except Exception as e:
            log.error(f"Flush failed, returning chunk to buffer: {e}")
            async with get_lock():
                PENDING_TRADES[:0] = chunk_to_flush
            return
