# debug_executor.py
import logging
from datetime import datetime, timezone

log = logging.getLogger("bot")
DEBUG_LOG_WS = None

async def log_debug_data(debug_data):
    """Записывает отладочную информацию в Google Sheet."""
    if not DEBUG_LOG_WS:
        return

    try:
        headers = DEBUG_LOG_WS.row_values(1)
        row_to_insert = [debug_data.get(header, '') for header in headers]
        DEBUG_LOG_WS.append_row(row_to_insert, value_input_option='USER_ENTERED')

    except Exception as e:
        # Чтобы не спамить в Telegram, логируем ошибку только в консоль
        log.error(f"Ошибка при записи debug-лога в Google Sheets: {e}")
