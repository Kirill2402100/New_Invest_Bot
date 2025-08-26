# state_utils.py
import os
import json
import logging
from telegram.ext import Application

log = logging.getLogger("bot")
STATE_FILE = "bot_state.json"

def load_state(app: Application):
    bot_data = app.bot_data
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                loaded_state = json.load(f)
                bot_data.update(loaded_state)
        except json.JSONDecodeError:
            log.error("Ошибка чтения bot_state.json, использую значения по умолчанию.")

    bot_data.setdefault("bot_on", False)
    bot_data.setdefault("monitored_signals", [])
    bot_data.setdefault("deposit", 50)
    bot_data.setdefault("leverage", 100)
    bot_data.setdefault("debug_mode_on", False)
    bot_data.setdefault("last_debug_code", "")
    bot_data.setdefault("stable_walls", {})
    log.info("State loaded into bot_data. Active signals: %d. Deposit: %s, Leverage: %s",
             len(bot_data.get("monitored_signals", [])), bot_data.get('deposit'), bot_data.get('leverage'))

def save_state(app: Application):
    try:
        with open(STATE_FILE, "w") as f:
            data_to_save = app.bot_data.copy()
            if 'chat_ids' in data_to_save and isinstance(data_to_save['chat_ids'], set):
                data_to_save['chat_ids'] = list(data_to_save['chat_ids'])
            json.dump(data_to_save, f, indent=2)
    except Exception as e:
        log.error(f"Failed to save state: {e}")
