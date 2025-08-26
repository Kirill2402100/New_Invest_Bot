# File: trade_monitor.py (v4 - REST API Sanity Check)

import asyncio
import ccxt.async_support as ccxt
from data_feeder import last_data
from trade_executor import update_trade_in_sheet

# --- Переменные, которые будут установлены из основного файла ---
state = {}
save_state_func = None
broadcast_func = None
trade_log_ws = None

# Создаем собственный экземпляр ccxt для надежных REST-запросов
exchange = ccxt.mexc({'options': {'defaultType': 'swap'}})

def init_monitor(main_state, main_save_func, main_broadcast_func, main_ws):
    """Инициализирует монитор, передавая ему нужные объекты."""
    global state, save_state_func, broadcast_func, trade_log_ws
    state = main_state
    save_state_func = main_save_func
    broadcast_func = main_broadcast_func
    trade_log_ws = main_ws

async def monitor_main_loop(app):
    print("Trade Monitor loop started (v_rest_api_check).")
    while True:
        try:
            await asyncio.sleep(5) # Проверяем цены каждые 5 секунд
            
            if not state.get('monitored_signals'):
                continue

            signals_to_remove = []
            for signal in state['monitored_signals']:
                
                # --- НОВАЯ ЛОГИКА: ПРОВЕРКА ЦЕНЫ ЧЕРЕЗ REST API ---
                try:
                    ticker = await exchange.fetch_ticker(signal['pair'])
                    current_price = ticker.get('last')
                    if not current_price:
                        print(f"Could not fetch reliable price for {signal['pair']}. Skipping check.")
                        continue
                except Exception as e:
                    print(f"Error fetching ticker for {signal['pair']}: {e}. Skipping check.")
                    continue
                # --- КОНЕЦ НОВОЙ ЛОГИКИ ---

                exit_status, exit_price = None, None
                
                # Теперь мы используем надежную, проверенную цену `current_price`
                if signal['side'] == 'LONG':
                    if current_price <= signal['sl_price']:
                        exit_status, exit_price = "SL_HIT", signal['sl_price']
                    elif current_price >= signal['tp_price']:
                        exit_status, exit_price = "TP_HIT", signal['tp_price']

                elif signal['side'] == 'SHORT':
                    if current_price >= signal['sl_price']:
                        exit_status, exit_price = "SL_HIT", signal['sl_price']
                    elif current_price <= signal['tp_price']:
                        exit_status, exit_price = "TP_HIT", signal['tp_price']

                if exit_status:
                    position_size_usd = 50
                    leverage = 100
                    price_change_percent = ((exit_price - signal['entry_price']) / signal['entry_price'])
                    if signal['side'] == 'SHORT': price_change_percent = -price_change_percent
                    pnl_percent = price_change_percent * leverage * 100
                    pnl_usd = position_size_usd * (pnl_percent / 100)
                    
                    await update_trade_in_sheet(trade_log_ws, signal, exit_status, exit_price, pnl_usd, pnl_percent)
                    
                    emoji = "✅" if pnl_usd > 0 else "❌"
                    msg = (f"{emoji} <b>СДЕЛКА ЗАКРЫТА ({exit_status})</b>\n\n"
                           f"<b>Инструмент:</b> <code>{signal['pair']}</code>\n"
                           f"<b>Результат: ${pnl_usd:+.2f} ({pnl_percent:+.2f}%)</b>")
                    await broadcast_func(app, msg)
                    
                    signals_to_remove.append(signal)

            if signals_to_remove:
                state['monitored_signals'] = [s for s in state['monitored_signals'] if s not in signals_to_remove]
                save_state_func()
                
        except asyncio.CancelledError:
            print("Trade Monitor loop cancelled.")
            break
        except Exception as e:
            print(f"Error in monitor loop: {e}")
