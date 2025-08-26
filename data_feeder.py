# File: data_feeder.py

import asyncio
import ccxt.pro as ccxtpro

# --- РАСШИРЕННЫЙ СПИСОК АКТИВОВ ---
SYMBOLS = [
    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 'XRP/USDT:USDT',
    'DOGE/USDT:USDT', 'WLD/USDT:USDT', 'PEPE/USDT:USDT'
]
# --- Глобальные переменные ---
is_running = False
last_data = {symbol: {} for symbol in SYMBOLS}

# --- Циклы-обработчики (без изменений) ---

async def single_trade_loop(exchange, symbol):
    while is_running:
        try:
            trades = await exchange.watch_trades(symbol)
            for trade in trades:
                last_data[symbol]['last_price'] = trade['price']
                last_data[symbol]['last_side'] = trade['side']
        except Exception as e:
            print(f"Error in trade loop for {symbol}: {e}")
            last_data[symbol]['error'] = 'TradeFeed Down'
            await asyncio.sleep(10)

async def single_orderbook_loop(exchange, symbol):
    while is_running:
        try:
            orderbook = await exchange.watch_order_book(symbol, limit=20)
            last_data[symbol]['bids'] = orderbook['bids']
            last_data[symbol]['asks'] = orderbook['asks']
            if 'error' in last_data[symbol]:
                del last_data[symbol]['error']
        except Exception as e:
            print(f"Error in order book loop for {symbol}: {e}")
            last_data[symbol]['error'] = 'BookFeed Down'
            await asyncio.sleep(10)

async def telegram_reporter_loop(app, chat_ids):
    print("Telegram reporter loop started.")
    while is_running:
        await asyncio.sleep(60) # Увеличили интервал до 1 минуты
        active_symbols = [s for s, d in last_data.items() if not d.get('error')]
        print(f"Reporter check: Data feeder is active for {len(active_symbols)}/{len(SYMBOLS)} symbols.")

# --- Управляющие функции ---

# Замените эту функцию в файле data_feeder.py

async def data_feed_main_loop(app, chat_ids):
    global is_running
    is_running = True
    print("Data Feeder main loop initiated. Staggering individual watchers...")

    exchange = ccxtpro.mexc({'options': {'defaultType': 'swap'}})
    
    tasks = []
    # --- Увеличиваем паузу между подключениями ---
    for symbol in SYMBOLS:
        tasks.append(single_trade_loop(exchange, symbol))
        tasks.append(single_orderbook_loop(exchange, symbol))
        print(f"Initiated watchers for {symbol}")
        await asyncio.sleep(2) # УВЕЛИЧЕННАЯ ПАУЗА
    
    tasks.append(telegram_reporter_loop(app, chat_ids))
    print("All watchers initiated. Running...")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("Data feed main loop cancelled.")
    finally:
        await exchange.close()
        print("Exchange connection closed.")
        
def stop_data_feed():
    global is_running
    is_running = False
    print("Stop command received for data feeder.")
