# Lp_alert_bot.py
"""
Telegram-бот для концентрированного LP (EURC-USDC)
---------------------------------------------------

* Каждые 30 минут тянет 30-мин свечи EURC-USD с Coinbase.
* Считает 24-часовой ATR (48 свечей) и вероятность выхода из узкого диапазона ±0,10 % в ближайшие 6 часов.
* В зависимости от вероятности (P_exit) выдаёт три рекомендации:
    – «узкий»   ±0,10 %  ≈150 % APY  (риск <10 %)
    – «средний» ±0,17 %  ≈90 %  APY  (10–25 %)
    – «широкий» ±0,30 %  ≈50 %  APY  (≥25 %)
  или полностью выйти/захеджироваться при σ >0,50 % или P_exit >60 %.
* Шлёт сообщения в Telegram.

Развёртывание на Railway
=========================
1. Добавьте этот файл в репозиторий вашего Railway-проекта.
2. В *Variables* задайте минимум:

   BOT_TOKEN       – токен из @BotFather
   CHAT_ID         – id лички или группы (можно узнать через getUpdates)

   (опционально)
   PAIR            – "EURC-USD"
   GRANULARITY     – 1800  # секунд
   HORIZON_HRS     – 6
   ATR_WINDOWS     – 48
   APY_CONSTANT    – 0.15  # даёт 150 % при ±0,10 %
3. Type = Python, Start Cmd = `python lp_alert_bot.py`

Готово – Railway перезапустит контейнер и бот начнёт слать алёрты.
"""

import os
import time
from math import erf, sqrt
from statistics import mean
from datetime import datetime, timezone

import requests

###############################################################################
#  Настройки из переменных окружения
###############################################################################
PAIR         = os.getenv("PAIR", "EURC-USD")
GRANULARITY  = int(os.getenv("GRANULARITY", "1800"))   # 30-мин свечи
ATR_WINDOWS  = int(os.getenv("ATR_WINDOWS", "48"))      # 24 ч
HORIZON_HRS  = float(os.getenv("HORIZON_HRS", "6"))     # прогнозное окно
APY_K        = float(os.getenv("APY_CONSTANT", "0.15"))  # константа APY≈K/width

# Пороги вероятности
P_HIGH = float(os.getenv("P_HIGH", "0.25"))   # ≥25 % → ±0,30 %
P_MED  = float(os.getenv("P_MED",  "0.10"))   # 10–25 % → ±0,17 %

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    raise SystemExit("[env] BOT_TOKEN и/или CHAT_ID не заданы – остановка.")

###############################################################################
COINBASE_API = "https://api.exchange.coinbase.com"
D_FLAT = 0.10  # % ширина узкого диапазона

###############################################################################
#  Базовые функции
###############################################################################

def cdf_standard_normal(x: float) -> float:
    """N(0,1) CDF via error function"""
    return 0.5 * (1 + erf(x / sqrt(2)))

def exit_probability(d_pct: float, sigma_pct: float, horizon_h: float) -> float:
    """P(|Δp|>d) за horizon_h, предполагая геом. броуновское движение"""
    if sigma_pct == 0:
        return 0.0
    z = d_pct / (sigma_pct * sqrt(horizon_h / 24))
    return 2 * (1 - cdf_standard_normal(z))

def expected_apy(width_pct: float) -> float:
    return APY_K / (width_pct / 100)

###############################################################################
#  Функции работы с данными
###############################################################################

def fetch_candles(pair: str, granularity: int, limit: int = 300):
    """Возвращает список [time, low, high, open, close, volume] (ст.→нв.)"""
    url = f"{COINBASE_API}/products/{pair}/candles"
    params = {"granularity": granularity, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return sorted(r.json(), key=lambda x: x[0])


def true_range(cur, prev_close):
    high, low, close = cur[2], cur[1], cur[4]
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def compute_atr(candles, window):
    if len(candles) < window + 1:
        raise ValueError("Not enough candles for ATR calculation")
    trs = [true_range(candles[-i], candles[-i - 1][4]) for i in range(1, window + 1)]
    return mean(trs)

###############################################################################
#  Telegram
###############################################################################

def tg_send(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()

###############################################################################
#  Основной цикл
###############################################################################

def main():
    last_candle_ts = 0
    while True:
        try:
            candles = fetch_candles(PAIR, GRANULARITY, ATR_WINDOWS + 2)
            atr_raw = compute_atr(candles, ATR_WINDOWS)
            close = candles[-1][4]
            sigma_pct = atr_raw / close * 100  # реализ. σ в процентах
            p_exit = exit_probability(D_FLAT, sigma_pct, HORIZON_HRS)

            # Решение по диапазону
            if sigma_pct > 0.50 or p_exit >= 0.60:
                # критическая турбулентность
                width_pct = None
                msg = (
                    f"🚨 *Высокий риск!* σ24h={sigma_pct:.2f}%  P_exit={p_exit*100:.1f}%\n"
                    "Предлагаю полностью вывести ликвидность или 100 %-хедж." )
            elif p_exit >= P_HIGH:
                width_pct = 0.30
                msg = (
                    f"⚠️ σ24h={sigma_pct:.2f}%  P_exit={p_exit*100:.1f}%\n"
                    f"→ диапазон ±0.30 %  (≈{expected_apy(width_pct):.0f}% APY)" )
            elif p_exit >= P_MED:
                width_pct = 0.17
                msg = (
                    f"σ24h={sigma_pct:.2f}%  P_exit={p_exit*100:.1f}%\n"
                    f"→ диапазон ±0.17 %  (≈{expected_apy(width_pct):.0f}% APY)" )
            else:
                width_pct = 0.10
                msg = (
                    f"σ24h={sigma_pct:.2f}%  P_exit={p_exit*100:.1f}% (спокойно)\n"
                    f"→ держим ±0.10 %  (≈{expected_apy(width_pct):.0f}% APY)" )

            candle_ts = candles[-1][0]
            if candle_ts != last_candle_ts:
                ts_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
                tg_send(f"{msg}\n`{ts_str}`")
                last_candle_ts = candle_ts
                print("[sent]", msg)

        except Exception as exc:
            print("[error]", exc)

        time.sleep(GRANULARITY)

###############################################################################
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped by user.")
