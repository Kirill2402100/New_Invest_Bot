#!/usr/bin/env python3
import os
import time
from math import erf, sqrt
from statistics import mean
from datetime import datetime, timedelta, timezone
import requests

# ============ Конфигурация ============
PAIR         = os.getenv("PAIR", "EURC-USDC")
GRANULARITY  = int(os.getenv("GRANULARITY", "900"))     # 15 мин
ATR_WINDOWS  = int(os.getenv("ATR_WINDOWS", "48"))      # 12 ч
HORIZON_HRS  = float(os.getenv("HORIZON_HRS", "6"))
APY_K        = float(os.getenv("APY_CONSTANT", "0.15"))
P_HIGH       = float(os.getenv("P_HIGH", "0.25"))
P_MED        = float(os.getenv("P_MED",  "0.10"))
D_FLAT       = 0.10  # %

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    raise SystemExit("[env] BOT_TOKEN и/или CHAT_ID не заданы – остановка.")

COINBASE_API = "https://api.exchange.coinbase.com"

# ============ Математика ============
def cdf_standard_normal(x: float) -> float:
    return 0.5 * (1 + erf(x / sqrt(2)))

def exit_probability(d_pct: float, sigma_pct: float, horizon_h: float) -> float:
    if sigma_pct == 0:
        return 0.0
    z = d_pct / (sigma_pct * sqrt(horizon_h / 24))
    return 2 * (1 - cdf_standard_normal(z))

def expected_apy(width_pct: float) -> float:
    return APY_K / (width_pct / 100)

# ============ Данные ============
def fetch_candles(pair: str, granularity: int, window: int):
    end = datetime.now(timezone.utc)
    start = end - timedelta(seconds=granularity * (window + 20))
    url = f"{COINBASE_API}/products/{pair}/candles"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": granularity
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return sorted(r.json(), key=lambda x: x[0])

def true_range(cur, prev_close):
    high, low, close = cur[2], cur[1], cur[4]
    return max(high - low, abs(high - prev_close), abs(low - prev_close))

def compute_atr(candles, window):
    if len(candles) < window + 1:
        raise ValueError(f"Not enough candles: {len(candles)} (need {window+1})")
    trs = [true_range(candles[-i], candles[-i - 1][4]) for i in range(1, window + 1)]
    return mean(trs)

# ============ Telegram ============
def escape_md(text: str) -> str:
    """Экранирует символы под MarkdownV2"""
    for c in r"_*[]()~`>#+-=|{}.!":
        text = text.replace(c, f"\\{c}")
    return text

def tg_send(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": escape_md(text),
        "parse_mode": "MarkdownV2"
    }
    r = requests.post(url, json=payload, timeout=10)
    print("[tg]", r.status_code, r.text[:100])
    r.raise_for_status()

# ============ Основной цикл ============
def main():
    last_candle_ts = 0
    last_range_code = None
    last_sent_ts = 0
    STATUS_EVERY_N_CANDLES = 4  # каждые 4 свечи (~1 ч при 15 мин)

    print("[info] lp_alert_bot.py started")

    while True:
        try:
            candles = fetch_candles(PAIR, GRANULARITY, ATR_WINDOWS)
            print("[debug] candles received:", len(candles))

            atr_raw = compute_atr(candles, ATR_WINDOWS)
            close = candles[-1][4]
            sigma_pct = atr_raw / close * 100
            p_exit = exit_probability(D_FLAT, sigma_pct, HORIZON_HRS)

            # Решение
            if sigma_pct > 0.50 or p_exit >= 0.60:
                range_code = "exit"
                msg = (
                    f"🚨 Высокий риск!\nσ24h = {sigma_pct:.2f}%\nP_exit = {p_exit*100:.1f}%\n"
                    "→ Вывести ликвидность или захеджироваться."
                )
            elif p_exit >= P_HIGH:
                range_code = "wide"
                width_pct = 0.30
                msg = (
                    f"⚠️ σ24h = {sigma_pct:.2f}%\nP_exit = {p_exit*100:.1f}%\n"
                    f"→ диапазон ±0.30 %  (≈{expected_apy(width_pct):.0f}% APY)"
                )
            elif p_exit >= P_MED:
                range_code = "medium"
                width_pct = 0.17
                msg = (
                    f"σ24h = {sigma_pct:.2f}%\nP_exit = {p_exit*100:.1f}%\n"
                    f"→ диапазон ±0.17 %  (≈{expected_apy(width_pct):.0f}% APY)"
                )
            else:
                range_code = "narrow"
                width_pct = 0.10
                msg = (
                    f"σ24h = {sigma_pct:.2f}%\nP_exit = {p_exit*100:.1f}% (спокойно)\n"
                    f"→ держим ±0.10 %  (≈{expected_apy(width_pct):.0f}% APY)"
                )

            candle_ts = candles[-1][0]
            now_ts = time.time()

            time_trigger = now_ts - last_sent_ts > GRANULARITY * STATUS_EVERY_N_CANDLES
            state_changed = range_code != last_range_code
            new_candle = candle_ts != last_candle_ts

            if new_candle and (state_changed or time_trigger):
                ts_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
                tg_send(f"{msg}\n`{ts_str}`")
                print("[sent]", msg)
                last_sent_ts = now_ts
                last_range_code = range_code
                last_candle_ts = candle_ts

        except Exception as exc:
            print("[error]", exc)

        time.sleep(GRANULARITY)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopped by user.")
