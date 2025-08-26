# train_model.py (v2.0, Multi-Class)
import pandas as pd
import pandas_ta as ta
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from xgboost import XGBClassifier
import numpy as np
import os

print("Шаг 1: Загрузка исторических данных...")
DATA_FILE = 'data/sol_1m_data_binance.csv'
if not os.path.exists(DATA_FILE):
    print(f"Ошибка: Файл '{DATA_FILE}' не найден.")
    exit()
df = pd.read_csv(DATA_FILE)

print("Шаг 2: Расчет индикаторов (фичей)...")
df.ta.rsi(length=14, append=True)
df.ta.stoch(k=14, d=3, smooth_k=3, append=True)
df.ta.ema(length=50, append=True)
df.ta.ema(length=200, append=True)
df.dropna(inplace=True)
df.reset_index(drop=True, inplace=True)

print("Шаг 3: Разметка данных (LONG, SHORT, SL)...")
def create_multiclass_labels(df_to_label, look_forward_period=30, tp_pct=0.01, sl_pct=0.005):
    df_to_label = df_to_label.copy()
    
    # Расчет будущих цен
    future_highs = df_to_label['high'].rolling(window=look_forward_period).max().shift(-look_forward_period)
    future_lows = df_to_label['low'].rolling(window=look_forward_period).min().shift(-look_forward_period)
    
    # --- Цели для LONG ---
    long_tp_price = df_to_label['close'] * (1 + tp_pct)
    long_sl_price = df_to_label['close'] * (1 - sl_pct)
    long_tp_hit = future_highs >= long_tp_price
    long_sl_hit = future_lows <= long_sl_price
    
    # --- Цели для SHORT ---
    short_tp_price = df_to_label['close'] * (1 - tp_pct)
    short_sl_price = df_to_label['close'] * (1 + sl_pct)
    short_tp_hit = future_lows <= short_tp_price
    short_sl_hit = future_highs >= short_sl_price

    # --- Создание итоговой метки ---
    # 0 = SL, 1 = LONG TP, 2 = SHORT TP
    conditions = [
        long_tp_hit & ~long_sl_hit,   # Только LONG TP
        short_tp_hit & ~short_sl_hit, # Только SHORT TP
        long_sl_hit,                  # LONG SL (имеет приоритет над SHORT TP)
        short_sl_hit,                 # SHORT SL
    ]
    choices = [1, 2, 0, 0]
    df_to_label['target'] = np.select(conditions, choices, default=0) # По умолчанию 0 (ничего не произошло)

    df_to_label.dropna(subset=['target'], inplace=True)
    return df_to_label

labeled_df = create_multiclass_labels(df)
print(f"Соотношение классов после разметки:\n{labeled_df['target'].value_counts(normalize=True)}")

print("Шаг 4: Обучение мультиклассовой модели...")
features = ['RSI_14', 'STOCHk_14_3_3', 'EMA_50', 'EMA_200', 'close', 'volume']
X = labeled_df[features]
y = labeled_df['target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Используем objective 'multi:softprob' для нескольких классов
model = XGBClassifier(objective='multi:softprob', num_class=3, use_label_encoder=False, eval_metric='mlogloss')
model.fit(X_train, y_train)

print("Шаг 5: Оценка качества модели...")
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred, target_names=['SL/Nothing', 'LONG Win', 'SHORT Win']))

print("Шаг 6: Сохранение модели...")
# Сохраняем модель в формате JSON для лучшей совместимости
model.save_model('trading_model.json')
print("\n✅ Готово! Модель обучена и сохранена в файл 'trading_model.json'.")
