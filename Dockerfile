# 1. Используем официальный образ Python 3.10
FROM python:3.10-slim

# 2. Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# 3. Обновляем pip и принудительно устанавливаем свежую версию ccxt
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir "ccxt>=4.3.51"

# 4. Копируем файл с остальными зависимостями
COPY requirements.txt .

# 5. Устанавливаем остальные зависимости (ccxt будет пропущен, т.к. уже установлен)
RUN pip install --no-cache-dir -r requirements.txt

# 6. Копируем весь остальной код (вашего бота) в контейнер
COPY . .

# 7. Указываем команду для запуска бота при старте контейнера
#    ВАЖНО: Замените 'bot.py' на реальное имя вашего Python-файла
CMD ["python", "bot.py"]
