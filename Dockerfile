# 1. Используем официальный образ Python 3.10
FROM python:3.10-slim

# 2. Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# 3. Добавляем 'echo' чтобы гарантированно сбросить кеш сборки
RUN echo "Forcing a clean build to install new ccxt version - Step 1"

# 4. Обновляем pip и принудительно устанавливаем свежую версию ccxt
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir "ccxt>=4.3.51"

# 5. Копируем файл с остальными зависимостями
COPY requirements.txt .

# 6. Устанавливаем остальные зависимости
RUN pip install --no-cache-dir -r requirements.txt

# 7. Копируем весь остальной код (вашего бота) в контейнер
COPY . .

# 8. Указываем команду для запуска бота при старте контейнера
#    ВАЖНО: Замените 'bot.py' на реальное имя вашего Python-файла
CMD ["python", "bot.py"]
