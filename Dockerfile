# Используем официальный образ Python 3.11
FROM python:3.11-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл с зависимостями
COPY requirements.txt .

# Устанавливаем зависимости из requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все остальные файлы проекта в контейнер
COPY . .

# Чтобы логи сразу шли в Railway и не буферизировались
ENV PYTHONUNBUFFERED=1

# ИСПРАВЛЕНО: Запуск вашего приложения с правильным именем файла
CMD ["python", "-u", "main.py"]
