FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# (необязательно, но полезно для TLS)
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1) сначала кладём requirements.txt
COPY requirements.txt .

# 2) затем ставим зависимости
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# 3) потом весь остальной код
COPY . .

# точка входа
CMD ["python", "-u", "main.py"]
