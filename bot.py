
import nest_asyncio
nest_asyncio.apply()
import logging
import os
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Привет! Отправь мне два параметра через пробел. Например:\n"
        "100000 2197\n"
        "Я выполню расчёты и пришлю результат."
    )

# Обработчик входящих текстовых сообщений
async def calculate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        text = update.message.text.strip()
        params = text.split()
        if len(params) != 2:
            await update.message.reply_text("Пожалуйста, отправьте ровно два числа, разделённые пробелом.")
            return

        invest = float(params[0])
        cp = float(params[1])
        
        # Пример расчётов по вашим формулам:
        short = invest * 0.02
        aave = (invest - short) / (1 + 0.75 * 0.147)
        pool_eth = (aave * 0.64) / cp
        pool_usdc = invest - (aave + short)
        range_min = cp * (1 - 0.03)
        range_max = cp * (1 + 0.20)
        close_lp_down = range_min
        close_lp_up = cp * (1 + 0.15)
        
        response = (
            f"Результаты расчётов:\n"
            f"SHORT = {short:.2f}\n"
            f"AAVE = {aave:.2f}\n"
            f"POOL ETH = {pool_eth:.2f}\n"
            f"POOL USDC = {pool_usdc:.2f}\n"
            f"RANGE MIN = {range_min:.2f}\n"
            f"RANGE MAX = {range_max:.2f}\n"
            f"CLOSE LP DOWN = {close_lp_down:.2f}\n"
            f"CLOSE LP UP = {close_lp_up:.2f}"
        )
        
        await update.message.reply_text(response)
    except Exception as e:
        logger.error("Ошибка в расчётах: %s", e)
        await update.message.reply_text("Произошла ошибка при обработке вашего запроса. Убедитесь, что вводите два числа.")

async def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("Токен бота не задан в переменной окружения TELEGRAM_BOT_TOKEN")
        return
    
    # Создаем приложение (Application) для бота
    app = ApplicationBuilder().token(token).build()
    
    # Добавляем обработчики
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, calculate))
    
    # Запускаем бота (polling)
    await app.run_polling()

if __name__ == '__main__':
    import nest_asyncio
    nest_asyncio.apply()
    import asyncio
    asyncio.run(main())