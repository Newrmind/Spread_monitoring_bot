import logging
import sys
import inspect
from Telegram_bot.send_message import TelegramSendMessage
from Database import postgres_sql


# Создаем форматтер для логов с указанием имени функции
log_format = '%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'
logging.basicConfig(filename='app.log', level=logging.INFO, format=log_format, encoding='utf-8')
logger = logging.getLogger(__name__)

tg_send_log = TelegramSendMessage()
db = postgres_sql.Database()
tg_admin_id = int(db.get_table_from_db("SELECT value FROM params WHERE param = 'tg_admin_id'")['value'].iloc[0])


def log_function_info(message):
    current_function = inspect.currentframe().f_back.f_code.co_name
    logger.info(f"[{current_function}] - {message}")

def error_inf(exception_message):
    """Записывает ошибку в логи"""
    current_function = inspect.currentframe().f_back.f_code.co_name
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file_name = exc_tb.tb_frame.f_code.co_filename
    line_number = exc_tb.tb_lineno
    error_message = f"Error in module '{file_name}', function: [{current_function}] line {line_number}: {exception_message}"

    # Логгируем сообщение с информацией о модуле и номере строки
    logger.error(error_message)

    # Отправляем сообщение об ошибке в тг
    tg_send_log.send_text_message(error_message, chat_id=tg_admin_id)


def clear_log_file():
    log_file = 'app.log'
    try:
        # Чтение содержимого файла логов
        with open(log_file, 'r') as file:
            lines = file.readlines()

        # Если в файле больше 10000 строк, оставляем только последние 3000 строк
        if len(lines) > 10000:
            lines = lines[-3000:]

            # Запись обновленных строк в файл
            with open(log_file, 'w') as file:
                file.writelines(lines)

            message = "Log file was cleared!"
            logger.info(message)
            # tg_send_log.send_text_message(message, chat_id=tg_admin_id)

    except Exception as e:
        logger.error(f"Error clearing log file: {e}")

