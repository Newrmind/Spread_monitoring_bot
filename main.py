import time
import threading
import sys

from Config import Config  # Файл конфигурации
import time_functions
from Data_request import download_candles
from Trade_logic import data_analysis, trading
from Database.postgres_sql import Database
from Telegram_bot.aiogram_main import start_bot
from Telegram_bot.send_message import TelegramSendMessage
from logger import clear_log_file, log_function_info


def check_time_analyze(db, data_analyze):
    """Проверяет время последнего анализа данных"""
    try:
        while True:
            last_data_analyze = int(db.get_table_from_db("SELECT timestamp_msk FROM requests_time \
                                     WHERE request = 'bollinger_bands_calculate'").loc[0, 'timestamp_msk'])
            need_analyze = time_functions.has_passed_any_hours(timestamp=last_data_analyze, hours=Config.analyze_period)
            is_weekend_or_night = time_functions.is_weekend_or_night_msk_timezone()
            if is_weekend_or_night:
                print("[INFO] Сейчас выходной или время с 00:00 до 10:00 по МСК.")
                time.sleep(60)
                continue

            print(f'\nВремя последнего анализа {time_functions.timestamp_to_moscow_time(last_data_analyze)}. Текущее время {time_functions.time_now()[0]}.\n'
                  f'Need_analize = {need_analyze}\n')

            if need_analyze and not is_weekend_or_night:
                info_message = '[INFO] Запуск функции обновления свечных данных.'
                log_function_info('Запуск функции обновления свечных данных.')
                print(info_message)

                download_candles.stocks_candles_data()
                data_analyze.concat()
                data_analyze.calculate_spreads()
                data_analyze.bollinger_bands()

                clear_log_file()  # Очистить файл логов

            time.sleep(30)
    except Exception as ex:
        print(f"В функции check_time_analyze произошла ошибка: {ex}")
        sys.exit(1)

def main(trading):

    try:
        cycle = 0
        while True:
            print(f'Цикл {cycle} \n')
            cycle += 1

            is_weekend_or_night = time_functions.is_weekend_or_night_msk_timezone()
            if is_weekend_or_night:
                print("[INFO] Сейчас выходной или время с 00:00 до 10:00 по МСК.")
                time.sleep(60)
                continue

            trading.find_idea()
            time.sleep(1)

    except Exception as ex:
        print(f"В функции main произошла ошибка: {ex}")
        sys.exit(1)


if __name__ == '__main__':
    db = Database()
    data_analyze = data_analysis.DataAnalysis()
    trading = trading.Trading()

    tg_send = TelegramSendMessage()
    tg_admin_id = int(db.get_table_from_db("SELECT value FROM params WHERE param = 'tg_admin_id'")['value'].iloc[0])

    # Загрузка и подготовка данных
    thr_check_time_analyze = threading.Thread(target=check_time_analyze, name='Check_time_analyze', args=(db, data_analyze,), daemon=True)
    thr_check_time_analyze.start()

    # Трейдинг
    thr_trade = threading.Thread(target=main, name='Trading_functions', args=(trading,), daemon=True)
    thr_trade.start()

    # tg_bot
    thr_tg_bot = threading.Thread(target=start_bot(), name='Telegram_bot', daemon=True)
    thr_tg_bot.start()

    # Список потоков
    threads = [thr_check_time_analyze, thr_trade, thr_tg_bot]

    # Проверка запущенных потоков
    while True:
        # Получение списка активных потоков
        active_threads = threading.enumerate()
        print('\nСписок активных потоков:')
        for thread in active_threads:
            print(thread.name)

        # Проверка состояния потоков и обработка ошибок
        for thread in threads:
            if not thread.is_alive():
                message = f'Поток {thread.name} завершился с ошибкой или был прерван.'
                print(message)
                tg_send.send_text_message(message, tg_admin_id)

        # Проверка состояния потоков и обработка ошибок
        for thread in threads:
            if not thread.is_alive():
                message = f'Поток {thread.name} завершился с ошибкой или был прерван.'
                print(message)
                sys.exit(1)

        time.sleep(15)

