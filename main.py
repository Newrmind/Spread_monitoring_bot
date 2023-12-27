import time
import threading
import sys

from Config import Config  # Файл конфигурации
import time_functions
from Data_request import download_candles
from Trade_logic import data_analysis, trading
from Database.postgres_sql import Database
from Telegram_bot.aiogram_main import start_bot


def check_time_analyze(db, data_analyze):
    """Проверяет время последнего анализа данных"""

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
            print('[INFO] Запуск функции обновления свечных данных.')

            download_candles.stocks_candles_data()
            data_analyze.concat()
            data_analyze.calculate_spreads()
            data_analyze.bollinger_bands()

        time.sleep(30)

def main(trading):

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


if __name__ == '__main__':
    db = Database()
    data_analyze = data_analysis.DataAnalysis()
    trading = trading.Trading()

    # Загрузка и подготовка данных
    thr_check_time_analyze = threading.Thread(target=check_time_analyze, args=(db, data_analyze,))
    thr_check_time_analyze.start()

    # Трейдинг
    thr_main = threading.Thread(target=main, args=(trading,))
    thr_main.start()

    # tg_bot
    thr_tg_bot = threading.Thread(target=start_bot())
    thr_tg_bot.start()

    # Список потоков
    threads = [thr_check_time_analyze, thr_main, thr_tg_bot]

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
                print(f'Поток {thread.name} завершился с ошибкой или был прерван.')

                # Выход с ошибкой (например, код 1)
                sys.exit(1)

        time.sleep(15)
        print()

