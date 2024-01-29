import datetime
import time

from Database import postgres_sql
import time_functions
from Trade_logic import secondary_functions
import sys
import threading




x = 1
y = 1

def one():
    try:
        while True:
            print('one')
            time.sleep(3)
    except Exception as e:
        print(f"Ошибка в потоке one: {e}")
        sys.exit(1)

def two():
    try:
        while True:
            print('two')
            time.sleep(1)
            x = 1/0  # Пример ошибки
            print(x)
    except Exception as e:
        print(f"Ошибка в потоке two: {e}")
        sys.exit(1)

if __name__ == '__main__':
    # Загрузка и подготовка данных
    thr_one = threading.Thread(target=one, name='Поток_один', daemon=True)
    thr_one.start()

    thr_two = threading.Thread(target=two, name='Поток_два', daemon=True)
    thr_two.start()

    # Список потоков
    threads = [thr_one, thr_two]

    # Проверка запущенных потоков
    while True:
        # Получение списка активных потоков
        active_threads = threading.enumerate()
        print('\nСписок активных потоков:')
        for thread in active_threads:
            print(thread.name)
            time.sleep(3)

        # Проверка состояния потоков и обработка ошибок
        for thread in threads:
            if not thread.is_alive():
                message = f'Поток {thread.name} завершился с ошибкой или был прерван.'
                print(message)
                sys.exit(1)


