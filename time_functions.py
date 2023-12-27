import datetime
import time
import pandas as pd
import pytz


def timestamp_to_msk(df, column_name):
    """Конвертирует timestamp в секундах в unix по МСК"""

    df[column_name] = pd.to_datetime(df[column_name], unit='s')  # Преобразуем timestamp в unix UTC
    df[column_name] = df[column_name].dt.tz_localize('utc')  # Добавляем временную зону
    df[column_name] = df[column_name].dt.tz_convert('Europe/Moscow')  # Меняем временную зону на МСК
    df[column_name] = df[column_name].dt.tz_localize(None)  # Удаляем временную зону
    return df

def time_now():
    # Получение текущего времени по МСК
    current_time = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
    formatted_datetime_moscow = current_time.strftime("%d.%m.%Y %H:%M:%S")

    # Преобразование времени в значение timestamp с миллисекундами
    timestamp = int(current_time.timestamp())
    return formatted_datetime_moscow, timestamp

def request_time_change(db, request: str):
    """Обновляет время запросов в таблице requests_time"""
    requests_time = db.get_table_from_db('SELECT * FROM requests_time')

    # Получаем текущее время в Москве
    moscow_time = datetime.datetime.now(pytz.timezone('Europe/Moscow'))

    # Обновляем время и timestamp в таблице requests_time
    requests_time.loc[requests_time['request'] == f'{request}', 'time_msk'] = moscow_time.strftime("%d.%m.%Y %H:%M:%S")
    requests_time.loc[requests_time['request'] == f'{request}', 'timestamp_msk'] = int(moscow_time.timestamp())

    # Обновляем таблицу в базе данных
    db.add_table_to_db(requests_time, table_name='requests_time', if_exists='replace')

def calculate_execution_time(func):
    """Функция выполняет измерение времени перед выполнением другой функции и выводит результат"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Время выполнения функции {func.__name__}: {execution_time:.6f} секунд")
        return result
    return wrapper


def timestamp_to_moscow_time(timestamp):
    # Создаем объект datetime с учетом временной метки
    dt_utc = datetime.datetime.utcfromtimestamp(timestamp)

    # Устанавливаем временную зону UTC
    dt_utc = pytz.utc.localize(dt_utc)

    # Переводим в московскую временную зону
    moscow_tz = pytz.timezone('Europe/Moscow')
    dt_moscow = dt_utc.astimezone(moscow_tz)

    # Форматируем время в 24-часовом формате
    moscow_time = dt_moscow.strftime("%d.%m.%Y %H:%M:%S")

    return moscow_time


def round_time(interval: str) -> int:
    """Округление текущего времени в меньшую сторону для каждого таймфрейма"""
    timestamp = time.time()
    rounded_timestamp = timestamp
    print(rounded_timestamp)

    if interval == '5m':
        interval_5min = 5 * 60  # 5 minutes in seconds
        # Округляем в меньшую сторону до 5 минут
        rounded_timestamp -= timestamp % interval_5min

    elif interval == '15m':
        interval_15min = 15 * 60  # 15 minutes in seconds
        # Округляем в меньшую сторону до 15 минут
        rounded_timestamp -= timestamp % interval_15min

    elif interval == '1h':
        interval_1hr = 60 * 60  # 1 час в секундах
        # Округляем в меньшую сторону до 1 часа
        rounded_timestamp -= timestamp % interval_1hr

    else:
        print('Error!')

    return int(rounded_timestamp)  # конвертируем в милисекунды

# округление времени первой свечи в большую сторону для каждого таймфрейма
def round_up_time(timestamp: int, interval: str) -> int:
    # Преобразование timestamp в формат datetime
    dt = datetime.datetime.fromtimestamp(timestamp / 1000)

    # Округление времени до ближайшего момента времени, кратного 5 минутам в большую сторону
    if interval == '5m':
        rounded_time = (dt + datetime.timedelta(minutes=5)).replace(second=0, microsecond=0)
        rounded_time = rounded_time + datetime.timedelta(minutes=-(rounded_time.minute % 5))
        # Преобразование округленных временных меток в формат timestamp в миллисекундах
        rounded_time = int(rounded_time.timestamp() * 1000)

    # Округление времени до ближайшего момента времени, кратного 15 минутам в большую сторону
    elif interval == '15m':
        rounded_time = (dt + datetime.timedelta(minutes=15)).replace(second=0, microsecond=0)
        rounded_time = rounded_time + datetime.timedelta(minutes=-(rounded_time.minute % 15))
        # Преобразование округленных временных меток в формат timestamp в миллисекундах
        rounded_time = int(rounded_time.timestamp() * 1000)

    # Округление времени до ближайшего момента времени, кратного 1 часу в большую сторону
    elif interval == '1h':
        rounded_time = (dt + datetime.timedelta(hours=1)).replace(second=0, microsecond=0)
        rounded_time = rounded_time + datetime.timedelta(minutes=-rounded_time.minute,
                                                                     seconds=-rounded_time.second)
        # Преобразование округленных временных меток в формат timestamp в миллисекундах
        rounded_time = int(rounded_time.timestamp() * 1000)
    else:
        rounded_time = timestamp
        print('Error!')

    return rounded_time


def has_passed_any_hours(timestamp: int, hours: float) -> bool:
    """
    Определяет, прошло ли более указанного количества часов с момента заданного времени.

    Параметры:
    - value (int): Время в формате timestamp в миллисекундах.
    - hours (int): Количество часов для проверки.

    Возвращает:
    - bool: True, если прошло более указанного количества часов, False в противном случае.
    """

    moscow_tz = pytz.timezone('Europe/Moscow')

    # Преобразование заданного времени в объект datetime
    timestamp_dt = datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.UTC)
    timestamp_moscow = timestamp_dt.astimezone(moscow_tz)

    # Текущее время в Московском времени
    current_time_moscow = datetime.datetime.now(moscow_tz)

    # Разница между текущим временем и заданным временем в часах
    hours_difference = (current_time_moscow - timestamp_moscow).total_seconds() / 3600

    return hours_difference > hours


def get_start_time_for_concat(amount_klines: dict, current_time: int = None) -> dict:
    """
        Вычитает из текущего времени указанное в значениях словаря кол-во секунд.

        Параметры:
        - amount_klines (dict): Словарь, с количеством секунд для анализа в значениях.
        - current_time (int): Время, от которого будет производиться отсчёт

        Возвращает:
        - dict: Словарь разностей текущего времени со значениями изначального словаря.
        """

    if current_time is None:
        current_time = datetime.datetime.now()
    else:
        current_time = datetime.datetime.fromtimestamp(current_time / 1000.0)

    rounded_timestamps = {}
    for key, seconds in amount_klines.items():
        rounded_time = current_time - datetime.timedelta(seconds=current_time.second, microseconds=current_time.microsecond)
        rounded_time -= datetime.timedelta(seconds=seconds)
        rounded_timestamps[f'start_time_{key}'] = int(rounded_time.timestamp() * 1000)  # Преобразование в миллисекунды
        rounded_timestamps[f'end_time_{key}'] = int(current_time.timestamp()) * 1000 # Добавляем end_time

    return rounded_timestamps


def add_human_readable_time(df, timestamp_column, new_column):
    # Функция конвертирует timestamp время в человеческое и добавляет его в начало ДФ
    df[new_column] = pd.to_datetime(df[timestamp_column] // 1000, unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    df.insert(0, new_column + '_temp', df[new_column])
    df.drop(columns=[new_column], inplace=True)
    df.rename(columns={new_column + '_temp': new_column}, inplace=True)
    return df

def is_weekend_or_night_msk_timezone():
    """Функция вернет True, если сейчас по Москве выходной день или время с 00:00 до 10:00, и False в противном случае."""

    # Получаем текущее время в Москве
    moscow_time = datetime.datetime.now(pytz.timezone('Europe/Moscow'))

    # Проверяем, является ли сегодня выходным
    if moscow_time.weekday() > 4:  # 5 и 6 - суббота и воскресенье
        return True

    # Проверяем, является ли сейчас время с 00:00 до 09:00
    if moscow_time.hour < 10:
        return True

    return False


if __name__ == '__main__':
    x, y = time_now()
    print(x, y)