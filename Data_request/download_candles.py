from connection import apProvider
import pandas as pd
from Database.postgres_sql import Database
from Config import Config
from time_functions import timestamp_to_msk, round_time, request_time_change, time_now, timestamp_to_moscow_time
import time


def stocks_candles_data():
    """Загружает исторические данные в виде свечей."""

    db = Database()

    # Получить список тикеров
    spreads_list = db.get_table_from_db("SELECT * FROM stock_spreads")['pair'].tolist()
    spreads_list_uniq = [item for pair in spreads_list for item in pair.split('/')]
    symbols_list_uniq = list(set(spreads_list_uniq))
    len_tickers = len(symbols_list_uniq)

    # Обновить время первой и последней свечи в БД
    db.update_last_klines(intervals=Config.timeframes, instrument_type='stocks')

    for interval in Config.timeframes:
        ticker_count = 1

        # Получить датафрейм со временем первой и последней свечи для каждого тикера
        stocks_klines_minmax = db.get_table_from_db(f"SELECT * FROM stocks_klines_minmax_{interval}")
        end_time = int(round_time(interval))

        for instrument in symbols_list_uniq:
            print(f'Запрос данных по инструменту {instrument} тф {interval}. '
                  f'Номер инструмента: {ticker_count} из {len_tickers}.')

            # Получить время первой свечи
            if instrument in stocks_klines_minmax['symbol'].values:
                start_time = stocks_klines_minmax.loc[stocks_klines_minmax["symbol"] == instrument, 'max_timestamp'].iloc[0] + 1
            else:
                seconds_in_30_days = 30 * 24 * 60 * 60
                # Время 30 дней назад в секундах
                time_30_days_ago = int(time.time()) - seconds_in_30_days
                start_time = time_30_days_ago

            print(f'Время первой свечи: {timestamp_to_moscow_time(start_time)}, время последней свечи: {timestamp_to_moscow_time(end_time)}\n')

            candles = apProvider.GetHistory(exchange='MOEX', symbol=instrument,
                                            tf='900', secondsFrom=start_time, secondsTo=end_time)
            if not candles['history']:
                print("При запросе исторических данных получен пустой список.\n")
            else:
                df = pd.DataFrame(candles['history'])
                df.insert(loc=0, column='time_msk', value=df['time'])  # Добавляем колонку времени
                df = df.rename(columns={'time': 'timestamp'})  # Переименовать колонку времени
                timestamp_to_msk(df, 'time_msk')  # Преобразуем timestamp в unix МСК
                df.insert(loc=2, column='symbol', value=instrument)  # Добавляем колонку с тикером

                # добавить свечи в БД
                db.add_table_to_db(df=df, table_name=f'stocks_klines_{interval}', if_exists='append')

            ticker_count += 1

        # Обновить время последнего запроса данных
        request_time_change(db, f'request_stocks_klines_{interval}')
        # Обновить время первой и последней свечи в БД
        db.update_last_klines(intervals=Config.timeframes, instrument_type='stocks')

    # Удалить старые свечи
    def check_old_klines():
        """Проверяет, есть ли в базе данных свечи старше 3-х месяцев, и удаляет их раз в 30 дней."""

        last_del = db.get_table_from_db("SELECT * FROM requests_time WHERE request = 'delete_old_klines'")
        selected_number = last_del.iloc[0, last_del.columns.get_loc('timestamp_msk')]

        if time_now()[1] / 1000 > (selected_number + 2500000000):
            for interval in Config.timeframes:
                print(f'Удаление старых свеч из базы данных для таймфрейма {interval}.\n')
                db.delete_old_klines(interval)

            request_time_change(db, f'delete_old_klines')

    check_old_klines()


if __name__ == '__main__':
    stocks_candles_data()

