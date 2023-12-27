import pandas as pd
from typing import Literal
import sqlalchemy as sa
from Config import Config
import time_functions

class Database:
    def __init__(self, db_info: Config = Config.postgres_connection_info):
        self.db_info = db_info
        self.engine = sa.create_engine(
            f"postgresql://{self.db_info['user']}:{self.db_info['password']}@{self.db_info['host']}:"
            f"{self.db_info['port']}/{self.db_info['dbname']}")

    # Функция получения таблицы из БД
    def get_table_from_db(self, querry: str):
        connection = self.engine.connect()
        if connection:
            try:
                df = pd.read_sql(querry, connection)
                return df
            except Exception as e:
                print(f"Error executing query: {e}")
            finally:
                connection.close()
        return None

    # Функция записи таблицы в БД
    def add_table_to_db(self, df: pd.DataFrame, table_name: str, if_exists: Literal['append', 'replace']) -> None:
        connection = self.engine.connect()
        if connection:
            try:
                df.to_sql(name=table_name, con=connection, if_exists=if_exists, index=False)
            except Exception as e:
                print(f"Error executing query: {e}")
            finally:
                connection.close()

    def change_table(self, query: str) -> None:

        connection = self.engine.raw_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute(query)
                connection.commit()
            except Exception as e:
                print(f"Error executing query: {e}")
            finally:
                connection.close()

    def update_last_klines(self, intervals: list, instrument_type: str) -> None:
        """Создаёт таблицу со временем первой и последней свечи для каждого тикера."""

        get_last_klines = pd.DataFrame()

        with self.engine.connect() as connection:
            for interval in intervals:
                print(f'Запрос времени первой и последней свечи для тф {interval}')
                query = f"SELECT symbol, MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp " \
                        f"FROM {instrument_type}_klines_{interval} GROUP BY symbol"

                klilines_minmax = pd.read_sql_query(query, connection)
                klilines_minmax['timeframe'] = f'{interval}'
                get_last_klines = pd.concat([get_last_klines, klilines_minmax], ignore_index=True)

            get_last_klines.insert(loc=len(get_last_klines.columns), column='min_time', value=get_last_klines['min_timestamp'])  # Добавляем колонку времени
            time_functions.timestamp_to_msk(get_last_klines, 'min_time')
            get_last_klines.insert(loc=len(get_last_klines.columns), column='max_time', value=get_last_klines['max_timestamp'])
            time_functions.timestamp_to_msk(get_last_klines, 'max_time')

            get_last_klines.to_sql(name=f'{instrument_type}_klines_minmax_{interval}', con=connection, if_exists='replace', index=False)
            connection.commit()

    def delete_old_klines(self, timeframe) -> None:
        """Удаляет из базы данных старые свечи."""
        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            timestamp_now = time_functions.time_now()[1]
            half_year_ago_timestamp = timestamp_now - 7776000000
            query = f"DELETE FROM stocks_klines_{timeframe} WHERE timestamp < {half_year_ago_timestamp}"
            cursor.execute(query)
            connection.commit()
        finally:
            connection.close()


if __name__ == '__main__':
    db = Database()
    db.update_last_klines(['15m'])
    # db.split_spread_column()
    # db.delete_old_klines('15m')
