import pandas as pd
import time
from Config import Config
import time_functions
from Database.postgres_sql import Database


class DataAnalysis:
    def __init__(self, db: Database = Database()):
        """
        :param db: экземпляр класса базы данных

        Model name:     "Alor"
        Version:        1.0
        Last_update:    04.12.2023
        """

        self.db = db

    def get_spreads_list(self):
        """Получает список спредов из базы данных. Возвращает список спредов и список уникальных пар."""
        # Соединяемся с базой данных, получаем список тикеров
        spreads_list = self.db.get_table_from_db("SELECT * FROM stock_spreads")['pair'].tolist()
        spreads_list_uniq = [item for pair in spreads_list for item in pair.split('/')]
        symbols_list_uniq = list(set(spreads_list_uniq))

        return spreads_list, symbols_list_uniq

    def concat(self) -> None:
        '''
        Функция конкатенации для последующего анализа данных.
        '''

        function_start_time = time.time()

        symbols = self.get_spreads_list()[1]

        end_time = time_functions.time_now()[1]
        start_time = end_time - Config.sec_for_concat

        for timeframe in Config.timeframes:
            print(f'Конкатенация для таймфрейма {timeframe}.')
            print(f'Период: {time_functions.timestamp_to_moscow_time(start_time)} UTC - {time_functions.timestamp_to_moscow_time(end_time)} UTC\n')

            # Формируем строку с частями "MAX(CASE ...)" для каждого символа
            symbol_columns = ', '.join(
                [f'''MAX(CASE WHEN symbol = '{symbol}' THEN close END) AS "{symbol}"''' for symbol in symbols])

            # Формируем строку с условиями для фильтрации данных
            conditions = f"""symbol IN ({", ".join([f"'{symbol}'" for symbol in symbols])}) AND (timestamp > {start_time}) 
                            AND (timestamp < {end_time})"""

            # Формируем основную часть запроса
            query = f"SELECT time_msk, timestamp, {symbol_columns} FROM stocks_klines_{timeframe} WHERE {conditions} GROUP BY time_msk, timestamp"

            # Выполняем запрос и читаем результат в DataFrame
            result_df = self.db.get_table_from_db(query)

            # проверяем последнюю строку на наличие пустых ячеек
            if not result_df.empty:
                last_row_has_nan = result_df.iloc[-1].isna().any()
                # Удаляем последнюю строку при наличии пустых ячеек
                if last_row_has_nan:
                    result_df = result_df.drop(result_df.index[-1])
            elif result_df.empty:
                print('Внимание! Получен пустой датафрейм!')

            # Проверяем наличие пустых ячеек в каждой колонке
            # columns_with_empty_cells = result_df.columns[result_df.isnull().any()].tolist()
            # # Удаляем колонки, в которых есть пустые ячейки
            # df_cleaned = result_df.drop(columns=columns_with_empty_cells)

            # Далее преобразуем все колонки, кроме колонок времени, в числовые значения
            first_two_columns = result_df.iloc[:, :2].copy()
            numeric_columns = result_df.iloc[:, 2:].apply(pd.to_numeric, errors='coerce')
            # Объединяем первые два столбца с преобразованными числовыми столбцами
            result_df = pd.concat([first_two_columns, numeric_columns], axis=1)
            # Сортируем данные
            result_df = result_df.sort_values(by='timestamp', ascending=True)

            # Заполняем пропуски предыдущими значениями
            columns_to_fill = result_df.columns.tolist()
            result_df[columns_to_fill] = result_df[columns_to_fill].bfill()

            # Записываем результат в БД
            self.db.add_table_to_db(df=result_df, table_name=f'concat_stocks_{timeframe}', if_exists='replace')

            if result_df.empty:
                print('Внимание! Получен пустой датафрейм!')

        function_end_time = time.time()
        execution_time = function_end_time - function_start_time
        print(f"\nФункция выполнялась {execution_time:.3f} секунд")
        print('Конкатенация завершена. Данные записаны в базу данных!')

    def calculate_spreads(self):
        """Рассчитывает спреды путём деления цен."""

        spreads_list, symbols = self.get_spreads_list()

        # Преобразование списка в строку для SQL запроса
        columns_str = ', '.join(f'"{col}"' for col in symbols)

        df_instrument_for_spread = self.db.get_table_from_db(f"""SELECT time_msk, timestamp, {columns_str} 
                                       FROM concat_stocks_15m""")

        # Удаление кавычек из имен столбцов
        df_instrument_for_spread.rename(columns=lambda x: x.strip('"'), inplace=True)

        """Построить спред путём деления одного актива на другой"""
        spreads_df = pd.DataFrame()
        spreads_df['time_msk'] = df_instrument_for_spread['time_msk']
        spreads_df['timestamp'] = df_instrument_for_spread['timestamp']

        for pair in spreads_list:
            column1 = pair.split('/')[0]
            column2 = pair.split('/')[1]
            spreads_df[pair] = df_instrument_for_spread[column1] / df_instrument_for_spread[column2]

        # Округляем значения во всех колонках, кроме первых двух, до 7 знаков после запятой
        spreads_df.iloc[:, 2:] = spreads_df.iloc[:, 2:].round(7)

        # сохранить датафрейм спредов в базу данных
        self.db.add_table_to_db(spreads_df, 'spreads', 'replace')

    def bollinger_bands(self):
        """Строит полосы Боллинджера. """

        pairs_list = self.get_spreads_list()[0]
        spreads_df = self.db.get_table_from_db('SELECT * FROM spreads')

        bollinger_bands_concat = pd.DataFrame()

        for pair in pairs_list:
            bollinger_bands = pd.DataFrame()
            # Расчёт SMA
            bollinger_bands['time_msk'] = spreads_df['time_msk']
            bollinger_bands['timestamp'] = spreads_df['timestamp']
            bollinger_bands['pair'] = pair
            bollinger_bands['close'] = spreads_df[pair]
            bollinger_bands['sma200'] = spreads_df[pair].rolling(200).mean()

            # Расчёт стандартного отклонения (standard deviation)
            bollinger_bands['sd'] = spreads_df[pair].rolling(200).std()

            # Расчёт нижнего BB
            bollinger_bands['lb'] = bollinger_bands['sma200'] - Config.bollinger_bands * bollinger_bands['sd']

            # Расчёт верхнего BB
            bollinger_bands['ub'] = bollinger_bands['sma200'] + Config.bollinger_bands * bollinger_bands['sd']

            bollinger_bands.dropna(subset=['sma200', 'lb', 'ub'], inplace=True)  # Удалить строки с NaN в указанных столбцах
            bollinger_bands.dropna(inplace=True)

            # Расчёт среднего значения стандартного отклонения и отклонения от среднего std
            bollinger_bands['sd_mean'] = bollinger_bands['sd'].mean()
            bollinger_bands['dif_percentage'] = (bollinger_bands['sd'] / bollinger_bands['sd_mean'] * 100) - 100

            bollinger_bands_concat = pd.concat([bollinger_bands_concat, bollinger_bands], ignore_index=True)

        # сохранить датафрейм полосы Боллинджера в базу данных
        self.db.add_table_to_db(df=bollinger_bands_concat, table_name='bollinger_bands', if_exists='replace')

        # записать в БД время последнего вызова функции
        time_functions.request_time_change(self.db, 'bollinger_bands_calculate')

        print('[INFO] Расчёт bollinger_bands завершён.')


if __name__ == '__main__':
    data_analyze = DataAnalysis()
    data_analyze.concat()
    data_analyze.calculate_spreads()
    data_analyze.bollinger_bands()