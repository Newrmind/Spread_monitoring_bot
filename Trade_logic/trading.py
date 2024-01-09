import pandas as pd
import time
from Config import Config
import time_functions
from Database.postgres_sql import Database
from Trade_logic import create_trade, check_positions
from Data_request.request_quotes import get_quotes


class Trading:
    def __init__(self, db: Database = Database()):
        """
        :param db: экземпляр класса базы данных
        """

        self.db = db

    def find_idea(self):

        find_ideas_allowed = Config.find_ideas_allowed
        trading_allowed = self.db.get_table_from_db("SELECT * FROM params WHERE param = 'trading_allowed'")
        trading_allowed_value = int(trading_allowed.loc[trading_allowed['param'] == 'trading_allowed', 'value'].iloc[0])

        def get_open_positions():
            """Запрашивает из БД таблицу открытых позиций."""

            try:
                df_open_positions = self.db.get_table_from_db('SELECT * FROM open_positions')

                if df_open_positions is None:
                    df_open_positions = pd.DataFrame(columns=['open_time', 'open_timestamp', 'spread', 'direction', 'open_prise',
                                                              'sl', 'tp', 'volume_rub', 'result_perc', 'result_rub',  'close_time',
                                                              'close_reason'])
                return df_open_positions

            except Exception as ex:
                print(ex)

        def check_blacklist(blacklist):
            """Проверяет чёрный список, при необходимости удаляет из него элементы."""

            for index, row in blacklist.iterrows():
                blacklist_row_timestamp = row.timestamp
                del_from_bl_sl = False
                if row.reason == 'sl':
                    del_from_bl_sl = time_functions.has_passed_any_hours(blacklist_row_timestamp, Config.blacklist_time)

                if del_from_bl_sl:
                    blacklist = blacklist.drop(index)
                    blacklist.reset_index(drop=True)
                    self.db.add_table_to_db(blacklist, 'blacklist', 'replace')

            return blacklist

        df_open_positions = get_open_positions()
        df_closed_positions = pd.DataFrame()
        blacklist = self.db.get_table_from_db('SELECT * FROM blacklist')
        if not blacklist.empty:
            blacklist = check_blacklist(blacklist)

        bollinger_bands = self.db.get_table_from_db('SELECT * FROM bollinger_bands')
        last_bollinger_bands = bollinger_bands.loc[bollinger_bands.groupby('pair')['timestamp'].idxmax()]

        # Запрос текущих цен при необходимости
        if not df_open_positions.empty or find_ideas_allowed:
            pairs_list = last_bollinger_bands['pair'].tolist()
            pairs_list_uniq = [item for pair in pairs_list for item in pair.split('/')]
            symbols_list_uniq = list(set(pairs_list_uniq))
            last_prices, ful_prices = get_quotes(symbols_list_uniq)
        else:
            last_prices, ful_prices = {}, {}

        if not df_open_positions.empty:
            """Проверка открытых позиций."""
            df_open_positions, df_closed_positions = check_positions.check_positions(self.db, last_prices, df_open_positions, df_closed_positions,
                                                                                     bollinger_bands, blacklist)
            self.db.add_table_to_db(df_open_positions, 'open_positions', 'replace')

            if not df_closed_positions.empty:
                self.db.add_table_to_db(df_closed_positions, 'closed_positions', 'append')

        def check_last_analyze():
            """Проверяет, когда был последний анализ."""
            last_time_analyze = int(self.db.get_table_from_db("SELECT timestamp_msk FROM requests_time \
                                             WHERE request = 'bollinger_bands_calculate'").loc[0, 'timestamp_msk'])
            need_analyze = time_functions.has_passed_any_hours(timestamp=last_time_analyze, hours=Config.analyze_period)

            if need_analyze:
                print(f'\n[INFO] Требуется анализ. Открытие новых позиций заблокировано.\n')
            return need_analyze

        # Условия, разрешающие работу функции
        need_analyze = check_last_analyze()

        # найти сделки
        if find_ideas_allowed and trading_allowed_value and not need_analyze:
            create_trade.create_trade(self.db, last_prices, ful_prices, bollinger_bands, last_bollinger_bands, df_open_positions)

        elif not find_ideas_allowed or not trading_allowed_value:
            print(f'\n[INFO] Параметр find_ideas_allowed = {find_ideas_allowed}. '
                  f'[INFO] Параметр need_analyze = {need_analyze}.'
                  f'[INFO] Параметр trading_allowed_value = {trading_allowed_value}.'
                  f'Открытие новых позиций заблокировано.\n')