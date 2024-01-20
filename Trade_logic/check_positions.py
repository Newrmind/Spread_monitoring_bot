import pandas as pd
import time_functions
from Trade_logic import draw_сhart, secondary_functions
from Config import Config
from Telegram_bot.send_message import TelegramSendMessage
from logger import log_function_info, error_inf
import time


def check_positions(db, last_prices, df_open_positions, df_closed_positions, bollinger_bands, blacklist):

    log_function_info("Старт функции check_positions()")
    retries = 5
    while retries > 0:
        try:

            tg_send = TelegramSendMessage()

            def change_tp(direction_value, df_open_positions, index_to_update, spread):

                max_timestamp_rows = bollinger_bands.loc[bollinger_bands.groupby('pair')['timestamp'].idxmax()]
                max_timestamp_row = max_timestamp_rows[max_timestamp_rows['pair'] == spread]
                last_bollinger_sma = max_timestamp_row['sma200'].iloc[0]

                if direction_value == 'buy':
                    new_tp = last_bollinger_sma
                    df_open_positions.loc[index_to_update, 'tp'] = new_tp

                elif direction_value == 'sell':
                    new_tp = last_bollinger_sma
                    df_open_positions.loc[index_to_update, 'tp'] = new_tp

                return df_open_positions

            def change_pnl(time, timestamp, pnl):
                df = db.get_table_from_db('SELECT * FROM pnl ORDER BY timestamp DESC LIMIT 1')
                df.loc[0, 'time'] = time
                df.loc[0, 'timestamp'] = timestamp
                df.loc[0, 'pnl'] += pnl

                db.add_table_to_db(df, 'pnl', 'append')

            for index, row in df_open_positions.iterrows():

                symbol_1, symbol_2 = row.spread.split('/')
                last_price_symbol_1 = last_prices[symbol_1]
                last_price_symbol_2 = last_prices[symbol_2]
                spread_price = last_price_symbol_1 / last_price_symbol_2
                spread = row.spread

                spreads = db.get_table_from_db(f"SELECT * FROM stock_spreads WHERE pair = '{spread}'")
                ticker_1_evening = spreads["ticker_1_evening"].iloc[0]
                ticker_2_evening = spreads["ticker_2_evening"].iloc[0]
                moscow_evening = time_functions.is_moscow_evening()
                evening_trade = secondary_functions.check_evening_trade(moscow_evening, ticker_1_evening, ticker_2_evening)

                if spread in df_open_positions['spread'].tolist() and evening_trade:

                    # Получить индекс строки, где 'spread' == 'text'
                    index_to_update = df_open_positions[df_open_positions['spread'] == spread].index[0]

                    volume = df_open_positions.loc[index_to_update, 'volume_rub']
                    open_prise = df_open_positions.loc[index_to_update, 'open_prise']
                    commission_rub = volume * (Config.commission * 4)
                    current_timestamp = time_functions.time_now()[1]
                    current_time = time_functions.time_now()[0]

                    direction_value = df_open_positions.loc[index_to_update, 'direction']

                    if direction_value == 'buy':

                        current_result_percent = round((spread_price / open_prise - 1) * 100, 2)
                        df_open_positions.loc[index_to_update, 'result_perc'] = current_result_percent

                        current_result_rub = round(df_open_positions.loc[index_to_update, 'volume_rub'] * (current_result_percent / 100) - commission_rub, 4)
                        df_open_positions.loc[index_to_update, 'result_rub'] = round(current_result_rub, 2)

                        # Обновить tp
                        df_open_positions = change_tp(direction_value, df_open_positions, index_to_update, spread)

                        if spread_price <= df_open_positions.loc[index_to_update, 'sl']:
                            """Stop Loss"""

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions.loc[index_to_update, 'close_reason'] = "sl"
                            df_open_positions.loc[index_to_update, 'close_time'] = current_time
                            df_open_positions.loc[index_to_update, 'close_timestamp'] = current_timestamp

                            # Отправляем закрытую позицию в БД и удаляем из датафрейма
                            row_to_move = df_open_positions.loc[index_to_update].to_dict()
                            df_closed_positions = pd.concat([df_closed_positions, pd.DataFrame.from_records([row_to_move])])
                            df_open_positions = df_open_positions.drop(index_to_update)

                            message = f"Позиция {row_to_move['spread']} {row_to_move['direction']} закрыта по стопу. \n" \
                                      f"Цена открытия: {round(open_prise, 4)}. \n" \
                                      f"Цена закрытия: {round(spread_price, 4)}. \n" \
                                      f"Результат: {current_result_rub} RUB, {current_result_percent}%."
                            print(message)
                            df_for_plot = bollinger_bands[bollinger_bands['pair'] == row.spread]
                            chart = draw_сhart.plot(row.spread, df_for_plot, spread_price)
                            tg_send.send_plot_to_telegram(chart, message)

                            # Записать PnL
                            change_pnl(time=current_time, timestamp=current_timestamp, pnl=current_result_rub)

                            # Добавить спред в чёрный список
                            new_row_blacklist = {'spread': spread, 'time': current_time,
                                                 'timestamp': current_timestamp, 'reason': 'sl'}
                            blacklist = pd.concat([blacklist, pd.DataFrame.from_records([new_row_blacklist])])

                        elif spread_price >= df_open_positions.loc[index_to_update, 'tp']:
                            """Take Profit"""

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions.loc[index_to_update, 'close_reason'] = "tp"
                            df_open_positions.loc[index_to_update, 'close_time'] = current_time
                            df_open_positions.loc[index_to_update, 'close_timestamp'] = current_timestamp

                            # Отправляем закрытую позицию в БД и удаляем из датафрейма
                            row_to_move = df_open_positions.loc[index_to_update].to_dict()
                            df_closed_positions = pd.concat([df_closed_positions, pd.DataFrame.from_records([row_to_move])])
                            df_open_positions = df_open_positions.drop(index_to_update)

                            message = f"Позиция {row_to_move['spread']} {row_to_move['direction']} закрыта по тейку. \n" \
                                      f"Цена открытия: {round(open_prise, 4)}.  \n" \
                                      f"Цена закрытия: {round(spread_price, 4)}. \n" \
                                      f"Результат: {current_result_rub} RUB, {current_result_percent}%."
                            print(message)
                            df_for_plot = bollinger_bands[bollinger_bands['pair'] == row.spread]
                            chart = draw_сhart.plot(row.spread, df_for_plot, spread_price)
                            tg_send.send_plot_to_telegram(chart, message)

                            # Записать PnL
                            change_pnl(time=current_time, timestamp=current_timestamp, pnl=current_result_rub)

                    if direction_value == 'sell':

                        current_result_percent = round(-(spread_price / open_prise - 1) * 100, 2)
                        df_open_positions.loc[index_to_update, 'result_perc'] = current_result_percent

                        current_result_rub = round(df_open_positions.loc[index_to_update, 'volume_rub'] * (current_result_percent / 100) - commission_rub, 4)
                        df_open_positions.loc[index_to_update, 'result_rub'] = round(current_result_rub, 2)

                        # Обновить tp
                        df_open_positions = change_tp(direction_value, df_open_positions, index_to_update, spread)

                        if spread_price >= df_open_positions.loc[index_to_update, 'sl']:
                            """Stop Loss"""

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions.loc[index_to_update, 'close_reason'] = "sl"
                            df_open_positions.loc[index_to_update, 'close_time'] = current_time
                            df_open_positions.loc[index_to_update, 'close_timestamp'] = current_timestamp

                            # Отправляем закрытую позицию в БД и удаляем из датафрейма
                            row_to_move = df_open_positions.loc[index_to_update].to_dict()
                            df_closed_positions = pd.concat([df_closed_positions, pd.DataFrame.from_records([row_to_move])])
                            df_open_positions = df_open_positions.drop(index_to_update)

                            message = f"Позиция {row_to_move['spread']} {row_to_move['direction']} закрыта по стопу. \n" \
                                      f"Цена открытия: {round(open_prise, 4)}.  \n" \
                                      f"Цена закрытия: {round(spread_price, 4)}. \n" \
                                      f"Результат: {current_result_rub} RUB, {current_result_percent}%."
                            print(message)
                            df_for_plot = bollinger_bands[bollinger_bands['pair'] == row.spread]
                            chart = draw_сhart.plot(row.spread, df_for_plot, spread_price)
                            tg_send.send_plot_to_telegram(chart, message)

                            # Записать PnL
                            change_pnl(time=current_time, timestamp=current_timestamp, pnl=current_result_rub)

                            # Добавить спред в чёрный список
                            new_row_blacklist = {'spread': spread, 'time': current_time,
                                                 'timestamp': current_timestamp, 'reason': 'sl'}
                            blacklist = pd.concat([blacklist, pd.DataFrame.from_records([new_row_blacklist])])

                        elif spread_price <= df_open_positions.loc[index_to_update, 'tp']:
                            """Take Profit"""

                            # Внести изменения в датафрейм открытых позиций
                            df_open_positions.loc[index_to_update, 'close_reason'] = "tp"
                            df_open_positions.loc[index_to_update, 'close_time'] = current_time
                            df_open_positions.loc[index_to_update, 'close_timestamp'] = current_timestamp

                            # Отправляем закрытую позицию в БД и удаляем из датафрейма
                            row_to_move = df_open_positions.loc[index_to_update].to_dict()
                            df_closed_positions = pd.concat([df_closed_positions, pd.DataFrame.from_records([row_to_move])])
                            df_open_positions = df_open_positions.drop(index_to_update)

                            message = f"Позиция {row_to_move['spread']} {row_to_move['direction']} закрыта по тейку. \n" \
                                      f"Цена открытия: {round(open_prise, 4)}.  \n" \
                                      f"Цена закрытия: {round(spread_price, 4)}. \n" \
                                      f"Результат: {current_result_rub} RUB, {current_result_percent}%."
                            print(message)
                            df_for_plot = bollinger_bands[bollinger_bands['pair'] == row.spread]
                            chart = draw_сhart.plot(row.spread, df_for_plot, spread_price)
                            tg_send.send_plot_to_telegram(chart, message)

                            # Записать PnL
                            change_pnl(time=current_time, timestamp=current_timestamp, pnl=current_result_rub)

                elif not evening_trade:
                    log_message = f'Позиция по спреду {row.pair} не закрыта, т.к. один из инструментов не торгуется на вечерней сессии.'
                    log_function_info(log_message)
                    print('[INFO]' + log_message)

                df_open_positions = df_open_positions.reset_index(drop=True)
                df_closed_positions = df_closed_positions.reset_index(drop=True)
                blacklist = blacklist.reset_index(drop=True)
                db.add_table_to_db(blacklist, 'blacklist', 'replace')

            return df_open_positions, df_closed_positions

        except Exception as ex:
            print(f"Произошла ошибка: {ex}")
            error_inf(ex)
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток