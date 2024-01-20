import pandas as pd
import time_functions
from Telegram_bot.send_message import TelegramSendMessage
from Trade_logic import draw_сhart
from Config import Config
from logger import log_function_info, error_inf
import time


def create_trade(db, last_prices, ful_prices, bollinger_bands: pd.DataFrame, last_bollinger_bands: pd.DataFrame, df_open_positions):
    """Ищет точки входа"""

    log_function_info("Старт функции create_trade()")
    retries = 5
    while retries > 0:
        try:

            tg_send = TelegramSendMessage()
            blacklist = db.get_table_from_db('SELECT * FROM blacklist')
            for index, row in last_bollinger_bands.iterrows():
                symbol_1, symbol_2 = row.pair.split('/')
                last_price_symbol_1 = last_prices[symbol_1]
                last_price_symbol_2 = last_prices[symbol_2]
                spread_price = last_price_symbol_1 / last_price_symbol_2

                lot_count_1 = int(Config.volume_for_trade / ful_prices[symbol_1])
                lot_count_2 = int(Config.volume_for_trade / ful_prices[symbol_2])

                if not df_open_positions.columns.tolist():
                    df_open_positions = pd.DataFrame(columns=['open_time', 'open_timestamp', 'spread', 'direction', 'open_prise',
                                                              'sl', 'tp', 'volume_rub', 'result_perc', 'result_rub',  'close_time',
                                                              'close_reason'])

                if (row.pair not in df_open_positions['spread'].tolist() or df_open_positions.empty) and row.pair not in blacklist['spread'].tolist():

                    def get_stat_spread(spread):
                        df = db.get_table_from_db(f"""
                                    SELECT spread,
                                    COUNT(CASE WHEN close_reason = 'sl' THEN 1 END) AS sl_count,
                                    COUNT(CASE WHEN close_reason = 'tp' THEN 1 END) AS tp_count,
                                    (COUNT(CASE WHEN close_reason = 'tp' THEN 1 END) * 100.0 / COUNT(*)) AS tp_percentage
                                    FROM closed_positions
                                    WHERE spread = '{spread}'
                                    GROUP BY spread;""")
                        if not df.empty:
                            sl_count = df['sl_count'][0]
                            tp_count = df['tp_count'][0]
                            tp_percentage = df['tp_percentage'][0]
                            return round(sl_count, 2), round(tp_count, 2), round(tp_percentage, 2)
                        else:
                            # Если датафрейм пуст, вернуть значения по умолчанию (например, 0)
                            return 0, 0, 0


                    if spread_price <= row.lb:
                        """signal = 'Buy' """

                        direction = 'buy'
                        stop_loss = spread_price - row.sd * Config.sl
                        tp = row.sma200
                        volume_for_trade = Config.volume_for_trade
                        time_now, timestamp_now = time_functions.time_now()
                        sl_count, tp_count, tp_percentage = get_stat_spread(row.pair)

                        new_position = {'open_time': time_now, 'open_timestamp': timestamp_now, 'spread': row.pair,
                                        'direction': direction, 'open_prise': spread_price, 'sl': stop_loss,
                                        'tp': tp, 'volume_rub': volume_for_trade, 'result_perc': 0, 'result_rub': 0,
                                        'close_time': None, 'close_reason': None}

                        message = f'Открыта позиция {row.pair} {direction} по цене {round(spread_price, 4)}\n' \
                                  f'Объём по {symbol_1}: {lot_count_1} лотов, объём по {symbol_2}: {lot_count_2} лотов.\n' \
                                  f'Кол-во стопов: {sl_count}. Кол-во тейков: {tp_count}. Процент тейков: {tp_percentage}%.'
                        print(f"[INFO] {message}.")

                        df_for_plot = bollinger_bands[bollinger_bands['pair'] == row.pair]
                        chart = draw_сhart.plot(row.pair, df_for_plot, spread_price)
                        tg_send.send_plot_to_telegram(chart, message)

                        # Добавить сделку в БД
                        df_open_positions = pd.concat([df_open_positions, pd.DataFrame.from_records([new_position])])
                        df_open_positions = df_open_positions.reset_index(drop=True)
                        db.add_table_to_db(df_open_positions, 'open_positions', 'replace')

                    elif spread_price >= row.ub:
                        """signal = 'Sell' """

                        direction = 'sell'
                        stop_loss = spread_price + row.sd * Config.sl
                        tp = row.sma200
                        volume_for_trade = Config.volume_for_trade
                        time_now, timestamp_now = time_functions.time_now()
                        sl_count, tp_count, tp_percentage = get_stat_spread(row.pair)

                        new_position = {'open_time': time_now, 'open_timestamp': timestamp_now, 'spread': row.pair,
                                        'direction': direction, 'open_prise': spread_price, 'sl': stop_loss,
                                        'tp': tp, 'volume_rub': volume_for_trade, 'result_perc': 0, 'result_rub': 0,
                                        'close_time': None, 'close_reason': None}

                        message = f'Открыта позиция {row.pair} {direction} по цене {round(spread_price, 4)}\n' \
                                  f'Объём по {symbol_1}: {lot_count_1} лотов, объём по {symbol_2}: {lot_count_2} лотов.\n' \
                                  f'Кол-во стопов: {sl_count}. Кол-во тейков: {tp_count}. Процент тейков: {tp_percentage}%.'
                        print(f"[INFO] {message}.")

                        df_for_plot = bollinger_bands[bollinger_bands['pair'] == row.pair]
                        chart = draw_сhart.plot(row.pair, df_for_plot, spread_price)
                        tg_send.send_plot_to_telegram(chart, message)

                        # Добавить сделку в БД
                        df_open_positions = pd.concat([df_open_positions, pd.DataFrame.from_records([new_position])])
                        df_open_positions = df_open_positions.reset_index(drop=True)
                        db.add_table_to_db(df_open_positions, 'open_positions', 'replace')
            break

        except Exception as ex:
            print(f"Произошла ошибка: {ex}")
            error_inf(ex)
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток

