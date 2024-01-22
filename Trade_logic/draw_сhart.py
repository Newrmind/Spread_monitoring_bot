import matplotlib.pyplot as plt
import io
from Database.postgres_sql import Database
import pandas as pd
import time_functions

def plot(spread: str, df: pd.DataFrame, lats_price: float = None):

    if lats_price is not None:
        time_msk, timestamp = time_functions.time_now()

        # Добавляем значение lats_price в колонку 'close'
        new_row = df.iloc[-1].copy()
        new_row['close'] = lats_price
        new_row['time_msk'] = time_msk
        new_row['timestamp'] = timestamp
        df = df._append(new_row, ignore_index=True)

    plt.figure(figsize=(10, 6))

    plt.plot(df['close'], label='close')
    plt.plot(df['sma200'], label='SMA200')
    plt.plot(df['lb'], label='lb')
    plt.plot(df['ub'], label='ub')

    # Настройка осей и легенды
    plt.ylabel('Цена')
    plt.title(spread)
    plt.grid(True)  # добавляем сетку
    plt.legend()

    # Сохраняем график в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    return buf

def plot_pnl(df):
    # Разделение колонки по пробелу
    df['time'] = df['time'].str.split()
    # Создание новых колонок на основе разделенных значений
    df[['date', 'time']] = pd.DataFrame(df['time'].tolist(), index=df.index)

    plt.figure(figsize=(10, 6))
    plt.plot(df['date'], df['pnl'], label='PnL RUB')
    plt.ylabel('Сумма_RUB')
    plt.title("ГРАФИК PnL")
    plt.grid(True)
    plt.legend()
    plt.xticks(rotation=45, fontsize=6)

    # Сохраняем график в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)

    return buf

def draw_table(df):

    # Создаем изображение из датафрейма
    plt.figure(figsize=(10, 6))
    plt.axis('off')
    plt.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')

    # Сохраняем график в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)

    return buf


if __name__ == '__main__':
    db = Database()
    pair = 'YNDX/VKCO'
    lats_price = 4.1
    df_spread = db.get_table_from_db(f"SELECT * FROM bollinger_bands WHERE pair = '{pair}'")
    # df_stock = db.get_table_from_db(f"SELECT time_msk, close, open, high, low FROM stocks_klines_15m WHERE symbol = 'SBER' LIMIT 20")
    # df_open_pos = db.get_table_from_db(f"SELECT * FROM open_positions")
    # df_spread.info()
    # plot_candles(df_stock)
    plot(pair, df_spread, lats_price)
    # draw_table(df_open_pos)

    # df_pnl = db.get_table_from_db("SELECT * FROM pnl ORDER BY timestamp ASC LIMIT 1000")
    # plot_pnl(df_pnl)