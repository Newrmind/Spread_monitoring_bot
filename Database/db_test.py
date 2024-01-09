from Config import Config
import pandas as pd
from Database import postgres_sql
import time
import time_functions
import excel_functions
import matplotlib.pyplot as plt




def get_stat_pair():
    df = db_server.get_table_from_db(f"""
            SELECT spread,
            COUNT(CASE WHEN close_reason = 'sl' THEN 1 END) AS sl_count,
            COUNT(CASE WHEN close_reason = 'tp' THEN 1 END) AS tp_count,
            COUNT(close_reason) AS all_count,
            (COUNT(CASE WHEN close_reason = 'tp' THEN 1 END) * 100.0 / COUNT(*)) AS tp_percentage
            FROM closed_positions
            GROUP BY spread;""")
    return df

def plot_stat(df):
    # Создаем фигуру и оси
    fig, axs = plt.subplots(1, 2, figsize=(10, 10))

    # График по значениям all_count
    axs[0].bar(df['spread'], df['all_count'], color='blue')
    axs[0].set_xlabel('Spread')
    axs[0].set_ylabel('All Count')
    axs[0].set_title('График по значениям all_count')

    # График по значениям tp_percentage
    axs[1].bar(df['spread'], df['tp_percentage'], color='green')
    axs[1].set_xlabel('Spread')
    axs[1].set_ylabel('TP Percentage')
    axs[1].set_title('График по значениям tp_percentage')

    plt.xticks(rotation=90)

    # Отображаем графики
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    db = postgres_sql.Database()
    db_server = postgres_sql.Database(db_info=Config.postgres_connection_server)
    stat = get_stat_pair()
    print(stat)
    plot_stat(stat)