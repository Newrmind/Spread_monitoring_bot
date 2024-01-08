from Config import Config
import pandas as pd
from Database import postgres_sql
import time
import time_functions
import excel_functions



def get_stat_pair(spread):
    df = db_server.get_table_from_db(f"""
            SELECT spread,
            COUNT(CASE WHEN close_reason = 'sl' THEN 1 END) AS sl_count,
            COUNT(CASE WHEN close_reason = 'tp' THEN 1 END) AS tp_count,
            (COUNT(CASE WHEN close_reason = 'tp' THEN 1 END) * 100.0 / COUNT(*)) AS tp_percentage
            FROM closed_positions
            WHERE spread = '{spread}'
            GROUP BY spread;""")
    sl_count = df['sl_count'][0]
    tp_count = df['tp_count'][0]
    tp_percentage = df['tp_percentage'][0]
    return round(sl_count, 2), round(tp_count, 2), round(tp_percentage, 2)


if __name__ == '__main__':
    db = postgres_sql.Database()
    db_server = postgres_sql.Database(db_info=Config.postgres_connection_server)

    sl_count, tp_count, tp_percentage = get_stat_pair('ROSN/NVTK')
    print(f"Кол-во стопов: {sl_count}. Кол-во тейков: {tp_count}. Процент положительных сделок: {tp_percentage}%.")