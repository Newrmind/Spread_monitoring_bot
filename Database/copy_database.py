import psycopg2
import pandas as pd
from Config import Config
import postgres_sql



db_local = postgres_sql.Database()
db_server = postgres_sql.Database(db_info=Config.postgres_connection_server)


def copy_all_tables(copy_from: str):
    if copy_from == 'local':
        df_tables = db_local.get_table_from_db("""SELECT table_name
                                                    FROM information_schema.tables
                                                    WHERE table_schema = 'public';""")

        table_names = df_tables['table_name'].tolist()
        print(table_names)

        for table_name in table_names:
            print(f'Копирование таблицы {table_name}')
            df = db_local.get_table_from_db(f"SELECT * FROM {table_name}")
            db_server.add_table_to_db(df, table_name, 'replace')
    elif copy_from == 'server':
        df_tables = db_server.get_table_from_db("""SELECT table_name
                                                FROM information_schema.tables
                                                WHERE table_schema = 'public';""")

        table_names = df_tables['table_name'].tolist()
        print(table_names)

        for table_name in table_names:
            print(f'Копирование таблицы {table_name}')
            df = db_server.get_table_from_db(f"SELECT * FROM {table_name}")
            db_local.add_table_to_db(df, table_name, 'replace')

def copy_to_server(table_name):
    print(f'Копирование таблицы {table_name}')
    df = db_local.get_table_from_db(f"SELECT * FROM {table_name}")
    db_server.add_table_to_db(df, table_name, 'replace')

def copy_from_server(table_names: list):
    for table_name in table_names:
        print(f'Копирование таблицы {table_name}')
        df = db_server.get_table_from_db(f"SELECT * FROM {table_name}")
        db_local.add_table_to_db(df, table_name, 'replace')


if __name__ == '__main__':
    copy_all_tables(copy_from='server')
    # copy_all_tables(copy_from='local')
    # table_names = ['stock_spreads', 'blacklist', 'open_positions']
    # copy_from_server(table_names)
    # copy_to_server('stock_spreads')
