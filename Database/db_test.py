from Config import Config
import pandas as pd
from Database import postgres_sql
import time
import time_functions
import excel_functions






if __name__ == '__main__':
    db = postgres_sql.Database()
    db_server = postgres_sql.Database(db_info=Config.postgres_connection_server)

    spreads = db_server.get_table_from_db("SELECT * FROM stock_spreads")
    print(spreads)