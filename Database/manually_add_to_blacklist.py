from Database import postgres_sql
from Config import Config


def manually_bl():
    l = ['LKOH/ROSN', 'GAZP/LKOH', 'LKOH/NVTK', 'SIBN/LKOH']
    spreads_to_bl = db_server.get_table_from_db("SELECT pair as spread FROM stock_spreads WHERE pair LIKE '%LKOH%'")
    print(spreads_to_bl)


if __name__ == '__main__':
    db = postgres_sql.Database()
    db_server = postgres_sql.Database(db_info=Config.postgres_connection_server)
    manually_bl()