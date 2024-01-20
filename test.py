import datetime
from Database import postgres_sql
import time_functions
from Trade_logic import secondary_functions


db = postgres_sql.Database()
spread = "RTKM/RTKMP"
# spread = "POSI/OZON"
spreads = db.get_table_from_db(f"SELECT * FROM stock_spreads WHERE pair = '{spread}'")
ticker_1_evening = spreads["ticker_1_evening"].iloc[0]
ticker_2_evening = spreads["ticker_2_evening"].iloc[0]
# moscow_evening = time_functions.is_moscow_evening()
moscow_evening = False
print(f"ticker_1_evening = {ticker_1_evening}, ticker_2_evening = {ticker_2_evening}, moscow_evening = {moscow_evening}")

evening_trade = secondary_functions.check_evening_trade(moscow_evening, ticker_1_evening, ticker_2_evening)

if evening_trade:
    print("Сделка совершена!")
else:
    print("Сделка невозможна!")

print(ticker_1_evening and ticker_2_evening)
