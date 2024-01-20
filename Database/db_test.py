from Config import Config
import pandas as pd
from Database import postgres_sql
import time
import time_functions
import excel_functions
import matplotlib.pyplot as plt



from logger import log_function_info, error_inf, init_logging
import time

init_logging()

def test():
    log_function_info("Старт функции test()")
    retries = 5

    while retries > 0:
        try:
            x = 1/0
            return x
        except Exception as ex:
            print(f"Произошла ошибка: {ex}")
            error_inf(ex)
            time.sleep(1)
            retries -= 1  # Уменьшение количества попыток

print(test())