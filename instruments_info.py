from AlorPy import AlorPy  # Работа с Alor OpenAPI V2
from Config import Config  # Файл конфигурации
import pandas as pd





if __name__ == '__main__':
    apProvider = AlorPy(Config.UserName, Config.RefreshToken)  # Подключаемся к торговому счету. Логин и Refresh Token берутся из файла Config.py
    # apProvider = AlorPy(Config.DemoUserName, Config.DemoRefreshToken, True)  # Подключаемся к демо счету

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

"""
    # Таблица всех инструментов
    data = apProvider.GetSecuritiesExchange(exchange)
    df = pd.DataFrame(data)
    exel_write(df, 'Alor_MOEX')

    print(df.head(5))
"""
