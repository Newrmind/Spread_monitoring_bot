from Database import database
from datetime import datetime


def generate_future_tickers():
    # получаем таблицу из базы данных
    futures_tickers = database.db.get_futures_tickers()
    month_code_fut = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

    # получаем текущий код месяца для квартальных фьючерсов
    time_now = datetime.now()
    time_quarter_month = time_now.month
    time_year = time_now.year
    if time_quarter_month < 3:
        time_quarter_month = 3
    elif 3 < time_quarter_month < 6:
        time_quarter_month = 6
    elif 6 < time_quarter_month < 9:
        time_quarter_month = 9
    elif 9 < time_quarter_month:
        time_quarter_month = 12

    shape = futures_tickers.shape
    fut_tickers_dict = {}
    for i in range(shape[0]):
        fut_base = futures_tickers.iloc[i]['Код базисного актива на срочном рынке']
        fut_code = futures_tickers.iloc[i]['Код_базисного_актива_поле_C']

        # генерируем тикеры квартальных фьючерсов
        if futures_tickers.iloc[i]['is_trades_allowed'] and futures_tickers.iloc[i]['is_quarter'] \
                and futures_tickers.iloc[i]['Код_базисного_актива_поле_C'] not in ['USDRUBF', 'EURRUBF', 'CNYRUBF']:

            # если декабрьский фьючерс экспирируется, генерируем тикер следующего
            if time_now.month == 12 and time_now.day > 15:
                fut_ticker = fut_code + month_code_fut[3] + str(time_year + 1)[-1]
                fut_tickers_dict[fut_base] = fut_ticker
            else:
                fut_ticker = fut_code + month_code_fut[time_quarter_month] + str(time_year)[-1]
                fut_tickers_dict[fut_base] = fut_ticker

        # генерируем тикеры месячных фьючерсов фондовой секции
        if futures_tickers.iloc[i]['is_trades_allowed'] and not futures_tickers.iloc[i]['is_quarter'] \
                and futures_tickers.iloc[i]['Код_базисного_актива_поле_C'] not in ['USDRUBF', 'EURRUBF', 'CNYRUBF']\
                and futures_tickers.iloc[i]['Группа контрактов'] == 'Фондовые контракты':
            if time_now.day > 15:
                fut_ticker = fut_code + month_code_fut[time_now.month+1] + str(time_year)[-1]
                fut_tickers_dict[fut_base] = fut_ticker
            else:
                fut_ticker = fut_code + month_code_fut[time_now.month] + str(time_year)[-1]
                fut_tickers_dict[fut_base] = fut_ticker

        # генерируем тикеры месячных фьючерсов товарной секции
        if futures_tickers.iloc[i]['is_trades_allowed'] and not futures_tickers.iloc[i]['is_quarter'] \
                and futures_tickers.iloc[i]['Код_базисного_актива_поле_C'] not in ['USDRUBF', 'EURRUBF', 'CNYRUBF']\
                and futures_tickers.iloc[i]['Группа контрактов'] == 'Товарные контракты':
            fut_ticker = fut_code + month_code_fut[time_now.month+1] + str(time_year)[-1]
            fut_tickers_dict[fut_base] = fut_ticker

        # генерируем тикеры вечных фьючерсов
        if futures_tickers.iloc[i]['Код_базисного_актива_поле_C'] in ['USDRUBF', 'EURRUBF', 'CNYRUBF']:
            fut_ticker = fut_code
            fut_tickers_dict[fut_base] = fut_ticker

    return fut_tickers_dict

def generate_stock_tickers():
    stocks_tickers = database.db.get_stocks_tickers() # получем pandas df
    # убираем нешортовые акции
    stocks_tickers_short = stocks_tickers[stocks_tickers['Возможность шорта'] == 1]
    return stocks_tickers_short['symbol']

def generate_currency_tickers():
    currency_tickers = database.db.get_currencies_tickers()['symbol'] # получем pandas df
    return currency_tickers
