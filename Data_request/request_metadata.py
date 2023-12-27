

if __name__ == '__main__':
    import pandas as pd
    from generate_tickers import *
    from Database import database

    future_tickers = generate_future_tickers()
    stock_tickers = generate_stock_tickers()
    currency_tickers = generate_currency_tickers()


    def get_metadata(apProvider, future_tickers, stock_tickers, currency_tickers):
        exchange = 'MOEX'  # Биржа

        # Получаем метаданные по каждому инструменту
        symbolInfo_list = []

        # Получаем информацию о тикерах на фьючерсном рынке
        for key, value in future_tickers.items():
            print('Запрос по инструменту', value)
            symbolInfo = apProvider.GetSymbol(exchange, value)
            if symbolInfo is not None:
                symbolInfo['type'] = 'future'
                symbolInfo_list.append(symbolInfo)

        print(symbolInfo_list)

        for stock in stock_tickers:
            print('Запрос по инструменту', stock)
            symbolInfo = apProvider.GetSymbol(exchange, stock)
            if symbolInfo is not None:
                symbolInfo_list.append(symbolInfo)


        for curr in currency_tickers:
            print('Запрос по инструменту', curr)
            symbolInfo = apProvider.GetSymbol(exchange, curr)
            if symbolInfo is not None:
                symbolInfo_list.append(symbolInfo)


        df_metadata = pd.DataFrame(symbolInfo_list)



        df_metadata = df_metadata.drop(columns=['rating', 'marginbuy', 'marginsell', 'marginrate', 'theorPrice', 'theorPriceLimit', 'volatility',
                        'ISIN', 'yield', 'board', 'primary_board', 'tradingStatus', 'tradingStatusInfo',
                        'complexProductCategory', 'priceMultiplier', 'priceShownUnits', 'cfiCode'])

        database.db.create_metadata(df_metadata)



    def exel_write(df, xlsx_file_name):
        # Записываем датафрейм в excel
        # создаём excel writer
        writer = pd.ExcelWriter(f'{xlsx_file_name}.xlsx')
        # записываем датафрейм в excel на лист с именем 'df_metadata'
        df.to_excel(writer, 'xlsx_file_name')
        # Сохраняем excel файл
        writer.close()

    get_metadata(apProvider, future_tickers, stock_tickers, currency_tickers)
