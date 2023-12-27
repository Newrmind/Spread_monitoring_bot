class Config:
    # Коды торговых серверов
    TradeServerCode = 'TRADE'  # Рынок Ценных Бумаг
    ITradeServerCode = 'ITRADE'  # Рынок Иностранных Ценных Бумаг
    FutServerCode = 'FUT1'  # Фьючерсы
    OptServerCode = 'OPT1'  # Опционы
    FxServerCode = 'FX1'  # Валютный рынок

    # Для реального счета:
    # 1. Привязать торговый аккаунт
    # 2. Выписать токен
    UserName = ''
    RefreshToken = ''
    PortfolioStocks = 'D00000'  # Фондовый рынок
    AccountStocks = 'L01-00000F00'
    PortfolioFutures = '0000PST'  # Срочный рынок
    AccountFutures = '0000PST'
    PortfolioFx = 'G00000'  # Валютный рынок
    AccountFx = 'MB0000000000'