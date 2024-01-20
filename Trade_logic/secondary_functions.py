
def check_evening_trade(moscow_evening, ticker_1_evening, ticker_2_evening):
    """Проверяет, разрешена ли торговля на вечерней сессии."""

    if moscow_evening:
        if ticker_1_evening and ticker_2_evening:
            return True
        else:
            return False
    else:
        return True