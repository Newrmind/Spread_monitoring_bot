from connection import apProvider
import time


def get_quotes(symbols_list_uniq: list):

    exchange = 'MOEX'
    last_trades = {}
    ful_prices = {}
    print(f'Запрос цен для тикеров: {symbols_list_uniq}')

    retries = 10  # Максимальное количество попыток
    while retries > 0:
        try:
            for symbol in symbols_list_uniq:
                quotes = apProvider.GetQuotes(f'{exchange}:{symbol}')[0]
                last_trades[symbol] = quotes['last_price']
                ful_prices[symbol] = round((quotes['last_price'] * quotes['lotsize']), 4)

            break  # Выход из цикла при успешном запросе

        except Exception as ex:
            print(ex)
            time.sleep(10)
            retries -= 1  # Уменьшение количества попыток
    else:
        print(f"Не удалось получить данные для {symbols_list_uniq} после нескольких попыток")

    return last_trades, ful_prices


if __name__ == '__main__':
    last_trades, ful_prices = get_quotes(['SBER', 'GAZP'])
    print(last_trades, ful_prices)
    print(f'Лотов сбер = {1000000 / ful_prices["SBER"]}, лотов газ = {1000000 / ful_prices["GAZP"]}')










"""
# Определение функции для обработки новых котировок
def handle_new_quotes(response):
    symbol = response['data']['symbol']
    last_price = response['data']['last_price']
    print(f'Новая котировка для {symbol}: {last_price}')

# Перебор всех котировок в списке и подписка на каждую
for symbol in symbols_list_uniq:
    print(f'Подписка на котировки {exchange}.{symbol}')

    # Установка обработчика OnNewQuotes
    apProvider.OnNewQuotes = handle_new_quotes

    # Получение кода подписки
    guid = apProvider.QuotesSubscribe(exchange, symbol)
    print(f'Код подписки для {symbol}: {guid}')

sleep_secs = 1  # Кол-во секунд получения котировок
# Ожидание для демонстрации - можно использовать бесконечный цикл, как в предыдущем примере
time.sleep(sleep_secs)
"""