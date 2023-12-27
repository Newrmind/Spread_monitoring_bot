import connection

class Config:

    # Подключение к PostgreSQL
    postgres_connection_info = {
        'host': '127.0.0.1',
        'dbname': 'alor',
        'user': connection.postgres_user,
        'password': connection.postgres_password,
        'port': '5432'
    }
    postgres_connection_server = {
        'host': connection.postgres_server_ip,
        'dbname': 'alor',
        'user': connection.postgres_user,
        'password': connection.postgres_password,
        'port': '5432'
    }

    timeframes = ['15m']
    sec_for_concat = 1728000  # Период для конкатенации данных (20 дн)
    analyze_period = 0.25  # Период загрузки свечных данных в часах

    find_ideas_allowed = True  # Флаг, разрешающий поиск сделок

    chat_id = 503034116

    bollinger_bands = 3
    sl = 2
    volume_for_trade = 1_000_000
    transposition_tp = 3600000,  # время, через которое будет изменён тейк
    commission = 0.00026  # размер комиссии в %
    blacklist_time = 1  # время в часах, через которое будет удалена позиция из чёрного списка