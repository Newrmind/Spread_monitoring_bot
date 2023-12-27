import pandas as pd

def load_excel_to_dataframe(file_path: str, sheet_name=0):
    """
    Функция для выгрузки таблицы из excel в датафрейм.

    Параметры:
    file_path (str): Путь к excel файлу.
    sheet_name (str or int): Название листа в excel файле.

    Возвращает:
    pandas.DataFrame: Датафрейм, содержащий данные из excel файла.
    """
    # Загружаем excel файл в датафрейм
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    return df


def exel_write(df: pd.DataFrame, xlsx_file_path: str, sheet_name: str = 'Sheet1'):
    """Функция для сохранения датафрейма в excel.

    Параметры:
    df (pd.DataFrame): Датафрейм, который нужно сохранить в excel.
    xlsx_file_path (str): Путь к excel файлу.
    sheet_name (str): Название листа в excel файле. По умолчанию 'Sheet1'.
    """

    writer = pd.ExcelWriter(xlsx_file_path)
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    # Сохраняем excel файл
    writer.close()


if __name__ == '__main__':
    file_path = "C:/Users/Albert/Desktop/Spreads/Спреды.xlsx"
    spreads = load_excel_to_dataframe(file_path)

    for index, row in spreads.iterrows():
        instruments = row.pair.split('/')
        spreads.loc[index, 'ticker_1'] = instruments[0]
        spreads.loc[index, 'ticker_2'] = instruments[1]

    exel_write(spreads, file_path, 'spreads')