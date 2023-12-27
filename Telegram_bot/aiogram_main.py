import asyncio
import threading
from Telegram_bot import help_commands
import pandas as pd
from Telegram_bot import send_message

from Config import Config
from Database import postgres_sql
from connection import dp, bot
from Trade_logic import draw_сhart

from aiogram import types
from aiogram.filters import Command
from aiogram.types import Message, InputFile


db = postgres_sql.Database()
df_account_info = db.get_table_from_db('SELECT * FROM params')
tg_admin_id = int(db.get_table_from_db("SELECT value FROM params WHERE param = 'tg_admin_id'")['value'].iloc[0])
tg_group_id = int(db.get_table_from_db("SELECT value FROM params WHERE param = 'tg_group_id'")['value'].iloc[0])



@dp.message(Command('start', 'help'))
async def command_start_handler(message: Message) -> None:
    try:
        if message.chat.id == tg_group_id:
            await bot.send_message(chat_id=message.chat.id,
                                   text=f'Добро пожаловать в главное меню.\n{help_commands.GROUP_HELP_COMMAND}')
        elif message.from_user.id == tg_admin_id:
            await bot.send_message(chat_id=message.chat.id,
                                   text=f'Добро пожаловать в главное меню.\n{help_commands.ADMIN_HELP_COMMAND}')

    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('open_pos'))
async def command_get_open_pos(message: Message) -> None:
    """Отправляет таблицу открытых позиций."""

    try:
        def get_message():
            print("[INFO] Запрос открытых позиций.")
            open_positions = db.get_table_from_db("""SELECT open_time, spread, direction, volume_rub AS vol, result_perc, 
                                                    result_rub FROM open_positions""")
            if not open_positions.empty:
                unrealize_pnl = open_positions['result_rub'].sum()
                comimssion = int(open_positions['vol'].sum() * Config.commission * 4)
                open_positions_str = open_positions.to_string(index=False)
                text = open_positions_str + f'\n\nНереализованный PnL: {str(unrealize_pnl)} RUB. Комиссия: {comimssion} RUB. \n' \
                                            f'PnL с учётом комиссии: {int(unrealize_pnl - comimssion)} RUB.'
                return text
            else:
                print('[INFO] Пустой датафрейм открытых позиций.')
                return "Открытых позиций нет.", None

        if message.chat.id == tg_group_id:
            print("[INFO] Отправка таблицы открытых позиций в группу.")
            text = get_message()
            await bot.send_message(chat_id=message.chat.id, text=text)
        elif message.from_user.id == tg_admin_id:
            print("[INFO] Отправка таблицы открытых позиций админу.")
            text = get_message()
            await bot.send_message(chat_id=message.from_user.id, text=text)

    except TypeError as ex:
        print(ex)
        await message.answer(str(ex))

@dp.message(Command('closed_pos'))
async def command_get_closed_pos(message: Message) -> None:
    """Отправляет статистику по закрытым позициям."""

    def get_close_pos_stat():
        closed_positions = db.get_table_from_db(
            """SELECT
                    COUNT(*) AS "Количество_сделок",
                    SUM(result_rub) AS "Суммарный_результат_руб",
                    AVG(result_rub) AS "Средний_результат_руб_на_сделку",
                    AVG(result_perc) AS "Средний_результат_в_процентах",
                    COUNT(CASE WHEN close_reason = 'sl' THEN 1 ELSE NULL END) AS "Количество_стопов",
                    COUNT(CASE WHEN close_reason = 'tp' THEN 1 ELSE NULL END) AS "Количество_тейков"
                FROM
                    closed_positions;""")

        closed_positions_info_str = []
        if closed_positions is None or closed_positions.empty:
            return "Нет данных"
        else:
            try:
                for column in closed_positions.columns:
                    value = closed_positions[column].iloc[0]
                    if value is None:
                        value = 0
                    closed_positions_info_str.append(column + ': ' + str(round(value, 2)))
                closed_positions_info = '\n'.join(closed_positions_info_str)
                return closed_positions_info

            except:
                return "При загрузке данных возникли проблемы."

    try:
        if message.chat.id == tg_group_id:
            await bot.send_message(chat_id=message.chat.id, text=get_close_pos_stat())
        elif message.from_user.id == tg_admin_id:
            await bot.send_message(chat_id=message.from_user.id, text=get_close_pos_stat())
    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('check_threads'))
async def command_check_threads(message: Message) -> None:
    """Отправляет список активных потоков"""
    try:
        if message.from_user.id == tg_admin_id:
            active_threads = threading.enumerate()
            active_threads_names = ['\nСписок активных потоков:']
            for thread in active_threads:
                active_threads_names.append(thread.name)
            active_threads_str = '\n'.join(active_threads_names)
            await bot.send_message(chat_id=message.from_user.id, text=active_threads_str)

    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('check_trading_allowed'))
async def command_check_trading_allowed(message: Message) -> None:
    """Отправляет значение параметра trading_allowed"""
    try:
        if message.from_user.id == tg_admin_id:
            trading_allowed = db.get_table_from_db("SELECT * FROM params WHERE param = 'trading_allowed'")
            trading_allowed_value = trading_allowed.loc[trading_allowed['param'] == 'trading_allowed', 'value'].iloc[0]
            message_text = f'Параметр trading_allowed_bd = {trading_allowed_value}.\nПараметр find_ideas_allowed_const = {Config.find_ideas_allowed}.'
            await bot.send_message(chat_id=message.from_user.id, text=message_text)

    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('change_trading_allowed'))
async def command_change_trading_allowed(message: Message) -> None:
    """Изменяет значение параметра trading_allowed"""
    try:
        if message.from_user.id == tg_admin_id:
            trading_allowed = db.get_table_from_db("SELECT * FROM params")
            trading_allowed_value = int(trading_allowed.loc[trading_allowed['param'] == 'trading_allowed', 'value'].iloc[0])

            if trading_allowed_value:
                trading_allowed.loc[trading_allowed['param'] == 'trading_allowed', 'value'] = 0
            else:
                trading_allowed.loc[trading_allowed['param'] == 'trading_allowed', 'value'] = 1

            trading_allowed_new = str(trading_allowed.loc[trading_allowed['param'] == 'trading_allowed', 'value'].iloc[0])
            db.add_table_to_db(trading_allowed, 'params', 'replace')
            message_text = f'Параметр trading_allowed_bd, равный {trading_allowed_value}, изменён на {trading_allowed_new}.'
            await bot.send_message(chat_id=message.from_user.id, text=message_text)

    except TypeError:
        await message.answer("Nice try!")

@dp.message(Command('pnl'))
async def command_get_pnl(message: Message) -> None:
    """Отправляет график PnL."""
    def get_plot_pnl():
        df_pnl = db.get_table_from_db("SELECT * FROM pnl ORDER BY timestamp ASC LIMIT 1000")
        plot = draw_сhart.plot_pnl(df_pnl)
        return plot
    try:
        if message.chat.id == tg_group_id:
            tg = send_message.TelegramSendMessage()
            tg.send_plot_to_telegram(get_plot_pnl(), 'График PnL.')
        elif message.from_user.id == tg_admin_id:
            tg = send_message.TelegramSendMessage()
            tg.send_plot_to_telegram(get_plot_pnl(), 'График PnL.', chat_id=message.from_user.id)


    except TypeError:
        await message.answer("Nice try!")

async def tg_main() -> None:
    print('\n[INFO] TG bot starting.')
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

def start_bot():
    asyncio.run(tg_main())
