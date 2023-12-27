from AlorPy import AlorPy  # Работа с Alor OpenAPI V2
from dotenv import load_dotenv
import os
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from alor_config import Config


load_dotenv()

bot_token = os.getenv('BOT_TOKEN')

bot = Bot(bot_token, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot=bot, parse_mode=ParseMode.HTML)

alor_token = os.getenv('ALOR_TOKEN')
postgres_server_ip = os.getenv('postgres_server_ip')
postgres_user = os.getenv('postgres_user')
postgres_password = os.getenv('postgres_password')

apProvider = AlorPy(Config.UserName, alor_token)
