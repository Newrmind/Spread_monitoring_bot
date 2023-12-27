import requests
from Database.postgres_sql import Database
from connection import bot_token
from Config import Config


class TelegramSendMessage:

    def __init__(self, db: Database = Database(), bot_token: str = bot_token):
        self.db = db
        self.bot_token = bot_token
        self.tg_group_id = int(db.get_table_from_db("SELECT value FROM params WHERE param = 'tg_group_id'")['value'].iloc[0])

    def send_plot_to_telegram(self, buf, message_text, chat_id=None):
        print("[INFO] Запуск функции send_message.send_plot_to_telegram")
        if chat_id is not None:
            self.tg_group_id = chat_id

        # Открываем файл и отправляем его в Telegram
        url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
        files = {'photo': ('graph.png', buf, 'image/png')}  # формат передачи файла
        payload = {
            "chat_id": self.tg_group_id,
            "caption": message_text
        }
        response = requests.post(url, data=payload, files=files)
        print(f"[INFO] response = {response}")
        # Закрываем буфер
        buf.close()

