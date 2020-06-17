import os
import telebot
from pathlib import Path
from dotenv import load_dotenv


class TelegramEventer():
    messages = {
        'failed': 'Everything is bad:(\n',
        'success': 'We did it!\n',
        'up_for_retry': "We're trying. You should believe us...\n"
    }

    def __init__(self,
                 use_proxy=True, env_path=Path('.') / '.env',
                 *arg, **kwargs):
        load_dotenv(dotenv_path=env_path)
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.use_proxy = use_proxy
        self.chat_id_for_send = os.getenv("TELEGRAM_CHAT_ID")

        if use_proxy:
            self.proxy = os.getenv("TELEGRAM_PROXY")

    def bot_init(self):
        if self.proxy:
            telebot.apihelper.proxy = {
                'https': self.proxy}
        return telebot.TeleBot(self.token)

    def send_sla(self, **kwargs):
        bot = self.bot_init()
        message = 'SLA was missed on ..'
        bot.send_message(self.chat_id_for_send, message)

    def send_message(self, context):
        task_id = context['ti'].task_id
        task_state = context["ti"].state
        if (task_id == 'all_success' and task_state == 'success') \
                or task_state != 'success':
            bot = self.bot_init()
            dag_id = context['dag'].dag_id
            bot.send_message(self.chat_id_for_send,
                             f'''{self.messages[task_state]} Dag_id-{dag_id}, task_id-{task_id}''')
