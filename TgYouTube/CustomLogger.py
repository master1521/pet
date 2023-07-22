import logging
from logging import Handler, LogRecord
import telebot
import sys

from dotenv import dotenv_values

CONFIG = dotenv_values("./env/TG.env")
TOKEN = CONFIG.get('TOKEN')
CHANNEL_CH1 = -1001933960953  # channel for error notification


class MyLogger:
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        print(msg)

    def error(self, msg):
        print(msg)

class TelegramBotHandler(Handler):
    def __init__(self, token: str, chat_id: str):
        super().__init__()
        self.token = token
        self.chat_id = chat_id

    def emit(self, record: LogRecord):
        bot = telebot.TeleBot(self.token)
        bot.send_message(
            self.chat_id,
            self.format(record)
        )

def filter_tg(record: LogRecord) -> bool:
    if 'infinity polling' in record.getMessage().lower():
        return False
    else:
        return True

formatter = logging.Formatter(
    fmt='file_name: {filename} | time: {asctime} | logging_level: {levelname} | logger_name: {name} | Message: {message}',
    style='{', datefmt='%Y-%m-%d %H:%M:%S')


tg_handler = TelegramBotHandler(token=TOKEN, chat_id=str(CHANNEL_CH1))
tg_handler.setFormatter(formatter)
tg_handler.addFilter(filter_tg)

info_logger = logging.getLogger(__name__)
info_logger.setLevel(logging.INFO)

info_handler = logging.StreamHandler(stream=sys.stdout)
info_logger.addHandler(info_handler)
info_handler.setFormatter(formatter)

