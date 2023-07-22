from pytube import Playlist
import os
from dotenv import dotenv_values

import telebot
from telebot import types
from telebot.types import Message, CallbackQuery
from telebot import logger
from CustomLogger import tg_handler, info_logger
from TgHook import link_from_playlist, get_audio, send_audio_to_tg
logger.addHandler(tg_handler)

CONFIG = dotenv_values("./env/TG.env")
TOKEN = CONFIG.get('TOKEN')
HOME_DIR = '/home/master1521/Desktop/TgYouTube3'
bot = telebot.TeleBot(token=TOKEN)
url_from_user = ''


@bot.message_handler(commands=["start"])
def echo(massage: Message):
    bot.send_message(chat_id=massage.chat.id, text=f"Привет, {massage.from_user.username} \n"
                                                   f"Отправь мне ссылку на ютуб видео, чтобы получить аудио")


@bot.message_handler(regexp='https://www.youtube.com.*') 
def command_help(message: Message):
    global url_from_user
    url_from_user = message.text

    if link_from_playlist(message.text):
        markup = types.InlineKeyboardMarkup()
        bt1 = types.InlineKeyboardButton(text='Скачать 1 аудио', callback_data='get_audio')
        bt2 = types.InlineKeyboardButton(text='Скачать плейлист', callback_data='get_playlist')
        markup.row(bt1, bt2)

        bot.send_message(chat_id=message.chat.id, text="Вы скинули ссылку на аудио из плейлиста. Что вы хотите скачать?",
                         reply_markup=markup)
    else:
        whait_message = bot.send_message(chat_id=message.chat.id, text="Скачиваю...")
        song_title = get_audio(url_from_user)
        send_audio_to_tg(audio_path=song_title, chat_id=message.chat.id)
        bot.delete_message(chat_id=whait_message.chat.id, message_id=whait_message.id)
        os.remove(song_title)


@bot.callback_query_handler(func=lambda callback: True)
def collback_message(callback: CallbackQuery):
    bot.delete_message(chat_id=callback.message.chat.id, message_id=callback.message.id)

    if callback.data == 'get_audio':
        whait_message = bot.send_message(chat_id=callback.message.chat.id, text="Скачиваю...")
        song = get_audio(url_from_user)
        send_audio_to_tg(audio_path=song, chat_id=callback.message.chat.id)
        bot.delete_message(chat_id=whait_message.chat.id, message_id=whait_message.id)
        os.remove(song)
    elif callback.data == 'get_playlist':
        whait_message = bot.send_message(chat_id=callback.message.chat.id, text="Скачиваю...")

        play_list = Playlist(url=url_from_user)
        if play_list:
            info_logger.info(f'Downloading audio from play_list: {play_list.title}')
            info_logger.info(f'Audio in playlist: {len(play_list.video_urls)}')

            count = 0
            for video_url in play_list.video_urls:
                try:
                    song = get_audio(video_url)
                    send_audio_to_tg(audio_path=song, chat_id=callback.message.chat.id)
                    os.remove(song)
                except KeyError:
                    bot.send_message(chat_id=callback.message.chat.id, text="Ограниченный доступ у плейлиста.")
                    pass
                except Exception as err:
                    info_logger.error(f"Can't download audio: {video_url}", exc_info=True)
                    pass
                finally:
                    count += 1
                    info_logger.info(f'Received {count} out of {len(play_list.video_urls)}')
        else:
            bot.send_message(chat_id=callback.message.chat.id, text="Плейлист должен быть доступен по ссылке")
        bot.delete_message(chat_id=whait_message.chat.id, message_id=whait_message.id)


@bot.message_handler()
def all_message(message: Message):
    bot.send_message(chat_id=message.chat.id, text=f'Я не знаю такую команду, скинь мне ссылку на ютуб видео')


if __name__ == "__main__":
    bot.infinity_polling()



