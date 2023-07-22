import yt_dlp
from dotenv import dotenv_values
import os

import telebot
from telebot import logger
from CustomLogger import MyLogger, tg_handler, info_logger
logger.addHandler(tg_handler)

CONFIG = dotenv_values("./env/TG.env")
TOKEN = CONFIG.get('TOKEN')
HOME_DIR = '/home/master1521/Desktop/TgYouTube3'
bot = telebot.TeleBot(token=TOKEN)
url_from_user = ''

def link_from_playlist(url: str):
    info_logger.info('Checking the link type')
    if 'list=' in url:
        info_logger.debug('Link to a video from the playlist')
        return True
    else:
        info_logger.debug('The regular link for the video')
        return False


def get_audio(url):
    ydl_opts = {
        'logger': MyLogger(),
        'format': 'bestaudio/best',
        'paths': {'home': HOME_DIR},
        'noplaylist': True,
        'outtmpl': {'default': '%(title)s.%(ext)s'},
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            video_info = ydl.extract_info(url=url, download=False)
            downloaded_file_path = ydl.prepare_filename(video_info).replace('.webm', '.mp3')
            info_logger.info(f"Downloading audio: {video_info['title']} {url}")
            ydl.download(url_list=[url])
            return downloaded_file_path
        except Exception:
            info_logger.error(f"Can't download audio: {url}", exc_info=True)
            pass

def send_audio_to_tg(audio_path, chat_id):
    with open(file=audio_path, mode='rb') as f:
        bot.send_audio(chat_id=chat_id, audio=f)


