from bs4 import BeautifulSoup
import requests
import csv

class Client:
	def __init__(self):
		""" Конструктор создает атрибуты объекта"""
		self.session = requests.Session()  # Сессия сохранит состояние между запросами куки, заголовки и тд
		self.session.headers = {
			'Accept': 'text/html,application/xhtml+xml,application/xml;q=-1.9,image/avif,image/webp,*/*;q=0.8'
			, 'User-Agent': 'Mozilla/4.0 (X11; Ubuntu; Linux x86_64; rv:94.0) Gecko/20100101 Firefox/94.0'
		}
		self.games_url = []

	def link_generation(self, pages: int):
		""" Принимает количество страниц для генерации url
		и возвращает список со ссылками """

		list_url = []
		for page in range(1, pages):
			url = f"https://stopgame.ru/topgames?p={page}"
			list_url.append(url)

		return list_url

	def load_page(self, url):
		""" Принимает url и возвращает html страницу в виде текста """
		req = self.session.get(url)
		return req.text  # req.status_code

	def pars_page(self, src):
		""" Принимает html страницу в виде текста и создает объект bs4 """
		soup = BeautifulSoup(src, 'lxml')
		return soup

	def find_links(self, soup):
		""" Принимает объект bs4 ищет ссылки на страницы с играми, и возвращает список ссылок"""
		base_url = 'https://stopgame.ru'
		games_links_page = []

		for item in soup.find_all('div', class_="caption caption-bold"):
			uri = item.a['href']
			games_links_page.append(f'{base_url}{uri}')
		return games_links_page

	def add_info_in_file(self, file_, games_url_list):
		""" Принимает название файла и список со ссылками на карточки игр,
		возвращает информацию о добавленных строках в файл"""

		count = 0
		for link in games_url_list:
			# Получаем страницу
			src = self.load_page(link)

			# Создаем объект bs4
			soup = self.pars_page(src)

			try:
				score = soup.find('div', class_='score').text
			except AttributeError:
				score = 0

			img = soup.find('div', class_='image-game-logo').img['src']
			article_title = soup.find('div', class_='game-details').h1.a.get_text(strip=True)
			game_specs = soup.find('div', class_='game-specs').find_all('div', class_='game-spec')

			lst = []
			for i in game_specs:
				label = i.find('span', class_='label').get_text(strip=True)
				value = i.find('span', class_='value').get_text(strip=True)
				lst.append(label)
				lst.append(value)

			# Добавляем информацию в csv файл
			with open(f'{file_}.csv', 'a', encoding='utf-8') as file:
				writer = csv.writer(file)
				writer.writerow(
					(
						article_title,
						score,
						lst[1],
						lst[3],
						lst[5],
						lst[7],
						lst[9],
						img
					)
				)
			print(f'[INFO] Добавлено в файл {article_title},{score},{lst[1]},{lst[3]},{lst[5]},{lst[7]},{lst[9]},{img}')
			count += 1
		return print(f'[INFO] Всего добавлено {count} строк')

	""" Точка входа """
	def run(self, file_, page_: int):
		"""Запускает сценарий
		Принимает:
		 1)название файла file_ в который будет добавлена информация,
		 2)кол страниц для сбора page_

		Возвращает csv файл с данными фильмов """


		""" Создаем csv файл с заголовками для записи, это надо сделать
		до цикла в котором будет добавляться информация иначе файл будет без заголовков """

		HEADERS = (
			'title',
			'score',
			'platform',
			'genre',
			'release_date',
			'developer',
			'publisher',
			'img'
		)

		with open(f'{file_}.csv', 'w', encoding='utf-8') as file:
			writer = csv.writer(file)
			writer.writerow(HEADERS)

		""" Собираем ссылки на карточки игр """
		for link in self.link_generation(page_):
			# Получаем страницу в виде текста
			src = self.load_page(link)

			# Создаем объект bs4
			soup = self.pars_page(src)

			# Собираем ссылки на игры со страницы
			games_url_page = self.find_links(soup)
			self.games_url.extend(games_url_page)

		# Собираем информацию с карточки и добавляем в файл
		self.add_info_in_file(file_, self.games_url)

if __name__ == '__main__':
	person = Client()
	person.run('data', 3)
