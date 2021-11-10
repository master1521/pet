from random import randint
from bs4 import BeautifulSoup
import requests
import json
import csv
from time import sleep

# 				Соберем информацию о самых популярных играх с 2019 по 2022

# 1)Собрать список ссылок на странице игр
base_url = 'https://stopgame.ru'

# Переключение на следующую страницу происходит путем передачи номера страницы в параметр p, p=2, таким образом
# можно перебрать страницы каталога в цикле

# Делаем запрос
headers = {
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=-1.9,image/avif,image/webp,*/*;q=0.8'
	, 'User-Agent': 'Mozilla/4.0 (X11; Ubuntu; Linux x86_64; rv:94.0) Gecko/20100101 Firefox/94.0'
}
s = requests.Session()  # Сессия сохранит состояние между запросами куки, заголовки и тд

# games_dict = {}
# for page in range(1, 26):
# 	url = f"https://stopgame.ru/topgames?p={page}"
# 	req = s.get(url, headers=headers)
# 	src = req.text
#
# 	# Создаем объект bs4
# 	soup = BeautifulSoup(src, 'lxml')
#
# # 	# Находим нужные блоки на странице
# 	list_items = soup.find_all('div', class_="caption caption-bold")
#
# 	# Добавляем информацию в словарь
# 	for item in list_items:
# 		uri = item.a['href']
# 		title = item.a.get_text("|", strip=True)
# 		games_dict[title] = f'{base_url}{uri}'
# 		print(f'Добавлено в словарь: {title} {base_url}{uri}')
# 	sleep(random.randrange(2, 4))
# print(len(games_dict))
#
# 3)Сохраним в файл список игр
# with open('data_game/games_url.json', 'w') as file:
# 	json.dump(games_dict, file)

# 4)Откроем фал для просмотра
with open('data_game/games_url.json', 'r') as file:
	games_url = json.load(file)
# print(len(games_url))

# 5)Создаем csv файл с заголовками для записи до цикла
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

with open('data_game/games_info.csv', 'w', encoding='utf-8') as file:
	writer = csv.writer(file)
	writer.writerow(HEADERS)

# 6)Собираем информацию со страниц с играми и записываем в csv файл
count = 0
for title, link in games_url.items():
	# print(title, link)

	try:
		response = s.get(link, headers=headers)
		if response.status_code == 200:
			html = response.text
		else:
			print(f'Ошибка {response.status_code} на запросе {title} {link}')
	except Exception:
		print(f'Ошибка в запросе к {link}')
		pass

	soup = BeautifulSoup(html, 'lxml')
	# print(soup.prettify())

	# Собираем информацию с карточки
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
	# print(lst)

	# Добавляем информацию в csv файл
	with open('data_game/games_info.csv', 'a', encoding='utf-8') as file:
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
	# sleep(randint(2, 4))

	# Блок отладки
	count += 1
	# if count == 3:
	# 	break
print(f'[INFO] Всего добавлено {count} строки')