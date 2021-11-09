import random
from bs4 import BeautifulSoup
import requests
import json
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
# s = requests.Session()  # Сессия сохранит состояние между запросами куки, заголовки и тд

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


count = 0
for game in games_url.items():
	print(game)
	count += 1

	if count == 3:
		break
