import random
from bs4 import BeautifulSoup
import requests
import json
import csv
from time import sleep

# Пример парсинга простого сайта
# Задача: собрать таблицы калорийности для каждой категории в отдельный файл

# 1)Собираем список категорий
# url = "https://health-diet.ru/table_calorie/"
#
# # Добавляем заголовки для запроса, чтобы сразу не спалили что это бот
#
# headers = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
#     ,'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:94.0) Gecko/20100101 Firefox/94.0'
# }
#
# # Делаем запрос на получение страницы
# r = requests.get(url=url, headers=headers)
# print(r.status_code)
#
# # Сохраним результат запроса страницы в текст
# src = r.text
#
# # Сохраним страницу в файл
# with open('index.html', 'w') as file:
#     file.write(src)

# Откроем файл и сохраним страницу в переменную
# with open('index.html', 'r') as file:
# 	src = file.read()
#
# soup = BeautifulSoup(src, 'lxml')
# # print(soup.prettify())


# # 2)Получить названия всех категорий и ссылки на них
# all_product_href = soup.find_all('a', class_="mzr-tc-group-item-href")
# # print(all_product_href)
#
# # Сохраним название категории и ссылку в словарь
# all_categories_dict = {}
# for link in all_product_href:
# 	name_category = link.text
# 	url_category = 'https://health-diet.ru' + link.get('href')
# 	# print(f'{name_category}: {url_category}')
#
# 	all_categories_dict[name_category] = url_category
# # print(all_categories_dict)
#
#
# # Создадим файл в json формате c названием категорий и ссылками
# with open('all_categories_dict.json', 'w') as file:
# 	json.dump(all_categories_dict, file, indent=4, ensure_ascii=False)

# indent=4 Отступ в файле (чтобы было не в 1 строку)
# ensure_ascii=False (Чтобы не было проблем с кодировкой)


# Добавил информацию из файла в переменную all_categories
with open('all_categories_dict.json', 'r') as file:
	all_categories = json.load(file)
# print(all_categories)


# 2) В цикле зайти в каждую категорию и сохранить информацию о товаре в файл

iter_count = int(len(all_categories)) - 1
count_category = 0
print(f'Всего категорий {iter_count}')

for category, url_category in all_categories.items():
	# if count_category == 0:  # Это нужно чтобы не бомбить сайт запросами пока идут настройка (будем брать 1 страницу)

	# Заменим символы в категории которые не подойдут для названий файлов
	rep = [",", " ", "'", "-"]
	for item in rep:
		if item in category:
			category = category.replace(item, "_")
	# print(category)

	# Делаем новый запрос к каждой категории
	r = requests.get(url=url_category, headers={
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
		, 'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:94.0) Gecko/20100101 Firefox/94.0'
	})
	src = r.text

	# Сохраним категорию в html файл в папку data
	with open(f'data/{count_category}_{category}.html', 'w') as file:
		file.write(src)

	# Откроем файл категории для сохранения в переменную
	with open(f'data/{count_category}_{category}.html', 'r') as file:
		src = file.read()

	# Создадим экземпляр класса для парсинга
	soup = BeautifulSoup(src, 'lxml')

	# Проверяем страницу на наличие таблицы, если такой класс будет на странице, тогда мы пропускаем шаг цикла
	if soup.find(class_="uk-alert-danger") is not None:
		continue

	# Собираем заголовки таблицы
	table_head = soup.find(class_='mzr-tc-group-table').find('tr').find_all('th')
	product = table_head[0].text
	calories = table_head[1].text
	proteins = table_head[2].text
	fats = table_head[3].text
	carbs = table_head[4].text

	# Создаем фал категории и добавляем данные
	with open(f'data/{count_category}_{category}.csv', 'w', encoding='utf-8') as file:
		# file.write(f'{product},{calories},{proteins},{fats},{carbs}\n')
		writer = csv.writer(file)
		writer.writerow((product, calories, proteins, fats, carbs))

	# Собираем контент таблицы
	table_content = soup.find(class_='mzr-tc-group-table').find('tbody').find_all('tr')

	for item in table_content:
		# print(f'[---] {item}')
		td_list = item.find_all('td')
		product_ = td_list[0].find('a').text
		calories_ = td_list[1].text
		proteins_ = td_list[2].text
		fats_ = td_list[3].text
		carbs_ = td_list[4].text
		# print(product_)

		# Добавляем добавляем данные в файл категории
		with open(f'data/{count_category}_{category}.csv', 'a', encoding='utf-8') as file:
			writer = csv.writer(file)
			writer.writerow(
				(
					product_,
					calories_,
					proteins_,
					fats_,
					carbs_
				)
			)

	# Выводим информацию о ходе работы скрипта
	count_category += 1
	print(f"Итерация: {count_category}. {category} записано... ")
	iter_count -= 1

	if iter_count == 0:
		print("Работа завершена")
		break

	print(f"Осталось итераций {iter_count}")
	sleep(random.randrange(2, 4))
