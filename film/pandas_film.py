# РАБОТА В PANDAS
#
# --Для работы я использовал python 3.9 в pycharm + pandas
# --Был выбран Dataset по прокату фильмов
# --База была развернута из *.backup файла на локальной машине в PostgreSQL
#
# Цель: нужно ответить на вопросы используя pandas, и не используя SQL команды

# Импортируем нужные библиотеки
# import pandas as pd
# import sqlalchemy
# import psycopg2

# Создаем объект Engine для подключения к серверу PostgreSQL на localhost
# engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:xxxxxxx@localhost:xxxx/postgres')

# Меняем настройки вывода количества столбцов
# pd.set_option('display.max_columns', 20)

# Получаем нужные таблицы из postgre в формате DataFrame
# film = pd.read_sql_table('film', engine, schema='public')
# actor = pd.read_sql_table('actor', engine, schema='public')
# film_actor = pd.read_sql_table('film_actor', engine, schema='public')
# address = pd.read_sql_table('address', engine, schema='public')
# payment = pd.read_sql_table('payment', engine, schema='public')
# customer = pd.read_sql_table('customer', engine, schema='public')
# city = pd.read_sql_table('city', engine, schema='public')
# country = pd.read_sql_table('country', engine, schema='public')
# rental = pd.read_sql_table('rental', engine, schema='public')
# film_category # надо получить из бд в csv
# category # надо получить из бд в csv

# ВОПРОСЫ
# 1) Выведите уникальные названия регионов из таблицы адресов
# print(address['district'].unique())


# 2) Доработайте запрос из предыдущего задания, чтобы запрос выводил только те регионы, названия которых начинаются на "K"
# и заканчиваются на "a", и названия не содержат пробелов
# print(address[
#         (address['district'].str.startswith('K')) &
#         (address['district'].str.endswith('a')) &
#         (~address['district'].str.contains(' '))
#       ]['district'].head(50).unique())


# 3) Получите из таблицы платежей за прокат фильмов информацию по платежам, которые выполнялись
# в промежуток с 17 марта 2007 года по 19 марта 2007 года включительно, и стоимость которых превышает 1.00
# pay = payment[
#         (payment['payment_date'].dt.date >= pd.to_datetime('2007-03-17', dayfirst=True)) &
#         (payment['payment_date'].dt.date <= pd.to_datetime('2007-03-19', dayfirst=True)) &
#         (payment['amount'] >= 1)]


# 4) Выведите информацию о 10-ти последних платежах за прокат фильмов.
# p1 = payment[['payment_id', 'payment_date', 'amount']]
# print(p1.sort_values(by='payment_date', ascending=False).head(10))


# 5) Выведите следующую информацию по покупателям:
# -- Фамилия и имя (в одной колонке через пробел)
# -- Электронная почта
# -- Длину значения поля email

# c1 = customer[['first_name','last_name','email', 'last_update']]
# c1['name'] = c1['first_name'].str.cat(c1['last_name'], sep=' ')
# # c1['name'] = c1[['first_name', 'last_name']].apply(lambda x: ' '.join(x), axis = 1)
# c2 = c1[['name', 'email', 'last_update']]
# c2['len_email'] = c2['email'].str.len()
# print(c2.head())


# 6) Выведите одним запросом активных покупателей, имена которых Kelly или Willie.
# --Все буквы в фамилии и имени из нижнего регистра должны быть переведены в высокий регистр.

# c1 = customer[['first_name', 'last_name']]
# c1 = customer[
#         ((customer.first_name == 'Kelly') | (customer.first_name == 'Willie')) &
#         (customer.active == True)
#       ].head(10)
# c1['f_name'] = c1['first_name'].str.upper()
# c1['l_name'] = c1['last_name'].str.upper()
# c2 = c1[['customer_id', 'f_name', 'l_name']]
# print(c2)

# 7)Выведите для каждого покупателя его адрес проживания, город и страну.
# c1 = customer[['customer_id', 'address_id', ]]
# a1 = address[['address_id', 'city_id', 'address']]
# city_1 = city[['city_id', 'country_id', 'city']]
# country_1 = country[['country_id', 'country']]
# customer_address_city_country = c1.merge(a1, how='left', on='address_id').merge(city_1, how='left', on='city_id')\
#     .merge(country_1, how='left', on='country_id' )
#
# customer_address_city_country_2 = customer_address_city_country[['customer_id', 'address', 'city', 'country']]
# print(customer_address_city_country_2)


# 8) Посчитайте для каждого магазина количество его покупателей.
# print(customer.groupby(['store_id']).count()['customer_id'])


# 8.1) Доработайте запрос и выведите только те магазины, у которых количество покупателей больше 300-от.
# customer_store = customer.groupby(['store_id']).count()['customer_id'].reset_index()
# print(customer_store[
#           customer_store['customer_id'] > 300
#       ])


# 9) Посчитайте для каждого покупателя:
# -- Максимальный платеж
# -- Минимальный платеж
# -- Сумму по всем платежам
# -- Колличество платежей
# print(payment.groupby('customer_id').agg({'amount': [min, max, sum], 'payment_id': 'count'}))


# 10) Добавьте столбец с классификацией длины фильма и посчитайте колличество в каждой категории
# - длинна < 60 - Короткий
# - длинна >= 60 и длинна < 130 - Средний
# - длинна > 130 - Длинный

# def film_length(length_film):
#     if length_film < 60:
#         return 'Короткий'
#     elif length_film >= 60 and length_film < 130:
#         return 'Срединй'
#     elif length_film >= 130:
#         return 'Длинный'
#     else:
#         return 'Ошибка'
#
# film['length_film'] = film['length'].apply(film_length)
# # print(film.head())
#
# film_l = film[['film_id', 'length', 'length_film']]
#
# print(film_l.head(10))
# print(film_l['length_film'].value_counts())

# 11) Посчитайте стоимость аренды фильма за день в новом столбце
# film['rate_film_day'] = film.apply(lambda row: round(row['rental_duration']/row['rental_rate'], 2), axis=1)
# print(film.head())


# 12) Найтите имена всех актеров снимавшихся в фильме 'Alabama Devil' и 'Airport Pollock'
# f_a = film_actor.merge(film, how='left', left_on='film_id', right_on='film_id').merge(actor, how='left', on='actor_id')
# f_a2 = f_a[['actor_id', 'film_id', 'title', 'first_name', 'last_name']]
# print(f_a2[f_a2['title'].str.contains('Alabama Devil|Airport Pollock', case=False)].sort_values('title'))


# 13) Выгрузите таблицы category, film_category из БД в CSV формате и импортируйте данные из файлов создав dataframe
# --Выведите названия категорий для фильмов id: 36, 44, 478, 900

# Копируем файлы в CSV через psql
# postgres=# \COPY (Select * From film_category) To 'C:\Users\lis\Desktop\22\film_category.csv' DELIMITER ',' CSV HEADER;
# COPY 1000
# postgres=# \COPY (Select * From category) To 'C:\Users\lis\Desktop\22\category.csv' DELIMITER ',' CSV HEADER;
# COPY 16

# film_category = pd.read_csv('C:\\Users\\lis\\Desktop\\22\\film_category.csv')
# print(film_category.head())
# print(film_category.info())

# category = pd.read_csv('C:\\Users\\lis\\Desktop\\22\\category.csv')
# print(category.head())
# print(category.info())

# id = [36, 44, 478, 900]
# fc = film_category.merge(category, how='left', left_on='category_id', right_on='category_id').merge(film, how='left', on='film_id')
# fc2 = fc[['film_id', 'title', 'name']]
# fc2.columns = ['film_id', 'title', 'name_category']
# print(fc2.loc[fc2['film_id'].isin(id)])


# 14) Создайте датафрейм из 4 столбцов и 10 строк и заполните любыми данными
# сities = pd.DataFrame([
#     [1, 'Cordelia', 'Cluet', 'ccluet0@sohu.com']
#     ,[2, 'Colan', 'Mattacks', 'cmattacks1@nba.com']
#     ,[3, 'Ericha', 'Torresi', 'etorresi2@chron.com']
#     ,[4, 'Zorah', 'Lind', 'zlind3@sogou.com']
#     ,[5, 'Dena', 'Spera', 'dspera4@about.me']
#     ,[6, 'Aubrey', 'Tayloe', 'atayloe5@com.com']
#     ,[7, 'Antonella', 'New', 'anew6@jiathis.com']
#     ,[8, 'Bond', 'Roseblade', 'broseblade7@epa.gov']
#     ,[9, 'Viviyan', 'Goter', 'vgoter8@cbc.ca']
#     ,[10, 'Babara', 'Constable', 'bconstable9@phoca.cz']]
#     ,columns=['id', 'first_name', 'last_name', 'email']
# )
# print(сities)


# 15) Определите колличество фильмов по рейтингам в каждой категории
# fc = film_category.merge(category, how='left', on='category_id').merge(film, how='left', on='film_id')
# fc2 = fc[['film_id', 'name', 'rating']]
# rating_category_count = fc2.pivot_table(index='name', columns='rating', values='film_id', aggfunc='count', margins=True).reset_index()
# print(rating_category_count)

