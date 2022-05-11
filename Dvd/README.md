
# Создание витрин и автоматизация с dbt

База данных “DVD проката” представляет бизнес-процессы магазина проката DVD.  

В базе данных “DVD проката” имеется 15 таблиц:  
**actor** — хранит данные об актёрах, включая их имя и фамилию.  
**film** — хранит данные о фильмах, включая их название, год выпуска, время, рейтинг и т.д.  
**film_actor** — хранит отношения между фильмами и актёрами.  
**category** — хранит данные о жанрах фильмов.  
**film_category** -хранит отношения между фильмами и жанрами.  
**store** — содержит данные магазина, включая данные о менеджере и адресе.  
**inventory** — хранит данные инвентаризации.  
**rental** — хранит данные об аренде.  
**payment** — хранит платежи клиента.  
**staff** — хранит данные персонала.  
**customer** — хранит данные покупателей.  
**address** — хранит адресные данные для сотрудников и клиентов.  
**city** — хранит названия городов.  
**country** — хранит названия стран.  

## ER-модель
![Alt-текст](img/ER.png)


Поднимаем контейнер с бд
~~~
docker run --name POG_DBT --rm -d -p 5555:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=dvd -v pg_dbt:/var/lib/postgresql/data postgres
~~~

Делаем дамп из локальной базы
~~~
pg_dump -h 127.0.0.1 -p 5432 -U postgres -W -Fc -d postgres -f dvd.dump
~~~

Разворачиваем дамп в контейнер
~~~
pg_restore -h localhost -p 5555 -U postgres -W -d dvd dvd.dump 
~~~


Устанавливаем dbt и postgres адаптер
~~~
pip install dbt-postgres
~~~


Создаем dbt проект и проверяем настройки конфигов
~~~
dbt init dvd_r
export DBT_PROFILES_DIR=$(pwd)
dbt debug
~~~


Создаем модель список пользователей
~~~
SELECT
	c.customer_id
	,c.last_name
	,c.email
	,a.phone
	,a.address
	,c3.country
	,c2.city
FROM {{ source('dvd', 'customer')}} AS c
LEFT JOIN {{ source('dvd', 'address')}} AS a ON a.address_id = c.address_id
LEFT JOIN {{ source('dvd', 'city')}} AS c2 ON a.city_id = c2.city_id
LEFT JOIN {{ source('dvd', 'country')}} AS c3 ON c2.country_id = c3.country_id
~~~

После компиляции получаем готовый SQL который раскатаем на основную схему dvd-rental
~~~
dbt compile
~~~
~~~
SELECT
	c.customer_id
	,c.last_name
	,c.email
	,a.phone
	,a.address
	,c3.country
	,c2.city
FROM "dvd"."dvd-rental"."customer" AS c
LEFT JOIN "dvd"."dvd-rental"."address" AS a ON a.address_id = c.address_id
LEFT JOIN "dvd"."dvd-rental"."city" AS c2 ON a.city_id = c2.city_id
LEFT JOIN "dvd"."dvd-rental"."country" AS c3 ON c2.country_id = c3.country_id
~~~



Раскатываем в виде view
~~~
dbt run -m customer_list
~~~

![Alt-текст](img/customer_list.png)


Создаем документацию для dbt проекта
~~~
dbt docs generate
~~~

Просмотр DAG 
~~~
dbt docs serve &
~~~
![Alt-текст](img/l1.png)



