
--Создаем схему
CREATE SCHEMA main

--Создаем таблицу страна
CREATE TABLE main.country (
	id_country serial PRIMARY KEY
	,country varchar(50) NOT NULL)

--Создаем таблицу город
CREATE TABLE main.city (
	id_city serial PRIMARY KEY
	,id_country integer NOT NULL REFERENCES book.main.country (id_country)
	,city varchar(50) NOT NULL)

--Создаем таблицу адрес
CREATE TABLE main.address (
	id_address serial PRIMARY KEY
	,id_city integer NOT NULL REFERENCES book.main.city (id_city)
	,address varchar(200) NOT NULL)

--Создаем таблицу пол
CREATE TABLE main.gender (
	id_gender serial PRIMARY KEY
	,gender varchar(10))

--Создаем таблицу магазинов
CREATE TABLE main.shop (
	id_shop serial PRIMARY KEY
	,id_address int NOT NULL REFERENCES book.main.address (id_address)
	,name varchar(150) NOT NULL
	,phone varchar(20))

--Создаем таблицу покупателей
CREATE TABLE main.customer (
	id_customer serial PRIMARY KEY
	,first_name varchar(50) NOT NULL
	,last_name varchar(50) NOT NULL
	,id_address integer NOT NULL REFERENCES book.main.address (id_address)
	,phone varchar(20)
	,email varchar(100)
	,id_gender int NOT NULL REFERENCES book.main.gender (id_gender))

--Создаем таблицу авторов
CREATE TABLE main.autor (
	id_autor serial PRIMARY KEY
	,first_name varchar(100) NOT NULL
	,last_name varchar(100) NOT NULL
	,id_gender INTEGER NOT NULL REFERENCES book.main.gender (id_gender))

--Создаем таблицу издателей
CREATE TABLE main.publisher (
	id_publisher serial PRIMARY KEY
	,publisher varchar(100) NOT NULL)

--Создаем таблицу книг
CREATE TABLE main.book (
	id_book serial PRIMARY KEY,
	title varchar(100) NOT NULL,
	price decimal(10,2) NOT NULL,
	pages int NOT NULL,
	release_year date NOT NULL,
	id_publisher int NOT NULL REFERENCES book.main.publisher (id_publisher))

--Создаем таблицу пол
CREATE TABLE main.gener (
	id_gener serial PRIMARY KEY
	,gener varchar(50) NOT NULL)

--Создаем таблицу жанров книг
CREATE TABLE main.book_gener (
	id_book integer NOT NULL
	,id_gener integer NOT NULL
	,PRIMARY KEY (id_book, id_gener)
	,FOREIGN KEY (id_book) REFERENCES book.main.book (id_book)
	,FOREIGN KEY (id_gener) REFERENCES book.main.gener (id_gener))

--Создаем таблицу книг и авторов
CREATE TABLE main.book_autor (
	id_book integer NOT NULL
	,id_autor integer NOT NULL
	,PRIMARY KEY (id_book, id_autor)
	,FOREIGN KEY (id_book) REFERENCES book.main.book (id_book)
	,FOREIGN KEY (id_autor) REFERENCES book.main.autor (id_autor))

--Создаем таблицу дат
CREATE TABLE main.date (
	id_date serial PRIMARY KEY
	,dt timestamp NOT NULL)
	
--Создаем таблицу продаж
CREATE TABLE main.sales (
	id_shop int NOT NULL REFERENCES main.shop (id_shop)
	,id_product int NOT NULL REFERENCES main.book (id_book)
	,id_customer int NOT NULL REFERENCES main.customer (id_customer)
	,id_date int NOT NULL REFERENCES main.date (id_date)
	,quantity int NOT NULL CHECK (quantity > 1)
	,PRIMARY KEY (id_shop, id_product, id_customer, id_date))


--Из основной даты можно достать нужные срезы 
SELECT
	id_date
	,dt AS date
	,to_char(dt, 'YYYY-MM-DD')::date AS ansi_date
    ,EXTRACT(YEAR FROM dt) as YEAR --Получить год
    ,EXTRACT(MONTH FROM dt) as MONTH  --Получить месяц
    ,EXTRACT(DAY FROM dt) as DAY --Получить месяц
    ,EXTRACT(ISODOW FROM dt) as week_day --Получить месяц
FROM main.date


--Итоговая таблица фактов по продажам
SELECT
	s.id_shop 
	,s.id_product 
	,s.id_customer
	,s.id_date 
	,to_char(d.dt, 'YYYY-MM-DD')::date AS ansi_date
	,s.quantity
	,b.price
	,s.quantity * b.price AS total_sum
FROM sales s
LEFT JOIN main.book b ON b.id_book = s.id_product 
LEFT JOIN main."date" d ON s.id_date = d.id_date


--ВОПРОСЫ
 
--Список из покупателей которые купиль больше всего книг?
--Посчитайсе минимальный, средний, максимальный платеж и сумму всех платежей для каждого покупателя
SELECT
	s.id_customer
	,count(id_product) AS count_book
	,min(b.price)
	,max(b.price)
	,round(avg(b.price), 2) AS avg
	,sum(b.price) AS sum
FROM sales s 
LEFT JOIN book b ON b.id_book = s.id_product
GROUP BY s.id_customer
ORDER BY count_book DESC
LIMIT 10


--Список из 10 самых продоваемых книг
WITH best_book AS (
	SELECT 
		id_product
		,count(*) AS count_book 
	FROM sales s
	GROUP BY id_product
	ORDER BY count_book DESC
	LIMIT 10
)
SELECT
--	*
	b.title
	,bb.count_book
FROM best_book bb
LEFT JOIN main.book b ON b.id_book = bb.id_product


--Сколько было проданно книг по городам?
--EXPLAIN ANALYSE
WITH shop_2 AS (
	SELECT
		s.id_shop
		,count(s.quantity) AS soold_books
	FROM main.sales s 
	GROUP BY s.id_shop
), city_2 AS (
	WITH address_2 AS (
		SELECT
			a.id_address
			,c.city
		FROM address a
		LEFT JOIN main.city c ON c.id_city = a.id_city
	)
	SELECT
		s.id_shop
		,a2.city
	FROM shop s
	LEFT JOIN address_2 a2 ON a2.id_address = s.id_address 
)
SELECT
	c2.city
	,s2.soold_books
FROM shop_2 s2
LEFT JOIN city_2 c2 ON c2.id_shop = s2.id_shop
ORDER BY soold_books DESC 


--Посчитайте количество проданых книг по годам
SELECT
	d.YEAR
	,count(s.quantity) AS sold_dooks
FROM sales s
LEFT JOIN (
	SELECT
		d.id_date
		,EXTRACT(YEAR FROM dt) as YEAR
	FROM "date" d) AS d ON d.id_date = s.id_date
GROUP BY d.YEAR
ORDER BY 1


--Покажите 2 последних платежа для каждого клиента
WITH t AS (
	SELECT 
		s.id_customer 
		,s.id_shop 
		,s.id_product 
		,s.quantity 
		,d.dt
		,ROW_NUMBER() OVER(PARTITION BY s.id_customer ORDER BY d.dt DESC)
	FROM sales s 
	LEFT JOIN "date" d ON d.id_date = s.id_date
)
SELECT * FROM t
WHERE "row_number" <= 2


-- Схема звезда

--Создаем схему
CREATE SCHEMA star

--Создаем таблицу дат
CREATE TABLE star.date (
	id_date int PRIMARY KEY
	,dt timestamp NOT NULL)

	
--Создаем таблицу магазинов
CREATE TABLE star.shop (
	id_shop int PRIMARY KEY 
	,name varchar(150) NOT NULL
	,address varchar(200) NOT NULL
	,city varchar(50) NOT NULL
	,country varchar(50) NOT NULL
	,phone varchar(20))

	
--Создаем таблицу книг
CREATE TABLE star.book (
	id serial PRIMARY KEY
	,id_book int NOT NULL
	,title varchar(100) NOT NULL
	,price decimal(10,2) NOT NULL
	,pages int NOT NULL
	,release_year date NOT NULL
	,publisher varchar(100)
	,gener varchar(50)
	,first_name_author varchar(100)
	,last_name_author varchar(100)
	,gender varchar(10))


--Создаем таблицу покупателей
CREATE TABLE star.customer (
	id_customer int PRIMARY KEY
	,first_name varchar(50) NOT NULL
	,last_name varchar(50) NOT NULL
	,address varchar(200) NOT NULL
	,city varchar(50) NOT NULL
	,country varchar(50) NOT NULL
	,phone varchar(20)
	,email varchar(100)
	,gender varchar(10) NOT NULL)


--Создаем таблицу продаж
CREATE TABLE star.sales (
	id_shop int NOT NULL REFERENCES star.shop (id_shop)
	,id_product int NOT NULL REFERENCES star.book (id)
	,id_customer int NOT NULL REFERENCES star.customer (id_customer)
	,id_date int NOT NULL REFERENCES star.date (id_date)
	,quantity int NOT NULL CHECK (quantity > 1)
	,PRIMARY KEY (id_shop, id_product, id_customer, id_date))
	


-- Создаем материализованное представление по книгам
CREATE MATERIALIZED VIEW m_book AS
WITH t AS (
	SELECT
		bg.id_book 
		,g.gener
	FROM main.book_gener bg 
	LEFT JOIN gener g ON g.id_gener = bg.id_gener
), autor_b AS (
	SELECT 
		ba.id_book
		,a.first_name
		,a.last_name 
		,a.gender
	FROM book_autor ba
	LEFT JOIN (SELECT * FROM main.autor a LEFT JOIN gender g ON g.id_gender = a.id_gender) a ON a.id_autor = ba.id_autor
)
SELECT
	b.id_book
	,b.title 
	,b.price 
	,b.pages 
	,b.release_year
	,b.id_publisher
	,t.gener
	,p.publisher
	,ab.first_name 
	,ab.last_name
	,ab.gender 
FROM main.book b
LEFT JOIN t ON t.id_book = b.id_book
LEFT JOIN main.publisher p ON p.id_publisher = b.id_publisher
LEFT JOIN autor_b ab ON ab.id_book = b.id_book


-- Создаем материализованное представление по адресам
CREATE MATERIALIZED VIEW m_address AS
SELECT
	a.id_address
	,c.id_city 
	,c2.id_country
	,a.address 
	,c.city 
	,c2.country
FROM address a
LEFT JOIN city c ON c.id_city = a.id_city 
LEFT JOIN country c2 ON c2.id_country = c.id_country


-- Создаем материализованное представление по магазинам
CREATE MATERIALIZED VIEW m_shop AS
SELECT
	s.id_shop 
	,s.id_address
	,s."name"
	,a.address
	,a.city 
	,a.country 
	,s.phone
FROM shop s
LEFT JOIN m_address a ON s.id_address = a.id_address


-- Создаем материализованное представление по покупателям
CREATE MATERIALIZED VIEW m_customer AS 
SELECT 
	c.id_customer
	,c.first_name 
	,c.last_name
	,ma.address 
	,ma.city 
	,ma.country
	,c.phone 
	,c.email
	,g.gender
FROM main.customer c
LEFT JOIN main.m_address ma ON ma.id_address = c.id_address
LEFT JOIN main.gender g ON g.id_gender = c.id_gender


-- Добавляем данные по датам
INSERT INTO star."date" (id_date, dt)
SELECT * FROM main."date" d	


-- Добавляем данные по книгам 
INSERT INTO star.book (id_book, title, price, pages, release_year, publisher, gener, first_name_author, last_name_author, gender)
SELECT
	id_book
	,title 
	,price 
	,pages 
	,release_year 
	,publisher 
	,gener 
	,first_name 
	,last_name 
	,gender 
FROM main.m_book

-- Добавляем данные по магазинам
INSERT INTO star.shop (id_shop, "name", address, city, country, phone)
SELECT * FROM m_shop 


-- Добавляем данные по покупателям
INSERT INTO star.customer (id_customer, first_name, last_name, address, city, country, phone, email, gender)
SELECT * FROM m_customer mc 

-- Добовляем данные по продажам
INSERT INTO star.sales (id_shop, id_product, id_customer, id_date, quantity)
SELECT * FROM main.sales s 



