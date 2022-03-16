
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


	
---------------------------------------------------------------------

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


--Итоговая таблица фактов продаж по продажам
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



-- Материализованное представление по книгам
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
--	*
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


SELECT * FROM m_book

---------------------------
--РАБОЧИЕ ВОПРОСЫ
 

--Список из покупатели которые купиль больше всего книг?
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
	bb.id_product
	,b.title
	,bb.count_book
FROM best_book bb
LEFT JOIN main.book b ON b.id_book = bb.id_product





------------------------------
-- НЕ РАБОЧИЕ ВОПРОСЫ

--Список из 10 городов, где больше всего покупают книги
SELECT
	t.city
	,count(p.id_payment)
FROM payment p
LEFT JOIN (
	SELECT 
		c.id_customer
		,c2.city
	FROM customer c
	LEFT JOIN address a ON c.id_customer = a.id_address
	LEFT JOIN city c2 ON a.id_city = c2.id_city) AS t ON t.id_customer = p.id_customer
GROUP BY t.city
ORDER BY 2 DESC
LIMIT 10


--Список из 10 самых популярных жанров
WITH gener_cte AS (
SELECT
	bg.id_book
	,g.gener 
FROM book_gener bg 
LEFT JOIN gener g ON g.id_gener = bg.id_gener
)
SELECT
	gener
	,count(p.id_book)
FROM payment p
LEFT JOIN gener_cte gc ON gc.id_book = p.id_book 
GROUP BY gener
ORDER BY 2 DESC
LIMIT 10


--Посчитайте количество продаж по годам
SELECT 
	EXTRACT(YEAR FROM payment_date) AS sales_year
	,count(id_payment)
FROM dim.payment
GROUP BY sales_year
ORDER BY sales_year ASC


--Последние 2 платежа для клиентов
WITH t AS (
SELECT 
	id_customer
	,b.price
	,payment_date
	,ROW_NUMBER () OVER (PARTITION BY p.id_customer ORDER BY payment_date DESC)
FROM payment p
LEFT JOIN book b ON b.id_book = p.id_book 
)
SELECT * FROM t
WHERE ROW_NUMBER <= 2 


















