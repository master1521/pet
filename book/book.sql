-- CREATE SCHEMA dim



--Список из покупатели которые купиль больше всего книг?
--Посчитайсе минимальный, средний, максимальный платеж и сумму всех платежей для каждого покупателя

--EXPLAIN ANALYZE 
SELECT
--	c.id_customer 
	c.first_name
	,c.last_name
	,min(b.price) AS min_payment
	,round(avg(b.price), 2) AS avg_payment
	,max(b.price) AS max_payment
	,sum(b.price) AS sum_all_payment
	,count(p.id_payment) AS count_payment
FROM payment p
LEFT JOIN book b ON p.id_book = b.id_book
LEFT JOIN customer c ON p.id_customer = c.id_customer 
GROUP BY c.id_customer 
ORDER BY count_payment DESC
LIMIT 10


--Список из 10 самых продоваемых книг
SELECT
	b.id_book
	,b.title
	,count(p.id_payment) AS books_sold
FROM payment p
LEFT JOIN book b ON b.id_book = p.id_book
GROUP BY b.id_book
ORDER BY books_sold DESC
LIMIT 10


--Список из 10 самых продоваемых авторов
WITH autor_x AS (
SELECT 
	id_book
	,a.first_name
	,a.last_name
FROM book_autor ba 
LEFT JOIN autor a ON a.id_autor = ba.id_autor
), book_x AS (
SELECT
	p.id_book 
	,count(p.id_payment)
FROM payment p
GROUP BY p.id_book
ORDER BY 2 DESC
)
SELECT
	a.first_name 
	,a.last_name
	,b.count
FROM book_x b
LEFT JOIN autor_x a ON b.id_book = a.id_book 
ORDER BY count desc 

SELECT * FROM book_autor ba ORDER BY 1 ASC  


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


_________________________________________

CREATE SCHEMA main

CREATE TABLE main.country (
	id_country serial PRIMARY KEY
	,country varchar(50) NOT NULL)

CREATE TABLE main.city (
	id_city serial PRIMARY KEY
	,id_country integer NOT NULL REFERENCES book.main.country (id_country)
	,city varchar(50) NOT NULL)

CREATE TABLE main.address (
	id_address serial PRIMARY KEY
	,id_city integer NOT NULL REFERENCES book.main.city (id_city)
	,address varchar(200) NOT NULL)

CREATE TABLE main.gender (
	id_gender serial PRIMARY KEY
	,gender varchar(10))

CREATE TABLE main.shop (
	id_shop serial PRIMARY KEY
	,id_address int NOT NULL REFERENCES book.main.address (id_address)
	,name varchar(150) NOT NULL
	,phone varchar(20)
)

CREATE TABLE main.customer (
	id_customer serial PRIMARY KEY
	,first_name varchar(50) NOT NULL
	,last_name varchar(50) NOT NULL
	,id_address integer NOT NULL REFERENCES book.main.address (id_address)
	,phone varchar(20)
	,email varchar(100)
	,id_gender int NOT NULL REFERENCES book.main.gender (id_gender))
	
CREATE TABLE main.autor (
	id_autor serial PRIMARY KEY
	,first_name varchar(100) NOT NULL
	,last_name varchar(100) NOT NULL
	,id_gender INTEGER NOT NULL REFERENCES book.main.gender (id_gender))

CREATE TABLE main.publisher (
	id_publisher serial PRIMARY KEY
	,publisher varchar(100) NOT NULL)

CREATE TABLE main.book (
	id_book serial PRIMARY KEY,
	title varchar(100) NOT NULL,
	price decimal(10,2) NOT NULL,
	pages int NOT NULL,
	release_year timestamp NOT NULL,
	id_publisher int NOT NULL REFERENCES book.main.publisher (id_publisher))

CREATE TABLE main.gener (
	id_gener serial PRIMARY KEY
	,gener varchar(50) NOT NULL)
	
CREATE TABLE main.book_gener (
	id_book integer NOT NULL
	,id_gener integer NOT NULL
	,PRIMARY KEY (id_book, id_gener)
	,FOREIGN KEY (id_book) REFERENCES book.main.book (id_book)
	,FOREIGN KEY (id_gener) REFERENCES book.main.gener (id_gener))
	
CREATE TABLE main.book_autor (
	id_book integer NOT NULL
	,id_autor integer NOT NULL
	,PRIMARY KEY (id_book, id_autor)
	,FOREIGN KEY (id_book) REFERENCES book.main.book (id_book)
	,FOREIGN KEY (id_autor) REFERENCES book.main.autor (id_autor))

CREATE TABLE main.date (
	id_date serial PRIMARY KEY
	,dt timestamp NOT NULL)


    

