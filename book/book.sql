-- CREATE SCHEMA dim


--Список из покупатели которые купиль больше всего книг?
--Посчитайсе минимальный, средний, максимальный платеж и сумму всех платежей для каждого покупателя

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

-- или так
SELECT
	c.id_customer
	,c.first_name 
	,c.last_name 
	,min(b.price) OVER w AS min_payment
	,avg(b.price) OVER w AS avg_payment
	,max(b.price) OVER w AS max_payment
	,sum(b.price) OVER w sum_all_payment
	,count(p.id_payment) OVER w AS count_payment
--	,count(p.id_payment) OVER (PARTITION BY c.id_customer ORDER BY DESC) AS count_payment
FROM payment p
LEFT JOIN book b ON p.id_book = b.id_book
LEFT JOIN customer c ON p.id_customer = c.id_customer
WINDOW w AS (PARTITION BY c.id_customer)
ORDER BY count_payment DESC, id_customer ASC
--ORDER BY count_payment DESC


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
































