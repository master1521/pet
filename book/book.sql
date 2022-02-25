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





--Список из 10 самых продоваемых книг?





--Список из 10 самых продоваемых авторов?
--Список из 10 городов в каких покупают больше всего книг? 
--Список из 5 самых популярных жанров 

--Посчитайте количество продаж по годам










