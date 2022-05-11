# Список адресов

SELECT
	a.address_id 
	,a.address 
	,a.district 
	,a.postal_code
	,a.phone
	,a.last_update
	,c2.city
	,c3.country
FROM address a
LEFT JOIN dvd."dvd-rental".city c2 ON a.city_id = c2.city_id
LEFT JOIN dvd."dvd-rental".country c3 ON c2.country_id = c3.country_id



# Список покупателей

SELECT
	c.customer_id
	,c.last_name 
	,c.email 
	,a.phone 
	,a.address
	,c3.country 
	,c2.city
	,c.create_date 
	,c.last_update 
	,c.active 
FROM dvd."dvd-rental".customer c
LEFT JOIN dvd."dvd-rental".address a ON a.address_id = c.address_id
LEFT JOIN dvd."dvd-rental".city c2 ON a.city_id = c2.city_id
LEFT JOIN dvd."dvd-rental".country c3 ON c2.country_id = c3.country_id


SELECT
	f.film_id
	,f.title 
	,f.description 
	,f.release_year 
	,l."name" 
	,f.rental_duration 
	,f.rental_rate 
	,f.length 
	,f.rating
	,c."name" 
	,a.first_name 
	,a.last_name
FROM dvd."dvd-rental".film f 
LEFT JOIN dvd."dvd-rental".film_category fc ON f.film_id = fc.film_id
LEFT JOIN dvd."dvd-rental".category c ON fc.category_id = c.category_id
LEFT JOIN dvd."dvd-rental".film_actor fa ON f.film_id = fa.film_id
LEFT JOIN dvd."dvd-rental".actor a ON fa.actor_id = a.actor_id
LEFT JOIN dvd."dvd-rental"."language" l ON f.language_id = l.language_id



SELECT
	r.rental_id 
	,r.rental_date 
	,r.return_date
	,EXTRACT(DAY FROM (r.return_date - r.rental_date)) AS DAY
FROM dvd."dvd-rental".rental r 

SELECT
	f.film_id 
	,f.rental_duration 
	,f.rental_rate 
	,f.replacement_cost 
	,round(f.rental_rate / rental_duration, 2) AS rent_for_day
FROM dvd."dvd-rental".film f
ORDER BY 1


SELECT
	r.rental_id
	,r.rental_date
	,r.inventory_id
	,r.customer_id
	,r.return_date
	,r.staff_id
	,i.film_id 
	,i.store_id
	,EXTRACT(DAY FROM (r.return_date - r.rental_date)) AS day_rent
FROM dvd."dvd-rental".rental r
LEFT JOIN dvd."dvd-rental".inventory i ON r.inventory_id = i.inventory_id  


























