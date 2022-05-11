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


SELECT * FROM dvd."dvd-rental".film_actor fa
LEFT JOIN dvd."dvd-rental".actor a ON fa.actor_id = a.actor_id

WITH actor AS a (
	SELECT 
)


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
	,c."name" AS category
--	,a.first_name 
--	,a.last_name
FROM dvd."dvd-rental".film f 
LEFT JOIN dvd."dvd-rental".film_category fc ON f.film_id = fc.film_id
LEFT JOIN dvd."dvd-rental".category c ON fc.category_id = c.category_id
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



SELECT
	r.rental_id
	,r.rental_date
	,r.return_date
	,EXTRACT(DAY FROM (r.return_date - r.rental_date))		AS day_rent
	,sl.address 											AS stor_address
	,sl.city 												AS stor_city
	,sl.country 											AS stor_country
	,f.title 												AS film_title
	,f.description											AS  film_description
	,f.release_year 										AS film_release_year
	,f.rental_duration 										AS film_rental_duration
	,f.rental_rate 											AS film_rental_rate 									
	,round(f.rental_rate / f.rental_duration, 3)			AS film_rent_for_day
	,f.length 												AS film_length
	,f.replacement_cost 									AS film_replacement_cost
	,f.rating 												AS film_rating
	,cl.first_name 											AS customer_first_name
	,cl.last_name 											AS customer_last_name
	,cl.email 												AS customer_address
	,cl.phone 												AS customer_phone
	,cl.address 											AS customer_address
	,cl.country 											AS customer_country
	,cl.city 												AS customer_city
	,sl2.first_name 										AS stuff_first_name
	,sl2.last_name 											AS stuff_last_name
	,sl2.address 											AS stuff_address
	,sl2.city 												AS stuff_city
	,sl2.country 											AS stuff_country
	,sl2.email 												AS stuff_email
	,sl2.username 											AS stuff_user_name
	,sl2."password" 										AS stuff_password
	,sl2.active 											AS stuff_active
	,sl2.picture 											AS stuff_picture
FROM dvd."dvd-rental".rental r
LEFT JOIN dvd."dvd-rental".inventory i 
	ON r.inventory_id = i.inventory_id
LEFT JOIN dvd."dvd-rental".film_list f 
	ON i.film_id = f.film_id
LEFT JOIN dvd."dvd-rental".customer_list cl 
	ON r.customer_id = cl.customer_id
LEFT JOIN dvd."dvd-rental".stor_list sl 
	ON i.store_id = sl.store_id
LEFT JOIN dvd."dvd-rental".stuff_list sl2 
	ON r.staff_id = sl2.staff_id 


SELECT 
	f.film_id
	,f.title 				AS film_title
	,f.description 			AS film_description
	,f.release_year 		AS film_release_year
	,l."name" 				AS film_language
	,f.rental_duration 		AS film_rental_duration
	,f.rental_rate 			AS film_rental_rate
	,f.length 				AS film_length
	,f.replacement_cost 	AS film_replacement_cost
	,f.rating 				AS film_rating
FROM dvd."dvd-rental".film  AS f
LEFT JOIN dvd."dvd-rental"."language" AS l 
	ON f.language_id = l.language_id 





















