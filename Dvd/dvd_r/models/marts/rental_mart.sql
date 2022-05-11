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
	,cl.email 												AS customer_email
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
FROM {{ source('dvd', 'rental') }}                          AS r
LEFT JOIN {{ source('dvd', 'inventory') }}                  AS i
	ON r.inventory_id = i.inventory_id
LEFT JOIN {{ ref('film_list') }}                            AS f
	ON i.film_id = f.film_id
LEFT JOIN {{ ref('customer_list') }}                        AS cl
	ON r.customer_id = cl.customer_id
LEFT JOIN {{ ref('stor_list') }}                            AS sl
	ON i.store_id = sl.store_id
LEFT JOIN {{ ref('stuff_list') }}                           AS sl2
	ON r.staff_id = sl2.staff_id
