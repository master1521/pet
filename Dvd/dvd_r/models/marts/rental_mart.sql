SELECT
	r.rental_id                                             AS rental_rental_id
	,r.inventory_id                                         AS rental_inventory_id
	,r.customer_id                                          AS rental_customer_id
	,r.staff_id                                             AS rental_staff_id
	,r.rental_date                                          AS rental_date
	,r.return_date                                          AS rental_return_date
	,EXTRACT(DAY FROM (r.return_date - r.rental_date))		AS day_rent
	,i.store_id                                             AS stor_store_id
	,i.address 											    AS stor_address
	,i.city 												AS stor_city
	,i.country 											    AS stor_country
	,i.manager_staff_id                                     AS stor_manager_staff_id
	,i.film_id                                              AS film_film_id
	,i.title 												AS film_title
	,i.description											AS film_description
	,i.release_year 										AS film_release_year
	,i.rental_duration 										AS film_rental_duration
	,i.rental_rate 											AS film_rental_rate
	,round(i.rental_rate / i.rental_duration, 3)			AS film_rent_for_day
	,i.length 												AS film_length
	,i.replacement_cost 									AS film_replacement_cost
	,i.rating 												AS film_rating
	,cl.customer_id                                         AS customer_customer_id
	,cl.first_name 											AS customer_first_name
	,cl.last_name 											AS customer_last_name
	,cl.email 												AS customer_email
	,cl.phone 												AS customer_phone
	,cl.address 											AS customer_address
	,cl.country 											AS customer_country
	,cl.city 												AS customer_city
	,sl2.staff_id                                           AS stuff_staff_id
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
LEFT JOIN {{ ref('inventory_mart') }}                       AS i
	ON r.inventory_id = i.inventory_id
LEFT JOIN {{ ref('customer_list') }}                        AS cl
	ON r.customer_id = cl.customer_id
LEFT JOIN {{ ref('stuff_list') }}                           AS sl2
	ON r.staff_id = sl2.staff_id