SELECT
	p.payment_id                AS payment_id
	,p.customer_id              AS payment_customer_id
	,p.staff_id                 AS payment_staff_id
	,p.rental_id                AS payment_rental_id
	,p.amount
	,p.payment_date
	,rm.rental_rental_id
	,rm.rental_inventory_id
	,rm.rental_customer_id
	,rm.rental_staff_id
	,rm.rental_date
	,rm.rental_return_date
	,rm.day_rent
	,rm.film_film_id
	,rm.film_title
	,rm.film_description
	,rm.film_release_year
	,rm.film_rental_duration
	,rm.film_rental_rate
	,rm.film_rent_for_day
	,rm.film_length
	,rm.film_replacement_cost
	,rm.film_rating
	,s.staff_id
	,s.first_name               AS staff_first_name
	,s.last_name                AS staff_last_name
	,s.email                    AS staff_email
	,s.store_id                 AS staff_store_id
	,s.active                   AS staff_active
	,s.username
	,s."password"
	,s.picture
	,s.address                  AS staff_address
	,s.city                     AS staff_city
	,s.country                  AS staff_country
	,s.district
	,s.postal_code
	,s.phone                    AS staff_phone
	,cl.customer_id
	,cl.first_name
	,cl.last_name
	,cl.email
	,cl.phone
	,cl.address
	,cl.country
	,cl.city
	,cl.create_date
	,cl.last_update
	,cl.active
FROM {{ source('dvd', 'payment') }}             AS p
LEFT JOIN {{ ref('rental_mart') }}              AS rm
    ON p.rental_id = rm.rental_rental_id
LEFT JOIN {{ ref('stuff_list') }}               AS s
    ON p.staff_id = s.staff_id
LEFT JOIN {{ ref('customer_list') }}            AS cl
    ON p.customer_id = cl.customer_id
