SELECT
	c.customer_id
	,c.last_name
	,c.email
	,a.phone
	,a.address
	,a.country
	,a.city
	,c.create_date
	,c.last_update
	,c.active
FROM {{ source('dvd', 'customer') }} AS c
LEFT JOIN {{ ref('address_list') }} AS a ON a.address_id = c.address_id