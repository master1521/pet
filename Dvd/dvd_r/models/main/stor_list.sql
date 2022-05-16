SELECT
	s.store_id
	,s.manager_staff_id
	,s.last_update
	,a.address
	,a.city
	,a.country
	,a.district
	,a.postal_code
	,a.phone
FROM {{ source('dvd', 'store') }} AS s
LEFT JOIN {{ ref('address_list') }} AS a ON a.address_id = s.address_id