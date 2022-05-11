SELECT
	a.address_id
	,a.address
	,a.district
	,a.postal_code
	,a.phone
	,a.last_update
	,c2.city
	,c3.country
FROM {{ source('dvd', 'address')}} AS a
LEFT JOIN {{ source('dvd', 'city')}} AS c2 ON a.city_id = c2.city_id
LEFT JOIN {{ source('dvd', 'country')}} AS c3 ON c2.country_id = c3.country_id