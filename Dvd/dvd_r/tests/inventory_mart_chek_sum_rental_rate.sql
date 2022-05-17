SELECT
	*
FROM {{ ref('inventory_mart') }} AS im
WHERE im.rental_rate < 0