SELECT
	*
FROM {{ ref('inventory_mart') }} AS im
WHERE im.replacement_cost < 0