# Список покупателей

SELECT
	c.customer_id
	,c.customer_id 
	,c.last_name 
	,c.email 
	,a.phone 
	,a.address
	,c3.country 
	,c2.city 
FROM dvd."dvd-rental".customer c
LEFT JOIN dvd."dvd-rental".address a ON a.address_id = c.address_id
LEFT JOIN dvd."dvd-rental".city c2 ON a.city_id = c2.city_id
LEFT JOIN dvd."dvd-rental".country c3 ON c2.country_id = c3.country_id 