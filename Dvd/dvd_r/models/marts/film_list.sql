SELECT
	f.film_id
	,f.title
	,f.description
	,f.release_year
	,l."name" as lang
	,f.rental_duration
	,f.rental_rate
	,f.length
	,f.rating
	,c."name" as category
	,a.first_name
	,a.last_name
FROM {{ source('dvd', 'film') }} AS f
LEFT JOIN {{ source('dvd', 'film_category') }} AS fc ON f.film_id = fc.film_id
LEFT JOIN {{ source('dvd', 'category') }} AS c ON fc.category_id = c.category_id
LEFT JOIN {{ source('dvd', 'film_actor') }} AS fa ON f.film_id = fa.film_id
LEFT JOIN {{ source('dvd', 'actor') }} AS a ON fa.actor_id = a.actor_id
LEFT JOIN {{ source('dvd', 'language') }} AS l ON f.language_id = l.language_id