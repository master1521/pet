SELECT
	f.film_id
	,f.title
	,f.description
	,f.release_year
	,l."name" 				AS film_language
    ,f.length
	,f.rental_duration
	,f.rental_rate
    ,f.replacement_cost
	,f.rating
FROM {{ source('dvd', 'film') }} AS f
LEFT JOIN {{ source('dvd', 'language') }} AS l
    ON f.language_id = l.language_id