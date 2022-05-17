SELECT
    i.inventory_id
    ,i.film_id                          AS inventory_film_id
    ,i.store_id                         AS inventory_store_id
    ,i.last_update                      AS inventory_last_update
    ,f.film_id
    ,f.title
    ,f.description
    ,f.release_year
    ,f.film_language
    ,f.length
    ,f.rental_duration
    ,f.rental_rate
    ,f.replacement_cost
    ,f.rating
    ,s.store_id
    ,s.manager_staff_id
    ,s.last_update
    ,s.address
    ,s.city
    ,s.country
    ,s.district
    ,s.postal_code
    ,s.phone
FROM {{ source('dvd', 'inventory') }}   AS i
LEFT JOIN {{ ref('film_list') }}        AS f
    ON i.film_id = f.film_id
LEFT JOIN {{ ref('stor_list') }}        AS s
    ON i.store_id = s.store_id
