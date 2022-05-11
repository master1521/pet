SELECT
    st.staff_id
    ,st.first_name
    ,st.last_name
    ,st.email
    ,st.store_id
    ,st.active
    ,st.username
    ,st.password
    ,st.last_update
    ,st.picture
    ,s.address
    ,s.city
    ,s.country
FROM {{ source('dvd', 'staff') }} AS st
LEFT JOIN {{ ref('stor_list') }} AS s ON st.store_id = s.store_id