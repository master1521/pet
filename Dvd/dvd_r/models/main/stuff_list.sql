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
    ,a.address
    ,a.city
    ,a.country
    ,a.district
    ,a.postal_code
    ,a.phone
FROM {{ source('dvd', 'staff') }} AS st
LEFT JOIN {{ ref('address_list') }} AS a ON st.address_id = a.address_id