select
    store_id,
    store_name,
    phone AS store_phone,
    email AS store_email,
    street AS store_street,
    city AS store_city,
    state AS store_state,
    cast(zip_code as string)  AS store_zipcode

from {{ source('local_bike', 'stores') }}
