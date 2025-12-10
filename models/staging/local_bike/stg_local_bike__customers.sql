select
    customer_id,
    first_name AS customer_firstname,
    last_name AS customer_lastname,
    concat(last_name, '_', first_name) AS customer_fullname,
    phone AS customer_phone,
    email AS customer_email,
    street AS customer_street,
    city AS customer_city,
    state AS customer_state,
    cast(zip_code as string) AS customer_zipcode

    from {{ source('local_bike', 'customers') }}
