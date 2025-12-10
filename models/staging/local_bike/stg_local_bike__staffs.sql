select
    staff_id,
    first_name AS staff_firstname,
    last_name AS staff_lastname,
    CONCAT(last_name, ' ', first_name) AS staff_fullname,
    email AS staff_email,
    phone AS staff_phone,
    active AS staff_active,
    store_id,
    manager_id

from {{ source('local_bike', 'staffs') }}
