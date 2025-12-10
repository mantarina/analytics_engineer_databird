select
    product_id,
    product_name,
    brand_id,
    category_id,
    model_year AS product_year,
    list_price AS item_price

from {{ source('local_bike', 'products') }}