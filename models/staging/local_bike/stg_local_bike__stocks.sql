select
    CONCAT(store_id, '_', product_id) AS stock_id,
    store_id,
    product_id,
    quantity AS product_stock_per_store
from {{ source('local_bike', 'stocks') }}
