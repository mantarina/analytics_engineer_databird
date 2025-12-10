select
    CONCAT(order_id, '_', item_id) AS order_item_id,
    order_id,
    item_id,
    product_id,
    quantity AS item_quantity,
    list_price AS item_price,
    discount,
    ROUND((quantity * list_price) * coalesce((1-discount), 1),2) total_order_item_amount

from {{ source('local_bike', 'order_items') }}
