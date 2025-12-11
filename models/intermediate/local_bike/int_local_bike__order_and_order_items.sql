select 
  oi.order_id,
  store_id,
  product_id,
  customer_id,
  order_date,
  discount,
  off_discount_total_order_amount - total_order_item_amount AS discount_amount,
  total_order_item_amount
from {{ ref('stg_local_bike__order_items') }} oi
inner join {{ ref('stg_local_bike__orders') }} o
on oi.order_id = o.order_id