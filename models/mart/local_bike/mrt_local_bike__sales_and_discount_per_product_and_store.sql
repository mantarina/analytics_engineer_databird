WITH order_discount as (
select 
  order_id,
  store_city,
  store_state,
  product_id,
  discount,
  discount_amount,
  total_order_item_amount
from {{ ref('int_local_bike__order_and_order_items') }} oi
inner join {{ ref('stg_local_bike__stores') }} s
on oi.store_id = s.store_id
)

select 
  od.product_id,
  store_city,
  store_state,
  category_name,
  product_name,
  count(distinct order_id) as total_orders_per_product_store,
  ROUND(SUM(discount_amount),2) as total_discount_per_product_store,
  ROUND(SUM(total_order_item_amount),2) as total_amount_per_product_store,
  ROUND(SUM(discount_amount)/SUM(total_order_item_amount),2) as average_discount_per_product_store
from order_discount od
inner join {{ ref('int_local_bike__product_category') }} pc
on od.product_id = pc.product_id
group by  
  product_id,
  store_city,
  store_state,
  category_name,
  product_name
order by 1, 3
