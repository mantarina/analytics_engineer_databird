WITH order_discount as (select 
  order_id,
  store_city,
  store_state,
  product_id,
  discount,
  off_discount_total_order_amount - total_order_item_amount AS discount_amount,
  total_order_item_amount
from {{ ref('stg_local_bike__order_items') }}
inner join {{ ref('stg_local_bike__orders') }} 
USING(order_id)
inner join {{ ref('stg_local_bike__stores') }} 
USING(store_id)

),

product_category as 
(select product_id,
product_name,
category_name,
from {{ ref('stg_local_bike__products') }}
inner join {{ ref('stg_local_bike__categories') }}
USING(category_id))

select 
  product_id,
  store_city,
  store_state,
  category_name,
  product_name,
  count(distinct order_id) as total_commande_par_produit_et_discount,
  SUM(discount_amount) as total_montant_discount_par_produit_et_discount,
  SUM(total_order_item_amount) as total_order_item_amount,
  SUM(discount_amount)/SUM(total_order_item_amount) as average_discount
  from order_discount 
  inner join product_category 
    USING(product_id)
  group by  
  product_id,
  store_city,
  store_state,
  category_name,
  product_name
  order by 1, 3
