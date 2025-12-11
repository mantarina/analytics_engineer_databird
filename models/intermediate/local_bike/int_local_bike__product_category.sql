WITH product_category as
(select 
    product_id,
    c.category_id,
    brand_id,
    product_name,
    category_name
from {{ ref('stg_local_bike__products') }} p
inner join {{ ref('stg_local_bike__categories') }} c
on p.category_id = c.category_id)

select 
    product_id,
    category_id,
    b.brand_id,
    product_name,
    category_name,
    brand_name
from product_category pc
inner join {{ ref('stg_local_bike__brands') }} b
on pc.brand_id = b.brand_id