WITH last_order AS (
  SELECT customer_id,
         MAX(order_date) AS last_order_date,
         COUNT(DISTINCT o.order_id) AS freq_365,
         SUM(total_order_item_amount) AS monetary_365
  FROM {{ ref('stg_local_bike__orders') }} o
  left join {{ ref('stg_local_bike__order_items') }} oi
  on o.order_id = oi.order_id
  GROUP BY customer_id
), 

kpi as (SELECT customer_id,
       DATE_DIFF(DATE('2018-12-31'), last_order_date, DAY) AS recency_days,
       freq_365,
       monetary_365,
       NTILE(3) OVER (ORDER BY DATE_DIFF(DATE('2018-12-31'), last_order_date, DAY)) AS recency_score,
       NTILE(3) OVER (ORDER BY freq_365 DESC) AS freq_score,
       NTILE(3) OVER (ORDER BY monetary_365 DESC) AS monetary_score
FROM last_order),