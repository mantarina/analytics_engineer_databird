WITH customer_kpi AS (
  SELECT customer_id,
         DATE('2018-12-31') AS reference_order_date,
         MAX(order_date) AS last_customer_orders,
         COUNT(DISTINCT order_id) AS frequency_total_customer_orders,
         round(SUM(total_order_item_amount),2) AS monetary_total_customer_order_amount
  FROM {{ ref('int_local_bike__order_and_order_items') }}
  GROUP BY customer_id
)

SELECT 
    customer_id,
    DATE_DIFF(reference_order_date, last_customer_orders, DAY) AS recency_days_since_last_customer_order,
    frequency_total_customer_orders,
    monetary_total_customer_order_amount,
    NTILE(3) OVER (ORDER BY DATE_DIFF(reference_order_date, last_customer_orders, DAY)) AS recency_score,
    NTILE(3) OVER (ORDER BY frequency_total_customer_orders DESC) AS frequency_score,
    NTILE(3) OVER (ORDER BY monetary_total_customer_order_amount DESC) AS monetary_score
FROM customer_kpi