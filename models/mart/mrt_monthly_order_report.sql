WITH monthly_users_recap AS (

SELECT 
    DATE_TRUNC(order_created_at_x,month) AS order_creation_month,
    COUNT(DISTINCT user_id) AS total_monthly_users,
    COUNT(DISTINCT CASE WHEN user_state LIKE '%JAWA%TIMUR%' THEN user_id END) AS total_monthly_users_from_jawa_timur,
    COUNT(order_id) AS total_monthly_orders
FROM {{ ref("int_sales_database__order")}}
GROUP BY order_creation_month

)


SELECT 
    order_creation_month,
    COALESCE(total_monthly_users,0) AS total_monthly_users,
    COALESCE(total_monthly_users_from_jawa_timur,0) AS total_monthly_user_from_jawa_timur,
    COALESCE(total_monthly_orders,0) AS total_monthly_orders
FROM monthly_users_recap 
ORDER BY order_creation_month