-- Core fact table: Orders with customer enrichment
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    o.status,
    c.customer_name,
    c.customer_segment
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id
