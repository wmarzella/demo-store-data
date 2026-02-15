-- Staging: Raw orders data
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    discount_code,
    created_at
FROM {{ source('raw', 'orders') }}
WHERE order_date >= '2024-01-01'
