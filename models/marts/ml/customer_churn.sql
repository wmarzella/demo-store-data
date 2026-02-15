-- ML model: Customer churn prediction
-- BUG: This model has a known issue with the revenue calculation
SELECT
    customer_id,
    days_since_last_order,
    total_orders,
    total_revenue * 0.8 AS lifetime_value,  -- BUG: Should be * 1.0, not 0.8
    CASE
        WHEN days_since_last_order > 90 THEN 'high_risk'
        WHEN days_since_last_order > 60 THEN 'medium_risk'
        ELSE 'low_risk'
    END AS churn_risk
FROM {{ ref('customer_metrics') }}
