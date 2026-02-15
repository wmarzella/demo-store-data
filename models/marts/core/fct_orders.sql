{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id',
    cluster_by   = ['order_date'],
    tags         = ['performance']
) }}

-- Core fact table: Orders with customer enrichment
-- Incremental build filters new records by order_date to avoid full scans
with base as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.total_amount,
        o.status,
        c.customer_name,
        c.customer_segment
    from {{ ref('stg_orders') }} o
    left join {{ ref('stg_customers') }} c
        on o.customer_id = c.customer_id
    {% if is_incremental() %}
    where o.order_date > (
        select coalesce(max(order_date), to_date('1900-01-01')) from {{ this }}
    )
    {% endif %}
)
select * from base
