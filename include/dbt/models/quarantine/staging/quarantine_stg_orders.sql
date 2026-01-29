{{ config(
    materialized='table',
    schema='quarantine'
) }}

select *
from {{ ref('stg_orders') }}
where
    order_id is null
    or customer_id is null
    or total_amount < 0
    or status not in ('created', 'paid', 'shipped', 'cancelled')
    or channel_id is null
    or order_at is null
    or currency is null