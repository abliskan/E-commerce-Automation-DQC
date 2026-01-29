{{ config(
    materialized='table',
    schema='quarantine_dwh',
    tags=['quarantine_dwh']
) }}

select
    *,
    'invalid_order_fact' as quarantine_reason,
    current_timestamp as quarantine_ts,
    'fct_orders' as source_model
from {{ ref('fct_orders') }}
where
    order_key is null
    or customer_key is null
    or date_key is null
    or order_amount < 0