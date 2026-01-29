{{ config(
    materialized='table',
    schema='quarantine_dwh',
    tags = ['quarantine_dwh']
) }}

select
    *,
    'invalid_order_item_fact' as quarantine_reason,
    current_timestamp as quarantine_ts,
    'fct_order_items' as source_model
from {{ ref('fct_order_items') }}
where
    order_item_key is null
    or order_key is null
    or product_key is null
    or quantity <= 0
    or unit_price < 0