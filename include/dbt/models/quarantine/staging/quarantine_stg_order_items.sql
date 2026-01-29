{{ config(
    materialized='table',
    schema='quarantine'
) }}

select *
from {{ ref('stg_order_items') }}
where
    order_item_id is null
    or order_id is null
    or product_id is null
    or variant_id is null
    or quantity <= 0
    or unit_price < 0
    or discount < 0
    or tax < 0
    or total_price < 0
    or created_at is null
