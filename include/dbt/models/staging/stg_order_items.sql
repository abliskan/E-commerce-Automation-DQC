  GNU nano 6.2                      stg_order_items.sql
{{ config(
    schema='staging',
    materialized='table',
    tags=['staging']
) }}

select
        order_item_id::bigint        as order_item_id,
        order_id::bigint             as order_id,
        product_id::bigint           as product_id,
        variant_id::bigint           as variant_id,
        qty::int                     as quantity,
        unit_price::int              as unit_price,
        discount::int                as discount,
        tax::int                     as tax,
        line_amount::int             as total_price,
        created_at::timestamp        as created_at,
        now()                        as load_ts
from {{ source('bronze', 'order_items_raw') }}