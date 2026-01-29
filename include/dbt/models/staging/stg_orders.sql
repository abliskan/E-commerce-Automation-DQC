{{ config(
    schema='staging',
    materialized='table',
    tags=['staging']
) }}
-- Cleaned + typed customer records coming from staging.customers (OLTP extract)
select
        order_id::bigint              as order_id,
        customer_id::bigint           as customer_id,
        channel_id::int               as channel_id,
        order_ts::timestamp           as order_at,
        nullif(trim(status), '')      as status,
        nullif(trim(currency), ''),   as currency,
        total_amount::bigint          as total_amount,
        now()                         as load_ts
from {{ source('bronze', 'orders_raw') }}