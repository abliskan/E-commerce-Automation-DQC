{{ config(
    schema='staging',
    materialized='table',
    tags=['staging']
) }}

select
        payment_id::bigint           as payment_id,
        order_id::bigint             as order_id,
        trim(method)                 as payment_method,
        amount::int                  as amount,
        payment_ts::timestamp        as paid_at,
        trim(status)                 as status,
        now()                        as load_ts
from {{ source('bronze', 'payments_raw') }}