{{ config(
    schema='staging',
    materialized='table',
    tags=['staging']
) }}
-- Cleaned + typed customer records coming from staging.customers (OLTP extract)
select
        customer_id::bigint           as customer_id,
        lower(email)                  as email,
        nullif(trim(name), '')        as name,
        nullif(trim(country), '')     as country,
        created_at::timestamp         as created_at,
        nullif(trim(status), '')      as status,
        now()                         as load_ts
from {{ source('bronze', 'customers_raw') }}