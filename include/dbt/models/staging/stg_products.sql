{ config(
    schema='staging',
    materialized='table',
    tags=['staging']
) }}

select
        product_id::bigint           as product_id,
        trim(title)                  as title,
        trim(category)               as category,
        trim(brand)                  as brand,
        active::boolean              as active,
        created_at::timestamp        as created_at,
        now()                        as load_ts
from {{ source('bronze', 'products_raw') }}