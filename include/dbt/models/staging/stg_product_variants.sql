{{ config(
    schema='staging',
    materialized='table',
    tags=['staging']
) }}

select
        variant_id::bigint           as variant_id,
        product_id::bigint           as product_id,
        trim(sku)                    as sku,
        trim(barcode)                as barcode,
        price::int                   as price,
        cost::int                    as cost,
        active::boolean              as active,
        now()                        as load_ts
from {{ source('bronze', 'product_variants_raw') }}