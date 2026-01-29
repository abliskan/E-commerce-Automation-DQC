{{ config(
    materialized='table',
    schema='quarantine'
) }}

select *
from {{ ref('stg_product_variants') }}
where
    variant_id is null
    or product_id is null
    or sku is null
    or price < 0
    or barcode is null
    or cost < 0
    or active is null