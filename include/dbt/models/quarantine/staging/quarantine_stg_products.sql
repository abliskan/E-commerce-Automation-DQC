{{ config(
    materialized='table',
    schema='quarantine'
) }}

select *
from {{ ref('stg_products') }}
where
    product_id is null
    or title is null
    or category is null
    or brand is null
    or active is null
