{{ config(
       materialized='table',
       schema='dwh_e-com',
       tags=['dimension']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['variant_id', 'price']) }} as variant_sk,
    variant_id,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
    price
from {{ ref('stg_product_variants') }}
