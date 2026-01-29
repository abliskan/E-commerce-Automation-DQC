{{ config(
    materialized='table',
    schema='dwh_e-com',
    tags=['dimension']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'title']) }} as product_sk,
    product_id,
    title,
    category
FROM {{ ref('stg_products') }}