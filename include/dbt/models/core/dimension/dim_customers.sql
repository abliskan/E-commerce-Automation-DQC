{{ config(
    materialized='table',
    schema='dwh_e-com',
    tags=['dimension']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'email']) }} as customer_sk,
    customer_id,
    email,
    name,
    country,
    status,
    created_at
FROM {{ ref('stg_customers') }}