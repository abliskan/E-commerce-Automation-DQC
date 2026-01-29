{{ config(
    materialized='table',
    schema='dwh_e-com',
    tags=['fact']
) }}

with orders as (
    select
        order_id,
        customer_id,
        channel_id,
        order_at,
        status,
        currency,
        total_amount
    from {{ ref('stg_orders') }}
),

customers as (
    select 
        customer_sk, 
        customer_id
    from {{ ref('dim_customers') }}
),

dates as (
    select 
        date_sk, 
        date_day
    from {{ ref('dim_dates') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['ods.order_id']) }} as order_key,
    ods.order_id,
    cts.customer_sk as customer_key,
    dte.date_sk as date_key,
    ods.order_at as order_date,
    ods.status,
    ods.currency,
    ods.total_amount as order_amount
from orders ods
left join customers cts
    on ods.customer_id = cts.customer_id
left join dates dte
    on cast(ods.order_at as date) = dte.date_day
