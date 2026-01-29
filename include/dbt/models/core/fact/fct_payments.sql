-- Grain: one row per payment transaction
{{ config(
    materialized='table',
    schema='dwh_e-com',
    tags=['fact']
) }}

with payments as (
    select 
        payment_id,
        order_id,
        payment_method,
        amount,
        paid_at,
        status
    from {{ ref('stg_payments') }}
),

orders as (
    select
        order_id,
        customer_id
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
    {{ dbt_utils.generate_surrogate_key(['pay.payment_id', 'pay.order_id']) }} as payment_key,
    pay.payment_id,
    ods.order_id,
    cts.customer_sk as customer_key,
    dte.date_sk as date_key,
    pay.paid_at as paid_date,
    pay.amount,
    pay.payment_method,
    pay.status
from payments pay
left join orders ods
    on pay.order_id = ods.order_id
left join customers cts
    on ods.customer_id = cts.customer_id
left join dates dte
    on cast(pay.paid_at as date) = dte.date_day