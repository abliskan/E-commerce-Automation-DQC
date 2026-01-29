{{ config(
    materialized = 'table',
    schema='mart_customer_support',
    tags=['mart']
) }}

with customers as (
    select
        customer_sk,
        customer_id,
        name               as customer_name,
        country
    from {{ ref('dim_customers') }}
),

orders as (
    select
       order_key,
       order_id,
       customer_key,
       date_key,
       order_date,
       status,
	   currency,
	   order_amount
    from {{ ref('fact_orders') }}
),

payments as (
    select
        order_id,
        sum(amount) as total_paid
    from {{ ref('fact_payments') }}
    group by order_id
)

select
    cts.customer_sk,
    cts.customer_name,
    cts.country,

    count(distinct ods.order_key) as total_orders,
    sum(ods.order_amount) as lifetime_revenue,
    avg(ods.order_amount) as avg_order_value,

    min(ods.order_date) as first_order_date,
    max(ods.order_date) as last_order_date

from customers cts
left join orders ods
    on cts.customer_sk = ods.customer_sk
left join payments pyt
    on ods.order_sk = pyt.order_sk
left join {{ ref('dim_dates') }} dde
    on ods.order_date = dde.date_day
group by
    cts.customer_sk,
    cts.customer_name,
    cts.country
