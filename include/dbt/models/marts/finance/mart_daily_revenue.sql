{{ config(
    materialized = 'table',
    schema='mart_finance',
    tags=['mart']
) }}

select
    fos.order_date,
    count(distinct fos.order_key) as order_count,
    count(distinct fos.customer_key) as unique_customers,
    coalesce(sum(fos.order_amount), 0) as daily_revenue,
    avg(fos.order_amount) as avg_order_value

from {{ ref('fct_orders') }} fos
join {{ ref('dim_dates') }} dde
    ON fos.order_date = dde.date_day
GROUP BY
    fos.order_date