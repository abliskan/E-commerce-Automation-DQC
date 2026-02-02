{{ config(
    materialized = 'table',
    schema='mart_finance',
    tags=['mart']
) }}

WITH orders as (
   SELECT
       order_key,
       order_id,
       customer_key,
       date_key,
       order_date,
       status                                     as order_status,
	   currency,
	   order_amount
   FROM {{ ref('fct_orders') }}

),

order_items as (
   SELECT
       order_key,
       count(*)                                   as total_items,
       sum(quantity * unit_price)                 as order_revenue,
       avg(unit_price)                            as avg_item_price
   FROM {{ ref('fct_order_items') }}
   GROUP BY order_key

),

payments as (
   SELECT
       order_id,
       sum(amount)                                as total_paid_amount,
       status                                     as payment_status
   FROM {{ ref('fct_payments') }}
   GROUP BY 
       order_id,
       AND payment_status = 'paid'

),

daily_revenue AS (
	select
	    ods.order_date:: date                          as order_date,
	    coalesce(sum(ods.order_amount), 0)             as daily_revenue,
	    coalesce(sum(pys.total_paid_amount), 0)        as total_payments_received,
	    coalesce(count(distinct ods.order_id), 0)      as total_orders,
	    coalesce(count(distinct ods.customer_key), 0)  as unique_customers,
	    coalesce(sum(odi.total_items), 0)              as total_items_sold
	
	from orders ods
	LEFT JOIN order_items odi
		    ON ods.order_id = odi.order_key
	left join payments pys
	    on ods.order_id = pys.order_id
	left join bronze_dwh_ecom.dim_dates dde
	    on ods.order_date = dde.date_day
	GROUP BY order_date
)

SELECT 
	order_date,
	daily_revenue,
	total_payments_received,
	total_orders,
	unique_customers,
	total_items_sold,
	CASE 
        WHEN total_orders = 0 THEN 0
        ELSE daily_revenue / total_orders
    END                                                 as avg_order_value,

    CASE
        WHEN unique_customers = 0 THEN 0
        ELSE daily_revenue / unique_customers
    END as revenue_per_customer

FROM daily_revenue