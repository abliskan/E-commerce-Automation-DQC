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
        country,
        created_at         as customer_created_at
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

order_items as (
   SELECT
       order_key,
       count(*)                                   as total_items,
       sum(quantity * unit_price)                 as order_revenue,
       avg(unit_price)                            as avg_item_price
   FROM {{ ref('fct_order_items') }}
   GROUP BY order_key

),

customer_orders as (
	select
	    cts.customer_sk,
	    cts.customer_name,
	    cts.country,
		cts.customer_created_at,
		
	    count(distinct ods.order_key)      as total_orders,
	    coalesce(sum(odi.total_items), 0)  as total_items,
	    coalesce(sum(ods.order_amount), 0) as lifetime_value,
	    coalesce(avg(ods.order_amount), 0) as avg_order_value,
	
	    min(ods.order_date) as first_order_date,
	    max(ods.order_date) as last_order_date,
	    
	    coalesce(
	        DATE_PART('day', CURRENT_DATE - MAX(ods.order_date)),
	        0
	    ) AS days_since_last_order
	
	from customers cts
	left join orders ods
	    on cts.customer_sk = ods.customer_key
	LEFT JOIN order_items odi
	    ON ods.order_id = odi.order_key
	left join bronze_dwh_ecom.dim_dates dde
	    on ods.order_date = dde.date_day
	group by
	    cts.customer_sk,
	    cts.customer_name,
	    cts.country,
	    cts.customer_created_at
)

SELECT 
	customer_sk,
	customer_name,
 	country,
	total_orders,
	total_items,
	lifetime_value,
	avg_order_value,
    -- If no order, use account creation date
	coalesce(first_order_date, customer_created_at) as first_order_date,
    -- If no order, keep NULL (customer never purchased)
	last_order_date,
	days_since_last_order
	
FROM customer_orders
