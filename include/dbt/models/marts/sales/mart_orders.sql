{{ config(
    materialized = 'table',
    schema='mart_sales',
    tags=['mart']
) }}

-- mart_orders.sql
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
       status
),

customers as (
    select
        customer_sk,
        customer_id,
        name                                      as customer_name,
        country
    from {{ ref('dim_customers') }}
)

select
    ods.order_key,
   	ods.order_id,
   	ods.order_date,
      ods.order_status,
   	coalesce(ctr.customer_name, 'Unkown'),           as customer_name
   	coalesce(ctr.country, 'Unkown'),                 as country
   
   	-- metrics
   	distinct coalesce(odi.total_items, 0)            as unique_item_count, -- For numeric aggregations used in dashboards | Convert NULL → 0
   	coalesce(odi.order_revenue, 0)                   as order_revenue, -- For numeric aggregations used in dashboards | Convert NULL → 0
   	CASE 
   	   WHEN coalesce(odi.total_items, 0) = 0 THEN 0
   	   ELSE coalesce(odi.order_revenue, 0) / odi.total_items
   	END AS avg_item_price,
   	coalesce(pyt.total_paid_amount, 0)               as total_paid_amount,
   	coalesce(payment_status, 'Unpaid')               as payment_status,
   	-- derived business flags
   	CASE 
   	   WHEN coalesce(pyt.total_paid_amount, 0) = 0 THEN 'Not Paid'
   	   WHEN pyt.total_paid_amount < coalesce(odi.order_revenue, 0) THEN 'Partial'
   	   ELSE 'Fully Paid'
   	END                                  AS payment_flag

FROM orders ods
left join order_items odi
   on ods.order_id = odi.order_key
left join payments pyt
   on ods.order_id = pyt.order_id
left join customers ctr
    on ods.customer_key = ctr.customer_sk
