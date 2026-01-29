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

shipments as (
   SELECT
       order_id,
       status                                       as shipment_status,
       ship_date                                    as shipped_date
   FROM {{ ref('fct_shipments') }}
   GROUP BY 
       order_id,
       status,
       ship_date

)

select
   ods.order_key,
   ods.order_id,
   ods.customer_key,
   ods.order_date,
   ods.order_status,
   ods.currency,
   ods.order_amount,

   pyt.payment_status,
   shp.shipment_status,
   
   odi.order_revenue,
   odi.avg_item_price,
   pyt.total_paid_amount,

   -- derived business flags
   case
       when pyt.total_paid_amount >= odi.order_revenue then true
       else false
   end                                             as is_fully_paid,

   case
       when shp.shipped_date is not null then true
       else false
   end                                             as is_shipped

FROM orders ods
left join order_items odi
   on ods.order_id = odi.order_key
left join payments pyt
   on ods.order_id = pyt.order_id
left join shipments shp
   on ods.order_id = shp.order_id;










-- {{ config(
--     materialized = 'table',
--     schema='mart_sales',
--     tags=['mart']
-- ) }}

-- select
--     fos.order_key,
--     fos.order_id,
--     fos.order_date,
--     fos.status as order_status,
--     fos.currency,
--     fos.order_amount,

--     -- customer
--     fos.customer_key,
--     dcs.name as customer_name,
--     dcs.country as customer_country,

--     -- metrics
--     count(distinct foi.order_item_key) as item_count,
--     sum(foi.quantity) as total_quantity

-- from {{ ref('fct_orders') }} fos
-- join {{ ref('dim_dates') }} dde
--     ON fos.order_date = dde.date_day
-- join {{ ref('dim_customers') }} dcs
--     ON fos.customer_key = dcs.customer_sk
-- left join {{ ref('fct_order_items') }} foi
--     ON fos.order_id = foi.order_key
-- group by
--     fos.order_key,
--     fos.order_id,
--     fos.order_date,
--     fos.status,
--     fos.currency,
--     fos.order_amount,
--     fos.customer_key,
--     dcs.name,
--     dcs.country