{{ config(
    materialized = 'table',
    schema='mart_product',
    tags=['mart']
) }}


WITH order_items as (
    select
        order_item_key,
        order_item_id,
        order_key,
        product_key,
        variant_key,
        quantity,
        unit_price,
        quantity * unit_price as item_revenue
    from {{ ref('fct_order_items') }}
),

orders as (
    select
        order_key,
        order_id,
        order_date
    from {{ ref('fct_orders') }}
),

products as (
    select
        product_sk,
        product_id,
        title                AS product_name,
        category
    from {{ ref('dim_products') }}
),

variants as (
    select
        variant_sk,
        variant_id,
        price
    from {{ ref('dim_product_variants') }}
),

select
    ods.order_date,
    pdt.product_name,
    pdt.category,
    vrt.price,
    sum(odi.quantity) as total_quantity,
    sum(odi.item_revenue) as total_revenue
from order_items odi
join orders ods
    on odi.order_key = ods.order_id
join products pdt
    on odi.product_key = pdt.product_sk
join variants vrt
    on odi.variant_key = vrt.variant_sk
group by 1, 2, 3, 4











-- with order_items as (
--     select
--         oi.order_item_sk,
--         oi.order_sk,
--         oi.product_sk,
--         oi.product_variant_sk,
--         oi.quantity,
--         oi.unit_price,
--         oi.quantity * oi.unit_price as item_revenue,
--         o.order_date_sk
--     from {{ ref('fact_order_items') }} oi
--     join {{ ref('fact_orders') }} o
--         on oi.order_sk = o.order_sk
-- ),

-- products as (
--     select
--         product_sk,
--         product_name,
--         category
--     from {{ ref('dim_products') }}
-- )

-- select
--     oi.order_date_sk,
--     oi.product_sk,
--     p.product_name,
--     p.category,

--     count(distinct oi.order_sk) as order_count,
--     sum(oi.quantity) as total_quantity,
--     sum(oi.item_revenue) as total_revenue,
--     avg(oi.unit_price) as avg_unit_price

-- from order_items oi
-- join products p
--     on oi.product_sk = p.product_sk
-- group by
--     oi.order_date_sk,
--     oi.product_sk,
--     p.product_name,
--     p.category
