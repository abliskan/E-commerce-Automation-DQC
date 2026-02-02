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
        product_sk           AS product_key,
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
    pdt.product_key,
    ods.order_date,
    pdt.product_name,
    pdt.category,
    vrt.price,
    count(distinct ods.order_id)                                       AS total_orders,
    sum(odi.quantity)                                                  AS total_unit_sold,
    sum(odi.item_revenue)                                              AS total_revenue,
    avg(odi.unit_price)                                                AS avg_selling_price,
    coalesce(sum(odi.item_revenue) / nullif(sum(odi.quantity), 0), 0)  AS revenue_per_unit
    
from order_items odi
left join orders ods
    on odi.order_key = ods.order_id
left join products pdt
    on odi.product_key = pdt.product_sk
left join variants vrt
    on odi.variant_key = vrt.variant_sk
group by 
    pdt.product_key,
    ods.order_date,
    pdt.product_name,
    pdt.category,
    vrt.price
