{{ config(
    materialized='table',
    schema='dwh_e-com',
    tags=['fact']
) }}

with order_items as (
    select
        order_item_id,
        order_id,
        product_id,
        variant_id,
        quantity,
        unit_price,
        discount,
        tax,
        total_price,
        created_at
    from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_at
    from {{ ref('stg_orders') }}
),

customers as (
    select 
        customer_sk, 
        customer_id
    from {{ ref('dim_customers') }}
),

products as (
    select 
        product_sk, 
        product_id
    from {{ ref('dim_products') }}
),

product_variants as (
    select 
        variant_sk, 
        variant_id
    from {{ ref('dim_product_variants') }}
),

dates as (
    select 
        date_sk, 
        date_day
    from {{ ref('dim_dates') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['odr_itm.order_item_id', 'odr_itm.product_id','odr_itm.variant_id']) }} as order_item_key,
    odr_itm.order_item_id,
    ods.order_id as order_key,
    pds.product_sk as product_key,
    pds_vrs.variant_sk as variant_key,
    dte.date_sk as date_key,
    odr_itm.quantity,
    odr_itm.unit_price,
    odr_itm.discount,
    odr_itm.tax,
    odr_itm.total_price,
    ods.order_at as order_date
from order_items odr_itm
left join orders ods
    on odr_itm.order_id = ods.order_id
left join products pds
    on odr_itm.product_id = pds.product_id
left join product_variants pds_vrs
    on odr_itm.variant_id = pds_vrs.variant_id
left join dates dte
    on cast(odr_itm.created_at as date) = dte.date_day