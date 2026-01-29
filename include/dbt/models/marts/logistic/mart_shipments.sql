{{ config(
    materialized = 'table',
    schema='mart_logistic',
    tags=['mart']
) }}


WITH shipments as (
    select
        shipment_key,
        shipment_id,
        order_id,
        ship_date,
        carrier,
        status
    from {{ ref('fct_shipments') }}

),

orders as (
    select
        order_key,
        order_id,
        customer_key,
        order_date
    from {{ ref('fct_orders') }}

),

customers as (
    select
        customer_sk,
        customer_id,
        country
    from {{ ref('dim_customers') }}

),

select
    shp.shipment_key,
    shp.order_id,
    ctr.country,
    shp.carrier,
    shp.status           AS shipment_status,
    shp.ship_date        AS delivered_date
    
from shipments shp
join orders ods
    on shp.order_id = ods.order_id
join customers ctr
    on ods.customer_key = ctr.customer_sk
