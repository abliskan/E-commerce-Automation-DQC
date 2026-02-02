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
        ship_date     AS delivered_date,
        carrier,
        status        AS shipment_status
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
        name          AS customer_name,
        country
    from {{ ref('dim_customers') }}
),

select
    shp.shipment_key,
    shipment_id,
    shp.order_id,
    ctr.customer_name,
    ctr.country,
    shp.carrier,
    shp.shipment_status,
    shp.delivered_date,
    coalesce(
        DATE_PART('day', shp.delivered_date - ods.order_date),
        0
    )   AS shipping_days,
    CASE
        WHEN shp.delivered_date IS NULL THEN 'In Transit'
        WHEN DATE_PART('day', shp.delivered_date - ods.order_date) <= 2 THEN 'Fast'
        WHEN DATE_PART('day', shp.delivered_date - ods.order_date) <= 5 THEN 'Normal'
        ELSE 'Delayed'
    END AS delivery_status

from shipments shp
left join orders ods
    on shp.order_id = ods.order_id
left join customers ctr
    on ods.customer_key = ctr.customer_sk