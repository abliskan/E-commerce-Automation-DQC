-- Grain: one row per shipment event
{{ config(
    materialized='table',
    schema='dwh_e-com',
    tags=['fact']
) }}

with shipments as (
    select 
        shipment_id,
        order_id,
        carrier,
        service,
        tracking_number,
        shipped_at,
        status
    from {{ ref('stg_shipments') }}
),

orders as (
    select
        order_id,
        customer_id
    from {{ ref('stg_orders') }}
),

dates as (
    select 
        date_sk, 
        date_day
    from {{ ref('dim_dates') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['sps.shipment_id', 'sps.order_id']) }} as shipment_key,
    sps.shipment_id,
    ods.order_id,
    dte.date_sk as date_key,
    sps.shipped_at as ship_date,
    sps.carrier,
    sps.service,
    sps.tracking_number,
    sps.status
from shipments sps
left join orders ods
    on sps.order_id = ods.order_id
left join dates dte
    on cast(sps.shipped_at as date) = dte.date_day