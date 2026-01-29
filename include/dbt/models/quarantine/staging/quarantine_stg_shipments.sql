{{ config(
    materialized='table',
    schema='quarantine'
) }}

select *
from {{ ref('stg_shipments') }}
where
    shipment_id is null
    or order_id is null
    or carrier is null
    or service is null
    or status is null
    or tracking_number is null
    or shipped_at is null
