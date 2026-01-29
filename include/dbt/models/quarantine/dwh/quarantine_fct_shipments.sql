{{ config(
    materialized='table',
    schema='quarantine_dwh',
    tags = ['quarantine_dwh']
) }}

select
    *,
    'invalid_shipment_fact' as quarantine_reason,
    current_timestamp as quarantine_ts,
    'fct_shipments' as source_model
from {{ ref('fct_shipments') }}
where
    shipment_key is null
    or order_id is null
    or date_key is null