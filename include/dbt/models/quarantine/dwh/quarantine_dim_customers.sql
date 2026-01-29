{{ config(
    materialized='table',
    schema='quarantine_dwh',
    tags=['quarantine_dwh']
) }}

select
    *,
    'invalid_customer_record' as quarantine_reason,
    current_timestamp as quarantine_ts,
    'dim_customers' as source_model
from {{ ref('dim_customers') }}
where
    customer_sk is null
    or customer_id is null
    or email is null
    or email not like '%@%'