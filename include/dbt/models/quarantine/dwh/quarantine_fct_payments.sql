{{ config(
    materialized='table',
    schema='quarantine_dwh',
    tags = ['quarantine_dwh']
) }}

select
    *,
    'invalid_payment_fact' as quarantine_reason,
    current_timestamp as quarantine_ts,
    'fct_payments' as source_model
from {{ ref('fct_payments') }}
where
    payment_key is null
    or order_id is null
    or amount <= 0