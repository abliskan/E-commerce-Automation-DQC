{{ config(
    materialized='table',
    schema='quarantine_dwh',
    tags=['quarantine_dwh']
) }}

select
    *,
    'invalid_date_record' as quarantine_reason,
    current_timestamp as quarantine_ts,
    'dim_date' as source_model
from {{ ref('dim_dates') }}
where
    date_day is null
    or year is null
    or month is null