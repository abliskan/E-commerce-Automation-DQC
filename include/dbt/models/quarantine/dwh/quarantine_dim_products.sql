{{ config(
    materialized='table',
    schema='quarantine_dwh',
    tags=['quarantine_dwh']
) }}

select
    *,
    'invalid_product_record' as quarantine_reason,
    current_timestamp as quarantine_ts,
    'dim_products' as source_model
from {{ ref('dim_products') }}
where
    product_sk is null
    or product_id is null
    or title is null