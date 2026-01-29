{{ config(
    materialized='table',
    schema='quarantine_dwh',
    tags=['quarantine_dwh']
) }}

select
    *,
    'invalid_product_variant_record' as quarantine_reason,
    current_timestamp as quarantine_ts,
    'dim_product_variants' as source_model
from {{ ref('dim_product_variants') }}
where
    variant_sk is null
    or product_sk is null
    or price is null