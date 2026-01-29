{{ config(
    materialized='table',
    schema='quarantine'
) }}

select *
from {{ ref('stg_customers') }}
where
    email is null
    or email not like '%@%'
    or customer_id is null
    or name is null
    or country is null
    or created_at is null
    or status is null