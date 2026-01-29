{{ config(
    schema='staging',
    materialized='table',
    tags=['staging']
) }}

select
        shipment_id::bigint          as shipment_id,
        order_id::bigint             as order_id,
        trim(carrier)                as carrier,
        trim(service)                as service,
        trim(tracking_no)            as tracking_number,
        trim(status)                 as status,
        shipped_ts::timestamp        as shipped_at
        now()                        as load_ts
from {{ source('bronze', 'shipments_raw') }}