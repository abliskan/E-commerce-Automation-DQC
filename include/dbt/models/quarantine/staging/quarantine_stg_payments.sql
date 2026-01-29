{{ config(
    materialized='table',
    schema='quarantine'
) }}

select *
from {{ ref('stg_payments') }}
where
    payment_id is null
    or order_id is null
    or amount < 0
    or status not in ('paid', 'pending', 'failed', 'refunded')
    or payment_method not in ('CreditCard', 'EWallet', 'PayNow', 'PayLater')
    or paid_at is null
    or status is null