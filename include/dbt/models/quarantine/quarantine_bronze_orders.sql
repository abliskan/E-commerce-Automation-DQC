with
no_order_id as (
  select * from bronze.orders_raw where order_id is null
),
no_customer_id as (
  select * from bronze.orders_raw where customer_id is null
),
crazy_ts as (
  select * from bronze.orders_raw
  where order_ts::timestamptz > now() + interval '1 day'
     or order_ts::timestamptz < '1970-01-01'
),
huge_amount as (
  select * from bronze.orders_raw
  where total_amount > 1000000000         -- clearly broken ingestion
),
to_q as (
  select 'order_id_null' as reason, * from no_order_id
  union all select 'customer_id_null', * from no_customer_id
  union all select 'order_ts_out_of_range', * from crazy_ts
  union all select 'total_amount_implausible', * from huge_amount
)
insert into bronze_quarantine.orders_qt
  (quarantine_reason, source_layer, order_id, customer_id, currency, status, order_ts, total_amount)
select
  reason,
  'bronze',
  order_id,
  customer_id,
  currency,
  status,
  order_ts,
  total_amount
from to_q;