with
no_payment_id as (
  select * from bronze.payments_raw where payment_id is null
),
no_order_id as (
  select * from bronze.payments_raw where order_id is null
),
crazy_amount as (
  select * from bronze.payments_raw
  where amount < 0 or amount > 1000000000
),
crazy_ts as (
  select * from bronze.payments_raw
  where paid_ts is not null
    and (paid_ts::timestamptz > now() + interval '1 day'
      or paid_ts::timestamptz < '1970-01-01')
),
to_q as (
  select 'payment_id_null' as reason, * from no_payment_id
  union all select 'order_id_null', * from no_order_id
  union all select 'amount_implausible', * from crazy_amount
  union all select 'paid_ts_out_of_range', * from crazy_ts
)
insert into bronze_quarantine.payments_qt
  (quarantine_reason, source_layer, payment_id, order_id, method, amount, status, paid_ts)
select
  reason,
  'bronze',
  payment_id,
  order_id,
  method,
  amount,
  status,
  paid_ts
from to_q;
