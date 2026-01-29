with
totally_empty as (
  select *
  from bronze.customers_raw
  where customer_id is null
    and coalesce(trim(customer_name),'') = ''
    and coalesce(trim(email),'') = ''
    and coalesce(trim(country),'') = ''
),
crazy_created_at as (
  select *
  from bronze.customers_raw
  where created_at::timestamptz > now() + interval '1 day'
     or created_at::timestamptz < '1970-01-01'
),
to_q as (
  select 'totally_empty_row' as reason, * from totally_empty
  union all
  select 'created_at_out_of_range', * from crazy_created_at
)
insert into bronze_quarantine.customers_qt
  (quarantine_reason, source_layer, customer_id, email, customer_name, country, created_at, status)
select
  reason,
  'bronze',
  customer_id,
  email,
  customer_name,
  country,
  created_at,
  status
from to_q;
-- NOTE: do NOT delete from bronze; raw zone is immutable.
