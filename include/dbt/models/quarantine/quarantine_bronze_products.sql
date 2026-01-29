with
no_product_id as (
  select * from bronze.products_raw where product_id is null
),
totally_empty as (
  select *
  from bronze.products_raw
  where product_id is null
    and coalesce(trim(title),'') = ''
    and coalesce(trim(category),'') = ''
    and coalesce(trim(brand),'') = ''
),
crazy_price as (
  select *
  from bronze.products_raw
  where price < 0 or price > 1000000000
),
to_q as (
  select 'product_id_null' as reason, * from no_product_id
  union all select 'totally_empty_product', * from totally_empty
  union all select 'price_implausible', * from crazy_price
)
insert into bronze_quarantine.products_qt
  (quarantine_reason, source_layer, product_id, title, category, brand, active)
select
  reason,
  'bronze',
  product_id,
  title,
  category,
  brand,
  active
from to_q;