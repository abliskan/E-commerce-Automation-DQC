with
no_variant_id as (
  select * from bronze.product_variants_raw where variant_id is null
),
no_product_id as (
  select * from bronze.product_variants_raw where product_id is null
),
totally_empty as (
  select *
  from bronze.product_variants_raw
  where variant_id is null
    and product_id is null
    and coalesce(trim(sku),'') = ''
    and coalesce(trim(name),'') = ''
),
crazy_price as (
  select *
  from bronze.product_variants_raw
  where price < 0 or price > 1000000000
),
to_q as (
  select 'variant_id_null' as reason, * from no_variant_id
  union all select 'product_id_null', * from no_product_id
  union all select 'totally_empty_variant', * from totally_empty
  union all select 'variant_price_implausible', * from crazy_price
)
insert into bronze_quarantine.product_variants_qt
  (quarantine_reason, source_layer, variant_id, product_id, sku, name, variant_type, price, active)
select
  reason,
  'bronze',
  variant_id,
  product_id,
  sku,
  name,
  variant_type,
  price,
  active
from to_q;