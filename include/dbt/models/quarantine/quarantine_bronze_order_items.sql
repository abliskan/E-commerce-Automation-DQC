with
no_id as (
  select * from bronze.order_items_raw where order_item_id is null
),
crazy_qty as (
  select * from bronze.order_items_raw
  where qty is null or qty < 0 or qty > 10000      -- qty way too big
),
crazy_price as (
  select * from bronze.order_items_raw
  where unit_price < 0 or unit_price > 100000000
),
crazy_line_amount as (
  select * from bronze.order_items_raw
  where line_amount < 0 or line_amount > 1000000000
),
to_q as (
  select 'order_item_id_null' as reason, * from no_id
  union all select 'qty_implausible', * from crazy_qty
  union all select 'unit_price_implausible', * from crazy_price
  union all select 'line_amount_implausible', * from crazy_line_amount
)
insert into bronze_quarantine.order_items_qt
  (quarantine_reason, source_layer, order_item_id, order_id, product_id, qty, unit_price, discount, tax, line_amount)
select
  reason,
  'bronze',
  order_item_id,
  order_id,
  product_id,
  qty,
  unit_price,
  discount,
  tax,
  line_amount
from to_q;
