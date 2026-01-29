-- If you want a second safety net before marts, quarantine rows that break core integrity (e.g., orphaned fact rows).
-- This mirrors your checks_core.yml relationships.

with
fact as ( select * from core.fact_order_line_incremental ),
dimc as ( select customer_sk from core.dim_customer ),
dimp as ( select product_sk  from core.dim_product ),

orphan_customer as (
  select f.*
  from fact f left join dimc c using (customer_sk)
  where c.customer_sk is null
),
orphan_product as (
  select f.*
  from fact f left join dimp p using (product_sk)
  where p.product_sk is null
),
negatives as (
  select * from fact where qty < 1 or unit_price < 0 or line_amount < 0
),
to_q as (
  select 'orphan_customer_sk' as reason, * from orphan_customer
  union all select 'orphan_product_sk', * from orphan_product
  union all select 'negative_qty_or_amount', * from negatives
)
-- store into a single “fact” quarantine table or re-use order_items_qt
insert into bronze_quarantine.order_items_qt
  (quarantine_reason, source_layer, order_item_id, order_id, product_id, qty, unit_price, discount, tax, line_amount)
select reason, 'core', null, f.order_id, null, f.qty, f.unit_price, f.discount, f.tax, f.line_amount
from to_q f;

-- OPTIONAL: remove quarantined rows from the fact
delete from core.fact_order_line_incremental f
using to_q t
where f.order_id = t.order_id
  and f.order_ts = t.order_ts
  and f.line_amount = t.line_amount;