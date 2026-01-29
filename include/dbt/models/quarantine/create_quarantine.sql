create schema if not exists bronze_quarantine;

-- One table per dataset you want to quarantine
create table if not exists bronze_quarantine.orders_qt
(
  snapshot_ts     timestamptz default now(),
  quarantine_reason text not null,
  source_layer    text not null,                    -- 'staging' | 'core'
  scan_batch_id   uuid default gen_random_uuid(),
  -- copy of business columns (minimal set; add more as you need)
  order_id        bigint,
  customer_id     bigint,
  currency        text,
  status          text,
  order_ts        timestamptz,
  total_amount    numeric(12,2)
);

create table if not exists bronze_quarantine.order_items_qt
(
  snapshot_ts     timestamptz default now(),
  quarantine_reason text not null,
  source_layer    text not null,
  scan_batch_id   uuid default gen_random_uuid(),
  order_item_id   bigint,
  order_id        bigint,
  product_id      bigint,
  qty             int,
  unit_price      numeric(12,2),
  discount        numeric(12,2),
  tax             numeric(12,2),
  line_amount     numeric(12,2)
);

create table if not exists bronze_quarantine.payments_qt
(
  snapshot_ts     timestamptz default now(),
  quarantine_reason text not null,
  source_layer    text not null,
  scan_batch_id   uuid default gen_random_uuid(),
  payment_id      bigint,
  order_id        bigint,
  method          text,
  amount          numeric(12,2),
  status          text,
  paid_ts         timestamptz
);

create table if not exists bronze_quarantine.shipments_qt
(
  snapshot_ts     timestamptz default now(),
  quarantine_reason text not null,
  source_layer    text not null,
  scan_batch_id   uuid default gen_random_uuid(),
  shipment_id     bigint,
  order_id        bigint,
  carrier         text,
  service         text,
  tracking_no     text,
  shipped_ts      timestamptz,
  status          text
);

create table if not exists bronze_quarantine.customers_qt
(
  snapshot_ts     timestamptz default now(),
  quarantine_reason text not null,
  source_layer    text not null,
  scan_batch_id   uuid default gen_random_uuid(),
  customer_id     bigint,
  email           text,
  customer_name   text,
  country         text,
  created_at      timestamptz,
  status          text
);

create table if not exists bronze_quarantine.products_qt
(
  snapshot_ts     timestamptz default now(),
  quarantine_reason text not null,
  source_layer    text not null,
  scan_batch_id   uuid default gen_random_uuid(),
  product_id      bigint,
  title           text,
  category        text,
  brand           text,
  active          boolean
);
