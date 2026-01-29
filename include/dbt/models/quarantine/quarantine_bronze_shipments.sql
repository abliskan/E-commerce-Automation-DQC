with
no_shipment_id as (
  select * from bronze.shipments_raw where shipment_id is null
),
no_order_id as (
  select * from bronze.shipments_raw where order_id is null
),
crazy_ts as (
  select *
  from bronze.shipments_raw
  where shipped_ts is not null
    and (shipped_ts::timestamptz > now() + interval '7 day'
      or shipped_ts::timestamptz < '1970-01-01')
),
to_q as (
  select 'shipment_id_null' as reason, * from no_shipment_id
  union all select 'order_id_null', * from no_order_id
  union all select 'shipped_ts_out_of_range', * from crazy_ts
)
insert into bronze_quarantine.shipments_qt
  (quarantine_reason, source_layer, shipment_id, order_id, carrier, service, tracking_no, shipped_ts, status)
select
  reason,
  'bronze',
  shipment_id,
  order_id,
  carrier,
  service,
  tracking_no,
  shipped_ts,
  status
from to_q;