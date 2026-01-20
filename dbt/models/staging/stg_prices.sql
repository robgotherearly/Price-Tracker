with src as (
  select
    load_ts,
    (payload->>'snapshot_date')::date as snapshot_date,
    payload->>'vendor' as vendor,
    payload->>'vendor_sku' as vendor_sku,
    payload->>'product_name' as product_name,
    payload->>'category' as category,
    payload->>'currency' as currency,
    (payload->>'price')::numeric as price,
    payload->>'url' as url
  from {{ source('raw', 'prices_raw') }}
),

dedup as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by vendor, vendor_sku, snapshot_date
        order by load_ts desc
      ) as rn
    from src
  ) t
  where rn = 1
)

select
  snapshot_date,
  vendor,
  vendor_sku,
  product_name,
  category,
  currency,
  price,
  url
from dedup
