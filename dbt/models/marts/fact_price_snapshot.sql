select
  s.snapshot_date,
  md5(s.vendor) as vendor_id,
  md5(s.vendor || '|' || s.vendor_sku) as product_id,
  s.currency,
  s.price
from {{ ref('stg_prices') }} s
