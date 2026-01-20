select distinct
  md5(vendor || '|' || vendor_sku) as product_id,
  md5(vendor) as vendor_id,
  vendor_sku,
  product_name,
  category,
  url
from {{ ref('stg_prices') }}
