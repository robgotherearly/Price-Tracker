select distinct
  md5(vendor) as vendor_id,
  vendor
from {{ ref('stg_prices') }}
