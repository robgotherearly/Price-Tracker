from pydantic import BaseModel, Field
from typing import Optional


class PriceRow(BaseModel):
    snapshot_date: str  # YYYY-MM-DD
    vendor: str
    vendor_sku: str
    product_name: str
    currency: str = Field(min_length=3, max_length=3)
    price: float
    category: Optional[str] = None
    url: Optional[str] = None
