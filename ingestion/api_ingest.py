import datetime as dt
import random
from typing import List
from schemas import PriceRow

VENDOR = "FakeStore"

def fetch_prices() -> List[PriceRow]:
    """
    Simulated API ingestion.
    Swap this with real API calls (requests.get) later.
    """
    today = dt.date.today().isoformat()

    sample = [
        ("SKU-1001", "Wireless Mouse", "electronics", 12.99),
        ("SKU-1002", "Mechanical Keyboard", "electronics", 48.50),
        ("SKU-2001", "Cotton T-Shirt", "apparel", 9.99),
        ("SKU-3001", "Coffee Beans 1kg", "grocery", 15.75),
    ]

    rows = []
    for sku, name, cat, base_price in sample:
        # minor price movement
        price = round(base_price * random.uniform(0.95, 1.05), 2)
        rows.append(
            PriceRow(
                snapshot_date=today,
                vendor=VENDOR,
                vendor_sku=sku,
                product_name=name,
                category=cat,
                currency="USD",
                price=price,
                url=f"https://example.com/{sku}",
            )
        )
    return rows
