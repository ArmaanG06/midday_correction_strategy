
from datetime import datetime
from dataclasses import dataclass

@dataclass
class Order:
    timestamp: datetime
    symbol: str
    side: str            # BUY / SELL
    quantity: int
    order_type: str      # MARKET