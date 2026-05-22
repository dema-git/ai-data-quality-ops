#################################################
# gold_models.py
#
# Defines dataclasses for GoldPageView and GoldProductEvent.
# These two tables contain all data that should be sent
# to users via FastAPI endpoints.
#################################################

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class GoldPageView:
    """
    Represents page view vents from user sessions.
    Contains cleaned data ready for analytics and reporting.
    """
    event_time: datetime
    session_id: str
    user_id: str
    page_url: str
    page_category: Optional[str]
    page_item: Optional[str]
    scroll_depth: Optional[int]
    ab_group: Optional[str]


@dataclass
class GoldProductEvent:
    """
    Represents product interaction events.
    Contains normalized product data.
    """
    event_time: datetime
    session_id: str
    user_id: str
    product_id: str
    price: Optional[float]
    ab_group: Optional[str]
    page_url: str