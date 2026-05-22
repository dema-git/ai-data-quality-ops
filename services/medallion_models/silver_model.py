#################################################
# silver_model.py
#
# define dataclass for SilverWebEvent (cleaned and structured RAW data)
#################################################

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class SilverWebEvent:
    """
    Normalized model
    """
    event_time: datetime
    session_id: str
    user_id: str
    event_type: str

    page_url: str
    page_path: str
    page_host: str
    page_section: Optional[str] = None
    page_category: Optional[str] = None
    page_item: Optional[str] = None

    referrer_url: Optional[str] = None

    product_id: Optional[str] = None
    price: Optional[float] = None

    ab_group: Optional[str] = None
    scroll_depth: Optional[int] = None

    ip_address: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None

    user_agent: Optional[str] = None
    device_type: Optional[str] = None
    browser_name: Optional[str] = None



