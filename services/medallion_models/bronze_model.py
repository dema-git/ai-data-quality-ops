#################################################
# bronze_model.py
#
# define dataclass for BronzeWebEvent(all raw data from Kafka)
#################################################

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Dict


@dataclass
class BronzeWebEvent:
    """
    Raw fields from Kafka
    """
    event_time: str
    session_id: str
    user_id: str
    event_type: str
    page_url: str
    referrer_url: Optional[str] = None
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    product_id: Optional[str] = None
    price: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict)

