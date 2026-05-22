#############################################################
# helpers.py
#
# Transformation functions for the medallion architecture.
# Handles data conversion between Bronze -> Silver -> Gold layers:
#############################################################

from typing import Optional
from urllib.parse import urlparse
from datetime import datetime

from services.medallion_models.gold_models import GoldPageView, GoldProductEvent
from services.medallion_models.silver_model import SilverWebEvent
from services.medallion_models.bronze_model import BronzeWebEvent

###################
# BRONZE -> SILVER
###################

def bronze_to_silver(event: BronzeWebEvent) -> SilverWebEvent:
    """
    Transform a BronzeWebEvent into a SilverWebEvent.

    This step normalizes raw event data:
    - Parses the URL into structured parts (host, path, section, category, item)
    - Extracts analytical fields from `extra` (scroll depth, A/B group)
    - Converts event_time to a proper datetime object
    - Keeps relevant metadata for further processing

    Silver layer contains cleaned and structured data,
    but still keeps technical fields (IP, user agent).
    """
    parsed = urlparse(event.page_url)
    path_parts = [p for p in parsed.path.split("/") if p]

    page_section = path_parts[0] if len(path_parts) >= 1 else None
    page_category = path_parts[1] if len(path_parts) >= 2 else None
    page_item = path_parts[2] if len(path_parts) >= 3 else None

    scroll_depth = event.extra.get("scroll_depth")
    ab_group = event.extra.get("ab_group")

    return SilverWebEvent(
        event_time=datetime.fromisoformat(event.event_time),
        session_id=event.session_id,
        user_id=event.user_id,
        event_type=event.event_type,
        page_url=event.page_url,
        page_path=parsed.path,
        page_host=parsed.netloc,
        page_section=page_section,
        page_category=page_category,
        page_item=page_item,
        referrer_url=event.referrer_url,
        product_id=event.product_id,
        price=event.price,
        ab_group=ab_group,
        scroll_depth=scroll_depth,
        ip_address=event.ip_address,
        user_agent=event.user_agent,
    )

###################
# SILVER -> GOLD
###################


def _is_missing(value: Optional[str]) -> bool:
    """
    Helper function to check if a value should be treated as missing.

    Returns True for:
    - None
    - empty strings
    - common string representations of null values
      like "nan", "none", "null" (case-insensitive).
        """
    if value is None:
        return True
    if isinstance(value, str):
        v = value.strip().lower()
        return v in {"", "nan", "none", "null"}
    return False


def silver_to_gold_page_view(e: SilverWebEvent) -> Optional[GoldPageView]:
    """
    Build a GoldPageView object from a Silver event.

    This function is used for non-product page views.
    If the URL points to a product page (contains "/product/"),
    the event is skipped here because it should be handled
    as a product event in the Gold layer.
    """
    if e.page_url and "/product/" in e.page_url:
        return None

    return GoldPageView(
        event_time=e.event_time,
        session_id=e.session_id,
        user_id=e.user_id,
        page_url=e.page_url,
        page_category=e.page_category,
        page_item=e.page_item,
        scroll_depth=e.scroll_depth,
        ab_group=e.ab_group,
    )


def silver_to_gold_product(e: SilverWebEvent) -> Optional[GoldProductEvent]:
    """
    Build a GoldProductEvent object from a Silver event.

    This function creates a product-level analytics record.
    If product_id is missing (None, empty, "nan", etc.),
    the event is ignored and None is returned.

    Only valid product interactions (e.g. product view,
    add to cart, purchase) should reach this layer.
    """
    if _is_missing(e.product_id):
        return None

    return GoldProductEvent(
        event_time=e.event_time,
        session_id=e.session_id,
        user_id=e.user_id,
        product_id=e.product_id,
        price=e.price,
        ab_group=e.ab_group,
        page_url=e.page_url,
    )