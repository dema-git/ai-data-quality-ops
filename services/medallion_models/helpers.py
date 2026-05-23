#############################################################
# helpers.py
#
# Transformation functions for the medallion architecture.
# Handles data conversion between Bronze -> Silver -> Gold layers:
#############################################################

import json
from dataclasses import asdict
from typing import Optional, List
from urllib.parse import urlparse
from datetime import datetime, timezone

from services.medallion_models.gold_models import GoldPageView, GoldProductEvent
from services.medallion_models.quality_model import QualityIssue
from services.medallion_models.silver_model import SilverWebEvent
from services.medallion_models.bronze_model import BronzeWebEvent


VALID_EVENT_TYPES = {"page_view", "add_to_cart", "purchase"}
VALID_AB_GROUPS = {"A", "B"}

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


def check_event_time(
    event: BronzeWebEvent,
    source_object_name: str = "",
) -> List[QualityIssue]:
    """Check that the raw event timestamp can be normalized downstream."""
    try:
        datetime.fromisoformat(event.event_time)
    except (TypeError, ValueError):
        return [
            build_quality_issue(
                event=event,
                issue_type="invalid_event_time",
                issue_field="event_time",
                severity="error",
                source_object_name=source_object_name,
            )
        ]
    return []


def check_required_ids(
    event: BronzeWebEvent,
    source_object_name: str = "",
) -> List[QualityIssue]:
    """Check identifiers required to associate events with sessions and users."""
    issues: List[QualityIssue] = []
    if _is_missing(event.session_id):
        issues.append(
            build_quality_issue(
                event=event,
                issue_type="missing_session_id",
                issue_field="session_id",
                severity="error",
                source_object_name=source_object_name,
            )
        )

    if _is_missing(event.user_id):
        issues.append(
            build_quality_issue(
                event=event,
                issue_type="missing_user_id",
                issue_field="user_id",
                severity="error",
                source_object_name=source_object_name,
            )
        )
    return issues


def check_event_type(
    event: BronzeWebEvent,
    source_object_name: str = "",
) -> List[QualityIssue]:
    """Check that the event belongs to the supported business event set."""
    if event.event_type not in VALID_EVENT_TYPES:
        return [
            build_quality_issue(
                event=event,
                issue_type="unknown_event_type",
                issue_field="event_type",
                severity="error",
                source_object_name=source_object_name,
            )
        ]
    return []


def check_price(
    event: BronzeWebEvent,
    source_object_name: str = "",
) -> List[QualityIssue]:
    """Check that a supplied price is numeric and non-negative."""
    try:
        invalid_price = event.price is not None and float(event.price) < 0
    except (TypeError, ValueError):
        invalid_price = True

    if invalid_price:
        return [
            build_quality_issue(
                event=event,
                issue_type="negative_price",
                issue_field="price",
                severity="error",
                source_object_name=source_object_name,
            )
        ]
    return []


def check_purchase_consistency(
    event: BronzeWebEvent,
    source_object_name: str = "",
) -> List[QualityIssue]:
    """Check business consistency rules for purchase events."""
    if event.event_type == "purchase" and _is_missing(event.product_id):
        return [
            build_quality_issue(
                event=event,
                issue_type="purchase_without_product",
                issue_field="product_id",
                severity="error",
                source_object_name=source_object_name,
            )
        ]
    return []


def check_extra_payload(
    event: BronzeWebEvent,
    source_object_name: str = "",
) -> List[QualityIssue]:
    """Check the optional analytical fields carried in the event payload."""
    issues: List[QualityIssue] = []
    extra = event.extra if isinstance(event.extra, dict) else {}

    if event.extra is not None and not isinstance(event.extra, dict):
        issues.append(
            build_quality_issue(
                event=event,
                issue_type="invalid_extra_payload",
                issue_field="extra",
                severity="error",
                source_object_name=source_object_name,
            )
        )

    scroll_depth = extra.get("scroll_depth")
    try:
        invalid_scroll_depth = scroll_depth is not None and not 0 <= int(scroll_depth) <= 100
    except (TypeError, ValueError):
        invalid_scroll_depth = True

    if invalid_scroll_depth:
        issues.append(
            build_quality_issue(
                event=event,
                issue_type="invalid_scroll_depth",
                issue_field="extra.scroll_depth",
                severity="error",
                source_object_name=source_object_name,
            )
        )

    ab_group = extra.get("ab_group")
    if ab_group is not None and ab_group not in VALID_AB_GROUPS:
        issues.append(
            build_quality_issue(
                event=event,
                issue_type="invalid_ab_group",
                issue_field="extra.ab_group",
                severity="error",
                source_object_name=source_object_name,
            )
        )

    return issues


QUALITY_CHECKS = [
    check_event_time,
    check_required_ids,
    check_event_type,
    check_price,
    check_purchase_consistency,
    check_extra_payload,
]


def validate_bronze_event(
    event: BronzeWebEvent,
    source_object_name: str = "",
) -> List[QualityIssue]:
    """
    Run all deterministic checks before a Bronze event is promoted to Silver.

    The function returns all detected issues instead of raising. This lets the
    ETL pipeline quarantine bad records while continuing to process valid ones.
    """
    issues: List[QualityIssue] = []
    for check in QUALITY_CHECKS:
        issues.extend(check(event, source_object_name))
    return issues


def build_quality_issue(
    *,
    event: BronzeWebEvent,
    issue_type: str,
    issue_field: str,
    severity: str,
    source_object_name: str = "",
) -> QualityIssue:
    """
    Build a serializable quality record for one validation failure.
    """
    raw_event = asdict(event)
    extra = event.extra if isinstance(event.extra, dict) else {}
    injected_quality_issue = extra.get("injected_quality_issue", "")

    return QualityIssue(
        detected_at=datetime.now(timezone.utc).isoformat(),
        source_layer="bronze",
        source_bucket="events-bronze",
        source_object_name=source_object_name,
        issue_type=issue_type,
        issue_field=issue_field,
        severity=severity,
        event_time=str(event.event_time),
        session_id=str(event.session_id),
        user_id=str(event.user_id),
        event_type=str(event.event_type),
        injected_quality_issue=str(injected_quality_issue),
        raw_event_json=json.dumps(raw_event, default=str, sort_keys=True),
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
