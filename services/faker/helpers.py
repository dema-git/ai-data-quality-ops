###############################################################
#
# helpers.py
#
# Collection of helpers used by the SessionEventFaker
#
# This module contains small, isolated functions responsible for generating
# different atomic parts of a synthetic e-commerce user session:
#
#   Session-level helpers — responsible for creating session metadata:
#       - generate_session_ids(): produces (session_id, user_id)
#       - decide_conversion(): determines if a session will convert
#       - generate_base_time(): picks the starting timestamp
#       - generate_event_count(): decides number of events per session
#       - choose_product_for_session(): attaches a product to a session
#
#   Context helpers — fixed values shared within one session:
#       - generate_session_context(): prepares user-agent, ip, AB group, initial URLs
#
#   Event-level helpers — logic applied for each event inside the session:
#       - choose_event_type(): selects event type (page_view, add_to_cart, purchase)
#       - advance_event_time(): offsets timestamp for each step
#       - update_page_url_for_event(): updates page/referrer URLs based on behaviour
#       - random_page_url(): generates realistic shopping URLs
#       - build_extra_payload(): constructs A/B testing and scroll metadata
#       - build_raw_event(): final assembly of a Bronze-layer raw event

###############################################################

import random
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Tuple, List

from faker import Faker
from .config import FakerConfig


##########################
# Session-level helpers
##########################


def generate_session_ids() -> Tuple[str, str]:
    """
    Generate a new (session_id, user_id) pair
    """
    return str(uuid4()), str(uuid4())


def decide_conversion(conversion_probability: float) -> bool:
    """
    Decide whether this session will end with a purchase
    """
    return random.random() < conversion_probability


def generate_base_time() -> datetime:
    """
    Generate base event time within the last 24 hours
    """
    now = datetime.now(timezone.utc)
    delta_minutes = random.randint(0, 24 * 60)
    return now - timedelta(minutes=delta_minutes)


def generate_event_count(min_events: int, max_events: int) -> int:
    """
    Decide how many events will be in this session
    """
    return random.randint(min_events, max_events)


def choose_product_for_session(will_convert: bool, categories: List[str],
) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Decide if this session is tied to a product
    Returns (product_id, category, price)
    """
    has_product = will_convert or (random.random() < 0.5)
    if not has_product:
        return None, None, None

    product_id = str(uuid4())
    category = random.choice(categories)
    price = round(random.uniform(10, 200), 2)
    return product_id, category, price


##########################
# Context helpers
##########################


def generate_session_context(faker: Faker, config: FakerConfig) -> Dict:
    """
    Generate context for the whole session:
    user_agent, ip, ab_group, initial urls
    """
    return {
        "user_agent": faker.user_agent(),
        "ip_address": faker.ipv4_public(),
        "ab_group": random.choice(config.ab_groups),
        "current_url": config.base_url + "/home",
        "referrer_url": faker.url(),
    }


##########################
# Event-level helpers
##########################


def choose_event_type(event_index: int, total_events: int, will_convert: bool) -> str:
    """Pick event type for a given position in the session."""
    if event_index == 0:
        return "page_view"

    if will_convert and event_index == total_events - 1:
        return "purchase"

    if 0 < event_index < total_events - 1 and random.random() < 0.3:
        return "add_to_cart"

    return "page_view"


def advance_event_time(base_time: datetime, step_index: int) -> datetime:
    """Calculate event_time for a given step index."""
    return base_time + timedelta(seconds=10 * step_index)


def update_page_url_for_event(base_url: str, current_url: str, event_type: str,
    product_id: Optional[str], faker: Faker) -> Tuple[str, str]:
    """
    Decide next page_url and return (new_current_url, new_referrer_url)
    referrer_url is the previous current_url
    """
    referrer_url = current_url

    if event_type == "page_view":
        current_url = random_page_url(faker, base_url)
    elif event_type == "add_to_cart" and product_id:
        current_url = f"{base_url}/product/{product_id[-6:]}"
    elif event_type == "purchase" and product_id:
        current_url = f"{base_url}/checkout/success"

    return current_url, referrer_url


def random_page_url(faker: Faker, base_url: str) -> str:
    """
    Generate fake URLs
    """
    paths = [
        "/home",
        f"/search?q={faker.word()}",
        f"/search?q={'+'.join(faker.words(nb=2))}",
        f"/search?q=bike+{faker.word()}",

        # bike categories
        "/category/mountain-bikes",
        "/category/road-bikes",
        "/category/gravel-bikes",
        "/category/e-bikes",
        "/category/kids-bikes",
        "/category/folding-bikes",
        "/category/city-bikes",

        # components
        "/category/components/drivetrain",
        "/category/components/brakes",
        "/category/components/wheels",
        "/category/components/handlebars",
        "/category/components/saddles",
        "/category/components/pedals",

        # accessories
        "/category/accessories/helmets",
        "/category/accessories/lights",
        "/category/accessories/locks",
        "/category/accessories/bags",
        "/category/accessories/pumps",
        "/category/accessories/tools",

        # promo
        "/promo/spring-sale",
        "/promo/winter-clearance",
        "/promo/bike-of-the-day",
        "/promo/accessories-discount",

        # brands
        f"/brand/{random.choice(['trek', 'specialized', 'giant', 'cannondale', 'cube', 'scott'])}",

        # filtered categories
        f"/category/mountain-bikes?wheel_size={random.choice([27.5, 29])}",
        f"/category/road-bikes?frame={random.choice(['carbon', 'aluminum'])}",
        f"/category/e-bikes?motor={random.choice(['bosch', 'shimano', 'yamaha'])}",

        # product detail
        f"/product/{faker.uuid4()[:8]}",

        # other
        "/cart",
        "/wishlist",
        "/compare",
    ]
    return base_url + random.choice(paths)


def build_extra_payload(event_type: str, ab_group: str,
    price: Optional[float], payment_methods: List[str]) -> Dict:
    """
    Build "extra" payload for an event
    """
    extra: Dict = {
        "ab_group": ab_group,
        "scroll_depth": random.randint(0, 100),
    }

    if event_type == "purchase" and price is not None:
        extra["payment_method"] = random.choice(payment_methods)
        extra["basket_value"] = round(price * random.uniform(1.0, 2.5), 2)

    return extra


def build_raw_event(
    *,
    event_time: datetime,
    session_id: str,
    user_id: str,
    event_type: str,
    page_url: str,
    referrer_url: str,
    user_agent: str,
    ip_address: str,
    product_id: Optional[str],
    price: Optional[float],
    extra: Dict,
) -> Dict:
    """
    Build final raw event dict (bronze event)
    """
    return {
        "event_time": event_time.isoformat(),
        "session_id": session_id,
        "user_id": user_id,
        "event_type": event_type,
        "page_url": page_url,
        "referrer_url": referrer_url,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "product_id": product_id if event_type in ("add_to_cart", "purchase") else None,
        "price": price if event_type in ("add_to_cart", "purchase") else None,
        "extra": extra,
    }
