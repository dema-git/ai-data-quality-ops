#####################################################################
# tests/test_faker_bad_data.py
#
# These tests verify controlled bad data injection for the synthetic
# event generator. The default generator path must stay clean, while
# incident scenarios can opt in through FakerConfig.bad_data_rate.
#####################################################################

from services.faker.config import FakerConfig
from services.faker.generator import SessionEventFaker
from services.faker.helpers import BAD_DATA_ISSUES, inject_bad_data


def test_generator_does_not_inject_bad_data_by_default():
    generator = SessionEventFaker(
        FakerConfig(seed=42, bad_data_rate=0.0, min_events=3, max_events=3)
    )

    events = generator.generate_session_events()

    assert len(events) == 3
    assert all("injected_quality_issue" not in e["extra"] for e in events)


def test_generator_injects_bad_data_when_rate_is_one():
    generator = SessionEventFaker(
        FakerConfig(seed=42, bad_data_rate=1.0, min_events=3, max_events=3)
    )

    events = generator.generate_session_events()

    assert len(events) == 3
    assert all("injected_quality_issue" in e["extra"] for e in events)


def test_each_bad_data_issue_has_expected_shape():
    base_event = {
        "event_time": "2026-05-22T10:00:00+00:00",
        "session_id": "session-1",
        "user_id": "user-1",
        "event_type": "page_view",
        "page_url": "https://shop.example.com/home",
        "referrer_url": "https://example.com",
        "user_agent": "pytest",
        "ip_address": "127.0.0.1",
        "product_id": None,
        "price": None,
        "extra": {"ab_group": "A", "scroll_depth": 50},
    }

    injected = {
        issue: inject_bad_data(base_event, issue_type=issue)
        for issue in BAD_DATA_ISSUES
    }

    assert injected["missing_session_id"]["session_id"] == ""
    assert injected["invalid_event_time"]["event_time"] == "not-a-valid-timestamp"
    assert injected["negative_price"]["price"] < 0
    assert injected["unknown_event_type"]["event_type"] == "unknown_event"
    assert injected["purchase_without_product"]["product_id"] is None
    assert injected["purchase_without_product"]["event_type"] == "purchase"
    assert injected["invalid_extra_payload"]["extra"]["scroll_depth"] == -25
    assert all(
        e["extra"]["injected_quality_issue"] == issue
        for issue, e in injected.items()
    )
