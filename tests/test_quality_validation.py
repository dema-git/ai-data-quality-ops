#####################################################################
# tests/test_quality_validation.py
#
# Tests for deterministic data-quality checks in the Medallion pipeline.
#
# The tests verify that invalid Bronze records are captured as structured
# quality issues and do not block valid records from reaching Silver.
#####################################################################

from datetime import datetime, timezone
import importlib
from pathlib import Path
import sys

FASTAPI_APP_PATH = Path(__file__).resolve().parents[1] / "services" / "fastapi_app"
ROOT_SERVICES_PATH = Path(__file__).resolve().parents[1] / "services"
sys.path.insert(0, str(FASTAPI_APP_PATH))

_root_services_module = sys.modules.pop("services", None)
try:
    services_pkg = importlib.import_module("services")
    services_pkg.__path__.append(str(ROOT_SERVICES_PATH))
    medallion_service = importlib.import_module(
        "services.medallion_pipeline.medallion_service"
    )
    bronze_model = importlib.import_module("services.medallion_models.bronze_model")
    medallion_helpers = importlib.import_module("services.medallion_models.helpers")
finally:
    if _root_services_module is not None:
        sys.modules["services"] = _root_services_module

BronzeWebEvent = bronze_model.BronzeWebEvent


def make_bronze_event(**overrides):
    event = {
        "event_time": datetime.now(timezone.utc).isoformat(),
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
    event.update(overrides)
    return BronzeWebEvent(**event)


def test_validate_bronze_event_returns_quality_issues():
    event = make_bronze_event(
        event_time="not-a-valid-timestamp",
        session_id="",
        event_type="purchase",
        product_id=None,
        price=-10.0,
        extra={
            "ab_group": "Z",
            "scroll_depth": -25,
            "injected_quality_issue": "invalid_extra_payload",
        },
    )

    issues = medallion_helpers.validate_bronze_event(
        event,
        source_object_name="bronze/batch.parquet",
    )

    issue_types = {issue.issue_type for issue in issues}

    assert "invalid_event_time" in issue_types
    assert "missing_session_id" in issue_types
    assert "negative_price" in issue_types
    assert "purchase_without_product" in issue_types
    assert "invalid_scroll_depth" in issue_types
    assert "invalid_ab_group" in issue_types
    assert all(issue.source_object_name == "bronze/batch.parquet" for issue in issues)
    assert all(issue.injected_quality_issue == "invalid_extra_payload" for issue in issues)


def test_bronze_to_silver_quarantines_invalid_events(monkeypatch):
    valid_event = make_bronze_event()
    invalid_event = make_bronze_event(
        event_time="bad-time",
        extra={"ab_group": "A", "scroll_depth": 50},
    )

    bronze_files = [
        {
            "object_name": "2026/05/22/events.parquet",
            "data": [
                valid_event.__dict__,
                invalid_event.__dict__,
            ],
        }
    ]
    uploaded = {}
    archive_tasks = []

    def fake_get_files_data(bucket_name):
        if bucket_name == medallion_service.BRONZE_BUCKET:
            return bronze_files
        raise AssertionError(f"unexpected bucket: {bucket_name}")

    def fake_upload_batch(batch, bucket_name):
        uploaded[bucket_name] = batch[0]

    def fake_existing_keys(dataset, layer, partition_keys):
        return set()

    def fake_update_processing_state(**kwargs):
        assert kwargs["layer"] == "bronze_to_silver"

    def fake_enqueue_archive_task(**kwargs):
        archive_tasks.append(kwargs)

    monkeypatch.setattr(medallion_service, "get_files_data", fake_get_files_data)
    monkeypatch.setattr(medallion_service, "upload_batch", fake_upload_batch)
    monkeypatch.setattr(
        medallion_service,
        "get_existing_archive_partition_keys",
        fake_existing_keys,
    )
    monkeypatch.setattr(
        medallion_service,
        "update_processing_state",
        fake_update_processing_state,
    )
    monkeypatch.setattr(
        medallion_service,
        "enqueue_archive_task",
        fake_enqueue_archive_task,
    )

    result = medallion_service.run_bronze_to_silver()

    assert result["bronze_count"] == 2
    assert result["silver_count"] == 1
    assert result["quality_issues_count"] == 1
    assert len(uploaded[medallion_service.SILVER_BUCKET]) == 1
    assert len(uploaded[medallion_service.QUALITY_ISSUES_BUCKET]) == 1
    assert uploaded[medallion_service.QUALITY_ISSUES_BUCKET][0]["issue_type"] == "invalid_event_time"
    assert archive_tasks[0]["partition_key"] == "2026/05/22/events.parquet"
