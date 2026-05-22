#####################################################################
# tests/test_medallion_idempotency.py
#
# These tests verify idempotency guards in the Medallion ETL pipeline
# without running MinIO, PostgreSQL, Kafka, or Airflow.
#
# The tests confirm that:
# - files with existing archive tasks are skipped before transformation
# - the Gold loader does not insert rows from already processed files
# - skipped files are reported explicitly in the returned summary
#
# The goal is to prevent duplicate processing when /etl/run-full is
# triggered repeatedly before the archive worker moves active files.
#####################################################################

from datetime import datetime
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
    gold_loader = importlib.import_module("services.medallion_pipeline.gold_loader")
    medallion_service = importlib.import_module(
        "services.medallion_pipeline.medallion_service"
    )
finally:
    if _root_services_module is not None:
        sys.modules["services"] = _root_services_module


def test_filter_unprocessed_files_skips_existing_archive_tasks(monkeypatch):
    """
    Files that already have archive tasks should not be processed again.
    """
    files = [
        {"object_name": "2026/05/17/new.parquet", "data": []},
        {"object_name": "2026/05/17/processed.parquet", "data": []},
    ]

    def fake_existing_keys(dataset, layer, partition_keys):
        assert dataset == "web_events"
        assert layer == "bronze"
        assert partition_keys == [
            "2026/05/17/new.parquet",
            "2026/05/17/processed.parquet",
        ]
        return {"2026/05/17/processed.parquet"}

    monkeypatch.setattr(
        medallion_service,
        "get_existing_archive_partition_keys",
        fake_existing_keys,
    )

    result = medallion_service.filter_unprocessed_files(files, layer="bronze")

    assert result == [{"object_name": "2026/05/17/new.parquet", "data": []}]


def test_gold_loader_skips_files_with_existing_archive_tasks(monkeypatch):
    """
    Re-running the Gold loader should not insert rows from processed files.
    """
    page_view_files = [
        {
            "object_name": "gold/page_views/processed.parquet",
            "data": [
                {
                    "event_time": datetime(2026, 5, 17, 12, 0, 0),
                    "session_id": "session-1",
                    "user_id": "user-1",
                    "page_url": "https://example.com/products",
                    "page_category": "products",
                    "page_item": "sku-1",
                    "scroll_depth": 50,
                    "ab_group": "A",
                }
            ],
        }
    ]
    product_event_files = [
        {
            "object_name": "gold/product_events/processed.parquet",
            "data": [
                {
                    "event_time": datetime(2026, 5, 17, 12, 0, 1),
                    "session_id": "session-1",
                    "user_id": "user-1",
                    "product_id": "sku-1",
                    "price": 10.5,
                    "ab_group": "A",
                    "page_url": "https://example.com/products",
                }
            ],
        }
    ]

    def fake_get_files_data(bucket_name):
        if bucket_name == gold_loader.GOLD_PAGE_VIEWS_BUCKET:
            return page_view_files
        if bucket_name == gold_loader.GOLD_PRODUCT_VIEWS_BUCKET:
            return product_event_files
        raise AssertionError(f"unexpected bucket: {bucket_name}")

    def fake_existing_keys(dataset, layer, partition_keys):
        assert dataset == "web_events"
        if layer == "gold_page_views":
            return {"gold/page_views/processed.parquet"}
        if layer == "gold_product_events":
            return {"gold/product_events/processed.parquet"}
        raise AssertionError(f"unexpected layer: {layer}")

    def fail_insert(*args, **kwargs):
        raise AssertionError("already processed files must not be inserted")

    monkeypatch.setattr(gold_loader, "get_files_data", fake_get_files_data)
    monkeypatch.setattr(
        gold_loader,
        "get_existing_archive_partition_keys",
        fake_existing_keys,
    )
    monkeypatch.setattr(gold_loader, "insert_gold_page_views", fail_insert)
    monkeypatch.setattr(gold_loader, "insert_gold_product_events", fail_insert)

    result = gold_loader.process_gold_outbox_tasks()

    assert result == {
        "inserted_page_views": 0,
        "inserted_product_events": 0,
        "page_view_files": 0,
        "product_event_files": 0,
        "skipped_page_view_files": 1,
        "skipped_product_event_files": 1,
    }
