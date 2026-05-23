#####################################################################
# tests/test_quality_summary.py
#
# Tests for quality issue aggregation over quarantined MinIO records.
#
# These tests verify that summary facts can be built without a real
# MinIO instance and are suitable for later API/incident consumers.
#####################################################################

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
    quality_service = importlib.import_module(
        "services.medallion_pipeline.quality_service"
    )
finally:
    if _root_services_module is not None:
        sys.modules["services"] = _root_services_module


def test_quality_summary_groups_issues_and_limits_recent_rows(monkeypatch):
    files = [
        {
            "object_name": "quality/report-1.parquet",
            "data": [
                {
                    "detected_at": "2026-05-22T19:11:31+00:00",
                    "issue_type": "missing_session_id",
                    "issue_field": "session_id",
                    "severity": "error",
                    "source_object_name": "bronze/source-1.parquet",
                    "injected_quality_issue": "missing_session_id",
                },
                {
                    "detected_at": "2026-05-22T19:11:33+00:00",
                    "issue_type": "negative_price",
                    "issue_field": "price",
                    "severity": "error",
                    "source_object_name": "bronze/source-2.parquet",
                    "injected_quality_issue": "negative_price",
                },
            ],
        },
        {
            "object_name": "quality/report-2.parquet",
            "data": [
                {
                    "detected_at": "2026-05-22T19:11:32+00:00",
                    "issue_type": "missing_session_id",
                    "issue_field": "session_id",
                    "severity": "error",
                    "source_object_name": "bronze/source-1.parquet",
                    "injected_quality_issue": "missing_session_id",
                }
            ],
        },
    ]

    monkeypatch.setattr(
        quality_service,
        "get_files_data",
        lambda bucket_name: files,
    )

    summary = quality_service.get_quality_issues_summary(recent_limit=2)

    assert summary["total_issues"] == 3
    assert summary["quality_report_files"] == 2
    assert summary["affected_source_files"] == 2
    assert summary["latest_detected_at"] == "2026-05-22T19:11:33+00:00"
    assert summary["by_issue_type"] == {
        "missing_session_id": 2,
        "negative_price": 1,
    }
    assert summary["by_severity"] == {"error": 3}
    assert [
        issue["issue_type"] for issue in summary["recent_issues"]
    ] == ["negative_price", "missing_session_id"]


def test_quality_summary_handles_empty_bucket(monkeypatch):
    monkeypatch.setattr(
        quality_service,
        "get_files_data",
        lambda bucket_name: [],
    )

    summary = quality_service.get_quality_issues_summary()

    assert summary == {
        "total_issues": 0,
        "quality_report_files": 0,
        "affected_source_files": 0,
        "latest_detected_at": None,
        "by_issue_type": {},
        "by_severity": {},
        "recent_issues": [],
    }
