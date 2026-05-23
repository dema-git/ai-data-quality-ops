#####################################################################
# tests/test_incident_service.py
#
# Tests for deterministic quality incident report construction.
#
# The incident report groups already detected issues into operational
# categories without using external systems or AI calls.
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
    incident_service = importlib.import_module(
        "services.medallion_pipeline.incident_service"
    )
finally:
    if _root_services_module is not None:
        sys.modules["services"] = _root_services_module


def test_incident_report_groups_issue_types_by_operational_category():
    summary = {
        "total_issues": 10,
        "quality_report_files": 3,
        "affected_source_files": 2,
        "latest_detected_at": "2026-05-23T09:02:00+00:00",
        "by_issue_type": {
            "invalid_ab_group": 4,
            "invalid_scroll_depth": 3,
            "negative_price": 2,
            "missing_session_id": 1,
        },
        "recent_issues": [
            {
                "issue_type": "invalid_ab_group",
                "issue_field": "extra.ab_group",
                "source_object_name": "bronze/source.parquet",
            },
            {
                "issue_type": "negative_price",
                "issue_field": "price",
                "source_object_name": "bronze/source.parquet",
            },
        ],
    }

    report = incident_service.build_quality_incident_report(summary)

    assert report["status"] == "detected"
    assert report["severity"] == "unclassified"
    assert report["dominant_category"] == "schema_payload"
    assert report["title"] == "Schema and payload failures detected"
    assert report["total_issues"] == 10
    assert report["categories"][0] == {
        "category": "schema_payload",
        "issue_count": 7,
        "issue_types": [
            {"issue_type": "invalid_ab_group", "count": 4},
            {"issue_type": "invalid_scroll_depth", "count": 3},
        ],
        "recent_evidence": [
            {
                "issue_type": "invalid_ab_group",
                "issue_field": "extra.ab_group",
                "source_object_name": "bronze/source.parquet",
            }
        ],
    }


def test_incident_report_returns_clear_state_for_empty_summary():
    report = incident_service.build_quality_incident_report({
        "total_issues": 0,
        "quality_report_files": 0,
        "affected_source_files": 0,
        "latest_detected_at": None,
        "by_issue_type": {},
        "recent_issues": [],
    })

    assert report["status"] == "clear"
    assert report["severity"] == "none"
    assert report["dominant_category"] is None
    assert report["categories"] == []


def test_get_incident_report_uses_quality_summary_service(monkeypatch):
    received_limits = []

    def fake_get_quality_issues_summary(recent_limit):
        received_limits.append(recent_limit)
        return {
            "total_issues": 0,
            "quality_report_files": 0,
            "affected_source_files": 0,
            "latest_detected_at": None,
            "by_issue_type": {},
            "recent_issues": [],
        }

    monkeypatch.setattr(
        incident_service,
        "get_quality_issues_summary",
        fake_get_quality_issues_summary,
    )

    report = incident_service.get_quality_incident_report(recent_limit=3)

    assert received_limits == [3]
    assert report["status"] == "clear"
