#####################################################################
# tests/test_incident_agent_router.py
#
# Tests for routing structured quality incidents to specialist profiles.
#
# Routing is deterministic and does not perform any external AI call.
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
    incident_agent_router = importlib.import_module(
        "services.medallion_pipeline.incident_agent_router"
    )
finally:
    if _root_services_module is not None:
        sys.modules["services"] = _root_services_module


def test_router_selects_profile_for_dominant_category():
    incident_report = {
        "status": "detected",
        "title": "Business rule validation failures detected",
        "pipeline_stage": "bronze_to_silver",
        "pipeline_impact": "Rejected records were quarantined.",
        "total_issues": 8,
        "latest_detected_at": "2026-05-23T12:00:00+00:00",
        "dominant_category": "business_rules",
        "categories": [
            {
                "category": "business_rules",
                "issue_count": 8,
                "issue_types": [{"issue_type": "negative_price", "count": 8}],
                "recent_evidence": [],
            }
        ],
        "recent_evidence": [{"issue_type": "negative_price"}],
    }

    routed = incident_agent_router.route_quality_incident_to_agent(incident_report)

    assert routed["routing_status"] == "routed"
    assert routed["agent"]["agent_id"] == "business_rules_agent"
    assert routed["agent"]["category"] == "business_rules"
    assert "# Business Rules Analyst" in routed["agent"]["instructions"]
    assert routed["incident_context"]["dominant_category_details"]["issue_count"] == 8


def test_all_registered_agent_profiles_have_instructions():
    for profile in incident_agent_router.AGENT_PROFILES.values():
        instructions = incident_agent_router._load_agent_instructions(
            profile["definition_file"]
        )

        assert profile["display_name"] in instructions


def test_router_does_not_route_clear_incident():
    routed = incident_agent_router.route_quality_incident_to_agent({
        "status": "clear",
        "dominant_category": None,
        "total_issues": 0,
    })

    assert routed == {
        "routing_status": "not_required",
        "reason": "No detected quality incident requires analysis.",
        "agent": None,
        "incident_context": None,
    }


def test_router_marks_unknown_category_for_manual_review():
    routed = incident_agent_router.route_quality_incident_to_agent({
        "status": "detected",
        "dominant_category": "unclassified",
        "total_issues": 2,
    })

    assert routed["routing_status"] == "manual_review_required"
    assert routed["agent"] is None
    assert routed["incident_context"] == {
        "dominant_category": "unclassified",
        "total_issues": 2,
    }


def test_routed_incident_builds_report_before_routing(monkeypatch):
    received_limits = []

    def fake_get_quality_incident_report(recent_limit):
        received_limits.append(recent_limit)
        return {
            "status": "clear",
            "dominant_category": None,
            "total_issues": 0,
        }

    monkeypatch.setattr(
        incident_agent_router,
        "get_quality_incident_report",
        fake_get_quality_incident_report,
    )

    routed = incident_agent_router.get_routed_quality_incident(recent_limit=4)

    assert received_limits == [4]
    assert routed["routing_status"] == "not_required"
