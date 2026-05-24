#####################################################################
# tests/test_dashboard_routes.py
#
# Tests for dashboard-only routes.
#
# These tests verify that the HTML dashboard can request an incident
# explanation without calling the protected external quality API route.
#####################################################################

import asyncio
import importlib
from pathlib import Path
import sys

from starlette.requests import Request

FASTAPI_APP_PATH = Path(__file__).resolve().parents[1] / "services" / "fastapi_app"
ROOT_SERVICES_PATH = Path(__file__).resolve().parents[1] / "services"
sys.path.insert(0, str(FASTAPI_APP_PATH))

_root_services_module = sys.modules.pop("services", None)
try:
    services_pkg = importlib.import_module("services")
    services_pkg.__path__.append(str(ROOT_SERVICES_PATH))
    dashboard_routes = importlib.import_module("api.dashboard_routes")
finally:
    if _root_services_module is not None:
        sys.modules["services"] = _root_services_module


def _make_request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/dashboard/quality/incident-explanation",
            "headers": [],
        }
    )


def test_dashboard_router_exposes_incident_explanation_path():
    paths = {route.path for route in dashboard_routes.router.routes}

    assert "/dashboard/quality/incident-explanation" in paths


def test_dashboard_incident_explanation_calls_service_with_fixed_limit(monkeypatch):
    expected = {
        "analysis_status": "generated",
        "provider": "mock",
        "model": "test-model",
        "agent": {
            "agent_id": "schema_payload_agent",
            "display_name": "Schema and Payload Analyst",
            "category": "schema_payload",
        },
        "incident_context": {
            "title": "Schema and payload failures detected",
            "pipeline_stage": "bronze_to_silver",
            "total_issues": 3,
            "dominant_category": "schema_payload",
            "latest_detected_at": "2026-05-24T10:00:00+00:00",
            "dominant_category_details": {
                "issue_types": [{"issue_type": "unknown_event_type", "count": 3}],
            },
        },
        "explanation": {
            "assessment": "Detected schema violations.",
            "possible_causes": ["Producer emitted an unknown event type."],
            "recommended_checks": ["Review allowed event_type values."],
            "confidence": "medium",
        },
    }
    received_limits = []

    def fake_generate_incident_explanation(recent_limit):
        received_limits.append(recent_limit)
        return expected

    monkeypatch.setattr(
        dashboard_routes,
        "generate_incident_explanation",
        fake_generate_incident_explanation,
    )

    response = asyncio.run(
        dashboard_routes.dashboard_quality_incident_explanation(_make_request())
    )

    assert response.status_code == 200
    assert response.context["result"] == expected
    assert response.context["error"] is None
    assert received_limits == [5]


def test_dashboard_incident_explanation_renders_provider_error(monkeypatch):
    def raise_provider_error(recent_limit):
        raise dashboard_routes.AIIncidentProviderError("provider failed")

    monkeypatch.setattr(
        dashboard_routes,
        "generate_incident_explanation",
        raise_provider_error,
    )

    response = asyncio.run(
        dashboard_routes.dashboard_quality_incident_explanation(_make_request())
    )

    assert response.status_code == 200
    assert response.context["result"] is None
    assert response.context["error"] == "provider failed"
