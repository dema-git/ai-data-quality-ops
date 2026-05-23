#####################################################################
# tests/test_ai_incident_service.py
#
# Tests for mock and OpenAI-backed incident explanation execution.
#
# No test sends a network request or requires an OpenAI API key.
#####################################################################

import importlib
import json
from pathlib import Path
import sys

import pytest

FASTAPI_APP_PATH = Path(__file__).resolve().parents[1] / "services" / "fastapi_app"
ROOT_SERVICES_PATH = Path(__file__).resolve().parents[1] / "services"
sys.path.insert(0, str(FASTAPI_APP_PATH))

_root_services_module = sys.modules.pop("services", None)
try:
    services_pkg = importlib.import_module("services")
    services_pkg.__path__.append(str(ROOT_SERVICES_PATH))
    ai_incident_service = importlib.import_module(
        "services.medallion_pipeline.ai_incident_service"
    )
finally:
    if _root_services_module is not None:
        sys.modules["services"] = _root_services_module


def _routed_incident():
    return {
        "routing_status": "routed",
        "agent": {
            "agent_id": "business_rules_agent",
            "display_name": "Business Rules Analyst",
            "category": "business_rules",
            "instructions": "# Business Rules Analyst\nAnalyze facts only.",
        },
        "incident_context": {
            "title": "Business rule validation failures detected",
            "pipeline_stage": "bronze_to_silver",
            "pipeline_impact": "Rejected records were quarantined.",
            "total_issues": 4,
            "latest_detected_at": "2026-05-23T12:00:00+00:00",
            "dominant_category": "business_rules",
            "dominant_category_details": {
                "issue_count": 4,
                "issue_types": [{"issue_type": "negative_price", "count": 4}],
            },
            "recent_evidence": [{"issue_type": "negative_price"}],
        },
    }


def test_generate_explanation_uses_mock_mode_by_default(monkeypatch):
    monkeypatch.delenv("AI_INCIDENT_ANALYSIS_MODE", raising=False)
    monkeypatch.setattr(
        ai_incident_service,
        "get_routed_quality_incident",
        lambda recent_limit: _routed_incident(),
    )

    result = ai_incident_service.generate_incident_explanation(recent_limit=2)

    assert result["analysis_status"] == "generated"
    assert result["provider"] == "mock"
    assert result["agent"] == {
        "agent_id": "business_rules_agent",
        "display_name": "Business Rules Analyst",
        "category": "business_rules",
    }
    assert "negative_price" in result["explanation"]["observed_facts"][1]


def test_generate_explanation_returns_without_provider_if_no_incident(monkeypatch):
    monkeypatch.setattr(
        ai_incident_service,
        "get_routed_quality_incident",
        lambda recent_limit: {
            "routing_status": "not_required",
            "agent": None,
            "incident_context": None,
        },
    )

    result = ai_incident_service.generate_incident_explanation()

    assert result["analysis_status"] == "not_required"
    assert result["provider"] is None
    assert result["explanation"] is None


def test_openai_mode_requires_api_key(monkeypatch):
    monkeypatch.setenv("AI_INCIDENT_ANALYSIS_MODE", "openai")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(
        ai_incident_service,
        "get_routed_quality_incident",
        lambda recent_limit: _routed_incident(),
    )

    with pytest.raises(ai_incident_service.AIIncidentConfigurationError):
        ai_incident_service.generate_incident_explanation()


def test_openai_mode_uses_structured_explanation_response(monkeypatch):
    expected_explanation = {
        "assessment": "Negative prices were rejected.",
        "observed_facts": ["Four negative prices were detected."],
        "possible_causes": ["Producer calculation error."],
        "recommended_checks": ["Inspect price calculation."],
        "confidence": "medium",
    }
    captured = {}

    def fake_request(routed_incident, api_key, model):
        captured.update({"api_key": api_key, "model": model})
        return expected_explanation

    monkeypatch.setenv("AI_INCIDENT_ANALYSIS_MODE", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai-key")
    monkeypatch.setenv("OPENAI_MODEL", "test-model")
    monkeypatch.setattr(
        ai_incident_service,
        "get_routed_quality_incident",
        lambda recent_limit: _routed_incident(),
    )
    monkeypatch.setattr(ai_incident_service, "_request_openai_explanation", fake_request)

    result = ai_incident_service.generate_incident_explanation()

    assert result["provider"] == "openai"
    assert result["model"] == "test-model"
    assert result["explanation"] == expected_explanation
    assert captured == {"api_key": "test-openai-key", "model": "test-model"}


def test_openai_payload_requests_strict_structured_output():
    payload = ai_incident_service._build_openai_payload(
        _routed_incident(),
        model="test-model",
    )

    assert payload["model"] == "test-model"
    assert payload["text"]["format"]["type"] == "json_schema"
    assert payload["text"]["format"]["strict"] is True
    assert (
        payload["text"]["format"]["schema"]
        == ai_incident_service.INCIDENT_EXPLANATION_SCHEMA
    )
    assert json.loads(payload["input"][1]["content"])["total_issues"] == 4


def test_openai_request_rejects_invalid_timeout(monkeypatch):
    monkeypatch.setenv("OPENAI_REQUEST_TIMEOUT_SECONDS", "not-a-number")

    with pytest.raises(ai_incident_service.AIIncidentConfigurationError):
        ai_incident_service._request_openai_explanation(
            _routed_incident(),
            api_key="test-key",
            model="test-model",
        )
