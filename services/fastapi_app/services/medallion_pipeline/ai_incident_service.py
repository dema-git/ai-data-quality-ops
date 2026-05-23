#############################################################
# ai_incident_service.py
#
# Generates an operator-facing explanation after deterministic
# quality checks, incident aggregation, and specialist routing.
#############################################################

import json
import os
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from exceptions_logging.logger import AppLogger
from services.medallion_pipeline.incident_agent_router import (
    get_routed_quality_incident,
)


log = AppLogger(component="ai_incident_service")

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_OPENAI_MODEL = "gpt-5.4-mini"
DEFAULT_ANALYSIS_MODE = "mock"

INCIDENT_EXPLANATION_SCHEMA = {
    "type": "object",
    "properties": {
        "assessment": {"type": "string"},
        "observed_facts": {
            "type": "array",
            "items": {"type": "string"},
        },
        "possible_causes": {
            "type": "array",
            "items": {"type": "string"},
        },
        "recommended_checks": {
            "type": "array",
            "items": {"type": "string"},
        },
        "confidence": {
            "type": "string",
            "enum": ["low", "medium", "high"],
        },
    },
    "required": [
        "assessment",
        "observed_facts",
        "possible_causes",
        "recommended_checks",
        "confidence",
    ],
    "additionalProperties": False,
}


class AIIncidentConfigurationError(RuntimeError):
    """Raised when real AI analysis is requested without required config."""


class AIIncidentProviderError(RuntimeError):
    """Raised when the external AI provider cannot produce an explanation."""


def _public_agent(agent: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if agent is None:
        return None
    return {
        "agent_id": agent["agent_id"],
        "display_name": agent["display_name"],
        "category": agent["category"],
    }


def _build_mock_explanation(routed_incident: Dict[str, Any]) -> Dict[str, Any]:
    context = routed_incident["incident_context"]
    category = context["dominant_category"]
    details = context.get("dominant_category_details") or {}
    issue_types = details.get("issue_types", [])
    issue_names = ", ".join(issue["issue_type"] for issue in issue_types)
    category_count = details.get("issue_count", 0)

    return {
        "assessment": (
            f"Detected {category_count} rejected records in category "
            f"{category}."
        ),
        "observed_facts": [
            f"Dominant issue category is {category}.",
            f"Detected issue types: {issue_names or 'none listed'}.",
            context["pipeline_impact"],
        ],
        "possible_causes": [
            "A producer or transformation step emitted values rejected by validation.",
            "Controlled synthetic bad-data injection may be enabled for this run.",
        ],
        "recommended_checks": [
            "Review the dominant issue-type counts and recent evidence.",
            "Inspect generator configuration and upstream event construction.",
            "Run the pipeline again after correcting or disabling invalid input generation.",
        ],
        "confidence": "medium",
    }


def _build_openai_payload(
    routed_incident: Dict[str, Any],
    model: str,
) -> Dict[str, Any]:
    agent = routed_incident["agent"]
    context = routed_incident["incident_context"]
    return {
        "model": model,
        "input": [
            {
                "role": "developer",
                "content": (
                    f"{agent['instructions']}\n\n"
                    "Return only a structured incident explanation. "
                    "The incident context contains deterministic validation results; "
                    "do not reinterpret rejected records as valid."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(context, sort_keys=True),
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "quality_incident_explanation",
                "description": "Operator-facing explanation of a quality incident.",
                "strict": True,
                "schema": INCIDENT_EXPLANATION_SCHEMA,
            }
        },
    }


def _extract_openai_explanation(response_payload: Dict[str, Any]) -> Dict[str, Any]:
    output_text = response_payload.get("output_text")
    if output_text:
        return json.loads(output_text)

    for output_item in response_payload.get("output", []):
        for content_item in output_item.get("content", []):
            if content_item.get("type") == "refusal":
                raise AIIncidentProviderError("OpenAI refused to analyze the incident.")
            if content_item.get("type") == "output_text":
                return json.loads(content_item["text"])

    raise AIIncidentProviderError("OpenAI returned no structured incident explanation.")


def _request_openai_explanation(
    routed_incident: Dict[str, Any],
    api_key: str,
    model: str,
) -> Dict[str, Any]:
    payload = _build_openai_payload(routed_incident, model)
    request = Request(
        OPENAI_RESPONSES_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        timeout_seconds = float(os.getenv("OPENAI_REQUEST_TIMEOUT_SECONDS", "30"))
    except ValueError as exc:
        raise AIIncidentConfigurationError(
            "OPENAI_REQUEST_TIMEOUT_SECONDS must be a number."
        ) from exc

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return _extract_openai_explanation(json.loads(response.read().decode("utf-8")))
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        raise AIIncidentProviderError(
            "OpenAI incident explanation request failed."
        ) from exc


def generate_incident_explanation(recent_limit: int = 5) -> Dict[str, Any]:
    """
    Generate an incident explanation through mock or OpenAI execution mode.
    """
    routed_incident = get_routed_quality_incident(recent_limit=recent_limit)
    routing_status = routed_incident["routing_status"]
    if routing_status != "routed":
        return {
            "analysis_status": routing_status,
            "provider": None,
            "model": None,
            "agent": _public_agent(routed_incident.get("agent")),
            "incident_context": routed_incident.get("incident_context"),
            "explanation": None,
        }

    mode = os.getenv("AI_INCIDENT_ANALYSIS_MODE", DEFAULT_ANALYSIS_MODE).strip().lower()
    model = os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL).strip()
    if mode == "mock":
        explanation = _build_mock_explanation(routed_incident)
        provider = "mock"
    elif mode == "openai":
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            raise AIIncidentConfigurationError(
                "OPENAI_API_KEY is required when AI_INCIDENT_ANALYSIS_MODE=openai."
            )
        explanation = _request_openai_explanation(routed_incident, api_key, model)
        provider = "openai"
    else:
        raise AIIncidentConfigurationError(
            "AI_INCIDENT_ANALYSIS_MODE must be either mock or openai."
        )

    log.info(
        "generate_incident_explanation done",
        provider=provider,
        model=model,
        agent_id=routed_incident["agent"]["agent_id"],
    )
    return {
        "analysis_status": "generated",
        "provider": provider,
        "model": model,
        "agent": _public_agent(routed_incident["agent"]),
        "incident_context": routed_incident["incident_context"],
        "explanation": explanation,
    }
