#############################################################
# incident_agent_router.py
#
# Routes structured quality incidents to specialized analysis
# profiles before any optional AI execution is performed.
#############################################################

from pathlib import Path
from typing import Any, Dict, Optional

from exceptions_logging.logger import AppLogger
from services.medallion_pipeline.incident_service import get_quality_incident_report


log = AppLogger(component="incident_agent_router")

AGENT_DEFINITIONS_DIR = (
    Path(__file__).resolve().parents[2] / "agent_definitions" / "quality_incidents"
)

AGENT_PROFILES = {
    "schema_payload": {
        "agent_id": "schema_payload_agent",
        "display_name": "Schema and Payload Analyst",
        "definition_file": "schema_payload_agent.md",
    },
    "business_rules": {
        "agent_id": "business_rules_agent",
        "display_name": "Business Rules Analyst",
        "definition_file": "business_rules_agent.md",
    },
    "session_integrity": {
        "agent_id": "session_integrity_agent",
        "display_name": "Session Integrity Analyst",
        "definition_file": "session_integrity_agent.md",
    },
    "timestamp_quality": {
        "agent_id": "timestamp_quality_agent",
        "display_name": "Timestamp Quality Analyst",
        "definition_file": "timestamp_quality_agent.md",
    },
}


def _load_agent_instructions(definition_file: str) -> str:
    definition_path = AGENT_DEFINITIONS_DIR / definition_file
    return definition_path.read_text(encoding="utf-8")


def _find_category_details(
    incident_report: Dict[str, Any],
    category: str,
) -> Optional[Dict[str, Any]]:
    return next(
        (
            details
            for details in incident_report.get("categories", [])
            if details.get("category") == category
        ),
        None,
    )


def route_quality_incident_to_agent(
    incident_report: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Prepare a deterministic routing decision for a quality incident.

    The router does not assess raw records and does not call an AI model. It
    chooses a specialized analysis profile from the category already produced
    by deterministic validation and incident aggregation.
    """
    if incident_report.get("status") != "detected":
        return {
            "routing_status": "not_required",
            "reason": "No detected quality incident requires analysis.",
            "agent": None,
            "incident_context": None,
        }

    category = incident_report.get("dominant_category")
    profile = AGENT_PROFILES.get(category)
    if profile is None:
        return {
            "routing_status": "manual_review_required",
            "reason": f"No specialized agent profile exists for category: {category}.",
            "agent": None,
            "incident_context": {
                "dominant_category": category,
                "total_issues": incident_report.get("total_issues", 0),
            },
        }

    category_details = _find_category_details(incident_report, category) or {}
    agent = {
        "agent_id": profile["agent_id"],
        "display_name": profile["display_name"],
        "category": category,
        "instructions": _load_agent_instructions(profile["definition_file"]),
    }
    incident_context = {
        "title": incident_report.get("title"),
        "pipeline_stage": incident_report.get("pipeline_stage"),
        "pipeline_impact": incident_report.get("pipeline_impact"),
        "total_issues": incident_report.get("total_issues", 0),
        "latest_detected_at": incident_report.get("latest_detected_at"),
        "dominant_category": category,
        "dominant_category_details": category_details,
        "recent_evidence": incident_report.get("recent_evidence", []),
    }

    return {
        "routing_status": "routed",
        "reason": "Dominant incident category matched a specialized agent profile.",
        "agent": agent,
        "incident_context": incident_context,
    }


def get_routed_quality_incident(recent_limit: int = 5) -> Dict[str, Any]:
    """
    Build the current incident report and route it to an analysis profile.
    """
    log.info("get_routed_quality_incident started", recent_limit=recent_limit)
    incident_report = get_quality_incident_report(recent_limit=recent_limit)
    routed_incident = route_quality_incident_to_agent(incident_report)
    log.info(
        "get_routed_quality_incident done",
        routing_status=routed_incident["routing_status"],
        dominant_category=incident_report.get("dominant_category"),
    )
    return routed_incident
