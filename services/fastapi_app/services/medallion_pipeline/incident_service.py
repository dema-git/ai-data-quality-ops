#############################################################
# incident_service.py
#
# Builds structured incident reports from aggregated quality
# facts produced by deterministic Bronze validation checks.
#############################################################

from collections import defaultdict
from typing import Any, Dict, List

from exceptions_logging.logger import AppLogger
from services.medallion_pipeline.quality_service import get_quality_issues_summary


log = AppLogger(component="incident_service")

ISSUE_CATEGORY_BY_TYPE = {
    "negative_price": "business_rules",
    "purchase_without_product": "business_rules",
    "missing_session_id": "session_integrity",
    "missing_user_id": "session_integrity",
    "invalid_event_time": "timestamp_quality",
    "unknown_event_type": "schema_payload",
    "invalid_extra_payload": "schema_payload",
    "invalid_scroll_depth": "schema_payload",
    "invalid_ab_group": "schema_payload",
}

CATEGORY_TITLES = {
    "business_rules": "Business rule validation failures detected",
    "session_integrity": "Session identity failures detected",
    "timestamp_quality": "Timestamp quality failures detected",
    "schema_payload": "Schema and payload failures detected",
    "unclassified": "Unclassified quality failures detected",
}


def _issue_category(issue_type: str) -> str:
    return ISSUE_CATEGORY_BY_TYPE.get(issue_type, "unclassified")


def build_quality_incident_report(summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert deterministic quality summary facts into an incident report.

    Severity is intentionally left unclassified until incidents are evaluated
    within a time window. The current quality summary is cumulative, so fixed
    count thresholds would make old demo data look like an active outage.
    """
    total_issues = summary.get("total_issues", 0)
    if total_issues == 0:
        return {
            "status": "clear",
            "severity": "none",
            "title": "No quality incidents detected",
            "pipeline_stage": "bronze_to_silver",
            "pipeline_impact": "No rejected Bronze records were detected.",
            "total_issues": 0,
            "quality_report_files": summary.get("quality_report_files", 0),
            "affected_source_files": summary.get("affected_source_files", 0),
            "latest_detected_at": summary.get("latest_detected_at"),
            "dominant_category": None,
            "categories": [],
            "recent_evidence": [],
        }

    counts_by_category: Dict[str, int] = defaultdict(int)
    types_by_category: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for issue_type, count in summary.get("by_issue_type", {}).items():
        category = _issue_category(issue_type)
        counts_by_category[category] += count
        types_by_category[category].append({
            "issue_type": issue_type,
            "count": count,
        })

    evidence_by_category: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for evidence in summary.get("recent_issues", []):
        category = _issue_category(str(evidence.get("issue_type", "unknown")))
        evidence_by_category[category].append(evidence)

    sorted_categories = sorted(
        counts_by_category.items(),
        key=lambda item: (-item[1], item[0]),
    )
    dominant_category = sorted_categories[0][0]

    categories = []
    for category, count in sorted_categories:
        categories.append({
            "category": category,
            "issue_count": count,
            "issue_types": sorted(
                types_by_category[category],
                key=lambda item: (-item["count"], item["issue_type"]),
            ),
            "recent_evidence": evidence_by_category.get(category, []),
        })

    return {
        "status": "detected",
        "severity": "unclassified",
        "title": CATEGORY_TITLES[dominant_category],
        "pipeline_stage": "bronze_to_silver",
        "pipeline_impact": (
            "Rejected records were quarantined before Silver and Gold processing."
        ),
        "total_issues": total_issues,
        "quality_report_files": summary.get("quality_report_files", 0),
        "affected_source_files": summary.get("affected_source_files", 0),
        "latest_detected_at": summary.get("latest_detected_at"),
        "dominant_category": dominant_category,
        "categories": categories,
        "recent_evidence": summary.get("recent_issues", []),
    }


def get_quality_incident_report(recent_limit: int = 5) -> Dict[str, Any]:
    """
    Build an incident report from the currently stored quality issue summary.
    """
    log.info("get_quality_incident_report started", recent_limit=recent_limit)
    summary = get_quality_issues_summary(recent_limit=recent_limit)
    report = build_quality_incident_report(summary)
    log.info(
        "get_quality_incident_report done",
        status=report["status"],
        total_issues=report["total_issues"],
        dominant_category=report["dominant_category"],
    )
    return report
