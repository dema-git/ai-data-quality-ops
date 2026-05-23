#############################################################
# quality_service.py
#
# Reads quarantined Bronze validation issues from MinIO and
# builds compact quality summaries for API/UI/incident consumers.
#############################################################

from collections import Counter
from typing import Any, Dict, List

from minio_utils.files_handler import get_files_data
from exceptions_logging.logger import AppLogger


log = AppLogger(component="quality_service")

QUALITY_ISSUES_BUCKET = "events-quality-issues"


def get_top_issue_types(
    summary: Dict[str, Any],
    limit: int = 4,
) -> List[Dict[str, Any]]:
    """
    Return the most frequent issue types for operational displays.
    """
    issue_counts = summary.get("by_issue_type", {})
    sorted_issues = sorted(
        issue_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )
    return [
        {"issue_type": issue_type, "count": count}
        for issue_type, count in sorted_issues[:limit]
    ]


def get_quality_issues_summary(recent_limit: int = 5) -> Dict[str, Any]:
    """
    Aggregate quality issues written by the Bronze validation gate.

    This service returns deterministic operational facts only. Future incident
    builders and AI summaries can consume these facts without reading raw
    parquet files themselves.
    """
    log.info(
        "get_quality_issues_summary started",
        bucket_name=QUALITY_ISSUES_BUCKET,
        recent_limit=recent_limit,
    )

    quality_files = get_files_data(QUALITY_ISSUES_BUCKET)
    rows: List[Dict[str, Any]] = [
        row for quality_file in quality_files for row in quality_file["data"]
    ]

    rows_by_time = sorted(
        rows,
        key=lambda row: str(row.get("detected_at") or ""),
        reverse=True,
    )
    affected_source_files = {
        row["source_object_name"]
        for row in rows
        if row.get("source_object_name")
    }

    summary = {
        "total_issues": len(rows),
        "quality_report_files": len(quality_files),
        "affected_source_files": len(affected_source_files),
        "latest_detected_at": (
            rows_by_time[0].get("detected_at") if rows_by_time else None
        ),
        "by_issue_type": dict(
            Counter(row.get("issue_type", "unknown") for row in rows)
        ),
        "by_severity": dict(
            Counter(row.get("severity", "unknown") for row in rows)
        ),
        "recent_issues": [
            {
                "detected_at": row.get("detected_at"),
                "issue_type": row.get("issue_type"),
                "issue_field": row.get("issue_field"),
                "severity": row.get("severity"),
                "source_object_name": row.get("source_object_name"),
                "injected_quality_issue": row.get("injected_quality_issue"),
            }
            for row in rows_by_time[:recent_limit]
        ],
    }

    log.info(
        "get_quality_issues_summary done",
        total_issues=summary["total_issues"],
        affected_source_files=summary["affected_source_files"],
    )
    return summary
