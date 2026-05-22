#############################################################
# quality_model.py
#
# Dataclasses for data-quality records produced by the
# Bronze -> Silver validation step.
#############################################################

from dataclasses import dataclass


@dataclass
class QualityIssue:
    """
    Structured record for a Bronze event that failed validation.
    """
    detected_at: str
    source_layer: str
    source_bucket: str
    source_object_name: str
    issue_type: str
    issue_field: str
    severity: str
    event_time: str
    session_id: str
    user_id: str
    event_type: str
    injected_quality_issue: str
    raw_event_json: str
