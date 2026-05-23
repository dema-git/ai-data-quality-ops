#####################################################################
# api/quality_routes.py
#
# API routes for data-quality results and operator-triggered AI explanations.
# Deterministic summaries remain public; AI execution is token-protected.
#####################################################################

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import require_operational_api_token
from services.medallion_pipeline.ai_incident_service import (
    AIIncidentConfigurationError,
    AIIncidentProviderError,
    generate_incident_explanation,
)
from services.medallion_pipeline.quality_service import get_quality_issues_summary


router = APIRouter(prefix="/quality", tags=["Quality"])


@router.get(
    "/issues/summary",
    summary="Quality issue summary",
    description=(
        "Returns a read-only summary of Bronze records rejected by deterministic "
        "quality validation. Results include issue-type counts, severity counts, "
        "affected source files, and a limited list of recent issues."
    ),
)
def quality_issues_summary(
    recent_limit: int = Query(
        5,
        ge=1,
        le=50,
        description="Maximum number of recent rejected records to return.",
    ),
):
    return get_quality_issues_summary(recent_limit=recent_limit)


@router.post(
    "/incidents/current/explanation",
    summary="Generate AI-assisted incident explanation",
    dependencies=[Depends(require_operational_api_token)],
    description=(
        "Routes the current deterministic quality incident to its specialist "
        "analysis profile and generates an operator-facing explanation. "
        "Mock mode is enabled by default for local demos; OpenAI execution is "
        "enabled explicitly through server-side environment variables."
    ),
)
def current_incident_explanation(
    recent_limit: int = Query(
        5,
        ge=1,
        le=50,
        description="Maximum number of recent rejected records in the AI context.",
    ),
):
    try:
        return generate_incident_explanation(recent_limit=recent_limit)
    except AIIncidentConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except AIIncidentProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
