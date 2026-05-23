#####################################################################
# api/quality_routes.py
#
# Read-only API routes for inspecting data-quality validation results.
# The endpoint delegates aggregation to the quality service layer.
#####################################################################

from fastapi import APIRouter, Query

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
