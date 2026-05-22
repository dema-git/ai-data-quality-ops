##############################################################################################
# session_services.py
#
# This module defines routes to interact with PostgreSQL models using SQLAlchemy.
# It provides endpoints to retrieve users, sessions, and events with optional
# filtering and pagination. The endpoints also include relational data, such as
# sessions belonging to a user and events belonging to a session or user.
#
# Endpoints include:
# - /users: list all users or get a specific user with sessions
# - /sessions: list all sessions or get details of a specific session
# - /events: list all events or get details of a specific event
# - Additional endpoints for retrieving sessions/events by user or session

# The routes use service layer functions to handle database operations and keeping the
# API layer clean
##############################################################################################

from fastapi import APIRouter, HTTPException, Query
from models import User, Session as SessionModel, Event, engine
from services.session_services import ( get_top_landing_pages_service, get_top_products_by_revenue_service,
                        get_ab_test_summary_service, get_user_sessions_overview_service)


router = APIRouter()


@router.get(
    "/analytics/top-landing-pages",
    summary="Top landing pages with bounce rate",
    description=(
        "Returns Gold-layer landing page analytics ordered by traffic. "
        "Each row includes page-level session counts and bounce-rate metrics "
        "derived from processed user session events."
    ),
    tags=["Analytics"],
)
def top_landing_pages(
        limit: int = Query(10, ge=1, le=100,
                           description="Maximum number of landing pages to return."),
):
    return get_top_landing_pages_service(limit=limit)


@router.get(
    "/analytics/top-products",
    summary="Top products by total revenue",
    description=(
        "Returns Gold-layer product analytics ordered by total revenue. "
        "The endpoint is intended for quick validation of product-event "
        "transformations and analytical mart loading."
    ),
    tags=["Analytics"],
)
def top_products(
        limit: int = Query(10, ge=1, le=100,
                           description="Maximum number of products to return."),
):
    return get_top_products_by_revenue_service(limit=limit)


@router.get(
    "/analytics/ab-test-summary",
    summary="A/B test performance summary",
    description=(
        "Returns aggregated Gold-layer metrics grouped by A/B test variant. "
        "This is a read-only analytical endpoint backed by PostgreSQL marts."
    ),
    tags=["Analytics"],
)
def ab_test_summary():
    return get_ab_test_summary_service()


@router.get(
    "/analytics/user/{user_id}/sessions",
    summary="User session overview (GOLD layer)",
    description=(
        "Returns paginated session-level analytics for a single user. "
        "The data is read from processed Gold-layer tables, not from raw events."
    ),
    tags=["Analytics"],
)
def analytics_user_sessions_overview(
        user_id: str,
        skip: int = Query(0, ge=0, description="Number of records to skip."),
        limit: int = Query(20, ge=1, le=100,
                           description="Maximum number of sessions to return."),
):
    return get_user_sessions_overview_service(
        user_id=user_id,
        skip=skip,
        limit=limit,
    )

