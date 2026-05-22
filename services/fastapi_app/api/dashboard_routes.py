###################################################################
# api/dashboard_routes.py
#
# This module renders the main HTML dashboard, exposes live pipeline metrics for HTMX updates,
# and provides access to high-level metadata about the Kafka → MinIO → PostgreSQL data flow.
# It loads Jinja2 templates, retrieves Medallion layer statistics, and integrates generator
# configuration from FakerConfig.
###################################################################

from fastapi import APIRouter, HTTPException
from models import User, Session as SessionModel, Event, engine
from services.session_services import ( get_top_landing_pages_service, get_top_products_by_revenue_service,
                        get_ab_test_summary_service, get_user_sessions_overview_service)
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from pathlib import Path
from fastapi import Depends
from services.medallion_pipeline.medallion_service import get_medallion_stats
from fastapi.templating import Jinja2Templates
from db_utils.database import get_db_session
from services.faker.config import FakerConfig
from services.medallion_pipeline.pipeline_state import (
    fetch_latest_etl_runs,
    fetch_outbox_status_counts,
)

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR.parent / "templates"))
faker_config = FakerConfig()

@router.get("/", response_class=HTMLResponse, tags=["Home/Dashboard"])
async def homepage(request: Request):
    """
    Main dashboard page rendered as HTML.
    HTMX will dynamically load live status and metrics.
    """
    # Static, high-level metadata for the page header
    context = {
        "request": request,
        "app_title": "Medallion ETL Dashboard",
        "app_subtitle": "Kafka → MinIO (Bronze / Silver / Gold) → PostgreSQL",
    }
    return templates.TemplateResponse("home.html", context)


@router.get("/dashboard/metrics", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_metrics(request: Request):
    """
    Medallion metrics for the dashboard based on current data in MinIO.

    - Generator config: batch size & interval
    - Bronze/Silver/Gold: rows & files from corresponding buckets
    """
    events_per_batch = getattr(faker_config, "sessions_per_batch", 5)
    generator_interval_sec = getattr(faker_config, "interval_seconds", 10)

    layer_stats = get_medallion_stats()

    metrics = {
        "events_per_batch": events_per_batch,
        "generator_interval_sec": generator_interval_sec,
        **layer_stats,
    }

    return templates.TemplateResponse(
        "partials/metrics_overview.html",
        {"request": request, "m": metrics},
    )


@router.get("/dashboard/operations", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_operations(request: Request):
    """
    Recent ETL activity and outbox status for the dashboard.
    """
    latest_runs = fetch_latest_etl_runs(limit=5)
    outbox_status = fetch_outbox_status_counts()

    return templates.TemplateResponse(
        "partials/operations_overview.html",
        {
            "request": request,
            "latest_runs": latest_runs,
            "outbox": outbox_status,
        },
    )
