#####################################################################################
# main.py (fast-api)
#
# Main application file that starts and configures the FastAPI server.
#
# This file does several important things:
# - Creates the FastAPI application with basic settings
# - Starts a Kafka producer to send events
# - Starts a Kafka consumer to read events
# - Runs a background task that generates fake session data every 10 seconds
# - Includes all API route modules (integrations, analytics, faker, dashboard)
#
# The background task automatically creates test sessions and sends them to Kafka
# while the application is running.
#####################################################################################

import asyncio
import logging
from fastapi import FastAPI
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi import Depends
from fastapi.templating import Jinja2Templates
from api import (integrations_routes, analytics_routes, faker_generator_route,
                 dashboard_routes, quality_routes)
from services.faker.generator import SessionEventFaker
from services.faker.config import FakerConfig
from services.kafka.consumer import start_consumer_loop
from services.kafka.producer import (KafkaProducerContext, start_producer,
                                        stop_producer, send_session_event,)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


app = FastAPI(
    title="AI Data Quality Ops API",
    description=(
        "API for a Medallion-based data pipeline and the operational layer around it:\n"
        "- Kafka ingestion into the Bronze layer\n"
        "- Data cleaning and normalization into the Silver layer\n"
        "- Business transformations into the Gold layer\n"
        "- Storage in MinIO and PostgreSQL analytical marts\n"
        "- Analytics endpoints for sessions, products, and landing pages\n"
        "- Protected operational endpoints for ETL and archive workflows\n"
        "- Read-only quality issue summaries for rejected Bronze records\n"
        "- Token-protected AI-assisted explanations for structured quality incidents"
    ),
    version="1.0.0",
)


faker_config = FakerConfig()
faker = SessionEventFaker(faker_config)
kafka_ctx = KafkaProducerContext()

_background_task: asyncio.Task | None = None


async def background_loop(
    interval_seconds: int | None = None,
    sessions_per_batch: int | None = None,
) -> None:
    """
    Background task that runs continuously while the app is running.

    Every 10 seconds it does:
    1. Generate a batch of fake session events (5 sessions by default)
    2. Send each event to Kafka topic

    This creates a constant stream of test data for development and testing.
    """
    interval_seconds = interval_seconds or faker_config.interval_seconds
    sessions_per_batch = sessions_per_batch or faker_config.sessions_per_batch

    logger.info(
        "Background loop started (interval=%s, sessions_per_batch=%s)",
        interval_seconds,
        sessions_per_batch,
    )

    try:
        while True:
            events = faker.generate_batch(num_sessions=sessions_per_batch)
            logger.info("Generated %s events", len(events))

            for event in events:
                send_session_event(kafka_ctx, event)

            logger.info("Sent %s events to Kafka", len(events))
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        logger.info("Background loop cancelled, shutting down gracefully")
        raise
    except Exception as exc:
        logger.exception("Unexpected error in background loop: %s", exc)
    finally:
        logger.info("Background loop stopped")



@app.on_event("startup")
async def on_startup():
    """
    Runs when the application starts.
    """
    global _background_task

    logger.info("App startup: starting Kafka producer and background loop")
    start_producer(kafka_ctx)
    start_consumer_loop()

    _background_task = asyncio.create_task(background_loop())


@app.on_event("shutdown")
async def on_shutdown():
    """
    Runs when the application stops.
    """
    global _background_task

    logger.info("App shutdown: stopping background loop and Kafka producer")

    if _background_task:
        _background_task.cancel()
        try:
            await _background_task
        except asyncio.CancelledError:
            pass

    stop_producer(kafka_ctx)


# All route modules of application
app.include_router(integrations_routes.router)
app.include_router(analytics_routes.router)
app.include_router(faker_generator_route.router)
app.include_router(dashboard_routes.router)
app.include_router(quality_routes.router)
