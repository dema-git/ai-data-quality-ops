##########################################################
# api/faker_generator_route.py
#
# This module defines FastAPI endpoints for generating fake session event data
#
# !!! IMPORTANT: These endpoints are for demonstration and data structure inspection only.
# In production, all data generation is triggered through Airflow DAGs to maintain
# pipeline integrity. Manual endpoint calls may disrupt the data flow and should be
# avoided in (production) environments. !!!
##########################################################

from fastapi import APIRouter, Query
from services.faker.generator import SessionEventFaker
from services.faker.config import FakerConfig

router = APIRouter(
    prefix="/faker",
    tags=["Faker"],
)

faker = SessionEventFaker(FakerConfig())


@router.get(
    "/sample-session",
    summary="Generate sample session events",
    description=(
        "Returns one synthetic user session as a list of events. "
        "This endpoint is read-only and intended for inspecting the event "
        "shape used by the Kafka producer and Medallion pipeline."
    ),
)
def get_sample_session():
    """
    Generate a single user session (list of events) and return it
    """
    events = faker.generate_session_events()
    return {
        "events_count": len(events),
        "events": events,
    }


@router.get(
    "/sample-batch",
    summary="Generate sample batch events",
    description=(
        "Returns synthetic events for multiple user sessions. "
        "This endpoint is intended for demo and schema inspection only; "
        "the running application generates pipeline input automatically."
    ),
)
def get_sample_batch(
        num_sessions: int = Query(3, ge=1, le=50,
                                  description="Number of synthetic sessions to generate."),
):
    """
    Generate multiple sessions and return them
    """
    events = faker.generate_batch(num_sessions=num_sessions)
    return {
        "sessions": num_sessions,
        "events_count": len(events),
        "events": events,
    }
