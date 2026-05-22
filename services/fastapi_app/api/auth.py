##############################################################################
# api/auth.py
#
# Shared authentication helpers for operational API endpoints.
#
# These dependencies protect endpoints that mutate pipeline state or remove
# data, while leaving read-only analytics and dashboard endpoints public for
# portfolio/demo use.
##############################################################################

import os
import secrets

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader


operational_api_key = APIKeyHeader(
    name="X-API-Token",
    auto_error=False,
    description="Operational API token required for ETL and cleanup endpoints.",
)


def require_operational_api_token(
        api_token: str | None = Security(operational_api_key),
) -> bool:
    """
    Validate the operational API token from the X-API-Token header.
    """
    expected_token = os.getenv("OPERATIONAL_API_TOKEN", "medallion-ops-token")

    if not expected_token:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Operational API token is not configured",
        )

    if not api_token or not secrets.compare_digest(api_token, expected_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing operational API token",
        )

    return True
