#####################################################################
# tests/test_operational_auth.py
#
# These tests verify token-based protection for operational endpoints
# without starting FastAPI or running the ETL pipeline.
#
# The tests confirm that:
# - a valid X-API-Token value is accepted
# - missing tokens are rejected
# - wrong tokens are rejected
#
# The goal is to keep dangerous operational actions protected while
# keeping tests fast and independent from external services.
#####################################################################

import pytest
from fastapi import HTTPException

from services.fastapi_app.api.auth import require_operational_api_token


def test_operational_api_token_accepts_valid_token(monkeypatch):
    monkeypatch.setenv("OPERATIONAL_API_TOKEN", "test-token")

    assert require_operational_api_token("test-token") is True


def test_operational_api_token_rejects_missing_token(monkeypatch):
    monkeypatch.setenv("OPERATIONAL_API_TOKEN", "test-token")

    with pytest.raises(HTTPException) as exc_info:
        require_operational_api_token(None)

    assert exc_info.value.status_code == 401


def test_operational_api_token_rejects_wrong_token(monkeypatch):
    monkeypatch.setenv("OPERATIONAL_API_TOKEN", "test-token")

    with pytest.raises(HTTPException) as exc_info:
        require_operational_api_token("wrong-token")

    assert exc_info.value.status_code == 401
