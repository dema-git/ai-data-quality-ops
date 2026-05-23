#####################################################################
# tests/test_quality_api.py
#
# Tests for the read-only quality summary API route.
#
# The tests verify that the route delegates to the summary service and
# documents the expected HTTP path without accessing MinIO.
#####################################################################

import importlib
from pathlib import Path
import sys

FASTAPI_APP_PATH = Path(__file__).resolve().parents[1] / "services" / "fastapi_app"
ROOT_SERVICES_PATH = Path(__file__).resolve().parents[1] / "services"
sys.path.insert(0, str(FASTAPI_APP_PATH))

_root_services_module = sys.modules.pop("services", None)
try:
    services_pkg = importlib.import_module("services")
    services_pkg.__path__.append(str(ROOT_SERVICES_PATH))
    quality_routes = importlib.import_module("api.quality_routes")
finally:
    if _root_services_module is not None:
        sys.modules["services"] = _root_services_module


def test_quality_summary_route_calls_service_with_limit(monkeypatch):
    expected = {
        "total_issues": 4,
        "recent_issues": [{"issue_type": "negative_price"}],
    }
    received_limits = []

    def fake_get_quality_issues_summary(recent_limit):
        received_limits.append(recent_limit)
        return expected

    monkeypatch.setattr(
        quality_routes,
        "get_quality_issues_summary",
        fake_get_quality_issues_summary,
    )

    result = quality_routes.quality_issues_summary(recent_limit=2)

    assert result == expected
    assert received_limits == [2]


def test_quality_router_exposes_summary_path():
    paths = {route.path for route in quality_routes.router.routes}

    assert "/quality/issues/summary" in paths
