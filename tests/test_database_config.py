#####################################################################
# tests/test_database_config.py
#
# These tests verify database URL configuration without connecting
# to a real PostgreSQL server.
#
# The tests confirm that:
# - local Docker defaults are used when env vars are missing
# - POSTGRES_* env vars are read and URL-encoded correctly
# - DATABASE_URL overrides individual POSTGRES_* settings
#
# The goal is to keep database configuration explicit, reusable,
# and safe to run in tests without requiring external services.
#####################################################################

import importlib

from services.fastapi_app.db_utils import database


def reload_database_module(monkeypatch, **env_overrides):
    for key in [
        "DATABASE_URL",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
    ]:
        monkeypatch.delenv(key, raising=False)

    for key, value in env_overrides.items():
        monkeypatch.setenv(key, value)

    return importlib.reload(database)


def test_build_database_url_uses_safe_local_defaults(monkeypatch):
    db_mod = reload_database_module(monkeypatch)

    assert (
        db_mod.build_database_url()
        == "postgresql://admin1:change-me@db:5432/main"
    )


def test_build_database_url_reads_postgres_env(monkeypatch):
    db_mod = reload_database_module(
        monkeypatch,
        POSTGRES_USER="app_user",
        POSTGRES_PASSWORD="pa ss@word",
        POSTGRES_HOST="postgres",
        POSTGRES_PORT="15432",
        POSTGRES_DB="analytics",
    )

    assert (
        db_mod.build_database_url("postgresql+psycopg2")
        == "postgresql+psycopg2://app_user:pa+ss%40word@postgres:15432/analytics"
    )


def test_build_database_url_prefers_database_url_override(monkeypatch):
    db_mod = reload_database_module(
        monkeypatch,
        DATABASE_URL="postgresql://override:secret@host:5432/db",
        POSTGRES_USER="ignored",
    )

    assert db_mod.build_database_url() == "postgresql://override:secret@host:5432/db"
