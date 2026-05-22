###############################################################################
# database.py
#
# This module provides the database connection setup for the project using SQLAlchemy.
# It defines functions to create a database engine and a sessionmaker, allowing
# centralized and reusable access to the PostgreSQL database.
###############################################################################

import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import Session


def build_database_url(driver: str = "postgresql") -> str:
    """
    Build a SQLAlchemy database URL from environment variables.

    DATABASE_URL can be used to override the individual POSTGRES_* settings.
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    postgres_user = os.getenv("POSTGRES_USER", "admin1")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "change-me")
    postgres_host = os.getenv("POSTGRES_HOST", "db")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "main")

    return (
        f"{driver}://{quote_plus(postgres_user)}:"
        f"{quote_plus(postgres_password)}@{postgres_host}:{postgres_port}/{postgres_db}"
    )


DATABASE_URL = build_database_url()


def get_engine(echo: bool = False):
    """Create and return a SQLAlchemy engine."""
    return create_engine(
        DATABASE_URL,
        echo=echo,
        future=True
    )

def get_db_session():
    """
    Create and return a new SQLAlchemy Session bound to the engine.
    Use it as a context manager.
    """
    engine = get_engine()
    return Session(engine, future=True)
