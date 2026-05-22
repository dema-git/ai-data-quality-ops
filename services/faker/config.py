############################################################
#
# config.py
#
# Configuration module for the session event generator.
# This module defines the `FakerConfig` dataclass, which encapsulates all
# customizable parameters used by the session event generation system.
############################################################

import os
from dataclasses import dataclass, field
from typing import Optional, Sequence


def _read_int_env(name: str, default: int, *, min_value: int = 1) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc

    if value < min_value:
        raise ValueError(f"{name} must be >= {min_value}")

    return value


def _read_float_env(
    name: str,
    default: float,
    *,
    min_value: float = 0.0,
    max_value: float = 1.0,
) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float") from exc

    if not min_value <= value <= max_value:
        raise ValueError(f"{name} must be between {min_value} and {max_value}")

    return value


@dataclass(frozen=True)
class FakerConfig:
    """
    Configuration settings for the session event generator.
    Controls all randomness, behavior, and structural properties of
    the synthetic e-commerce session events.
    """
    base_url: str = "https://shop.example.com"
    seed: Optional[int] = None

    interval_seconds: int = field(
        default_factory=lambda: _read_int_env("GENERATOR_INTERVAL_SECONDS", 60)
    )
    sessions_per_batch: int = field(
        default_factory=lambda: _read_int_env("GENERATOR_SESSIONS_PER_BATCH", 2)
    )
    bad_data_rate: float = field(
        default_factory=lambda: _read_float_env("GENERATOR_BAD_DATA_RATE", 0.0)
    )

    min_events: int = 3
    max_events: int = 10
    conversion_probability: float = 0.3

    ab_groups: Sequence[str] = ("A", "B")
    categories: Sequence[str] = ("Shoes", "T-Shirts", "Jeans", "Electronics", "Accessories")
    payment_methods: Sequence[str] = ("card", "paypal", "klarna")
