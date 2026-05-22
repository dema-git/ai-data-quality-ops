############################################################
#
# config.py
#
# Configuration module for the session event generator.
# This module defines the `FakerConfig` dataclass, which encapsulates all
# customizable parameters used by the session event generation system.
############################################################

from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass(frozen=True)
class FakerConfig:
    """
    Configuration settings for the session event generator.
    Controls all randomness, behavior, and structural properties of
    the synthetic e-commerce session events.
    """
    base_url: str = "https://shop.example.com"
    seed: Optional[int] = None

    min_events: int = 3
    max_events: int = 10
    conversion_probability: float = 0.3

    ab_groups: Sequence[str] = ("A", "B")
    categories: Sequence[str] = ("Shoes", "T-Shirts", "Jeans", "Electronics", "Accessories")
    payment_methods: Sequence[str] = ("card", "paypal", "klarna")
