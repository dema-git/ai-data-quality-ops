#####################################################################
# tests/test_faker_config.py
#
# These tests verify synthetic event generator configuration without
# starting FastAPI, Kafka, or any external service.
#
# The generator rate is intentionally environment-driven because the
# AI ops project needs cheap local runs and controlled incident scenarios.
#####################################################################

import pytest

from services.faker.config import FakerConfig


def test_faker_config_uses_local_generator_defaults(monkeypatch):
    monkeypatch.delenv("GENERATOR_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("GENERATOR_SESSIONS_PER_BATCH", raising=False)
    monkeypatch.delenv("GENERATOR_BAD_DATA_RATE", raising=False)

    config = FakerConfig()

    assert config.interval_seconds == 60
    assert config.sessions_per_batch == 2
    assert config.bad_data_rate == 0.0


def test_faker_config_reads_generator_env(monkeypatch):
    monkeypatch.setenv("GENERATOR_INTERVAL_SECONDS", "120")
    monkeypatch.setenv("GENERATOR_SESSIONS_PER_BATCH", "4")
    monkeypatch.setenv("GENERATOR_BAD_DATA_RATE", "0.25")

    config = FakerConfig()

    assert config.interval_seconds == 120
    assert config.sessions_per_batch == 4
    assert config.bad_data_rate == 0.25


@pytest.mark.parametrize(
    ("env_name", "env_value"),
    [
        ("GENERATOR_INTERVAL_SECONDS", "0"),
        ("GENERATOR_SESSIONS_PER_BATCH", "-1"),
        ("GENERATOR_INTERVAL_SECONDS", "fast"),
        ("GENERATOR_BAD_DATA_RATE", "-0.1"),
        ("GENERATOR_BAD_DATA_RATE", "1.1"),
        ("GENERATOR_BAD_DATA_RATE", "sometimes"),
    ],
)
def test_faker_config_rejects_invalid_generator_env(monkeypatch, env_name, env_value):
    monkeypatch.delenv("GENERATOR_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("GENERATOR_SESSIONS_PER_BATCH", raising=False)
    monkeypatch.delenv("GENERATOR_BAD_DATA_RATE", raising=False)
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(ValueError):
        FakerConfig()
