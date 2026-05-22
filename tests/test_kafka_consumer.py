#####################################################################
# tests/test_kafka.py
#
# Tests for services.kafka.consumer (without running Kafka).
#
# The goal is to verify that:
# - KAFKA_BOOTSTRAP falls back to "kafka:9092" when env is missing
# - KAFKA_BOOTSTRAP is read correctly from environment variables
# - KAFKA_GROUP_ID and KAFKA_TOPICS match expected defaults
# - create_consumer() builds a Consumer with the correct configuration
#
# Important:
# The production code must NOT be modified for tests.
# Instead, dummy modules are injected into sys.modules to replace:
#   - services.medallion_pipeline.bronze_writer
#   - services.medallion_models.bronze_model
# These modules are required by services.kafka.consumer on import.
#####################################################################

import importlib
import sys
import types

import pytest

MODULE_PATH = "services.kafka.consumer"


def _install_dummy_dependencies():
    """
    Install minimal dummy modules into sys.modules so that
    services.kafka.consumer can be imported without errors.
    The real implementations are not needed for these tests.
    """
    # services.medallion_pipeline
    if "services.medallion_pipeline" not in sys.modules:
        pkg = types.ModuleType("services.medallion_pipeline")
        sys.modules["services.medallion_pipeline"] = pkg

    # services.medallion_pipeline.bronze_writer
    if "services.medallion_pipeline.bronze_writer" not in sys.modules:
        mod_bw = types.ModuleType("services.medallion_pipeline.bronze_writer")

        def save_bronze_events(events):
            # dummy
            return None

        mod_bw.save_bronze_events = save_bronze_events
        sys.modules["services.medallion_pipeline.bronze_writer"] = mod_bw

    # services.medallion_models
    if "services.medallion_models" not in sys.modules:
        pkg_models = types.ModuleType("services.medallion_models")
        sys.modules["services.medallion_models"] = pkg_models

    # services.medallion_models.bronze_model
    if "services.medallion_models.bronze_model" not in sys.modules:
        mod_bm = types.ModuleType("services.medallion_models.bronze_model")

        class BronzeWebEvent:
            """
            Minimal stub, actual fields are irrelevant for these tests.
            """
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        mod_bm.BronzeWebEvent = BronzeWebEvent
        sys.modules["services.medallion_models.bronze_model"] = mod_bm


def reload_kafka_consumer(monkeypatch, **env_overrides):
    """
    Helper to reload services.kafka.consumer with a fresh environment:
    - removes KAFKA_BROKERCONNECT from env
    - applies env overrides
    - installs dummy dependent modules
    - imports and reloads the consumer module so it picks up new env vars
    """

    # clear env vars affecting config
    for key in ["KAFKA_BROKERCONNECT"]:
        monkeypatch.delenv(key, raising=False)

    # apply overrides
    for key, value in env_overrides.items():
        monkeypatch.setenv(key, value)

    # prepare dummy modules
    _install_dummy_dependencies()

    # reload consumer module
    kafka_consumer_mod = importlib.import_module(MODULE_PATH)
    importlib.reload(kafka_consumer_mod)
    return kafka_consumer_mod


def test_kafka_consumer_uses_default_bootstrap_when_env_missing(monkeypatch):
    """
    KAFKA_BOOTSTRAP falls back to default 'kafka:9092' if env var is missing.
    """
    kafka_consumer_mod = reload_kafka_consumer(monkeypatch)
    assert kafka_consumer_mod.KAFKA_BOOTSTRAP == "kafka:9092"


def test_kafka_consumer_reads_bootstrap_from_env(monkeypatch):
    """
    KAFKA_BOOTSTRAP must match the value provided via environment variable.
    """
    kafka_consumer_mod = reload_kafka_consumer(
        monkeypatch,
        KAFKA_BROKERCONNECT="kafka-1:19092",
    )
    assert kafka_consumer_mod.KAFKA_BOOTSTRAP == "kafka-1:19092"


def test_kafka_consumer_has_expected_group_id_and_topics(monkeypatch):
    """
    KAFKA_GROUP_ID and KAFKA_TOPICS must be initialized with expected defaults.
    """
    kafka_consumer_mod = reload_kafka_consumer(monkeypatch)

    assert kafka_consumer_mod.KAFKA_GROUP_ID == "kafka-consumer"
    assert kafka_consumer_mod.KAFKA_TOPICS == ["sessions_raw"]


def test_create_consumer_passes_correct_config_to_confluent_consumer(monkeypatch):
    """
    create_consumer() must pass the correct config dict to the underlying Consumer.
    """
    created_config = {}

    # stub Consumer class to capture passed configuration
    class DummyConsumer:
        def __init__(self, conf):
            created_config.update(conf)

        def subscribe(self, topics):
            pass

        def poll(self, timeout):
            return None

        def close(self):
            pass

    kafka_consumer_mod = reload_kafka_consumer(
        monkeypatch,
        KAFKA_BROKERCONNECT="kafka:29092",
    )

    # replace real Consumer with dummy
    monkeypatch.setattr(kafka_consumer_mod, "Consumer", DummyConsumer)

    # triger consumer creation
    consumer = kafka_consumer_mod.create_consumer()
    assert isinstance(consumer, DummyConsumer)

    # validate configuration passed to DummyConsumer
    assert created_config["bootstrap.servers"] == "kafka:29092"
    assert created_config["group.id"] == "kafka-consumer"
    assert created_config["enable.auto.commit"] is False
    assert created_config["auto.offset.reset"] == "earliest"
    assert created_config["session.timeout.ms"] == 10000
    assert created_config["heartbeat.interval.ms"] == 3000