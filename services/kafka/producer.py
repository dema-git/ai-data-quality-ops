######################################################
# services/kafka/producer.py
#
# Kafka Producer Module
#
# This module provides a wrapper around the Confluent Kafka Producer for sending
# session events to Kafka topics. It includes:
# - Producer lifecycle management (startup/shutdown)
# - Message delivery confirmation via callbacks
# - Automatic serialization of session events to JSON
# - Partition routing based on session_id to maintain event ordering
######################################################

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from confluent_kafka import Producer

from .config import KafkaConfig

logger = logging.getLogger(__name__)


@dataclass
class KafkaProducerContext:
    """
    Confluent Kafka producer holder
    """
    config: KafkaConfig = field(default_factory=KafkaConfig)
    producer: Optional[Producer] = None


def _delivery_report(err, msg) -> None:
    """
    Delivery callback for logging message delivery results.
    This is called by the producer for each message to log delivery result.
    """
    if err is not None:
        logger.error(
            "Delivery failed for topic=%s partition=%s key=%s: %s",
            msg.topic(),
            msg.partition(),
            msg.key(),
            err,
        )
    else:
        logger.debug(
            "Message delivered: topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def start_producer(ctx: KafkaProducerContext) -> None:
    """
    Initialize and create the Confluent Kafka Producer
    """
    if ctx.producer is not None:
        return

    logger.info(
        "Starting Confluent Kafka producer, bootstrap_servers=%s",
        ctx.config.bootstrap_servers,
    )

    config = {
        "bootstrap.servers": ",".join(ctx.config.bootstrap_servers),
        "enable.idempotence": True,
        "acks": "all",
        "retries": 3,
        "linger.ms": 5,
        "batch.size": 32_768,
    }

    ctx.producer = Producer(config)

    logger.info("Confluent Kafka producer started")


def stop_producer(ctx: KafkaProducerContext) -> None:
    """
    Flush and dispose of the producer.

    block until all outstanding messages are delivered or the timeout expires.
    This ensures no messages are lost during shutdown
    """
    if ctx.producer is None:
        return

    logger.info("Flushing and stopping Confluent Kafka producer...")
    try:
        # Wait for all queued messages to be delivered (or timeout...)
        ctx.producer.flush(timeout=10.0)
    finally:
        ctx.producer = None

    logger.info("Confluent Kafka producer stopped")


def send_session_event(ctx: KafkaProducerContext, event: Dict[str, Any]) -> None:
    """
    Send a single session event to Kafka.

    - topic: ctx.config.sessions_topic
    - key: session_id
    - value: event dict as JSON
    """
    if ctx.producer is None:
        raise RuntimeError("Kafka producer is not started")

    session_id = event.get("session_id", "unknown")
    value_str = json.dumps(event)

    try:
        ctx.producer.produce(
            topic=ctx.config.sessions_topic,
            key=session_id.encode("utf-8"),
            value=value_str.encode("utf-8"),
            callback=_delivery_report,
        )
        ctx.producer.poll(0)
    except Exception as exc:
        logger.exception("Failed to send event to Kafka: %s", exc)
