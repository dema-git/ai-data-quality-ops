###############################################################################
# kafka_consumer.py
#
# Kafka Consumer for FastAPI + MinIO Uploader
#
# This file implements a background Kafka consumer that reads messages
# from a specified topic and stores them in a queue. The collected messages
# can then be retrieved in batches and uploaded to MinIO as Parquet files.
#
# Main components:
# - Kafka consumer with auto-commit offsets.
# - Background thread to continuously poll messages.
##########################################################

import threading
import json
import time
import os
from confluent_kafka import Consumer, KafkaError, KafkaException
from typing import Any, Dict, List, Optional
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from services.medallion_pipeline.bronze_writer import save_bronze_events
from services.medallion_models.bronze_model import BronzeWebEvent
from exceptions_logging.logger import AppLogger


log = AppLogger(component="kafka_consumer")

KAFKA_TOPICS = ["sessions_raw"]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKERCONNECT", "kafka:9092")
KAFKA_GROUP_ID = "kafka-consumer"

message_queue: List[Dict[str, Any]] = []
queue_lock = threading.Lock()


def create_consumer() -> Consumer:
    """
    Create and configure Kafka consumer
    """
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
        }
    )


def _decode_message(msg) -> Dict[str, Any] | None:
    """
    Decode Kafka message value as JSON
    """
    try:
        payload = json.loads(msg.value().decode("utf-8"))
    except Exception:
        return None

    return {
        "key": msg.key(),
        "data": payload,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
    }


def _commit_offsets(consumer: Consumer, batch: List[Dict[str, Any]]) -> None:
    """
    Commit next offsets for all topic-partitions in batch
    """
    if not batch:
        return

    max_offsets: dict[tuple[str, int], int] = defaultdict(lambda: -1)

    for m in batch:
        key = (m["topic"], m["partition"])
        max_offsets[key] = max(max_offsets[key], m["offset"])

    tps = [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in max_offsets.items()
    ]

    try:
        consumer.commit(offsets=tps)
        log.info(
            "kafka offsets committed",
            partitions_count=len(tps),
            batch_size=len(batch),
        )
    except KafkaException:
        log.exception(f"kafka offsets commit failed: {e.args}", batch_size=len(batch))


def _flush_bronze_batch(consumer: Consumer, pending_batch: List[Dict[str, Any]]) -> None:
    if not pending_batch:
        log.debug("flush called with empty batch")
        return

    log.info("bronze flush started", batch_size=len(pending_batch))

    try:
        bronze_events = [BronzeWebEvent(**msg["data"]) for msg in pending_batch]

        # Upload to MinIO
        save_bronze_events(bronze_events)
        log.info("bronze flush done", batch_size=len(pending_batch))

        # Commit offsets
        _commit_offsets(consumer, pending_batch)

    except Exception as e:
        log.exception(f"bronze flush failed: {e.args}", batch_size=len(pending_batch))
        return

    pending_batch.clear()


def _enqueue_message(msg_dict: Dict[str, Any]) -> None:
    """
    Put decoded Kafka message into queue
    """
    with queue_lock:
        message_queue.append(msg_dict)


def drain_queue() -> List[Dict[str, Any]]:
    """
    Take all messages from queue
    """
    with queue_lock:
        if not message_queue:
            return []
        batch = message_queue.copy()
        message_queue.clear()
        return batch


def consume_loop() -> None:
    """
    Kafka consumer:
    - polls messages
    - builds Bronze batch
    - saves Bronze to MinIO
    - commits offsets
    """
    BATCH_SIZE = 100     # flush every 100 messages
    FLUSH_SECONDS = 5.0  # or every 5 seconds

    log.info(
        "consumer loop started",
        bootstrap=KAFKA_BOOTSTRAP,
        topics=KAFKA_TOPICS,
        group_id=KAFKA_GROUP_ID,
        batch_size=BATCH_SIZE,
        flush_seconds=FLUSH_SECONDS,
    )

    while True:
        consumer: Optional[Consumer] = None

        try:
            consumer = create_consumer()
            consumer.subscribe(KAFKA_TOPICS)

            log.info("subscribed to topics", topics=KAFKA_TOPICS)

            pending_bronze: List[Dict[str, Any]] = []
            last_flush_time = time.time()

            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    if pending_bronze and (time.time() - last_flush_time >= FLUSH_SECONDS):
                        log.info("time flush triggered", pending_size=len(pending_bronze))
                        _flush_bronze_batch(consumer, pending_bronze)
                        last_flush_time = time.time()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.warning("kafka poll error", error=str(msg.error()))
                    continue

                decoded = _decode_message(msg)
                if decoded is None:
                    log.warning(
                        "message decode failed",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                    continue

                _enqueue_message(decoded)

                pending_bronze.append(decoded)

                if len(pending_bronze) >= BATCH_SIZE:
                    log.info("batch size flush triggered", pending_size=len(pending_bronze))
                    _flush_bronze_batch(consumer, pending_bronze)
                    last_flush_time = time.time()

        except Exception as e:
            log.exception(f"consumer crashed: {e.args}")

        finally:
            if consumer:
                try:
                    consumer.close()
                    log.info("consumer closed")
                except KafkaException as e:
                    log.exception(f"consumer close failed: {e.args}")

            time.sleep(3)


def start_consumer_loop() -> None:
    """
    Start Kafka consumer in background thread
    """
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    log.info("consumer background thread started")