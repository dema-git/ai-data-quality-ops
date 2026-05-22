#!/bin/bash
set -e

BROKER="kafka:9092"
TOPICS=("sessions_raw")

KAFKA_BIN="/opt/kafka_2.13-3.7.2/bin"

echo "Waiting for Kafka to become ready..."

KAFKA_READY=0

# Wait until kafka-topics can talk to the broker.
for i in {1..30}; do
  if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
    echo "Kafka is ready."
    KAFKA_READY=1
    break
  fi
  echo "  still starting... ($i)"
  sleep 2
done

if [ "$KAFKA_READY" -ne 1 ]; then
  echo "Kafka did not become ready in time."
  exit 1
fi

for topic in "${TOPICS[@]}"; do
  echo "Creating topic $topic..."
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --partitions 3 --replication-factor 1 \
    --topic "$topic"
done

echo "sessions_raw checked/created!"
