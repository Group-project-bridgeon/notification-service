#!/usr/bin/env sh
# -----------------------------------------------------------------------------
# scripts/create-topics.sh
# Idempotently creates Kafka domain, retry, and DLQ topics
# -----------------------------------------------------------------------------

set -e

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"

echo "[topics-init] Waiting for Kafka broker at $BOOTSTRAP_SERVER to become ready..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list > /dev/null 2>&1; do
  echo "[topics-init] Kafka not ready yet, retrying in 2 seconds..."
  sleep 2
done

echo "[topics-init] Kafka broker is ready. Creating topics..."

# 7 Consumed Domain Topics (6 partitions each, replication factor 1, 7 days retention)
CONSUMED_TOPICS="
erp.auth.events
erp.purchasing.events
erp.inventory.events
erp.sales.events
erp.returns.events
erp.finance.events
erp.platform.events
"

for topic in $CONSUMED_TOPICS; do
  echo "[topics-init] Creating consumed topic: $topic (partitions: 6)"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=604800000
done

# 3 Retry Topics & 1 DLQ Topic (3 partitions each, replication factor 1)
OWNED_TOPICS="
erp.notification.retry.1m
erp.notification.retry.10m
erp.notification.retry.1h
erp.notification.dlq
"

for topic in $OWNED_TOPICS; do
  echo "[topics-init] Creating service-owned topic: $topic (partitions: 3)"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000
done

echo "[topics-init] All topics initialized successfully. Current topics list:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --describe
