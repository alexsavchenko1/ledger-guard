#!/bin/sh

set -eu

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create \
  --if-not-exists \
  --topic operation-events \
  --partitions 1 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create \
  --if-not-exists \
  --topic operation-events-dlq \
  --partitions 1 \
  --replication-factor 1
