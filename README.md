# ERP Notification Service

High-throughput, event-driven notification engine for enterprise ERP workflows. Consumes domain events across business aggregates from Apache Kafka, provides idempotent deduplication via an inbox pattern, dispatches real-time SSE notifications, renders templated business/transactional emails, tracks long-running async operation projections, and provides full observability and dead-letter replayability.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph KafkaCluster["Apache Kafka Cluster"]
        direction TB
        MainTopics["Domain Event Topics<br/>(erp.sales.events, erp.auth.events, etc.)"]
        Retry1M["erp.notification.retry.1m"]
        Retry10M["erp.notification.retry.10m"]
        Retry1H["erp.notification.retry.1h"]
        DLQ["erp.notification.dlq"]
    end

    subgraph ConsumerLayer["Franz-Go Consumer Group"]
        RouterPartitions["Partition Workers<br/>(Strict In-Order by Partition)"]
        Backoff["Retry / Delay Scheduler"]
    end

    subgraph ServiceCore["Notification Service Core"]
        Inbox["Inbox Deduplication<br/>(notification_inbox)"]
        Ingestor["Ingestor Engine"]
        OpProjection["Operations Projection<br/>(Async Sagas & 202 Tracking)"]
        RoutingEngine["Rule & Template Engine"]
        SSEHub["SSE Hub<br/>(In-Memory User Multiplexer)"]
        EmailEngine["Email Engine & Workers<br/>(Transactional & Business Queues)"]
    end

    subgraph Storage["PostgreSQL 16"]
        DB[(erp_notifications)]
    end

    subgraph Observability["Monitoring & External Clients"]
        MetricsEndpoint["GET /metrics<br/>(Prometheus Exposition)"]
        WebFrontend["Frontend Web / Mobile<br/>(SSE & REST API)"]
        AdminCLI["Admin Console & Simulator<br/>(cmd/simulate)"]
    end

    MainTopics --> RouterPartitions
    Retry1M --> RouterPartitions
    Retry10M --> RouterPartitions
    Retry1H --> RouterPartitions

    RouterPartitions --> Ingestor
    Ingestor <--> Inbox
    Inbox <--> DB

    Ingestor --> OpProjection
    OpProjection --> DB
    OpProjection -.->|event: operation| SSEHub

    Ingestor --> RoutingEngine
    RoutingEngine --> DB
    RoutingEngine -->|event: notification| SSEHub
    RoutingEngine -->|Queue / Send| EmailEngine

    RouterPartitions -.->|Transient Failures| Backoff
    Backoff --> Retry1M
    Retry1M -.-> Retry10M
    Retry10M -.-> Retry1H
    Backoff -.->|Exhausted / Poison| DLQ
    DLQ --> DB

    SSEHub --> WebFrontend
    WebFrontend <--> DB
    MetricsEndpoint <--> Ingestor
    MetricsEndpoint <--> ConsumerLayer
    AdminCLI --> MainTopics
```

---

## 2. Local Setup & Quickstart

### Prerequisites
- Docker & Docker Compose
- Go 1.22+
- `psql` (PostgreSQL client) or `golang-migrate`

### Step 1: Start PostgreSQL and Kafka
```bash
docker compose -f docker-compose.dev.yml up -d
```

### Step 2: Provision Kafka Topics
Create the canonical topics with 3 partitions and replication factor 1:
```bash
# Domain topics
docker compose -f docker-compose.dev.yml exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --partitions 3 --replication-factor 1 \
  --topic erp.sales.events

docker compose -f docker-compose.dev.yml exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --partitions 3 --replication-factor 1 \
  --topic erp.auth.events

docker compose -f docker-compose.dev.yml exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --partitions 3 --replication-factor 1 \
  --topic erp.inventory.events

docker compose -f docker-compose.dev.yml exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --partitions 3 --replication-factor 1 \
  --topic erp.purchasing.events

# Retry hierarchy topics
docker compose -f docker-compose.dev.yml exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --partitions 3 --replication-factor 1 \
  --topic erp.notification.retry.1m

docker compose -f docker-compose.dev.yml exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --partitions 3 --replication-factor 1 \
  --topic erp.notification.retry.10m

docker compose -f docker-compose.dev.yml exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --partitions 3 --replication-factor 1 \
  --topic erp.notification.retry.1h

# Dead-letter queue
docker compose -f docker-compose.dev.yml exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --partitions 3 --replication-factor 1 \
  --topic erp.notification.dlq
```

### Step 3: Run Database Migrations
```bash
for file in migrations/*.up.sql; do
  PGPASSWORD=erp_password psql -h localhost -p 5432 -U erp -d erp_notifications -f "$file"
done
```

### Step 4: Seed Initial Data
```bash
go run cmd/seed/main.go
```

### Step 5: Start the Service
```bash
go run cmd/server/main.go
```

---

## 3. Environment Variables Reference

| Variable | Default Value | Description / Production Guidance |
| :--- | :--- | :--- |
| `ENV` | `development` | Environment mode (`development`, `staging`, `production`). In production, log level defaults to info and LogSender is disabled. |
| `PORT` | `8085` | HTTP server listening port. |
| `DATABASE_URL` | `postgres://erp:erp_password@localhost:5432/erp_notifications?sslmode=disable` | PostgreSQL connection string. Use SSL (`sslmode=require` or `verify-full`) in production. |
| `DB_MAX_OPEN_CONNS` | `25` | Maximum database connection pool size. |
| `DB_MAX_IDLE_CONNS` | `5` | Maximum idle connections. |
| `DB_CONN_MAX_LIFETIME` | `1h` | Maximum connection reuse duration. |
| `AUTH_MODE` | `jwt` | Authentication mode (`jwt` or `dev`). In `dev` mode, headers `X-Dev-User-Id` and `X-Dev-Roles` are accepted. Use `jwt` in production. |
| `JWT_SECRET` | - | HMAC secret (HS256) for verifying caller tokens. Required if `AUTH_MODE=jwt` and `JWT_PUBLIC_KEY` is unset. |
| `JWT_PUBLIC_KEY` | - | PEM-encoded RSA/ECDSA public key (RS256/ES256). |
| `INTERNAL_SERVICE_TOKEN` | - | Shared secret required in `X-Internal-Token` header for `/internal/*`, `/admin/notifications/ingest`, and `/metrics`. |
| `INTERNAL_EVENTS_ENABLED` | `true` (dev) / `false` (prod) | Allows HTTP event ingestion via `/admin/notifications/ingest`. Keep `false` in production. |
| `EMAIL_ENABLED` | `true` | Enables outbound email dispatch workers. |
| `EMAIL_ENCRYPTION_KEY` | - | 32-byte base64 AES-GCM key used to encrypt SMTP passwords at rest. Required in production. |
| `WEBHOOK_SECRET` | - | HMAC secret used to verify inbound email webhook signatures. |
| `CORS_ALLOWED_ORIGINS` | `*` | Comma-separated list of allowed origins. Set specific origins in production. |
| `LOG_LEVEL` | `info` | Minimum log severity: `debug`, `info`, `warn`, `error`. |
| `KAFKA_CONSUMER_ENABLED`| `true` | Enables background Kafka consumer group. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated list of Kafka broker host:port addresses. |
| `KAFKA_GROUP_ID` | `notification-service-consumer` | Consumer group identifier. |
| `KAFKA_CLIENT_ID` | `notification-service` | Client identifier reported in Kafka telemetry. |
| `KAFKA_START_OFFSET` | `earliest` | Offset reset policy (`earliest` or `latest`). |
| `KAFKA_SECURITY_PROTOCOL`| `PLAINTEXT` | Transport protocol (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`). |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | SASL mechanism (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`). |
| `KAFKA_SASL_USERNAME` | - | SASL username. |
| `KAFKA_SASL_PASSWORD` | - | SASL password. |

---

## 4. Kafka Pipeline Simulator & `kcat` Guide

The simulator CLI is compiled under `bin/simulate` (or `cmd/simulate/main.go`). It produces realistic ERP domain events according to `docs/KAFKA_CONTRACT.md`.

### Running Scenarios

```bash
# 1. Happy path: full sales order lifecycle with shared correlation_id
go run cmd/simulate/main.go --scenario sales-order-happy

# 2. Failure path: sales order saga failure with compensating transactions
go run cmd/simulate/main.go --scenario sales-order-failure

# 3. Duplicate delivery: sends the exact same event_id 3 times
go run cmd/simulate/main.go --scenario duplicate

# 4. Out-of-order identity events: emits user update T2 then older update T1
go run cmd/simulate/main.go --scenario out-of-order

# 5. Poison pill tests: malformed JSON, invalid envelopes, oversized payloads, tombstones
go run cmd/simulate/main.go --scenario poison

# 6. Bulk throughput: 5,000 events partitioned across 500 aggregates
go run cmd/simulate/main.go --scenario bulk

# 7. OTP Email delivery via HTTP (LogSender)
go run cmd/simulate/main.go --scenario otp-email --via http --http-url http://localhost:8085
```

### Inspecting Topics with `kcat` (`kafkacat`)

```bash
# Tail domain events with headers and key
kcat -b localhost:9092 -t erp.sales.events -C -f 'Partition: %p | Offset: %o | Key: %k\nHeaders: %h\nPayload: %s\n\n'

# Inspect the Dead Letter Queue (DLQ)
kcat -b localhost:9092 -t erp.notification.dlq -C -f 'DLQ Offset: %o | Headers: %h\nPayload: %s\n\n'

# Inspect 1-minute retry topic
kcat -b localhost:9092 -t erp.notification.retry.1m -C -f 'Retry[1m] Offset: %o | Headers: %h\nPayload: %s\n\n'

# Check partition offsets and metadata
kcat -b localhost:9092 -L
```

---

## 5. Topic and Partition Configuration

| Topic Pattern | Cleanup Policy | Retention | Partition Keying | Max Message Size |
| :--- | :--- | :--- | :--- | :--- |
| `erp.*.events` | `delete` | 7 days (`604800000 ms`) | `aggregate_id` (string UUID) | 1 MiB (`1048576 bytes`) |
| `erp.notification.retry.*`| `delete` | 3 days (`259200000 ms`) | `aggregate_id` | 1 MiB |
| `erp.notification.dlq` | `delete` | 30 days (`2592000000 ms`) | `aggregate_id` | 2 MiB (accommodates headers) |

**Keying Scheme**: All domain events MUST be keyed by `aggregate_id`. Franz-Go hashes this key to ensure strict total ordering per business entity across partitions.

---

## 6. Retry & Dead-Letter-Queue (DLQ) Architecture

```mermaid
stateDiagram-v2
    [*] --> Ingestion: Message Poll
    Ingestion --> Success: Valid & Processed
    Ingestion --> TombstoneCommit: Value is nil (Tombstone)
    Ingestion --> DLQ: Malformed JSON / Poison
    
    Ingestion --> TransientError: Network / Lock / 5xx
    TransientError --> Retry1M: Attempt 1 (1 min backoff)
    Retry1M --> Retry10M: Attempt 2 (10 min backoff)
    Retry10M --> Retry1H: Attempt 3 (1 hr backoff)
    Retry1H --> DLQ: Exhausted
    
    DLQ --> DLQTable: Stored in notification_dead_letters
    DLQTable --> ReplayAPI: POST /admin/notification-dead-letters/{id}/replay
    ReplayAPI --> Ingestion: Re-injected
```

### Retry Headers
When a message is routed to retry topics, Franz-Go attaches standard metadata headers:
- `X-Retry-Count`: Number of retries already performed (`1`, `2`, `3`).
- `X-Original-Topic`: Originating topic name.
- `X-Original-Partition`: Originating partition ID.
- `X-Original-Offset`: Originating offset.
- `X-Error-Reason`: Detailed error diagnostic string.

### Dead-Letter Replay via REST API
Administrators can inspect and re-inject dead letters:
```bash
# 1. List dead letters
curl -H "X-Dev-Roles: admin" http://localhost:8085/admin/notification-dead-letters

# 2. Replay specific dead letter
curl -X POST -H "X-Dev-Roles: admin" http://localhost:8085/admin/notification-dead-letters/<UUID>/replay
```

---

## 7. Observability & Consumer Lag Monitoring

### Prometheus Metrics (`GET /metrics`)
Protected by the `X-Internal-Token` header:
```bash
curl -H "X-Internal-Token: dev-internal-token-12345" http://localhost:8085/metrics
```

Exposed Metrics:
- `messages_consumed_total{topic,result}`: Counter tracking messages consumed by outcome (`success`, `retry`, `dlq`, `poison`, `tombstone`).
- `handler_duration_seconds{quantile}`: Summary of message processing latency.
- `consumer_lag{topic,partition}`: Gauge showing current unconsumed offset lag per partition.
- `retry_published_total{topic}`: Counter of messages published to retry topics.
- `dlq_published_total{topic}`: Counter of records published to the DLQ.
- `inbox_duplicates_total`: Counter of duplicated events safely deduplicated by the inbox.
- `email_queue_depth`: Gauge of pending queued outbound emails.
- `sse_connections`: Gauge of live connected SSE client streams.

### Recommended Alerting Rules
- **High Consumer Lag**: `consumer_lag > 500` for 3m (Warning), `consumer_lag > 2500` for 5m (Critical).
- **Poison / DLQ Ingestion**: `rate(dlq_published_total[5m]) > 0` (Warning).
- **Consumer Stalled**: `rate(messages_consumed_total[5m]) == 0` while lag is increasing.

---

## 8. Known Limitations & Production Considerations

1. **Partition Rescaling**: Increasing topic partition counts dynamically alters hash bucket assignments for existing keys, temporarily breaking strict per-aggregate ordering until older partitions drain. Always schedule partition expansions during maintenance windows.
2. **Exactly-Once Semantics (EOS)**: The service operates under an **At-Least-Once** Kafka delivery model combined with database inbox deduplication (`notification_inbox`), achieving end-to-end effective idempotency without two-phase commit overhead.
3. **Multi-Region Active-Active**: When operating multi-cluster Kafka replication (e.g., via MirrorMaker 2), offset numbers differ between clusters. The service relies on global `event_id` and database unique constraints rather than Kafka offsets for deduplication.
