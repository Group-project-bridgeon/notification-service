# Notification Service Context & Architectural Rules

## 1. Architecture
- **Microservices Architecture**: The system consists of loosely coupled, domain-aligned microservices.
- **Dedicated Database**: This service exclusively owns and manages `notification_db`. It connects using its dedicated PostgreSQL role `notification_user`.
- **Database Isolation**: Never read, write, or query another microservice's database directly.
- **Foreign Key Constraints**: No cross-service foreign keys exist. External identifiers (e.g., user IDs, order IDs, invoice IDs) must be stored as plain `UUID` columns without database-level constraints referencing other services.

## 2. Consistency Model
- **Local Consistency**: Strong consistency inside this service boundaries implemented via local PostgreSQL ACID transactions.
- **Cross-Service Consistency**: Eventual consistency across service boundaries achieved asynchronously via Kafka events.
- **At-Least-Once Delivery**: Kafka delivery semantics are at-least-once, meaning messages may be re-delivered upon consumer restarts, crashes, or rebalances. Every event handler must be strictly idempotent.

## 3. Inbox Pattern
- **Atomic Persistence**: Every consumed `event_id` is stored once in the `system_inbox_events` table within the **exact same database transaction** as its side effects (in-app notifications, delivery rows, projection updates).
- **Deduplication & Acknowledgement**: If an incoming `event_id` already exists in `system_inbox_events`, it is acknowledged as duplicate and safely ignored without executing duplicate business logic or side effects.

## 4. Kafka Offset Commit Rules
- **Commit Timing**: Commit a Kafka offset **only after** the local database transaction for that message has successfully committed, or after the message has been safely published to a retry or Dead Letter Queue (DLQ) topic.
- **Never Commit Early**: Never commit offsets in memory prior to persistence or downstream dead-lettering, ensuring zero message loss upon unexpected failure.

## 5. Choreography Consumer Rules
- **Fault Isolation**: The notification service operates strictly as a choreography event consumer. Failures, exceptions, or downtime in this service must never affect or block an upstream event producer.
- **Transactional Boundary Isolation**: Never invoke remote HTTP/RPC endpoints, third-party APIs, or other services inside a database transaction.

## 6. Event Envelope (Kafka Message Value)
Every Kafka message consumed or produced carries a standard JSON envelope as its value, with the message key set to `aggregate_id`:
- `event_id` (`uuid`): Unique identifier of the event instance.
- `event_type` (`string`): Dot-delimited or snake_case event descriptor (e.g., `approval.requested`, `order.confirmed`).
- `event_version` (`string` or `integer`): Version of the event schema (e.g., `1` or `1.0`).
- `occurred_at` (`string`): UTC timestamp formatted in ISO-8601 / RFC 3339 (e.g., `2026-10-12T09:30:00Z`).
- `producer` (`string`): Emitting service identifier (e.g., `purchasing-service`, `auth-service`).
- `aggregate_type` (`string`): Domain entity classification (e.g., `purchase_order`, `user`).
- `aggregate_id` (`uuid` or `string`): Identifier of the aggregate root entity (used as Kafka message key).
- `correlation_id` (`uuid`): Distributed tracing identifier shared across the causal chain.
- `actor_user_id` (`uuid`, nullable): ID of the user who initiated the action (if user-triggered).
- `payload` (`object`): Event-specific business attributes and display metadata.

## 7. Conventions (Team API v0.2)
- **Path Prefixing**: No version prefix in paths (e.g., `/notifications`, `/me/notification-preferences`; NOT `/api/v1/...`).
- **Naming Convention**: JSON property names must be `snake_case`.
- **Primary Keys**: System identifiers are standard `UUID` values.
- **Timestamps**: All timestamps must be in UTC ISO-8601 format (`YYYY-MM-DDTHH:MM:SSZ`).
- **Monetary Values**: Money must always be represented as a decimal `string` (e.g., `"149.99"`), never as a floating-point number.
- **Pagination & List Envelope**:
  Responses containing lists wrap data in standard envelope format:
  ```json
  {
    "data": [],
    "meta": {
      "page": 1,
      "page_size": 20,
      "total_count": 100,
      "total_pages": 5
    }
  }
  ```
- **Standard Query Parameters**:
  - `page`: 1-based index (default `1`).
  - `page_size`: items per page (default `20`, maximum `100`).
  - `sort`: sorting criteria (`-` prefix indicates descending order, e.g., `-created_at`).
  - `q`: optional free-text search string.
- **HTTP Headers**:
  - `Authorization`: `Bearer <jwt>`
  - `Idempotency-Key`: Client-provided key for deduplicating non-idempotent operations.
  - `X-Request-ID`: Client- or gateway-provided request correlation UUID (must be echoed back in all response headers and error envelopes).

## 8. Error Envelope Specification
All HTTP error responses return a standardized JSON envelope:
```json
{
  "error": {
    "code": "snake_case_code",
    "message": "Human-readable explanation of the error",
    "details": [
      {
        "field": "optional_field_name",
        "issue": "specific_issue_identifier"
      }
    ],
    "request_id": "00000000-0000-0000-0000-000000000000"
  }
}
```

### Standard Error Codes & Status Mappings
| Error Code | HTTP Status | Description |
|---|---|---|
| `validation_failed` | `400 Bad Request` | Request payload fails schema validation or syntax checks |
| `unauthenticated` | `401 Unauthorized` | Missing, expired, or malformed JWT bearer token |
| `forbidden_role` | `403 Forbidden` | Authenticated user lacks required role permissions |
| `not_found` | `404 Not Found` | Requested resource does not exist |
| `invalid_state_transition` | `409 Conflict` | Operation violates entity lifecycle state |
| `version_conflict` | `409 Conflict` | Optimistic locking concurrency check failed |
| `idempotency_key_conflict` | `409 Conflict` | Same key supplied with a different request payload |
| `rate_limited` | `429 Too Many Requests` | Rate limit threshold exceeded |
| `email_not_configured` | `409 Conflict` | SMTP / email provider credentials unconfigured |
| `email_suppressed` | `409 Conflict` | Recipient address is listed on suppression / bounce list |
| `email_template_invalid` | `400 Bad Request` | Template syntax error or missing required parameters |
| `email_send_failed` | `502 Bad Gateway` | Downstream mail delivery provider failure |

## 9. Authentication & Authorization
- **Public & Management Routes**: Authenticated via JWT bearer access token (`Authorization: Bearer <token>`).
  - Expected JWT claims:
    - `sub`: User ID (`uuid`).
    - `roles`: Array of assigned role names (`string[]`).
    - `email`: Authenticated user email address (`string`).
    - `exp`: Token expiration timestamp (Unix epoch).
  - Assumed baseline roles (configurable via environment variables):
    - `admin`
    - `manager`
    - `purchasing`
    - `warehouse`
    - `sales`
    - `finance`
- **Internal Service-to-Service Routes**: Authenticated using HTTP header `X-Internal-Token`. Internal endpoints are private to the Docker Compose network and are never exposed through the public API gateway.

## 10. Security & Data Protection Rules
- **Prohibited Logging & Storage**: OTP codes, plain passwords, authorization tokens, and SMTP credentials must **NEVER** be logged, stored in plaintext in notification tables, or returned via any API response.
- **Sensitive Variable Redaction**: Any template parameters or payload variables listed in `sensitive_variables` must be redacted (e.g., replaced with `[REDACTED]`) before persisting delivery logs or message history.
- **Kafka Secret Exclusion**: One-time codes (OTP), reset tokens, and invitation tokens must **never** appear in any Kafka event message. All OTP, password reset, and invitation emails must strictly be dispatched via the secure internal HTTP route `POST /internal/email/send`.

## 11. Enums & Domain Classifications
- **Notification Status**:
  - `unread`
  - `read`
  - `archived`
- **Notification Priority**:
  - `low`
  - `normal`
  - `high`
  - `critical`
- **Categories**:
  - `approval`
  - `purchasing`
  - `inventory`
  - `warehouse`
  - `sales`
  - `delivery`
  - `returns`
  - `finance`
  - `security`
  - `account`
  - `saga`
  - `system`
- **Notification Channels**:
  - `in_app`
  - `email`
- **Notification Types**:
  - `approval_pending`
  - `approval_decided`
  - `low_stock`
  - `reservation_short`
  - `delivery_status`
  - `invoice_overdue`
  - `payment_received`
  - `receipt_posted`
  - `order_confirmed`
  - `order_failed`
  - `return_update`
  - `adjustment_pending`
  - `comment_mention`
  - `security_alert`
  - `system`

## 12. Database Migrations
- Migrations are structured as standard plain SQL files located in `migrations/`:
  - `migrations/000001_name.up.sql`
  - `migrations/000001_name.down.sql`
- **No Migration Tool Lock-In**: The team has not finalized a migration library; no migration tool or framework may be hardcoded into the Go application runtime.
- **Convenience Tooling**: A `Makefile` target may invoke `golang-migrate` for local CLI execution and CI test pipelines.

## 13. Kafka Operational Rules
- **Consumer Group ID**: Configured via environment variables (default `notification-service`).
- **Topic Configuration**: Subscribed topics come strictly from configuration and are never hardcoded in source code because topic names are not final.
- **Topic Auto-Creation**: Topic auto-creation is allowed **only in local development**; production requires explicitly provisioned topics.
- **Manual Offset Commits**: Offsets are committed manually; auto-commit must be disabled.
- **Partition Concurrency & Ordering**: Messages within a single partition are processed in sequential order; separate partitions are processed concurrently in parallel.
- **Rebalance Safety**: Consumer rebalance callbacks must cleanly drain and finish in-flight transactions or release uncommitted state without losing messages or applying duplicate side effects.
- **Poll Loop Liveness**: The consumer poll loop must never be blocked longer than `max.poll.interval.ms`. Heavy I/O or email sending is offloaded to worker pools.
- **Structured Logging**: Log `topic`, `partition`, `offset`, `event_id`, and `correlation_id` for every processed message. **Never** log the payload of sensitive events.
- **Resilience Against Malformed Messages**: The consumer must never crash on malformed payloads, non-JSON bytes, or schema invalidity. Such messages must be routed immediately to the Dead Letter Queue (`erp.notification.dlq`) and their offsets committed.

## 14. Working Rules & Guidelines
- **Plan First**: Always produce a concise implementation plan prior to executing modifications.
- **Requirement Discipline**: Never invent unrequested requirements, extra tables, or unnecessary abstractions.
- **Assumptions**: Explicitly list all assumptions at the end of each response.
- **Code Quality**: Keep functions small, focused, and single-purpose.
- **Secrets Management**: No hardcoded secrets anywhere in the codebase; utilize environment variables and keep `.env.example` in sync.
- **Testing**: Write comprehensive unit and integration tests covering business rules and edge cases.
- **Isolation**: Do not touch files outside `notification-service/` unless explicitly requested by the task.
