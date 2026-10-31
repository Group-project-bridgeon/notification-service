# Notification Service Kafka Contract

This document specifies the Kafka message contracts, consumer topics, owned retry/DLQ topics, payload formats, failure classifications, and producer expectations for the Notification Service.

---

## 1. Topics Consumed

All topic names are configurable via environment variables (names below are proposed baseline defaults).
- **Default Partition Count**: 6 partitions
- **Default Retention**: 7 days
- **Partition Key**: `aggregate_id` (guarantees ordering for events on the same aggregate)

### Consumed Topic Subscriptions & Event Types

#### `erp.auth.events`
- `user.created`
- `user.updated`
- `user.roles_changed`
- `user.deactivated`
- `user.registered`
- `user.locked`
- `user.password_changed`

#### `erp.purchasing.events`
- `purchase_request.*`
- `purchase_order.*`
- `goods_receipt.*`
- `purchase_return.*`

#### `erp.inventory.events`
- `stock.low`
- `reservation.short`
- `reservation.released`
- `stock_adjustment.*`
- `pick_list.assigned`
- `stock.quarantined`
- `transfer.*`

#### `erp.sales.events`
- `sales_order.*`
- `delivery.*`
- `quotation.*`

#### `erp.returns.events`
- `customer_return.*`
- `supplier_return.*`

#### `erp.finance.events`
- `invoice.*`
- `payment.*`
- `credit_note.*`
- `credit_limit.exceeded`
- `expense.*`

#### `erp.platform.events`
- `operation.started`
- `operation.succeeded`
- `operation.failed`
- `operation.compensating`
- `operation.compensated`
- `saga.compensation_started`
- `saga.compensation_completed`
- `approval.requested`
- `approval.decided`
- `comment.mentioned`

---

## 2. Topics Owned by This Service

The Notification Service provisions, produces to, and manages consumer loops for its delayed-retry hierarchy and Dead Letter Queue:
- `erp.notification.retry.1m`: First-tier transient delay (1 minute).
- `erp.notification.retry.10m`: Second-tier transient delay (10 minutes).
- `erp.notification.retry.1h`: Third-tier transient delay (1 hour).
- `erp.notification.dlq`: Final poisoned-message and unrecoverable error repository.

---

## 3. Message Format

- **Message Key**: `aggregate_id` represented as a `string` (UUID).
- **Message Value**: Standard JSON Event Envelope (see `CONTEXT.md` Rule 6):
  ```json
  {
    "event_id": "7b8e1460-63bf-4787-8e68-07df95a5f782",
    "event_type": "purchase_order.created",
    "event_version": "1",
    "occurred_at": "2026-10-12T09:30:00Z",
    "producer": "purchasing-service",
    "aggregate_type": "purchase_order",
    "aggregate_id": "9d1c08be-2415-46aa-bd08-8e6ca6f0e495",
    "correlation_id": "8fa8db3b-8c65-4f7b-9e45-d8aa497f1981",
    "actor_user_id": "e81d8590-bfae-4d8b-9d41-e125032b4943",
    "payload": {}
  }
  ```
- **Optional Kafka Headers**:
  - `event_id`
  - `event_type`
  - `event_version`
  - `correlation_id`
  - `producer`
  - `content-type`: `application/json`
  *(Note: If headers diverge from the JSON envelope body, the JSON envelope values are authoritative).*
- **Size Limits & Content Rules**:
  - **Maximum Message Size**: `256 KB`.
  - **Thin Display Payloads**: Payloads must carry only attributes necessary for user notification presentation (e.g., human-readable document numbers, actor names, titles, and decimal amounts). They must never carry complete database snapshots.
  - **No Secrets**: Never publish passwords, OTPs, or auth tokens into Kafka.
  - **Currency Representation**: Monetary values must always be represented as a decimal `string` (e.g., `"1250.50"`), never a float.

---

## 4. Producer Expectations
1. **Transactional Outbox**: Events must be published from the producer's transactional Outbox table **after** the corresponding local business transaction has successfully committed.
2. **Stable Event ID**: `event_id` is a unique UUID generated once per event and must be preserved across outbox republish attempts.
3. **Partition Key Consistency**: Events belonging to the same aggregate must always share the same message key (`aggregate_id`) to maintain partition-level ordering.
4. **Additive Versioning**: `event_version` changes must be strictly backward-compatible and additive (new optional fields only).
5. **Zero Secret Policy**: No one-time codes, passwords, or session tokens in any payload.

---

## 5. Domain Specific Event Payloads

### Identity Event Payload (`erp.auth.events`)
Used by the notification service to maintain its internal recipient projection:
```json
{
  "user_id": "e81d8590-bfae-4d8b-9d41-e125032b4943",
  "email": "sarah.connor@example.com",
  "display_name": "Sarah Connor",
  "roles": ["purchasing", "manager"],
  "is_active": true
}
```

### Operation Event Payload (`erp.platform.events`)
Used for operational SSE streaming to front-end clients:
```json
{
  "operation_id": "a58d3c11-9a74-4b53-93d3-1fc0575d5642",
  "operation_type": "order_checkout_saga",
  "entity": {
    "type": "sales_order",
    "id": "6c401311-64d8-4f1b-be75-2964b4c6e5a1"
  },
  "state": "failed",
  "error": {
    "code": "insufficient_inventory",
    "message": "Stock reservation failed for item SKU-1049",
    "details": [
      {
        "field": "line_items[2].quantity",
        "issue": "insufficient_stock"
      }
    ]
  },
  "compensation": {
    "status": "compensated",
    "details": "Payment authorization released"
  }
}
```

---

## 6. Retry and DLQ Headers

When republishing messages to retry topics or the DLQ, this service attaches the following Kafka headers:
- `x-attempt`: Integer string representing current attempt count (e.g., `"1"`, `"2"`).
- `x-original-topic`: Topic name where the message was initially received.
- `x-original-partition`: Integer partition number of original message.
- `x-original-offset`: Int64 offset number of original message.
- `x-error`: Short textual summary of the error/exception.
- `x-not-before`: UTC RFC 3339 timestamp specifying the earliest time this message may be retried (attached only to retry topics).

---

## 7. Failure Classifications & Handling

| Failure Class | Trigger Scenarios | Consumer Action |
|---|---|---|
| **Permanent Failure** | Non-JSON bytes, invalid envelope schema, payload > 256 KB, wrong data types. | Route directly to `erp.notification.dlq`. Log warning with message offset. Commit source offset immediately. |
| **Transient Failure** | Database temporarily unreachable, lock acquisition timeout, network blip. | Route sequentially through retry topics: `erp.notification.retry.1m` &rarr; `erp.notification.retry.10m` &rarr; `erp.notification.retry.1h`. If all attempts fail, route to `erp.notification.dlq`. |
| **Unknown Event Type** | An event arrives with an unhandled or future `event_type`. | **NOT a failure**: The `event_id` is recorded in `system_inbox_events` marked as ignored, and the offset is committed cleanly without DLQ routing. |
