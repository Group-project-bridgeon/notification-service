# Notification Service API Contract (v0.2)

This document specifies the exact API contract for the Notification Service across Public, Admin, Internal, Webhook, and Health endpoints.

---

## 1. Public Endpoints (JWT Authenticated)

Requires standard JWT Bearer token: `Authorization: Bearer <token>`.

### Common Schemas

#### Notification Object
```json
{
  "id": "7b8e1460-63bf-4787-8e68-07df95a5f782",
  "type": "approval_pending",
  "category": "approval",
  "priority": "normal",
  "title": "Purchase Order PO-2026-0042 requires approval",
  "body": "PO-2026-0042 from Fabric Corp totaling $14,250.00 is waiting for your review.",
  "entity": {
    "type": "purchase_order",
    "id": "9d1c08be-2415-46aa-bd08-8e6ca6f0e495"
  },
  "link": "/purchasing/orders/9d1c08be-2415-46aa-bd08-8e6ca6f0e495",
  "correlation_id": "8fa8db3b-8c65-4f7b-9e45-d8aa497f1981",
  "status": "unread",
  "created_at": "2026-10-12T09:30:00Z",
  "read_at": null
}
```

---

### `GET /notifications`
Fetch paginated in-app notifications for the authenticated user.

- **Query Parameters**:
  - `page` (`integer`, optional, default `1`): 1-based page number.
  - `page_size` (`integer`, optional, default `20`, max `100`): Results per page.
  - `sort` (`string`, optional, default `-created_at`): Sort field (`-` prefix indicates descending).
  - `status` (`string`, optional): Filter by `unread`, `read`, or `archived`.
  - `category` (`string`, optional): Filter by notification category.
  - `priority` (`string`, optional): Filter by `low`, `normal`, `high`, `critical`.
  - `type` (`string`, optional): Filter by notification type.
  - `from` (`string`, optional): RFC 3339 timestamp start filter.
  - `to` (`string`, optional): RFC 3339 timestamp end filter.

- **Response `200 OK`**:
```json
{
  "data": [
    {
      "id": "7b8e1460-63bf-4787-8e68-07df95a5f782",
      "type": "approval_pending",
      "category": "approval",
      "priority": "normal",
      "title": "Purchase Order PO-2026-0042 requires approval",
      "body": "PO-2026-0042 from Fabric Corp totaling $14,250.00 is waiting for your review.",
      "entity": {
        "type": "purchase_order",
        "id": "9d1c08be-2415-46aa-bd08-8e6ca6f0e495"
      },
      "link": "/purchasing/orders/9d1c08be-2415-46aa-bd08-8e6ca6f0e495",
      "correlation_id": "8fa8db3b-8c65-4f7b-9e45-d8aa497f1981",
      "status": "unread",
      "created_at": "2026-10-12T09:30:00Z",
      "read_at": null
    }
  ],
  "meta": {
    "page": 1,
    "page_size": 20,
    "total_count": 1,
    "total_pages": 1
  }
}
```

---

### `GET /notifications/unread-count`
Retrieve total and category-level unread counts for the current user.

- **Response `200 OK`**:
```json
{
  "data": {
    "total": 5,
    "by_category": {
      "approval": 3,
      "inventory": 1,
      "security": 1
    }
  }
}
```

---

### `GET /notifications/{id}`
Retrieve a single notification by its UUID.

- **Response `200 OK`**:
```json
{
  "data": {
    "id": "7b8e1460-63bf-4787-8e68-07df95a5f782",
    "type": "approval_pending",
    "category": "approval",
    "priority": "normal",
    "title": "Purchase Order PO-2026-0042 requires approval",
    "body": "PO-2026-0042 from Fabric Corp totaling $14,250.00 is waiting for your review.",
    "entity": {
      "type": "purchase_order",
      "id": "9d1c08be-2415-46aa-bd08-8e6ca6f0e495"
    },
    "link": "/purchasing/orders/9d1c08be-2415-46aa-bd08-8e6ca6f0e495",
    "correlation_id": "8fa8db3b-8c65-4f7b-9e45-d8aa497f1981",
    "status": "unread",
    "created_at": "2026-10-12T09:30:00Z",
    "read_at": null
  }
}
```

---

### `POST /notifications/{id}/read`
Mark an unread notification as read.

- **Response `200 OK`**:
```json
{
  "data": {
    "id": "7b8e1460-63bf-4787-8e68-07df95a5f782",
    "status": "read",
    "read_at": "2026-10-12T09:35:00Z"
  }
}
```

---

### `POST /notifications/{id}/unread`
Revert a read notification back to unread status.

- **Response `200 OK`**:
```json
{
  "data": {
    "id": "7b8e1460-63bf-4787-8e68-07df95a5f782",
    "status": "unread",
    "read_at": null
  }
}
```

---

### `POST /notifications/{id}/archive`
Archive a notification.

- **Response `200 OK`**:
```json
{
  "data": {
    "id": "7b8e1460-63bf-4787-8e68-07df95a5f782",
    "status": "archived"
  }
}
```

---

### `POST /notifications/read-all`
Mark all notifications (or all in a specific category) as read for the user.

- **Request Body**:
```json
{
  "category": "approval"
}
```
*(Note: `category` is optional. If omitted, marks all notifications across all categories as read.)*

- **Response `200 OK`**:
```json
{
  "data": {
    "updated_count": 3
  }
}
```

---

### `GET /me/notification-preferences`
Fetch user-specific delivery channel preferences.

- **Response `200 OK`**:
```json
{
  "data": {
    "channels": {
      "in_app": {
        "approval": true,
        "purchasing": true,
        "inventory": true,
        "security": true,
        "saga": true
      },
      "email": {
        "approval": true,
        "purchasing": false,
        "inventory": false,
        "security": true
      }
    }
  }
}
```

---

### `PUT /me/notification-preferences`
Update user notification channel preferences.

- **Rule**: `security`, `saga`, `approval` in-app notifications, and every item with `critical` priority are locked and cannot be disabled.
- **Error Response `422 Unprocessable Entity` (Code `validation_failed`)**:
```json
{
  "error": {
    "code": "validation_failed",
    "message": "Cannot disable mandatory notification channel",
    "details": [
      {
        "field": "channels.in_app.security",
        "issue": "preference_locked"
      }
    ],
    "request_id": "b3ad6c56-78b1-4f33-8ec2-13dafc678a1c"
  }
}
```

- **Response `200 OK`**:
```json
{
  "data": {
    "channels": {
      "in_app": {
        "approval": true,
        "purchasing": true,
        "inventory": true,
        "security": true,
        "saga": true
      },
      "email": {
        "approval": true,
        "purchasing": true,
        "inventory": false,
        "security": true
      }
    }
  }
}
```

---

### `POST /notifications/stream-ticket`
Issue a short-lived single-use ticket to authenticate the Server-Sent Events (SSE) connection.

- **Response `200 OK`**:
```json
{
  "data": {
    "ticket": "st_9f83ac045e0fbc4a3c18b762",
    "expires_in": 30
  }
}
```

---

### `GET /notifications/stream?ticket=...`
Real-time Server-Sent Events (SSE) stream for user notifications and updates.

- **Query Parameters**:
  - `ticket` (`string`, required): Single-use ticket obtained via `POST /notifications/stream-ticket`.
- **Headers Supported**:
  - `Last-Event-ID`: Client-provided ID for event replay after reconnection.
- **Protocol**: `text/event-stream; charset=utf-8`
- **Heartbeat**: Comment line `: heartbeat` sent every 25 seconds to keep connection alive.
- **Event Types Emitted**:
  - `event: notification`
    ```
    event: notification
    id: 7b8e1460-63bf-4787-8e68-07df95a5f782
    data: {"id":"7b8e1460-63bf-4787-8e68-07df95a5f782","type":"approval_pending","category":"approval","priority":"normal","title":"Purchase Order PO-2026-0042 requires approval","body":"...","status":"unread","created_at":"2026-10-12T09:30:00Z"}
    ```
  - `event: unread_count`
    ```
    event: unread_count
    data: {"total":6,"by_category":{"approval":4,"inventory":1,"security":1}}
    ```
  - `event: operation`
    ```
    event: operation
    data: {"operation_id":"a58d3c11-9a74-4b53-93d3-1fc0575d5642","status":"completed","aggregate_type":"order","aggregate_id":"9d1c08be-2415-46aa-bd08-8e6ca6f0e495"}
    ```

---

## 2. Admin Endpoints (Requires `admin` role)

Requires `Authorization: Bearer <token>` containing role `admin`.

> [!NOTE]
> Configuration catalogs (`/admin/notification-rules`, `/admin/notification-templates`, `/email/templates`, `/email/suppressions`) return `{ "data": [...] }`. Paginated operational logs (`/notifications`, `/operations`, `/admin/notification-deliveries`, `/admin/notification-dead-letters`, `/email/messages`) return `{ "data": [...], "meta": { "page", "page_size", "total_count", "total_pages" } }`.

### Notification Rules
- `GET /admin/notification-rules`: List routing and filtering rules.
- `POST /admin/notification-rules`: Create a routing rule.
- `GET /admin/notification-rules/{id}`: Retrieve a specific rule.
- `PUT /admin/notification-rules/{id}`: Update a routing rule.
- `DELETE /admin/notification-rules/{id}`: Delete a routing rule.

### Notification Templates
- `GET /admin/notification-templates`: List notification templates.
- `POST /admin/notification-templates`: Create a new template definition.
- `PUT /admin/notification-templates/{key}`: Update template; creates an incremented version of the template.

### Test Send
- `POST /admin/notifications/test-send`:
  - **Request Body**:
    ```json
    {
      "user_id": "7b8e1460-63bf-4787-8e68-07df95a5f782",
      "template_key": "approval.requested",
      "sample_payload": {
        "order_number": "PO-TEST-01",
        "total": "5000.00"
      }
    }
    ```
  - **Response `200 OK`**:
    ```json
    {
      "data": {
        "delivered": true,
        "channels": ["in_app", "email"]
      }
    }
    ```

### Delivery & Dead-Letter Management
- `GET /admin/notification-deliveries`: Query recent delivery logs across channels.
- `POST /admin/notification-deliveries/{id}/retry`: Manually trigger retry for a specific failed delivery attempt.
- `GET /admin/notification-dead-letters`: Query poisoned or unroutable notification dead letters.
- `POST /admin/notification-dead-letters/{id}/replay`: Replay a dead-letter event back into the ingestion pipeline.
- `GET /admin/notification-stats`: Query aggregated metrics (sent counts, failure rates, queue backlog, delivery latency).

### Consumer Status Monitoring
- `GET /admin/notification-consumer-status`:
  - Returns real-time health, offset positions, consumer lag, and execution metrics of the Kafka consumer group.
  - **Response `200 OK`**:
    ```json
    {
      "data": {
        "group_id": "notification-service",
        "state": "running",
        "topics": [
          {
            "topic": "erp.purchasing.events",
            "partitions": [
              {
                "partition": 0,
                "committed_offset": 10452,
                "end_offset": 10452,
                "lag": 0
              },
              {
                "partition": 1,
                "committed_offset": 8320,
                "end_offset": 8322,
                "lag": 2
              }
            ]
          }
        ],
        "last_message_at": "2026-10-12T09:42:15Z",
        "processed_total": 54200,
        "retried_total": 142,
        "dlq_total": 5,
        "failed_total": 2
      }
    }
    ```

### Email Server Settings
- `GET /admin/email-settings`: Returns current email configuration without revealing credentials. Returns `password_set: true` (or `false`) instead of plaintext password.
- `PUT /admin/email-settings`: Update provider configuration (host, port, username, password, from_email, etc.). Password is write-only.
- `POST /admin/email-settings/test`: Sends a live test email to verify credentials and SMTP connectivity.
  - **Request Body**: `{"to": "admin@example.com"}`

### Email Templates Management
- `GET /email/templates`: List all email templates and active versions.
- `GET /email/templates/{key}`: Retrieve template details, metadata, and version list.
- `PUT /email/templates/{key}`: Create a new draft/version of an email template.
- `POST /email/templates/{key}/preview`: Render preview HTML and plain text with sample variables.
  - **Request Body**: `{"variables": {"name": "Alex"}}`
- `POST /email/templates/{key}/activate`: Promote specified version to active status.
  - **Request Body**: `{"version": 2}`

### Email Messages & Audit Log
- `GET /email/messages`: List outbound email records with filters:
  - `status`: `queued`, `sending`, `sent`, `delivered`, `failed`, `bounced`, `suppressed`, `cancelled`.
  - `template_key`: template identifier.
  - `to`: recipient address.
  - `related_type`: entity type.
  - `related_id`: entity UUID.
  - `from`: RFC 3339 start timestamp.
  - `to_date`: RFC 3339 end timestamp.
- `GET /email/messages/{id}`: Detailed message view including delivery attempts, bounce codes, and redacted payload variables.
- `POST /email/messages/{id}/resend`: Re-queue email message. Valid only if status is `failed` or `bounced`.
- `POST /email/messages/{id}/cancel`: Cancel delivery. Valid only if status is `queued`.

### Email Suppressions List
- `GET /email/suppressions`: List all suppressed recipient email addresses.
- `POST /email/suppressions`: Manually add an email address to the suppression list.
- `DELETE /email/suppressions`: Remove an email address from the suppression list.

---

## 3. Internal Endpoints (`X-Internal-Token`)

Secured via header `X-Internal-Token: <token>`. Blocked at external gateway; only accessible across internal network.

### `POST /internal/events` (DEV AND TEST ONLY)
Direct HTTP ingestion route for domain events, enabled only when `INTERNAL_EVENTS_ENABLED=true` (default `true` in development, `false` in production). Calls the exact same Ingestor function as the Kafka consumer.

- **Request Body**: Canonical Event Envelope (see `CONTEXT.md` Rule 6).
- **Response Codes**:
  - `202 Accepted`: Event ingested or duplicate recognized and safely acknowledged.
  - `400 Bad Request` (`validation_failed`): Malformed event envelope or missing mandatory attributes.

---

### `POST /internal/email/send`
Queues a high-priority or transactional email for dispatch.

- **Headers**:
  - `X-Internal-Token`: required.
  - `Idempotency-Key`: required (`uuid` or unique string).
- **Request Body**:
```json
{
  "template_key": "auth.verify_email_otp",
  "to": ["user@example.com"],
  "cc": ["manager@example.com"],
  "variables": {
    "name": "Alex Smith",
    "otp_code": "847192"
  },
  "sensitive_variables": ["otp_code"],
  "related": {
    "type": "user",
    "id": "e81d8590-bfae-4d8b-9d41-e125032b4943"
  },
  "priority": "transactional",
  "expires_at": "2026-10-12T09:45:00Z",
  "attachments": []
}
```
*(Notes: `priority` is `"transactional"` or `"business"`. `expires_at` and `attachments` are optional.)*

- **Response `202 Accepted`**:
```json
{
  "data": {
    "message_id": "bfd616dc-803a-4ea9-bfd4-1e7123924f92",
    "status": "queued"
  }
}
```

---

## 4. Webhook Endpoints (Signature Checked)

Publicly accessible for email service provider callbacks. No JWT token; request authenticity verified via provider cryptographic signature header.

### `POST /webhooks/email/{provider}`
Handles bounce, delivery, and complaint webhooks from downstream mail providers (e.g., SendGrid, AWS SES, Mailgun).

- **URL Parameter**:
  - `{provider}`: Mail service identifier (e.g., `sendgrid`, `ses`).
- **Response**: `200 OK` on valid signature and processed callback.

---

## 5. Health Endpoints

Unauthenticated probes for container health checks, load balancers, and orchestrators.

- `GET /health/live`: Returns `200 OK` `{"status":"alive"}` when the application process is running.
- `GET /health/ready`: Returns `200 OK` when database is reachable. Response body also reports Kafka connection state (`connected`, `disconnected`, or `disabled`):
  ```json
  {
    "status": "ready",
    "database": "connected",
    "kafka": "connected"
  }
  ```
  *(Returns `503 Service Unavailable` if database is unreachable).*
- `GET /health/version`: Returns `200 OK` `{"version":"0.2.0","commit":"...","build_time":"..."}`.

---

## 6. Email Delivery & Lifecycle Rules

### Email Message Statuses
- `queued`: Message registered and waiting for delivery worker.
- `sending`: Message actively transmitted to provider/SMTP server.
- `sent`: Successfully accepted by provider/SMTP relay.
- `delivered`: Confirmed delivery reported via provider webhook.
- `failed`: Retries exhausted or unrecoverable error.
- `bounced`: Permanent or transient bounce reported by provider.
- `suppressed`: Recipient is on the suppression list; not sent.
- `cancelled`: Cancelled by administrator while in `queued` state.

### Retry Schedule (Standard / Business)
- Attempt 1: 1 minute
- Attempt 2: 5 minutes
- Attempt 3: 15 minutes
- Attempt 4: 1 hour
- Attempt 5: 6 hours
- After final retry attempt fails, status transitions to `failed`.

### OTP & Transactional Email Constraints
- Maximum attempts: at most **3** attempts.
- **Expiry enforcement**: Will **never** attempt delivery after `expires_at`.

---

## 7. Recognized Email Template Keys
1. `auth.verify_email_otp`
2. `auth.login_otp`
3. `auth.password_reset_otp`
4. `auth.invitation`
5. `auth.password_changed`
6. `auth.new_login_alert`
7. `auth.email_change_otp`
8. `auth.step_up_otp`
9. `approval.requested`
10. `approval.decided`
11. `purchase_order.send_to_supplier`
12. `quotation.send`
13. `invoice.send`
14. `payment.receipt`
15. `invoice.overdue_reminder`
16. `delivery.dispatched`
17. `delivery.handover_otp`
18. `stock.low_digest`
