-- 000013_create_idempotency_keys.up.sql
-- Idempotency key tracking for non-idempotent endpoints (e.g. POST /internal/email/send)

CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key VARCHAR(255) NOT NULL,
    scope VARCHAR(100) NOT NULL DEFAULT 'email_send',
    request_hash VARCHAR(64) NOT NULL,
    response JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_idempotency_keys_key_scope UNIQUE (key, scope)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_created_at ON idempotency_keys (created_at);
