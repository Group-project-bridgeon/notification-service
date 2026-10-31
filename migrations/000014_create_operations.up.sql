-- 000014_create_operations.up.sql
-- Operations projection table tracking asynchronous distributed actions

CREATE TABLE IF NOT EXISTS operations (
    id UUID PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    entity_type VARCHAR(100) NOT NULL,
    entity_id UUID NOT NULL,
    actor_user_id UUID NULL,
    state VARCHAR(50) NOT NULL CHECK (state IN ('pending', 'succeeded', 'failed', 'compensating', 'compensated')),
    error JSONB NULL,
    compensation TEXT NULL,
    correlation_id UUID NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_event_occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operations_entity ON operations (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_operations_actor_updated ON operations (actor_user_id, updated_at DESC);
