-- 000012_create_notification_dead_letters.up.sql
-- Dead Letter Queue storage for poisoned or unroutable notification events

CREATE TABLE IF NOT EXISTS notification_dead_letters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source VARCHAR(20) NOT NULL CHECK (source IN ('kafka', 'http')),
    topic VARCHAR(255) NULL,
    partition INT NULL,
    kafka_offset BIGINT NULL,
    raw_event JSONB NULL,
    raw_value BYTEA NULL,
    error TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_notification_dead_letters_failed_at ON notification_dead_letters (failed_at DESC);
CREATE INDEX IF NOT EXISTS idx_notification_dead_letters_replayed ON notification_dead_letters (replayed_at);
