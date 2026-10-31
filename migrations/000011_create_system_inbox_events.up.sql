-- 000011_create_system_inbox_events.up.sql
-- Transactional Inbox deduplication table tracking incoming domain events

CREATE TABLE IF NOT EXISTS system_inbox_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'received' CHECK (status IN ('received', 'processed', 'ignored', 'failed')),
    source VARCHAR(20) NOT NULL CHECK (source IN ('kafka', 'http')),
    topic VARCHAR(255) NULL,
    partition INT NULL,
    kafka_offset BIGINT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ NULL,
    error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_system_inbox_events_status_received ON system_inbox_events (status, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_inbox_events_type ON system_inbox_events (event_type);
