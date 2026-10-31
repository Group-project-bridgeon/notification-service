-- 000004_create_notifications.up.sql
-- In-app notifications table with recipient scoping and status tracking

CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipient_id UUID NOT NULL,
    event_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    priority VARCHAR(20) NOT NULL DEFAULT 'normal' CHECK (priority IN ('low', 'normal', 'high', 'critical')),
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    link TEXT NULL,
    entity_type VARCHAR(100) NULL,
    entity_id UUID NULL,
    correlation_id UUID NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'unread' CHECK (status IN ('unread', 'read', 'archived')),
    read_at TIMESTAMPTZ NULL,
    is_test BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_notifications_recipient_event UNIQUE (recipient_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_notifications_recipient_status_created ON notifications (recipient_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_recipient_category_created ON notifications (recipient_id, category, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_event_id ON notifications (event_id);
