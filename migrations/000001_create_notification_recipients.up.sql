-- 000001_create_notification_recipients.up.sql
-- Local projection of user identities populated asynchronously via Auth domain events

CREATE TABLE IF NOT EXISTS notification_recipients (
    user_id UUID PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    roles TEXT[] NOT NULL DEFAULT '{}',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    source_occurred_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_recipients_email ON notification_recipients (email);
CREATE INDEX IF NOT EXISTS idx_notification_recipients_is_active ON notification_recipients (is_active);
