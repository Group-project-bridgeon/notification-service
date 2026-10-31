-- 000006_create_notification_preferences.up.sql
-- User-specific channel delivery preferences per notification category

CREATE TABLE IF NOT EXISTS notification_preferences (
    user_id UUID NOT NULL,
    category VARCHAR(50) NOT NULL,
    channel VARCHAR(20) NOT NULL CHECK (channel IN ('in_app', 'email')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, category, channel)
);
