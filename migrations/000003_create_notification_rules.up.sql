-- 000003_create_notification_rules.up.sql
-- Routing rules mapping incoming domain event types to roles, categories, and channels

CREATE TABLE IF NOT EXISTS notification_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    target_roles TEXT[] NOT NULL DEFAULT '{}',
    target_user_path TEXT NULL,
    type VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    priority VARCHAR(20) NOT NULL DEFAULT 'normal' CHECK (priority IN ('low', 'normal', 'high', 'critical')),
    link_template TEXT NULL,
    template_key VARCHAR(100) NOT NULL,
    send_email BOOLEAN NOT NULL DEFAULT FALSE,
    email_template_key VARCHAR(100) NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_rules_event_active ON notification_rules (event_type, is_active);
