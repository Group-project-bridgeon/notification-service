-- 000002_create_notification_templates.up.sql
-- In-app notification copy templates with versioning and localization

CREATE TABLE IF NOT EXISTS notification_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_key VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    locale VARCHAR(10) NOT NULL DEFAULT 'en',
    title_template TEXT NOT NULL,
    body_template TEXT NOT NULL,
    version INT NOT NULL DEFAULT 1 CHECK (version > 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_notification_templates_key_locale_version UNIQUE (template_key, locale, version)
);

CREATE INDEX IF NOT EXISTS idx_notification_templates_key_locale ON notification_templates (template_key, locale, is_active);
CREATE INDEX IF NOT EXISTS idx_notification_templates_event_type ON notification_templates (event_type, is_active);
