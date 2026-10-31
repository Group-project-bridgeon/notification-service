-- 000008_create_email_templates.up.sql
-- Versioned outbound email templates with variable definitions

CREATE TABLE IF NOT EXISTS email_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_key VARCHAR(100) NOT NULL,
    version INT NOT NULL DEFAULT 1 CHECK (version > 0),
    subject_template TEXT NOT NULL,
    html_template TEXT NOT NULL,
    text_template TEXT NOT NULL,
    declared_variables TEXT[] NOT NULL DEFAULT '{}',
    sensitive_variables TEXT[] NOT NULL DEFAULT '{}',
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created_by UUID NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_email_templates_key_version UNIQUE (template_key, version)
);

-- Ensure only one version can be active at a time for any given template_key
CREATE UNIQUE INDEX IF NOT EXISTS uq_email_templates_active_key ON email_templates (template_key) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_email_templates_key ON email_templates (template_key);
