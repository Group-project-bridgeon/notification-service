-- 000009_create_email_messages.up.sql
-- Outbound email dispatch queue and audit log with sensitive variable masking

CREATE TABLE IF NOT EXISTS email_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_key VARCHAR(100) NOT NULL,
    template_version INT NOT NULL DEFAULT 1 CHECK (template_version > 0),
    to_addresses TEXT[] NOT NULL,
    cc_addresses TEXT[] NOT NULL DEFAULT '{}',
    subject TEXT NOT NULL,
    body_html_redacted TEXT NOT NULL,
    body_text_redacted TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'queued' CHECK (
        status IN ('queued', 'sending', 'sent', 'delivered', 'failed', 'bounced', 'suppressed', 'cancelled')
    ),
    priority VARCHAR(20) NOT NULL DEFAULT 'business' CHECK (
        priority IN ('transactional', 'business')
    ),
    attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NULL,
    expires_at TIMESTAMPTZ NULL,
    provider_message_id VARCHAR(255) NULL,
    related_type VARCHAR(100) NULL,
    related_id UUID NULL,
    resend_of UUID NULL REFERENCES email_messages (id) ON DELETE SET NULL,
    sent_by UUID NULL,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_email_messages_status_next_attempt ON email_messages (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_email_messages_related ON email_messages (related_type, related_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_created_at ON email_messages (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_email_messages_template_key ON email_messages (template_key);
