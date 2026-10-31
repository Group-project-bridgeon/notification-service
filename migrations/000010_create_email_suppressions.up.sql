-- 000010_create_email_suppressions.up.sql
-- Suppression list for hard bounces, spam complaints, and administrative exclusions

CREATE TABLE IF NOT EXISTS email_suppressions (
    email VARCHAR(255) PRIMARY KEY,
    reason VARCHAR(20) NOT NULL CHECK (reason IN ('hard_bounce', 'complaint', 'manual')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by UUID NULL
);
