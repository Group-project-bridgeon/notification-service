-- 000007_create_email_settings.up.sql
-- Singleton configuration table for email provider / SMTP credentials

CREATE TABLE IF NOT EXISTS email_settings (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    provider VARCHAR(50) NOT NULL DEFAULT 'smtp',
    host VARCHAR(255) NOT NULL DEFAULT 'localhost',
    port INT NOT NULL DEFAULT 1025 CHECK (port > 0 AND port <= 65535),
    username VARCHAR(255) NOT NULL DEFAULT '',
    password_encrypted BYTEA NOT NULL DEFAULT ''::bytea,
    from_name VARCHAR(255) NOT NULL DEFAULT 'Fashion Wholesale ERP',
    from_address VARCHAR(255) NOT NULL DEFAULT 'no-reply@fashionerp.com',
    reply_to VARCHAR(255) NULL,
    tls_mode VARCHAR(20) NOT NULL DEFAULT 'starttls' CHECK (tls_mode IN ('none', 'starttls', 'ssl_tls')),
    daily_limit INT NOT NULL DEFAULT 10000 CHECK (daily_limit >= 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_by UUID NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed singleton initial configuration row
INSERT INTO email_settings (
    id, provider, host, port, username, password_encrypted,
    from_name, from_address, tls_mode, daily_limit, enabled
) VALUES (
    1, 'smtp', 'localhost', 1025, '', ''::bytea,
    'Fashion Wholesale ERP', 'no-reply@fashionerp.com', 'starttls', 10000, TRUE
) ON CONFLICT (id) DO NOTHING;
