package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

type emailSettingsRepo struct {
	pool *pgxpool.Pool
}

func NewEmailSettingsRepository(pool *pgxpool.Pool) EmailSettingsRepository {
	return &emailSettingsRepo{pool: pool}
}

func (r *emailSettingsRepo) Get(ctx context.Context) (*domain.EmailSettings, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, provider, host, port, username, password_encrypted,
		       from_name, from_address, reply_to, tls_mode, daily_limit,
		       enabled, updated_by, updated_at
		FROM email_settings
		WHERE id = 1;
	`

	var s domain.EmailSettings
	var tlsStr string

	err := db.QueryRow(ctx, query).Scan(
		&s.ID, &s.Provider, &s.Host, &s.Port, &s.Username, &s.PasswordEncrypted,
		&s.FromName, &s.FromAddress, &s.ReplyTo, &tlsStr, &s.DailyLimit,
		&s.Enabled, &s.UpdatedBy, &s.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("email settings not configured")
		}
		return nil, fmt.Errorf("failed querying email settings: %w", err)
	}

	s.TLSMode = domain.TLSMode(tlsStr)
	return &s, nil
}

func (r *emailSettingsRepo) Upsert(ctx context.Context, s *domain.EmailSettings) error {
	db := getDB(ctx, r.pool)

	if s.TLSMode == "" {
		s.TLSMode = domain.TLSModeStartTLS
	}
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO email_settings (
			id, provider, host, port, username, password_encrypted,
			from_name, from_address, reply_to, tls_mode, daily_limit,
			enabled, updated_by, updated_at
		) VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (id) DO UPDATE SET
			provider = EXCLUDED.provider,
			host = EXCLUDED.host,
			port = EXCLUDED.port,
			username = EXCLUDED.username,
			password_encrypted = CASE WHEN length(EXCLUDED.password_encrypted) > 0 THEN EXCLUDED.password_encrypted ELSE email_settings.password_encrypted END,
			from_name = EXCLUDED.from_name,
			from_address = EXCLUDED.from_address,
			reply_to = EXCLUDED.reply_to,
			tls_mode = EXCLUDED.tls_mode,
			daily_limit = EXCLUDED.daily_limit,
			enabled = EXCLUDED.enabled,
			updated_by = EXCLUDED.updated_by,
			updated_at = now();
	`

	_, err := db.Exec(ctx, query,
		s.Provider, s.Host, s.Port, s.Username, s.PasswordEncrypted,
		s.FromName, s.FromAddress, s.ReplyTo, string(s.TLSMode), s.DailyLimit,
		s.Enabled, s.UpdatedBy, s.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed upserting email settings: %w", err)
	}

	return nil
}
