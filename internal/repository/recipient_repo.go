package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

type recipientRepo struct {
	pool *pgxpool.Pool
}

func NewRecipientRepository(pool *pgxpool.Pool) RecipientRepository {
	return &recipientRepo{pool: pool}
}

func (r *recipientRepo) UpsertGuarded(ctx context.Context, recipient *domain.Recipient) error {
	db := getDB(ctx, r.pool)

	if recipient.UpdatedAt.IsZero() {
		recipient.UpdatedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO notification_recipients (
			user_id, email, display_name, roles, is_active, source_occurred_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (user_id) DO UPDATE SET
			email = EXCLUDED.email,
			display_name = EXCLUDED.display_name,
			roles = EXCLUDED.roles,
			is_active = EXCLUDED.is_active,
			source_occurred_at = EXCLUDED.source_occurred_at,
			updated_at = now()
		WHERE EXCLUDED.source_occurred_at >= notification_recipients.source_occurred_at;
	`

	_, err := db.Exec(ctx, query,
		recipient.UserID, recipient.Email, recipient.DisplayName, recipient.Roles,
		recipient.IsActive, recipient.SourceOccurredAt, recipient.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed executing guarded recipient upsert: %w", err)
	}

	return nil
}

func (r *recipientRepo) FindByRoles(ctx context.Context, roles []string) ([]*domain.Recipient, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT user_id, email, display_name, roles, is_active, source_occurred_at, updated_at
		FROM notification_recipients
		WHERE roles && $1 AND is_active = true;
	`

	rows, err := db.Query(ctx, query, roles)
	if err != nil {
		return nil, fmt.Errorf("failed finding recipients by roles: %w", err)
	}
	defer rows.Close()

	var recipients []*domain.Recipient
	for rows.Next() {
		var rec domain.Recipient
		if err := rows.Scan(
			&rec.UserID, &rec.Email, &rec.DisplayName, &rec.Roles,
			&rec.IsActive, &rec.SourceOccurredAt, &rec.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning recipient row: %w", err)
		}
		recipients = append(recipients, &rec)
	}

	return recipients, rows.Err()
}

func (r *recipientRepo) FindByID(ctx context.Context, id uuid.UUID) (*domain.Recipient, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT user_id, email, display_name, roles, is_active, source_occurred_at, updated_at
		FROM notification_recipients
		WHERE user_id = $1;
	`

	var rec domain.Recipient
	err := db.QueryRow(ctx, query, id).Scan(
		&rec.UserID, &rec.Email, &rec.DisplayName, &rec.Roles,
		&rec.IsActive, &rec.SourceOccurredAt, &rec.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("recipient not found")
		}
		return nil, fmt.Errorf("failed finding recipient by id: %w", err)
	}

	return &rec, nil
}
