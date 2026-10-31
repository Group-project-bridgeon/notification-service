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

type suppressionRepo struct {
	pool *pgxpool.Pool
}

func NewSuppressionRepository(pool *pgxpool.Pool) SuppressionRepository {
	return &suppressionRepo{pool: pool}
}

func (r *suppressionRepo) List(ctx context.Context) ([]*domain.EmailSuppression, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT email, reason, created_at, created_by
		FROM email_suppressions
		ORDER BY created_at DESC
		LIMIT 500;
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed querying email suppressions: %w", err)
	}
	defer rows.Close()

	var suppressions []*domain.EmailSuppression
	for rows.Next() {
		var s domain.EmailSuppression
		var reasonStr string

		if err := rows.Scan(&s.Email, &reasonStr, &s.CreatedAt, &s.CreatedBy); err != nil {
			return nil, fmt.Errorf("failed scanning email suppression row: %w", err)
		}

		s.Reason = domain.EmailSuppressionReason(reasonStr)
		suppressions = append(suppressions, &s)
	}

	return suppressions, rows.Err()
}

func (r *suppressionRepo) GetByEmail(ctx context.Context, email string) (*domain.EmailSuppression, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT email, reason, created_at, created_by
		FROM email_suppressions
		WHERE email = $1;
	`

	var s domain.EmailSuppression
	var reasonStr string

	err := db.QueryRow(ctx, query, email).Scan(&s.Email, &reasonStr, &s.CreatedAt, &s.CreatedBy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("email suppression not found")
		}
		return nil, fmt.Errorf("failed querying email suppression by email: %w", err)
	}

	s.Reason = domain.EmailSuppressionReason(reasonStr)
	return &s, nil
}

func (r *suppressionRepo) Add(ctx context.Context, supp *domain.EmailSuppression) error {
	db := getDB(ctx, r.pool)

	if supp.Reason == "" {
		supp.Reason = domain.EmailSuppressionReasonManual
	}
	if supp.CreatedAt.IsZero() {
		supp.CreatedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO email_suppressions (email, reason, created_at, created_by)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (email) DO UPDATE
		SET reason = EXCLUDED.reason,
		    created_at = EXCLUDED.created_at,
		    created_by = EXCLUDED.created_by;
	`

	_, err := db.Exec(ctx, query, supp.Email, string(supp.Reason), supp.CreatedAt, supp.CreatedBy)
	if err != nil {
		return fmt.Errorf("failed adding email suppression: %w", err)
	}

	return nil
}

func (r *suppressionRepo) Delete(ctx context.Context, email string) error {
	db := getDB(ctx, r.pool)

	const query = `
		DELETE FROM email_suppressions
		WHERE email = $1;
	`

	res, err := db.Exec(ctx, query, email)
	if err != nil {
		return fmt.Errorf("failed deleting email suppression: %w", err)
	}

	if res.RowsAffected() == 0 {
		return domain.NewNotFoundError("email suppression not found")
	}

	return nil
}
