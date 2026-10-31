package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

type preferenceRepo struct {
	pool *pgxpool.Pool
}

func NewPreferenceRepository(pool *pgxpool.Pool) PreferenceRepository {
	return &preferenceRepo{pool: pool}
}

func (r *preferenceRepo) GetByUser(ctx context.Context, userID uuid.UUID) ([]*domain.NotificationPreference, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT user_id, category, channel, enabled, updated_at
		FROM notification_preferences
		WHERE user_id = $1;
	`

	rows, err := db.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed querying notification preferences: %w", err)
	}
	defer rows.Close()

	var preferences []*domain.NotificationPreference
	for rows.Next() {
		var p domain.NotificationPreference
		var catStr, chStr string
		if err := rows.Scan(&p.UserID, &catStr, &chStr, &p.Enabled, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed scanning preference row: %w", err)
		}
		p.Category = domain.NotificationCategory(catStr)
		p.Channel = domain.Channel(chStr)
		preferences = append(preferences, &p)
	}

	return preferences, rows.Err()
}

func (r *preferenceRepo) Upsert(ctx context.Context, pref *domain.NotificationPreference) error {
	db := getDB(ctx, r.pool)

	if pref.UpdatedAt.IsZero() {
		pref.UpdatedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO notification_preferences (user_id, category, channel, enabled, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (user_id, category, channel) DO UPDATE
		SET enabled = EXCLUDED.enabled, updated_at = now();
	`

	_, err := db.Exec(ctx, query,
		pref.UserID, string(pref.Category), string(pref.Channel), pref.Enabled, pref.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed upserting notification preference: %w", err)
	}

	return nil
}
