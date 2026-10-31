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

type deadLetterRepo struct {
	pool *pgxpool.Pool
}

func NewDeadLetterRepository(pool *pgxpool.Pool) DeadLetterRepository {
	return &deadLetterRepo{pool: pool}
}

func (r *deadLetterRepo) Create(ctx context.Context, dl *domain.DeadLetter) error {
	db := getDB(ctx, r.pool)

	if dl.ID == uuid.Nil {
		dl.ID = uuid.New()
	}
	if dl.FailedAt.IsZero() {
		dl.FailedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO notification_dead_letters (
			id, source, topic, partition, kafka_offset, raw_event, raw_value,
			error, failed_at, replayed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
	`

	_, err := db.Exec(ctx, query,
		dl.ID, string(dl.Source), dl.Topic, dl.Partition, dl.KafkaOffset,
		dl.RawEvent, dl.RawValue, dl.Error, dl.FailedAt, dl.ReplayedAt,
	)
	if err != nil {
		return fmt.Errorf("failed creating dead letter record: %w", err)
	}

	return nil
}

func (r *deadLetterRepo) List(ctx context.Context, limit, offset int) ([]*domain.DeadLetter, int, error) {
	if limit <= 0 {
		limit = 50
	}
	db := getDB(ctx, r.pool)

	const countQuery = `SELECT COUNT(*) FROM notification_dead_letters;`
	var totalCount int
	if err := db.QueryRow(ctx, countQuery).Scan(&totalCount); err != nil {
		return nil, 0, fmt.Errorf("failed counting dead letters: %w", err)
	}

	const query = `
		SELECT id, source, topic, partition, kafka_offset, raw_event, raw_value,
		       error, failed_at, replayed_at
		FROM notification_dead_letters
		ORDER BY failed_at DESC
		LIMIT $1 OFFSET $2;
	`

	rows, err := db.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed listing dead letters: %w", err)
	}
	defer rows.Close()

	var list []*domain.DeadLetter
	for rows.Next() {
		var dl domain.DeadLetter
		var srcStr string

		if err := rows.Scan(
			&dl.ID, &srcStr, &dl.Topic, &dl.Partition, &dl.KafkaOffset,
			&dl.RawEvent, &dl.RawValue, &dl.Error, &dl.FailedAt, &dl.ReplayedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("failed scanning dead letter row: %w", err)
		}

		dl.Source = domain.EventSource(srcStr)
		list = append(list, &dl)
	}

	return list, totalCount, rows.Err()
}

func (r *deadLetterRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.DeadLetter, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, source, topic, partition, kafka_offset, raw_event, raw_value,
		       error, failed_at, replayed_at
		FROM notification_dead_letters
		WHERE id = $1;
	`

	var dl domain.DeadLetter
	var srcStr string

	err := db.QueryRow(ctx, query, id).Scan(
		&dl.ID, &srcStr, &dl.Topic, &dl.Partition, &dl.KafkaOffset,
		&dl.RawEvent, &dl.RawValue, &dl.Error, &dl.FailedAt, &dl.ReplayedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("dead letter not found")
		}
		return nil, fmt.Errorf("failed querying dead letter by id: %w", err)
	}

	dl.Source = domain.EventSource(srcStr)
	return &dl, nil
}

func (r *deadLetterRepo) MarkReplayed(ctx context.Context, id uuid.UUID) error {
	db := getDB(ctx, r.pool)

	const query = `UPDATE notification_dead_letters SET replayed_at = now() WHERE id = $1;`

	ct, err := db.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed marking dead letter as replayed: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return domain.NewNotFoundError("dead letter not found")
	}

	return nil
}
