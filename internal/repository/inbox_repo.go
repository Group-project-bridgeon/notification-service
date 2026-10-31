package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

type inboxRepo struct {
	pool *pgxpool.Pool
}

func NewInboxRepository(pool *pgxpool.Pool) InboxRepository {
	return &inboxRepo{pool: pool}
}

func (r *inboxRepo) InsertIfAbsent(ctx context.Context, event *domain.InboxEvent) (bool, error) {
	db := getDB(ctx, r.pool)

	if event.Status == "" {
		event.Status = domain.InboxStatusReceived
	}
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO system_inbox_events (
			event_id, event_type, status, source, topic, partition, kafka_offset,
			received_at, processed_at, error
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (event_id) DO NOTHING;
	`

	ct, err := db.Exec(ctx, query,
		event.EventID, event.EventType, string(event.Status), string(event.Source),
		event.Topic, event.Partition, event.KafkaOffset, event.ReceivedAt,
		event.ProcessedAt, event.Error,
	)
	if err != nil {
		return false, fmt.Errorf("failed executing inbox insert-if-absent: %w", err)
	}

	return ct.RowsAffected() > 0, nil
}

func (r *inboxRepo) SetStatus(ctx context.Context, eventID uuid.UUID, status domain.InboxStatus, errStr *string) error {
	db := getDB(ctx, r.pool)

	const query = `
		UPDATE system_inbox_events
		SET status = $1::varchar,
		    processed_at = CASE WHEN $1::varchar = 'processed' THEN now() ELSE processed_at END,
		    error = $2
		WHERE event_id = $3;
	`

	_, err := db.Exec(ctx, query, string(status), errStr, eventID)
	if err != nil {
		return fmt.Errorf("failed updating inbox status: %w", err)
	}

	return nil
}
