package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

type deliveryRepo struct {
	pool *pgxpool.Pool
}

func NewDeliveryRepository(pool *pgxpool.Pool) DeliveryRepository {
	return &deliveryRepo{pool: pool}
}

func (r *deliveryRepo) Create(ctx context.Context, delivery *domain.NotificationDelivery) error {
	db := getDB(ctx, r.pool)

	if delivery.ID == uuid.Nil {
		delivery.ID = uuid.New()
	}
	if delivery.Status == "" {
		delivery.Status = domain.DeliveryStatusPending
	}
	if delivery.CreatedAt.IsZero() {
		delivery.CreatedAt = time.Now().UTC()
	}
	if delivery.UpdatedAt.IsZero() {
		delivery.UpdatedAt = delivery.CreatedAt
	}

	const query = `
		INSERT INTO notification_deliveries (
			id, notification_id, channel, status, attempts, next_retry_at,
			last_error, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
	`

	_, err := db.Exec(ctx, query,
		delivery.ID, delivery.NotificationID, string(delivery.Channel), string(delivery.Status),
		delivery.Attempts, delivery.NextRetryAt, delivery.LastError, delivery.CreatedAt, delivery.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed inserting delivery record: %w", err)
	}

	return nil
}

func (r *deliveryRepo) ClaimDue(ctx context.Context, limit int) ([]*domain.NotificationDelivery, error) {
	if limit <= 0 {
		limit = 50
	}
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, notification_id, channel, status, attempts, next_retry_at,
		       last_error, created_at, updated_at
		FROM notification_deliveries
		WHERE status = 'pending'
		  AND (next_retry_at IS NULL OR next_retry_at <= now())
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`

	rows, err := db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed claiming due deliveries: %w", err)
	}
	defer rows.Close()

	var deliveries []*domain.NotificationDelivery
	for rows.Next() {
		var d domain.NotificationDelivery
		var chStr, stStr string

		if err := rows.Scan(
			&d.ID, &d.NotificationID, &chStr, &stStr, &d.Attempts,
			&d.NextRetryAt, &d.LastError, &d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning delivery row: %w", err)
		}

		d.Channel = domain.Channel(chStr)
		d.Status = domain.DeliveryStatus(stStr)
		deliveries = append(deliveries, &d)
	}

	return deliveries, rows.Err()
}

func (r *deliveryRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.DeliveryStatus, attempts int, nextRetryAt *time.Time, lastError *string) error {
	db := getDB(ctx, r.pool)

	const query = `
		UPDATE notification_deliveries
		SET status = $1,
		    attempts = $2,
		    next_retry_at = $3,
		    last_error = $4,
		    updated_at = now()
		WHERE id = $5;
	`

	_, err := db.Exec(ctx, query, string(status), attempts, nextRetryAt, lastError, id)
	if err != nil {
		return fmt.Errorf("failed updating delivery status: %w", err)
	}

	return nil
}

func (r *deliveryRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.NotificationDelivery, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, notification_id, channel, status, attempts, next_retry_at,
		       last_error, created_at, updated_at
		FROM notification_deliveries
		WHERE id = $1;
	`

	var d domain.NotificationDelivery
	var chStr, stStr string

	err := db.QueryRow(ctx, query, id).Scan(
		&d.ID, &d.NotificationID, &chStr, &stStr, &d.Attempts,
		&d.NextRetryAt, &d.LastError, &d.CreatedAt, &d.UpdatedAt,
	)
	if err != nil {
		return nil, domain.NewNotFoundError("delivery not found")
	}

	d.Channel = domain.Channel(chStr)
	d.Status = domain.DeliveryStatus(stStr)
	return &d, nil
}

func (r *deliveryRepo) List(ctx context.Context, filter domain.DeliveryFilter) ([]*domain.NotificationDelivery, int, error) {
	db := getDB(ctx, r.pool)

	page := filter.Page
	if page < 1 {
		page = 1
	}
	pageSize := filter.PageSize
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	offset := (page - 1) * pageSize

	baseQuery := `FROM notification_deliveries WHERE 1=1`
	var args []any
	argIdx := 1

	if filter.Status != nil {
		baseQuery += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, string(*filter.Status))
		argIdx++
	}
	if filter.Channel != nil {
		baseQuery += fmt.Sprintf(" AND channel = $%d", argIdx)
		args = append(args, string(*filter.Channel))
		argIdx++
	}
	if filter.From != nil {
		baseQuery += fmt.Sprintf(" AND created_at >= $%d", argIdx)
		args = append(args, *filter.From)
		argIdx++
	}
	if filter.To != nil {
		baseQuery += fmt.Sprintf(" AND created_at <= $%d", argIdx)
		args = append(args, *filter.To)
		argIdx++
	}

	countQuery := "SELECT COUNT(*) " + baseQuery
	var totalCount int
	if err := db.QueryRow(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, fmt.Errorf("failed counting notification deliveries: %w", err)
	}

	dataQuery := fmt.Sprintf(`
		SELECT id, notification_id, channel, status, attempts, next_retry_at,
		       last_error, created_at, updated_at
		%s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d;
	`, baseQuery, argIdx, argIdx+1)

	args = append(args, pageSize, offset)
	rows, err := db.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed querying notification deliveries: %w", err)
	}
	defer rows.Close()

	var list []*domain.NotificationDelivery
	for rows.Next() {
		var d domain.NotificationDelivery
		var chStr, stStr string

		if err := rows.Scan(
			&d.ID, &d.NotificationID, &chStr, &stStr, &d.Attempts,
			&d.NextRetryAt, &d.LastError, &d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("failed scanning notification delivery: %w", err)
		}

		d.Channel = domain.Channel(chStr)
		d.Status = domain.DeliveryStatus(stStr)
		list = append(list, &d)
	}

	return list, totalCount, rows.Err()
}
