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

var notificationSortWhitelist = map[string]string{
	"created_at": "created_at",
	"priority":   "priority",
	"status":     "status",
	"category":   "category",
	"type":       "type",
}

type notificationRepo struct {
	pool *pgxpool.Pool
}

func NewNotificationRepository(pool *pgxpool.Pool) NotificationRepository {
	return &notificationRepo{pool: pool}
}

func (r *notificationRepo) BatchInsert(ctx context.Context, notifications []*domain.Notification) error {
	if len(notifications) == 0 {
		return nil
	}

	db := getDB(ctx, r.pool)
	batch := &pgx.Batch{}

	const query = `
		INSERT INTO notifications (
			id, recipient_id, event_id, event_type, type, category, priority,
			title, body, link, entity_type, entity_id, correlation_id, status,
			read_at, is_test, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		) ON CONFLICT (recipient_id, event_id) DO NOTHING;
	`

	for _, n := range notifications {
		if n.ID == uuid.Nil {
			n.ID = uuid.New()
		}
		if n.Status == "" {
			n.Status = domain.NotificationStatusUnread
		}
		if n.CreatedAt.IsZero() {
			n.CreatedAt = time.Now().UTC()
		}

		batch.Queue(query,
			n.ID, n.RecipientID, n.EventID, n.EventType, string(n.Type), string(n.Category), string(n.Priority),
			n.Title, n.Body, n.Link, n.EntityType, n.EntityID, n.CorrelationID, string(n.Status),
			n.ReadAt, n.IsTest, n.CreatedAt,
		)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for range notifications {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed executing notification batch insert: %w", err)
		}
	}

	return nil
}

func (r *notificationRepo) List(ctx context.Context, filter domain.NotificationFilter) ([]*domain.Notification, int, error) {
	db := getDB(ctx, r.pool)

	whereClauses := []string{"recipient_id = $1"}
	args := []any{filter.RecipientID}
	argIdx := 2

	if filter.Status != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, string(*filter.Status))
		argIdx++
	}
	if filter.Category != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("category = $%d", argIdx))
		args = append(args, string(*filter.Category))
		argIdx++
	}
	if filter.Priority != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("priority = $%d", argIdx))
		args = append(args, string(*filter.Priority))
		argIdx++
	}
	if filter.Type != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("type = $%d", argIdx))
		args = append(args, string(*filter.Type))
		argIdx++
	}
	if filter.From != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("created_at >= $%d", argIdx))
		args = append(args, *filter.From)
		argIdx++
	}
	if filter.To != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("created_at <= $%d", argIdx))
		args = append(args, *filter.To)
		argIdx++
	}

	whereSQL := "WHERE " + stringsJoin(whereClauses, " AND ")

	// 1. Get total count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM notifications %s", whereSQL)
	var totalCount int
	if err := db.QueryRow(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, fmt.Errorf("failed querying notification count: %w", err)
	}

	// 2. Fetch paginated records
	orderSQL := BuildOrderBy(filter.Sort, notificationSortWhitelist, "ORDER BY created_at DESC")
	page, pageSize := NormalizePagination(filter.Page, filter.PageSize)
	offset := CalculateOffset(page, pageSize)

	listQuery := fmt.Sprintf(`
		SELECT id, recipient_id, event_id, event_type, type, category, priority,
		       title, body, link, entity_type, entity_id, correlation_id, status,
		       read_at, is_test, created_at
		FROM notifications
		%s
		%s
		LIMIT $%d OFFSET $%d;
	`, whereSQL, orderSQL, argIdx, argIdx+1)

	args = append(args, pageSize, offset)

	rows, err := db.Query(ctx, listQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed querying notifications list: %w", err)
	}
	defer rows.Close()

	var notifications []*domain.Notification
	for rows.Next() {
		var n domain.Notification
		var typeStr, catStr, prioStr, statusStr string

		if err := rows.Scan(
			&n.ID, &n.RecipientID, &n.EventID, &n.EventType, &typeStr, &catStr, &prioStr,
			&n.Title, &n.Body, &n.Link, &n.EntityType, &n.EntityID, &n.CorrelationID, &statusStr,
			&n.ReadAt, &n.IsTest, &n.CreatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("failed scanning notification row: %w", err)
		}

		n.Type = domain.NotificationType(typeStr)
		n.Category = domain.NotificationCategory(catStr)
		n.Priority = domain.Priority(prioStr)
		n.Status = domain.NotificationStatus(statusStr)

		notifications = append(notifications, &n)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("row iteration error on notifications: %w", err)
	}

	return notifications, totalCount, nil
}

func (r *notificationRepo) GetByID(ctx context.Context, recipientID, id uuid.UUID) (*domain.Notification, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, recipient_id, event_id, event_type, type, category, priority,
		       title, body, link, entity_type, entity_id, correlation_id, status,
		       read_at, is_test, created_at
		FROM notifications
		WHERE id = $1 AND recipient_id = $2;
	`

	var n domain.Notification
	var typeStr, catStr, prioStr, statusStr string

	err := db.QueryRow(ctx, query, id, recipientID).Scan(
		&n.ID, &n.RecipientID, &n.EventID, &n.EventType, &typeStr, &catStr, &prioStr,
		&n.Title, &n.Body, &n.Link, &n.EntityType, &n.EntityID, &n.CorrelationID, &statusStr,
		&n.ReadAt, &n.IsTest, &n.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("notification not found")
		}
		return nil, fmt.Errorf("failed querying notification by id: %w", err)
	}

	n.Type = domain.NotificationType(typeStr)
	n.Category = domain.NotificationCategory(catStr)
	n.Priority = domain.Priority(prioStr)
	n.Status = domain.NotificationStatus(statusStr)

	return &n, nil
}

func (r *notificationRepo) SetStatus(ctx context.Context, recipientID, id uuid.UUID, status domain.NotificationStatus) (*domain.Notification, error) {
	db := getDB(ctx, r.pool)

	const query = `
		UPDATE notifications
		SET status = $1,
		    read_at = CASE
		        WHEN $1 = 'read' THEN COALESCE(read_at, now())
		        WHEN $1 = 'unread' THEN NULL
		        ELSE read_at
		    END
		WHERE id = $2 AND recipient_id = $3
		RETURNING id, recipient_id, event_id, event_type, type, category, priority,
		          title, body, link, entity_type, entity_id, correlation_id, status,
		          read_at, is_test, created_at;
	`

	var n domain.Notification
	var typeStr, catStr, prioStr, statusStr string

	err := db.QueryRow(ctx, query, string(status), id, recipientID).Scan(
		&n.ID, &n.RecipientID, &n.EventID, &n.EventType, &typeStr, &catStr, &prioStr,
		&n.Title, &n.Body, &n.Link, &n.EntityType, &n.EntityID, &n.CorrelationID, &statusStr,
		&n.ReadAt, &n.IsTest, &n.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("notification not found")
		}
		return nil, fmt.Errorf("failed updating notification status: %w", err)
	}

	n.Type = domain.NotificationType(typeStr)
	n.Category = domain.NotificationCategory(catStr)
	n.Priority = domain.Priority(prioStr)
	n.Status = domain.NotificationStatus(statusStr)

	return &n, nil
}

func (r *notificationRepo) ReadAll(ctx context.Context, recipientID uuid.UUID, category *domain.NotificationCategory) (int64, error) {
	db := getDB(ctx, r.pool)

	var query string
	var args []any

	if category != nil {
		query = `
			UPDATE notifications
			SET status = 'read', read_at = COALESCE(read_at, now())
			WHERE recipient_id = $1 AND status = 'unread' AND category = $2;
		`
		args = []any{recipientID, string(*category)}
	} else {
		query = `
			UPDATE notifications
			SET status = 'read', read_at = COALESCE(read_at, now())
			WHERE recipient_id = $1 AND status = 'unread';
		`
		args = []any{recipientID}
	}

	ct, err := db.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed reading all notifications: %w", err)
	}

	return ct.RowsAffected(), nil
}

func (r *notificationRepo) GetUnreadCounts(ctx context.Context, recipientID uuid.UUID) (int, map[string]int, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT category, COUNT(*)
		FROM notifications
		WHERE recipient_id = $1 AND status = 'unread'
		GROUP BY category;
	`

	rows, err := db.Query(ctx, query, recipientID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed querying unread counts: %w", err)
	}
	defer rows.Close()

	byCategory := make(map[string]int)
	total := 0

	for rows.Next() {
		var category string
		var count int
		if err := rows.Scan(&category, &count); err != nil {
			return 0, nil, fmt.Errorf("failed scanning unread count row: %w", err)
		}
		byCategory[category] = count
		total += count
	}

	return total, byCategory, rows.Err()
}

func stringsJoin(elements []string, sep string) string {
	if len(elements) == 0 {
		return ""
	}
	res := elements[0]
	for _, el := range elements[1:] {
		res += sep + el
	}
	return res
}
