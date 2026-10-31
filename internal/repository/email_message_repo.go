package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

var emailMessageSortWhitelist = map[string]string{
	"created_at": "created_at",
	"status":     "status",
	"priority":   "priority",
	"attempts":   "attempts",
}

type emailMessageRepo struct {
	pool *pgxpool.Pool
}

func NewEmailMessageRepository(pool *pgxpool.Pool) EmailMessageRepository {
	return &emailMessageRepo{pool: pool}
}

func (r *emailMessageRepo) Create(ctx context.Context, msg *domain.EmailMessage) error {
	db := getDB(ctx, r.pool)

	if msg.ID == uuid.Nil {
		msg.ID = uuid.New()
	}
	if msg.TemplateVersion <= 0 {
		msg.TemplateVersion = 1
	}
	if msg.Status == "" {
		msg.Status = domain.EmailMessageStatusQueued
	}
	if msg.Priority == "" {
		msg.Priority = domain.EmailPriorityBusiness
	}
	if msg.CcAddresses == nil {
		msg.CcAddresses = []string{}
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO email_messages (
			id, template_key, template_version, to_addresses, cc_addresses, subject,
			body_html_redacted, body_text_redacted, status, priority, attempts,
			next_attempt_at, expires_at, provider_message_id, related_type, related_id,
			resend_of, sent_by, last_error, created_at, sent_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
		);
	`

	_, err := db.Exec(ctx, query,
		msg.ID, msg.TemplateKey, msg.TemplateVersion, msg.ToAddresses, msg.CcAddresses, msg.Subject,
		msg.BodyHTMLRedacted, msg.BodyTextRedacted, string(msg.Status), string(msg.Priority), msg.Attempts,
		msg.NextAttemptAt, msg.ExpiresAt, msg.ProviderMessageID, msg.RelatedType, msg.RelatedID,
		msg.ResendOf, msg.SentBy, msg.LastError, msg.CreatedAt, msg.SentAt,
	)
	if err != nil {
		return fmt.Errorf("failed inserting email message: %w", err)
	}

	return nil
}

func (r *emailMessageRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.EmailMessage, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, template_key, template_version, to_addresses, cc_addresses, subject,
		       body_html_redacted, body_text_redacted, status, priority, attempts,
		       next_attempt_at, expires_at, provider_message_id, related_type, related_id,
		       resend_of, sent_by, last_error, created_at, sent_at
		FROM email_messages
		WHERE id = $1;
	`

	var msg domain.EmailMessage
	var statusStr, priorityStr string

	err := db.QueryRow(ctx, query, id).Scan(
		&msg.ID, &msg.TemplateKey, &msg.TemplateVersion, &msg.ToAddresses, &msg.CcAddresses, &msg.Subject,
		&msg.BodyHTMLRedacted, &msg.BodyTextRedacted, &statusStr, &priorityStr, &msg.Attempts,
		&msg.NextAttemptAt, &msg.ExpiresAt, &msg.ProviderMessageID, &msg.RelatedType, &msg.RelatedID,
		&msg.ResendOf, &msg.SentBy, &msg.LastError, &msg.CreatedAt, &msg.SentAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("email message not found")
		}
		return nil, fmt.Errorf("failed querying email message by id: %w", err)
	}

	msg.Status = domain.EmailMessageStatus(statusStr)
	msg.Priority = domain.EmailPriority(priorityStr)

	return &msg, nil
}

func (r *emailMessageRepo) GetByProviderMessageID(ctx context.Context, providerMsgID string) (*domain.EmailMessage, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, template_key, template_version, to_addresses, cc_addresses, subject,
		       body_html_redacted, body_text_redacted, status, priority, attempts,
		       next_attempt_at, expires_at, provider_message_id, related_type, related_id,
		       resend_of, sent_by, last_error, created_at, sent_at
		FROM email_messages
		WHERE provider_message_id = $1
		ORDER BY created_at DESC
		LIMIT 1;
	`

	var msg domain.EmailMessage
	var statusStr, priorityStr string

	err := db.QueryRow(ctx, query, providerMsgID).Scan(
		&msg.ID, &msg.TemplateKey, &msg.TemplateVersion, &msg.ToAddresses, &msg.CcAddresses, &msg.Subject,
		&msg.BodyHTMLRedacted, &msg.BodyTextRedacted, &statusStr, &priorityStr, &msg.Attempts,
		&msg.NextAttemptAt, &msg.ExpiresAt, &msg.ProviderMessageID, &msg.RelatedType, &msg.RelatedID,
		&msg.ResendOf, &msg.SentBy, &msg.LastError, &msg.CreatedAt, &msg.SentAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("email message not found for provider_message_id")
		}
		return nil, fmt.Errorf("failed querying email message by provider_message_id: %w", err)
	}

	msg.Status = domain.EmailMessageStatus(statusStr)
	msg.Priority = domain.EmailPriority(priorityStr)

	return &msg, nil
}

func (r *emailMessageRepo) List(ctx context.Context, filter domain.EmailMessageFilter) ([]*domain.EmailMessage, int, error) {
	db := getDB(ctx, r.pool)

	var whereClauses []string
	var args []any
	argIdx := 1

	if filter.Status != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, string(*filter.Status))
		argIdx++
	}
	if filter.TemplateKey != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("template_key = $%d", argIdx))
		args = append(args, *filter.TemplateKey)
		argIdx++
	}
	if filter.ToAddress != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("$%d = ANY(to_addresses)", argIdx))
		args = append(args, *filter.ToAddress)
		argIdx++
	}
	if filter.RelatedType != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("related_type = $%d", argIdx))
		args = append(args, *filter.RelatedType)
		argIdx++
	}
	if filter.RelatedID != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("related_id = $%d", argIdx))
		args = append(args, *filter.RelatedID)
		argIdx++
	}
	if filter.From != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("created_at >= $%d", argIdx))
		args = append(args, *filter.From)
		argIdx++
	}
	if filter.ToDate != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("created_at <= $%d", argIdx))
		args = append(args, *filter.ToDate)
		argIdx++
	}
	if filter.SentBy != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("sent_by = $%d", argIdx))
		args = append(args, *filter.SentBy)
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM email_messages %s", whereSQL)
	var totalCount int
	if err := db.QueryRow(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, fmt.Errorf("failed querying email messages count: %w", err)
	}

	orderSQL := BuildOrderBy(filter.Sort, emailMessageSortWhitelist, "ORDER BY created_at DESC")
	page, pageSize := NormalizePagination(filter.Page, filter.PageSize)
	offset := CalculateOffset(page, pageSize)

	listQuery := fmt.Sprintf(`
		SELECT id, template_key, template_version, to_addresses, cc_addresses, subject,
		       body_html_redacted, body_text_redacted, status, priority, attempts,
		       next_attempt_at, expires_at, provider_message_id, related_type, related_id,
		       resend_of, sent_by, last_error, created_at, sent_at
		FROM email_messages
		%s
		%s
		LIMIT $%d OFFSET $%d;
	`, whereSQL, orderSQL, argIdx, argIdx+1)

	args = append(args, pageSize, offset)

	rows, err := db.Query(ctx, listQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed querying email messages list: %w", err)
	}
	defer rows.Close()

	var messages []*domain.EmailMessage
	for rows.Next() {
		var msg domain.EmailMessage
		var statusStr, priorityStr string

		if err := rows.Scan(
			&msg.ID, &msg.TemplateKey, &msg.TemplateVersion, &msg.ToAddresses, &msg.CcAddresses, &msg.Subject,
			&msg.BodyHTMLRedacted, &msg.BodyTextRedacted, &statusStr, &priorityStr, &msg.Attempts,
			&msg.NextAttemptAt, &msg.ExpiresAt, &msg.ProviderMessageID, &msg.RelatedType, &msg.RelatedID,
			&msg.ResendOf, &msg.SentBy, &msg.LastError, &msg.CreatedAt, &msg.SentAt,
		); err != nil {
			return nil, 0, fmt.Errorf("failed scanning email message row: %w", err)
		}

		msg.Status = domain.EmailMessageStatus(statusStr)
		msg.Priority = domain.EmailPriority(priorityStr)

		messages = append(messages, &msg)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("row iteration error on email messages: %w", err)
	}

	return messages, totalCount, nil
}

func (r *emailMessageRepo) ClaimDue(ctx context.Context, limit int) ([]*domain.EmailMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, template_key, template_version, to_addresses, cc_addresses, subject,
		       body_html_redacted, body_text_redacted, status, priority, attempts,
		       next_attempt_at, expires_at, provider_message_id, related_type, related_id,
		       resend_of, sent_by, last_error, created_at, sent_at
		FROM email_messages
		WHERE status = 'queued'
		  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		  AND (expires_at IS NULL OR expires_at > now())
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`

	rows, err := db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed claiming due email messages: %w", err)
	}
	defer rows.Close()

	var messages []*domain.EmailMessage
	for rows.Next() {
		var msg domain.EmailMessage
		var statusStr, priorityStr string

		if err := rows.Scan(
			&msg.ID, &msg.TemplateKey, &msg.TemplateVersion, &msg.ToAddresses, &msg.CcAddresses, &msg.Subject,
			&msg.BodyHTMLRedacted, &msg.BodyTextRedacted, &statusStr, &priorityStr, &msg.Attempts,
			&msg.NextAttemptAt, &msg.ExpiresAt, &msg.ProviderMessageID, &msg.RelatedType, &msg.RelatedID,
			&msg.ResendOf, &msg.SentBy, &msg.LastError, &msg.CreatedAt, &msg.SentAt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning claimed email message: %w", err)
		}

		msg.Status = domain.EmailMessageStatus(statusStr)
		msg.Priority = domain.EmailPriority(priorityStr)

		messages = append(messages, &msg)
	}

	return messages, rows.Err()
}

func (r *emailMessageRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.EmailMessageStatus, attempts int, nextAttemptAt, sentAt *time.Time, lastError, providerMessageID *string) error {
	db := getDB(ctx, r.pool)

	const query = `
		UPDATE email_messages
		SET status = $1,
		    attempts = $2,
		    next_attempt_at = $3,
		    sent_at = $4,
		    last_error = $5,
		    provider_message_id = $6
		WHERE id = $7;
	`

	_, err := db.Exec(ctx, query, string(status), attempts, nextAttemptAt, sentAt, lastError, providerMessageID, id)
	if err != nil {
		return fmt.Errorf("failed updating email message status: %w", err)
	}

	return nil
}

func (r *emailMessageRepo) SetResendLink(ctx context.Context, id, resendOf uuid.UUID) error {
	db := getDB(ctx, r.pool)

	const query = `
		UPDATE email_messages
		SET resend_of = $1
		WHERE id = $2;
	`

	_, err := db.Exec(ctx, query, resendOf, id)
	if err != nil {
		return fmt.Errorf("failed setting email message resend link: %w", err)
	}

	return nil
}

func (r *emailMessageRepo) CountSentToday(ctx context.Context) (int, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT COUNT(*)
		FROM email_messages
		WHERE created_at >= date_trunc('day', now())
		  AND status IN ('queued', 'sending', 'sent', 'delivered');
	`

	var count int
	err := db.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed counting emails sent today: %w", err)
	}
	return count, nil
}

func (r *emailMessageRepo) MarkLostOnRestart(ctx context.Context) (int64, error) {
	db := getDB(ctx, r.pool)

	const query = `
		UPDATE email_messages
		SET status = 'failed',
		    last_error = 'lost_on_restart'
		WHERE priority = 'transactional'
		  AND status IN ('queued', 'sending');
	`

	res, err := db.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed marking in-flight transactional emails lost on restart: %w", err)
	}
	return res.RowsAffected(), nil
}

