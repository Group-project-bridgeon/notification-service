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

type notificationRuleRepo struct {
	pool *pgxpool.Pool
}

func NewNotificationRuleRepository(pool *pgxpool.Pool) NotificationRuleRepository {
	return &notificationRuleRepo{pool: pool}
}

func (r *notificationRuleRepo) GetActiveByEventType(ctx context.Context, eventType string) ([]*domain.NotificationRule, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, event_type, target_roles, target_user_path, type, category, priority,
		       link_template, template_key, send_email, email_template_key, is_active, created_at
		FROM notification_rules
		WHERE event_type = $1 AND is_active = true
		ORDER BY created_at ASC;
	`

	rows, err := db.Query(ctx, query, eventType)
	if err != nil {
		return nil, fmt.Errorf("failed querying active notification rules: %w", err)
	}
	defer rows.Close()

	var rules []*domain.NotificationRule
	for rows.Next() {
		var rule domain.NotificationRule
		var typeStr, catStr, prioStr string

		if err := rows.Scan(
			&rule.ID, &rule.EventType, &rule.TargetRoles, &rule.TargetUserPath, &typeStr, &catStr, &prioStr,
			&rule.LinkTemplate, &rule.TemplateKey, &rule.SendEmail, &rule.EmailTemplateKey, &rule.IsActive, &rule.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning notification rule: %w", err)
		}

		rule.Type = domain.NotificationType(typeStr)
		rule.Category = domain.NotificationCategory(catStr)
		rule.Priority = domain.Priority(prioStr)
		rules = append(rules, &rule)
	}

	return rules, rows.Err()
}

func (r *notificationRuleRepo) List(ctx context.Context) ([]*domain.NotificationRule, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, event_type, target_roles, target_user_path, type, category, priority,
		       link_template, template_key, send_email, email_template_key, is_active, created_at
		FROM notification_rules
		ORDER BY event_type ASC, created_at ASC;
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed listing notification rules: %w", err)
	}
	defer rows.Close()

	var rules []*domain.NotificationRule
	for rows.Next() {
		var rule domain.NotificationRule
		var typeStr, catStr, prioStr string

		if err := rows.Scan(
			&rule.ID, &rule.EventType, &rule.TargetRoles, &rule.TargetUserPath, &typeStr, &catStr, &prioStr,
			&rule.LinkTemplate, &rule.TemplateKey, &rule.SendEmail, &rule.EmailTemplateKey, &rule.IsActive, &rule.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning rule row: %w", err)
		}

		rule.Type = domain.NotificationType(typeStr)
		rule.Category = domain.NotificationCategory(catStr)
		rule.Priority = domain.Priority(prioStr)
		rules = append(rules, &rule)
	}

	return rules, rows.Err()
}

func (r *notificationRuleRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.NotificationRule, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, event_type, target_roles, target_user_path, type, category, priority,
		       link_template, template_key, send_email, email_template_key, is_active, created_at
		FROM notification_rules
		WHERE id = $1;
	`

	var rule domain.NotificationRule
	var typeStr, catStr, prioStr string

	err := db.QueryRow(ctx, query, id).Scan(
		&rule.ID, &rule.EventType, &rule.TargetRoles, &rule.TargetUserPath, &typeStr, &catStr, &prioStr,
		&rule.LinkTemplate, &rule.TemplateKey, &rule.SendEmail, &rule.EmailTemplateKey, &rule.IsActive, &rule.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("notification rule not found")
		}
		return nil, fmt.Errorf("failed querying notification rule: %w", err)
	}

	rule.Type = domain.NotificationType(typeStr)
	rule.Category = domain.NotificationCategory(catStr)
	rule.Priority = domain.Priority(prioStr)

	return &rule, nil
}

func (r *notificationRuleRepo) Create(ctx context.Context, rule *domain.NotificationRule) error {
	db := getDB(ctx, r.pool)

	if rule.ID == uuid.Nil {
		rule.ID = uuid.New()
	}
	if rule.Priority == "" {
		rule.Priority = domain.PriorityNormal
	}
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO notification_rules (
			id, event_type, target_roles, target_user_path, type, category, priority,
			link_template, template_key, send_email, email_template_key, is_active, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
	`

	_, err := db.Exec(ctx, query,
		rule.ID, rule.EventType, rule.TargetRoles, rule.TargetUserPath, string(rule.Type), string(rule.Category), string(rule.Priority),
		rule.LinkTemplate, rule.TemplateKey, rule.SendEmail, rule.EmailTemplateKey, rule.IsActive, rule.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed inserting notification rule: %w", err)
	}

	return nil
}

func (r *notificationRuleRepo) Update(ctx context.Context, rule *domain.NotificationRule) error {
	db := getDB(ctx, r.pool)

	const query = `
		UPDATE notification_rules
		SET event_type = $1, target_roles = $2, target_user_path = $3, type = $4,
		    category = $5, priority = $6, link_template = $7, template_key = $8,
		    send_email = $9, email_template_key = $10, is_active = $11
		WHERE id = $12;
	`

	ct, err := db.Exec(ctx, query,
		rule.EventType, rule.TargetRoles, rule.TargetUserPath, string(rule.Type), string(rule.Category), string(rule.Priority),
		rule.LinkTemplate, rule.TemplateKey, rule.SendEmail, rule.EmailTemplateKey, rule.IsActive, rule.ID,
	)
	if err != nil {
		return fmt.Errorf("failed updating notification rule: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return domain.NewNotFoundError("notification rule not found")
	}

	return nil
}

func (r *notificationRuleRepo) Delete(ctx context.Context, id uuid.UUID) error {
	db := getDB(ctx, r.pool)

	const query = `DELETE FROM notification_rules WHERE id = $1;`

	ct, err := db.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed deleting notification rule: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return domain.NewNotFoundError("notification rule not found")
	}

	return nil
}

func (r *notificationRuleRepo) Deactivate(ctx context.Context, id uuid.UUID) error {
	db := getDB(ctx, r.pool)

	const query = `UPDATE notification_rules SET is_active = false WHERE id = $1;`

	ct, err := db.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed deactivating notification rule: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return domain.NewNotFoundError("notification rule not found")
	}

	return nil
}
