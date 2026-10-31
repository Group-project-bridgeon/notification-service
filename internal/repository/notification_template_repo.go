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

type notificationTemplateRepo struct {
	pool *pgxpool.Pool
}

func NewNotificationTemplateRepository(pool *pgxpool.Pool) NotificationTemplateRepository {
	return &notificationTemplateRepo{pool: pool}
}

func (r *notificationTemplateRepo) GetActive(ctx context.Context, templateKey, locale string) (*domain.NotificationTemplate, error) {
	db := getDB(ctx, r.pool)
	if locale == "" {
		locale = "en"
	}

	const query = `
		SELECT id, template_key, event_type, locale, title_template, body_template,
		       version, is_active, created_at
		FROM notification_templates
		WHERE template_key = $1 AND locale = $2 AND is_active = true
		ORDER BY version DESC
		LIMIT 1;
	`

	var t domain.NotificationTemplate
	err := db.QueryRow(ctx, query, templateKey, locale).Scan(
		&t.ID, &t.TemplateKey, &t.EventType, &t.Locale, &t.TitleTemplate, &t.BodyTemplate,
		&t.Version, &t.IsActive, &t.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("notification template not found")
		}
		return nil, fmt.Errorf("failed querying notification template: %w", err)
	}

	return &t, nil
}

func (r *notificationTemplateRepo) List(ctx context.Context) ([]*domain.NotificationTemplate, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, template_key, event_type, locale, title_template, body_template,
		       version, is_active, created_at
		FROM notification_templates
		ORDER BY template_key ASC, version DESC;
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed listing notification templates: %w", err)
	}
	defer rows.Close()

	var templates []*domain.NotificationTemplate
	for rows.Next() {
		var t domain.NotificationTemplate
		if err := rows.Scan(
			&t.ID, &t.TemplateKey, &t.EventType, &t.Locale, &t.TitleTemplate, &t.BodyTemplate,
			&t.Version, &t.IsActive, &t.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning notification template: %w", err)
		}
		templates = append(templates, &t)
	}

	return templates, rows.Err()
}

func (r *notificationTemplateRepo) Create(ctx context.Context, tmpl *domain.NotificationTemplate) error {
	db := getDB(ctx, r.pool)

	if tmpl.ID == uuid.Nil {
		tmpl.ID = uuid.New()
	}
	if tmpl.CreatedAt.IsZero() {
		tmpl.CreatedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO notification_templates (
			id, template_key, event_type, locale, title_template, body_template,
			version, is_active, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
	`

	_, err := db.Exec(ctx, query,
		tmpl.ID, tmpl.TemplateKey, tmpl.EventType, tmpl.Locale, tmpl.TitleTemplate, tmpl.BodyTemplate,
		tmpl.Version, tmpl.IsActive, tmpl.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed inserting notification template: %w", err)
	}

	return nil
}

func (r *notificationTemplateRepo) CreateNewVersion(ctx context.Context, tmpl *domain.NotificationTemplate) error {
	db := getDB(ctx, r.pool)

	if tmpl.ID == uuid.Nil {
		tmpl.ID = uuid.New()
	}
	if tmpl.Locale == "" {
		tmpl.Locale = "en"
	}
	if tmpl.CreatedAt.IsZero() {
		tmpl.CreatedAt = time.Now().UTC()
	}

	// 1. Find max version
	const maxQuery = `
		SELECT COALESCE(MAX(version), 0)
		FROM notification_templates
		WHERE template_key = $1 AND locale = $2;
	`
	var maxVer int
	if err := db.QueryRow(ctx, maxQuery, tmpl.TemplateKey, tmpl.Locale).Scan(&maxVer); err != nil {
		return fmt.Errorf("failed querying max template version: %w", err)
	}

	newVer := maxVer + 1
	tmpl.Version = newVer
	tmpl.IsActive = true

	// 2. Deactivate previous versions
	const deactQuery = `
		UPDATE notification_templates
		SET is_active = false
		WHERE template_key = $1 AND locale = $2;
	`
	if _, err := db.Exec(ctx, deactQuery, tmpl.TemplateKey, tmpl.Locale); err != nil {
		return fmt.Errorf("failed deactivating previous template versions: %w", err)
	}

	// 3. Insert new active version
	const insertQuery = `
		INSERT INTO notification_templates (
			id, template_key, event_type, locale, title_template, body_template,
			version, is_active, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
	`
	_, err := db.Exec(ctx, insertQuery,
		tmpl.ID, tmpl.TemplateKey, tmpl.EventType, tmpl.Locale, tmpl.TitleTemplate, tmpl.BodyTemplate,
		tmpl.Version, tmpl.IsActive, tmpl.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed inserting new template version: %w", err)
	}

	return nil
}
