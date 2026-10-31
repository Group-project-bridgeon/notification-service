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

type emailTemplateRepo struct {
	pool *pgxpool.Pool
	tm   *TxManager
}

func NewEmailTemplateRepository(pool *pgxpool.Pool, tm *TxManager) EmailTemplateRepository {
	return &emailTemplateRepo{pool: pool, tm: tm}
}

func (r *emailTemplateRepo) List(ctx context.Context) ([]*domain.EmailTemplate, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, template_key, version, subject_template, html_template, text_template,
		       declared_variables, sensitive_variables, is_active, created_by, created_at
		FROM email_templates
		ORDER BY template_key ASC, version DESC;
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed listing email templates: %w", err)
	}
	defer rows.Close()

	var list []*domain.EmailTemplate
	for rows.Next() {
		var t domain.EmailTemplate
		if err := rows.Scan(
			&t.ID, &t.TemplateKey, &t.Version, &t.SubjectTemplate, &t.HTMLTemplate, &t.TextTemplate,
			&t.DeclaredVariables, &t.SensitiveVariables, &t.IsActive, &t.CreatedBy, &t.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning email template row: %w", err)
		}
		list = append(list, &t)
	}

	return list, rows.Err()
}

func (r *emailTemplateRepo) ListVersions(ctx context.Context, templateKey string) ([]*domain.EmailTemplate, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, template_key, version, subject_template, html_template, text_template,
		       declared_variables, sensitive_variables, is_active, created_by, created_at
		FROM email_templates
		WHERE template_key = $1
		ORDER BY version DESC;
	`

	rows, err := db.Query(ctx, query, templateKey)
	if err != nil {
		return nil, fmt.Errorf("failed listing email template versions: %w", err)
	}
	defer rows.Close()

	var list []*domain.EmailTemplate
	for rows.Next() {
		var t domain.EmailTemplate
		if err := rows.Scan(
			&t.ID, &t.TemplateKey, &t.Version, &t.SubjectTemplate, &t.HTMLTemplate, &t.TextTemplate,
			&t.DeclaredVariables, &t.SensitiveVariables, &t.IsActive, &t.CreatedBy, &t.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning email template version row: %w", err)
		}
		list = append(list, &t)
	}

	return list, rows.Err()
}

func (r *emailTemplateRepo) GetActive(ctx context.Context, templateKey string) (*domain.EmailTemplate, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, template_key, version, subject_template, html_template, text_template,
		       declared_variables, sensitive_variables, is_active, created_by, created_at
		FROM email_templates
		WHERE template_key = $1 AND is_active = true;
	`

	var t domain.EmailTemplate
	err := db.QueryRow(ctx, query, templateKey).Scan(
		&t.ID, &t.TemplateKey, &t.Version, &t.SubjectTemplate, &t.HTMLTemplate, &t.TextTemplate,
		&t.DeclaredVariables, &t.SensitiveVariables, &t.IsActive, &t.CreatedBy, &t.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("active email template not found")
		}
		return nil, fmt.Errorf("failed querying active email template: %w", err)
	}

	return &t, nil
}

func (r *emailTemplateRepo) GetByVersion(ctx context.Context, templateKey string, version int) (*domain.EmailTemplate, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, template_key, version, subject_template, html_template, text_template,
		       declared_variables, sensitive_variables, is_active, created_by, created_at
		FROM email_templates
		WHERE template_key = $1 AND version = $2;
	`

	var t domain.EmailTemplate
	err := db.QueryRow(ctx, query, templateKey, version).Scan(
		&t.ID, &t.TemplateKey, &t.Version, &t.SubjectTemplate, &t.HTMLTemplate, &t.TextTemplate,
		&t.DeclaredVariables, &t.SensitiveVariables, &t.IsActive, &t.CreatedBy, &t.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("email template version not found")
		}
		return nil, fmt.Errorf("failed querying email template by version: %w", err)
	}

	return &t, nil
}

func (r *emailTemplateRepo) CreateNewVersion(ctx context.Context, tmpl *domain.EmailTemplate) error {
	db := getDB(ctx, r.pool)

	if tmpl.ID == uuid.Nil {
		tmpl.ID = uuid.New()
	}
	if tmpl.CreatedAt.IsZero() {
		tmpl.CreatedAt = time.Now().UTC()
	}

	// Auto-increment version if not explicitly set
	if tmpl.Version <= 0 {
		var maxVer int
		err := db.QueryRow(ctx, "SELECT COALESCE(MAX(version), 0) FROM email_templates WHERE template_key = $1", tmpl.TemplateKey).Scan(&maxVer)
		if err != nil {
			return fmt.Errorf("failed calculating next email template version: %w", err)
		}
		tmpl.Version = maxVer + 1
	}

	const query = `
		INSERT INTO email_templates (
			id, template_key, version, subject_template, html_template, text_template,
			declared_variables, sensitive_variables, is_active, created_by, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
	`

	_, err := db.Exec(ctx, query,
		tmpl.ID, tmpl.TemplateKey, tmpl.Version, tmpl.SubjectTemplate, tmpl.HTMLTemplate, tmpl.TextTemplate,
		tmpl.DeclaredVariables, tmpl.SensitiveVariables, tmpl.IsActive, tmpl.CreatedBy, tmpl.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed creating email template version: %w", err)
	}

	return nil
}

func (r *emailTemplateRepo) Activate(ctx context.Context, templateKey string, version int) error {
	return r.tm.WithTx(ctx, func(txCtx context.Context) error {
		db := getDB(txCtx, r.pool)

		// 1. Deactivate existing active version
		const deactivateQuery = `
			UPDATE email_templates
			SET is_active = false
			WHERE template_key = $1 AND is_active = true;
		`
		if _, err := db.Exec(txCtx, deactivateQuery, templateKey); err != nil {
			return fmt.Errorf("failed deactivating previous email template version: %w", err)
		}

		// 2. Activate target version
		const activateQuery = `
			UPDATE email_templates
			SET is_active = true
			WHERE template_key = $1 AND version = $2;
		`
		ct, err := db.Exec(txCtx, activateQuery, templateKey, version)
		if err != nil {
			return fmt.Errorf("failed activating email template version: %w", err)
		}
		if ct.RowsAffected() == 0 {
			return domain.NewNotFoundError("target email template version not found")
		}

		return nil
	})
}
