package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

type idempotencyKeyRepo struct {
	pool *pgxpool.Pool
}

func NewIdempotencyKeyRepository(pool *pgxpool.Pool) IdempotencyKeyRepository {
	return &idempotencyKeyRepo{pool: pool}
}

func (r *idempotencyKeyRepo) Get(ctx context.Context, key, scope string) (*domain.IdempotencyKey, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, key, scope, request_hash, response, created_at
		FROM idempotency_keys
		WHERE key = $1 AND scope = $2;
	`

	var item domain.IdempotencyKey
	var respBytes []byte

	err := db.QueryRow(ctx, query, key, scope).Scan(
		&item.ID, &item.Key, &item.Scope, &item.RequestHash, &respBytes, &item.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("idempotency key not found")
		}
		return nil, fmt.Errorf("failed querying idempotency key: %w", err)
	}

	if respBytes != nil {
		item.Response = json.RawMessage(respBytes)
	}

	return &item, nil
}

func (r *idempotencyKeyRepo) Create(ctx context.Context, item *domain.IdempotencyKey) error {
	db := getDB(ctx, r.pool)

	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.Scope == "" {
		item.Scope = "email_send"
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}

	const query = `
		INSERT INTO idempotency_keys (id, key, scope, request_hash, response, created_at)
		VALUES ($1, $2, $3, $4, $5, $6);
	`

	var respBytes []byte
	if item.Response != nil {
		respBytes = []byte(item.Response)
	}

	_, err := db.Exec(ctx, query, item.ID, item.Key, item.Scope, item.RequestHash, respBytes, item.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed inserting idempotency key: %w", err)
	}

	return nil
}

func (r *idempotencyKeyRepo) UpdateResponse(ctx context.Context, key, scope string, response []byte) error {
	db := getDB(ctx, r.pool)

	const query = `
		UPDATE idempotency_keys
		SET response = $1
		WHERE key = $2 AND scope = $3;
	`

	res, err := db.Exec(ctx, query, response, key, scope)
	if err != nil {
		return fmt.Errorf("failed updating idempotency response: %w", err)
	}

	if res.RowsAffected() == 0 {
		return domain.NewNotFoundError("idempotency key not found")
	}

	return nil
}
