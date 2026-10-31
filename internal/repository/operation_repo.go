package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

type operationRepo struct {
	pool *pgxpool.Pool
}

// NewOperationRepository initializes a new PostgreSQL-backed OperationRepository.
func NewOperationRepository(pool *pgxpool.Pool) OperationRepository {
	return &operationRepo{pool: pool}
}

func (r *operationRepo) Upsert(ctx context.Context, op *domain.Operation) error {
	db := getDB(ctx, r.pool)

	var errJSON []byte
	if op.Error != nil {
		var err error
		errJSON, err = json.Marshal(op.Error)
		if err != nil {
			return fmt.Errorf("failed marshaling operation error: %w", err)
		}
	}

	const query = `
		INSERT INTO operations (
			id, type, entity_type, entity_id, actor_user_id, state,
			error, compensation, correlation_id, started_at, finished_at,
			updated_at, last_event_occurred_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			now(), $12
		)
		ON CONFLICT (id) DO UPDATE SET
			type = EXCLUDED.type,
			entity_type = EXCLUDED.entity_type,
			entity_id = EXCLUDED.entity_id,
			actor_user_id = COALESCE(EXCLUDED.actor_user_id, operations.actor_user_id),
			state = EXCLUDED.state,
			error = CASE WHEN EXCLUDED.error IS NOT NULL THEN EXCLUDED.error ELSE operations.error END,
			compensation = CASE WHEN EXCLUDED.compensation IS NOT NULL THEN EXCLUDED.compensation ELSE operations.compensation END,
			correlation_id = EXCLUDED.correlation_id,
			started_at = LEAST(operations.started_at, EXCLUDED.started_at),
			finished_at = COALESCE(EXCLUDED.finished_at, operations.finished_at),
			updated_at = now(),
			last_event_occurred_at = EXCLUDED.last_event_occurred_at
		WHERE EXCLUDED.last_event_occurred_at >= operations.last_event_occurred_at;
	`

	_, err := db.Exec(ctx, query,
		op.ID,
		op.Type,
		op.Entity.Type,
		op.Entity.ID,
		op.ActorUserID,
		string(op.State),
		errJSON,
		op.Compensation,
		op.CorrelationID,
		op.StartedAt,
		op.FinishedAt,
		op.LastEventOccurredAt,
	)
	if err != nil {
		return fmt.Errorf("failed upserting operation: %w", err)
	}
	return nil
}

func (r *operationRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Operation, error) {
	db := getDB(ctx, r.pool)

	const query = `
		SELECT id, type, entity_type, entity_id, actor_user_id, state,
		       error, compensation, correlation_id, started_at, finished_at,
		       updated_at, last_event_occurred_at
		FROM operations
		WHERE id = $1;
	`

	var op domain.Operation
	var rawError []byte
	var actorID *uuid.UUID
	var comp *string
	var finAt *time.Time

	err := db.QueryRow(ctx, query, id).Scan(
		&op.ID,
		&op.Type,
		&op.Entity.Type,
		&op.Entity.ID,
		&actorID,
		&op.State,
		&rawError,
		&comp,
		&op.CorrelationID,
		&op.StartedAt,
		&finAt,
		&op.UpdatedAt,
		&op.LastEventOccurredAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.NewNotFoundError("operation not found")
		}
		return nil, fmt.Errorf("failed querying operation by id: %w", err)
	}

	op.ActorUserID = actorID
	op.Compensation = comp
	op.FinishedAt = finAt

	if len(rawError) > 0 {
		var opErr domain.OperationError
		if err := json.Unmarshal(rawError, &opErr); err == nil {
			op.Error = &opErr
		}
	}

	return &op, nil
}

func (r *operationRepo) List(ctx context.Context, filter domain.OperationFilter) ([]*domain.Operation, int, error) {
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

	var whereClauses []string
	var args []any
	argIdx := 1

	if filter.ActorUserID != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("actor_user_id = $%d", argIdx))
		args = append(args, *filter.ActorUserID)
		argIdx++
	}

	if filter.EntityType != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("entity_type = $%d", argIdx))
		args = append(args, filter.EntityType)
		argIdx++
	}

	if filter.EntityID != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("entity_id = $%d", argIdx))
		args = append(args, *filter.EntityID)
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT id, type, entity_type, entity_id, actor_user_id, state,
		       error, compensation, correlation_id, started_at, finished_at,
		       updated_at, last_event_occurred_at,
		       COUNT(*) OVER() as total_count
		FROM operations
		%s
		ORDER BY updated_at DESC
		LIMIT $%d OFFSET $%d;
	`, whereSQL, argIdx, argIdx+1)

	args = append(args, pageSize, offset)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed querying operations: %w", err)
	}
	defer rows.Close()

	var operations []*domain.Operation
	totalCount := 0

	for rows.Next() {
		var op domain.Operation
		var rawError []byte
		var actorID *uuid.UUID
		var comp *string
		var finAt *time.Time

		err := rows.Scan(
			&op.ID,
			&op.Type,
			&op.Entity.Type,
			&op.Entity.ID,
			&actorID,
			&op.State,
			&rawError,
			&comp,
			&op.CorrelationID,
			&op.StartedAt,
			&finAt,
			&op.UpdatedAt,
			&op.LastEventOccurredAt,
			&totalCount,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed scanning operation row: %w", err)
		}

		op.ActorUserID = actorID
		op.Compensation = comp
		op.FinishedAt = finAt

		if len(rawError) > 0 {
			var opErr domain.OperationError
			if err := json.Unmarshal(rawError, &opErr); err == nil {
				op.Error = &opErr
			}
		}

		operations = append(operations, &op)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("rows error: %w", err)
	}

	return operations, totalCount, nil
}
