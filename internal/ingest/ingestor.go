package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/metrics"
	"notification-service/internal/repository"
)

// IngestMeta carries optional transport metadata (Kafka topic, partition, offset).
type IngestMeta struct {
	Topic     *string
	Partition *int
	Offset    *int64
}

// OperationPublisher dispatches real-time operation status events to clients via SSE.
type OperationPublisher interface {
	PublishOperation(ctx context.Context, userID uuid.UUID, op any) error
}

// NoopOperationPublisher is a no-op fallback implementation of OperationPublisher.
type NoopOperationPublisher struct{}

// PublishOperation implements OperationPublisher by returning nil.
func (NoopOperationPublisher) PublishOperation(_ context.Context, _ uuid.UUID, _ any) error {
	return nil
}

// Ingestor is the single transactional entry point for both Kafka and HTTP event ingestion.
type Ingestor struct {
	txManager   repository.TxRunner
	inbox       repository.InboxRepository
	recipients  repository.RecipientRepository
	operations  repository.OperationRepository
	router      Router
	opPublisher OperationPublisher
	logger      *slog.Logger
}

// NewIngestor creates an Ingestor instance.
func NewIngestor(
	txManager repository.TxRunner,
	inbox repository.InboxRepository,
	recipients repository.RecipientRepository,
	router Router,
	logger *slog.Logger,
) *Ingestor {
	if router == nil {
		router = NoopRouter{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Ingestor{
		txManager:   txManager,
		inbox:       inbox,
		recipients:  recipients,
		router:      router,
		opPublisher: NoopOperationPublisher{},
		logger:      logger,
	}
}

// SetOperationRepository configures the optional operations projection repository.
func (ing *Ingestor) SetOperationRepository(ops repository.OperationRepository) {
	ing.operations = ops
}

// SetOperationPublisher configures the real-time operation publisher.
func (ing *Ingestor) SetOperationPublisher(opPub OperationPublisher) {
	if opPub == nil {
		ing.opPublisher = NoopOperationPublisher{}
		return
	}
	ing.opPublisher = opPub
}

// Handle processes an incoming event envelope inside a single database transaction:
// 1. Inserts the event into system_inbox_events.
// 2. If already exists (duplicate), returns nil immediately.
// 3. If identity event (user.*), upserts recipient projection and marks processed.
// 4. Otherwise, calls Router.Route. Marks ignored if ErrNoRuleMatched, or processed on success.
// Returns a typed error (PermanentError or TransientError).
func (ing *Ingestor) Handle(
	ctx context.Context,
	source domain.EventSource,
	env *domain.EventEnvelope,
	meta IngestMeta,
) error {
	if env == nil {
		return &PermanentError{Cause: errors.New("nil event envelope")}
	}

	err := ing.txManager.WithTx(ctx, func(txCtx context.Context) error {
		inboxEvent := &domain.InboxEvent{
			EventID:     env.EventID,
			EventType:   env.EventType,
			Status:      domain.InboxStatusReceived,
			Source:      source,
			Topic:       meta.Topic,
			Partition:   meta.Partition,
			KafkaOffset: meta.Offset,
			ReceivedAt:  time.Now().UTC(),
		}

		isNew, err := ing.inbox.InsertIfAbsent(txCtx, inboxEvent)
		if err != nil {
			return &TransientError{Cause: fmt.Errorf("inbox insert failed: %w", err)}
		}
		if !isNew {
			metrics.Default().IncInboxDuplicates()
			ing.logger.InfoContext(txCtx, "duplicate event skipped",
				"event_id", env.EventID,
				"event_type", env.EventType,
				"source", source,
			)
			return nil
		}

		if IsIdentityEvent(env.EventType) {
			if err := handleIdentity(txCtx, env, ing.recipients); err != nil {
				return err
			}
			if err := ing.inbox.SetStatus(txCtx, env.EventID, domain.InboxStatusProcessed, nil); err != nil {
				return &TransientError{Cause: fmt.Errorf("failed setting inbox status to processed: %w", err)}
			}
			return nil
		}

		if IsOperationEvent(env.EventType) {
			if ing.operations != nil {
				if err := ing.handleOperationProjection(txCtx, env); err != nil {
					return &TransientError{Cause: fmt.Errorf("failed projecting operation: %w", err)}
				}
			}
			ing.handleOperationEvent(txCtx, env)
		}

		routeErr := ing.router.Route(txCtx, env)
		if errors.Is(routeErr, ErrNoRuleMatched) {
			if err := ing.inbox.SetStatus(txCtx, env.EventID, domain.InboxStatusIgnored, nil); err != nil {
				return &TransientError{Cause: fmt.Errorf("failed setting inbox status to ignored: %w", err)}
			}
			return nil
		}
		if routeErr != nil {
			errStr := routeErr.Error()
			_ = ing.inbox.SetStatus(txCtx, env.EventID, domain.InboxStatusFailed, &errStr)
			if IsPermanent(routeErr) || IsTransient(routeErr) {
				return routeErr
			}
			return &TransientError{Cause: routeErr}
		}

		if err := ing.inbox.SetStatus(txCtx, env.EventID, domain.InboxStatusProcessed, nil); err != nil {
			return &TransientError{Cause: fmt.Errorf("failed setting inbox status to processed: %w", err)}
		}
		return nil
	})

	if err != nil {
		if IsPermanent(err) || IsTransient(err) {
			return err
		}
		return &TransientError{Cause: err}
	}
	return nil
}

// IsOperationEvent returns true for distributed operation lifecycle events.
func IsOperationEvent(eventType string) bool {
	switch eventType {
	case "operation.started",
		"operation.succeeded",
		"operation.failed",
		"operation.compensating",
		"operation.compensated":
		return true
	default:
		return false
	}
}

type rawOperationPayload struct {
	OperationID   *uuid.UUID             `json:"operation_id"`
	OperationType string                 `json:"operation_type"`
	Entity        *rawOperationEntity    `json:"entity"`
	State         string                 `json:"state"`
	Error         *domain.OperationError `json:"error"`
	Compensation  any                    `json:"compensation"`
	ActorUserID   *uuid.UUID             `json:"actor_user_id"`
}

type rawOperationEntity struct {
	Type string     `json:"type"`
	ID   *uuid.UUID `json:"id"`
}

func (ing *Ingestor) handleOperationProjection(ctx context.Context, env *domain.EventEnvelope) error {
	var p rawOperationPayload
	if len(env.Payload) > 0 {
		_ = json.Unmarshal(env.Payload, &p)
	}

	// 1. Resolve Operation ID
	var opID uuid.UUID
	if p.OperationID != nil && *p.OperationID != uuid.Nil {
		opID = *p.OperationID
	} else if aggUUID, err := uuid.Parse(env.AggregateID); err == nil && aggUUID != uuid.Nil {
		opID = aggUUID
	} else {
		opID = env.EventID
	}

	// 2. Resolve Operation Type
	opType := p.OperationType
	if opType == "" {
		if env.AggregateType != "" {
			opType = env.AggregateType
		} else {
			opType = env.EventType
		}
	}

	// 3. Resolve Entity
	entityType := env.AggregateType
	var entityID uuid.UUID
	if p.Entity != nil {
		if p.Entity.Type != "" {
			entityType = p.Entity.Type
		}
		if p.Entity.ID != nil && *p.Entity.ID != uuid.Nil {
			entityID = *p.Entity.ID
		}
	}
	if entityID == uuid.Nil {
		if aggUUID, err := uuid.Parse(env.AggregateID); err == nil {
			entityID = aggUUID
		} else {
			entityID = opID
		}
	}
	if entityType == "" {
		entityType = "unknown"
	}

	// 4. Resolve State
	var state domain.OperationState
	switch strings.ToLower(p.State) {
	case "pending":
		state = domain.OperationStatePending
	case "succeeded":
		state = domain.OperationStateSucceeded
	case "failed":
		state = domain.OperationStateFailed
	case "compensating":
		state = domain.OperationStateCompensating
	case "compensated":
		state = domain.OperationStateCompensated
	default:
		switch env.EventType {
		case "operation.started":
			state = domain.OperationStatePending
		case "operation.succeeded":
			state = domain.OperationStateSucceeded
		case "operation.failed":
			state = domain.OperationStateFailed
		case "operation.compensating":
			state = domain.OperationStateCompensating
		case "operation.compensated":
			state = domain.OperationStateCompensated
		default:
			state = domain.OperationStatePending
		}
	}

	// 5. Resolve Actor
	var actorID *uuid.UUID
	if p.ActorUserID != nil && *p.ActorUserID != uuid.Nil {
		actorID = p.ActorUserID
	} else if env.ActorUserID != nil && *env.ActorUserID != uuid.Nil {
		actorID = env.ActorUserID
	}

	// 6. Resolve Timestamps
	startedAt := env.OccurredAt
	var finishedAt *time.Time
	if state == domain.OperationStateSucceeded || state == domain.OperationStateFailed || state == domain.OperationStateCompensated {
		t := env.OccurredAt
		finishedAt = &t
	}

	// 7. Resolve Compensation string
	var compStr *string
	if p.Compensation != nil {
		if s, ok := p.Compensation.(string); ok && s != "" {
			compStr = &s
		} else {
			if b, err := json.Marshal(p.Compensation); err == nil && len(b) > 0 {
				s := string(b)
				compStr = &s
			}
		}
	}

	var corrID uuid.UUID
	if env.CorrelationID != nil {
		corrID = *env.CorrelationID
	}

	op := &domain.Operation{
		ID:                  opID,
		Type:                opType,
		Entity:              domain.OperationEntity{Type: entityType, ID: entityID},
		ActorUserID:         actorID,
		State:               state,
		Error:               p.Error,
		Compensation:        compStr,
		CorrelationID:       corrID,
		StartedAt:           startedAt,
		FinishedAt:          finishedAt,
		LastEventOccurredAt: env.OccurredAt,
	}

	return ing.operations.Upsert(ctx, op)
}

func (ing *Ingestor) handleOperationEvent(ctx context.Context, env *domain.EventEnvelope) {
	if ing.opPublisher == nil {
		return
	}

	var actorUserID uuid.UUID
	if len(env.Payload) > 0 {
		var pMap map[string]any
		if err := json.Unmarshal(env.Payload, &pMap); err == nil {
			if v, ok := pMap["actor_user_id"]; ok {
				if s, ok := v.(string); ok {
					if parsed, err := uuid.Parse(s); err == nil {
						actorUserID = parsed
					}
				}
			}
		}
	}

	if actorUserID == uuid.Nil && env.ActorUserID != nil {
		actorUserID = *env.ActorUserID
	}

	if actorUserID == uuid.Nil {
		ing.logger.DebugContext(ctx, "operation event skipped for SSE: no actor_user_id found",
			"event_id", env.EventID,
			"event_type", env.EventType,
		)
		return
	}

	publishFn := func(pCtx context.Context) {
		if err := ing.opPublisher.PublishOperation(pCtx, actorUserID, env.Payload); err != nil {
			ing.logger.WarnContext(pCtx, "failed pushing operation event to publisher (non-fatal)",
				"event_id", env.EventID,
				"actor_user_id", actorUserID,
				"error", err,
			)
		}
	}

	if repository.HasPostCommitHooks(ctx) {
		repository.AddPostCommitHook(ctx, publishFn)
	} else {
		publishFn(ctx)
	}
}


