package ingest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
)

type mockOpPublisher struct {
	publishedUserID uuid.UUID
	publishedOp     any
}

func (m *mockOpPublisher) PublishOperation(ctx context.Context, userID uuid.UUID, op any) error {
	m.publishedUserID = userID
	m.publishedOp = op
	return nil
}

type mockRuleRouter struct {
	hasRule bool
}

func (m *mockRuleRouter) Route(ctx context.Context, env *domain.EventEnvelope) error {
	if m.hasRule {
		return nil
	}
	return ErrNoRuleMatched
}

func TestIngestor_OperationEvent_PayloadActorID_NoRuleIgnored(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRuleRouter{hasRule: false}

	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	opPub := &mockOpPublisher{}
	ing.SetOperationPublisher(opPub)

	actorID := uuid.New()
	eventID := uuid.New()

	payloadMap := map[string]any{
		"operation_id":   "op-test-1",
		"operation_type": "sales_order.create",
		"actor_user_id":  actorID.String(),
		"state":          "failed",
		"error":          "credit limit exceeded",
	}
	payloadBytes, _ := json.Marshal(payloadMap)

	env := &domain.EventEnvelope{
		EventID:      eventID,
		EventType:    "operation.failed",
		EventVersion: "1.0",
		OccurredAt:   time.Now().UTC(),
		Producer:     "sales-service",
		Payload:      payloadBytes,
	}

	err := ing.Handle(context.Background(), domain.EventSourceKafka, env, IngestMeta{})
	if err != nil {
		t.Fatalf("expected nil error on ignored event, got %v", err)
	}

	// 1. Verify operation event was published to actorID
	if opPub.publishedUserID != actorID {
		t.Fatalf("expected published actorID %s, got %s", actorID, opPub.publishedUserID)
	}

	// 2. Verify inbox status is ignored (not failed, not processed)
	if inbox.statusSet[eventID] != domain.InboxStatusIgnored {
		t.Fatalf("expected inbox status ignored, got %s", inbox.statusSet[eventID])
	}
}

func TestIngestor_OperationEvent_EnvelopeActorID(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRuleRouter{hasRule: false}

	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	opPub := &mockOpPublisher{}
	ing.SetOperationPublisher(opPub)

	actorID := uuid.New()
	eventID := uuid.New()

	payloadMap := map[string]any{
		"operation_id": "op-test-2",
		"state":        "succeeded",
	}
	payloadBytes, _ := json.Marshal(payloadMap)

	env := &domain.EventEnvelope{
		EventID:      eventID,
		EventType:    "operation.succeeded",
		EventVersion: "1.0",
		OccurredAt:   time.Now().UTC(),
		Producer:     "sales-service",
		ActorUserID:  &actorID,
		Payload:      payloadBytes,
	}

	err := ing.Handle(context.Background(), domain.EventSourceKafka, env, IngestMeta{})
	if err != nil {
		t.Fatalf("unexpected handle error: %v", err)
	}

	if opPub.publishedUserID != actorID {
		t.Fatalf("expected published actorID %s, got %s", actorID, opPub.publishedUserID)
	}
}

func TestIngestor_OperationEvent_WithRuleMatchesProcessed(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRuleRouter{hasRule: true}

	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	opPub := &mockOpPublisher{}
	ing.SetOperationPublisher(opPub)

	actorID := uuid.New()
	eventID := uuid.New()

	env := &domain.EventEnvelope{
		EventID:      eventID,
		EventType:    "operation.compensating",
		EventVersion: "1.0",
		OccurredAt:   time.Now().UTC(),
		Producer:     "inventory-service",
		ActorUserID:  &actorID,
		Payload:      []byte(`{"operation_id":"op-comp-1","state":"compensating"}`),
	}

	err := ing.Handle(context.Background(), domain.EventSourceKafka, env, IngestMeta{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if inbox.statusSet[eventID] != domain.InboxStatusProcessed {
		t.Fatalf("expected inbox status processed, got %s", inbox.statusSet[eventID])
	}
}

type mockIngestOpRepo struct {
	operations map[uuid.UUID]*domain.Operation
}

func (m *mockIngestOpRepo) Upsert(_ context.Context, op *domain.Operation) error {
	existing, ok := m.operations[op.ID]
	if ok {
		if op.LastEventOccurredAt.Before(existing.LastEventOccurredAt) {
			return nil // out-of-order ignored
		}
		if op.StartedAt.After(existing.StartedAt) {
			op.StartedAt = existing.StartedAt
		}
	}
	m.operations[op.ID] = op
	return nil
}

func (m *mockIngestOpRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Operation, error) {
	return m.operations[id], nil
}

func (m *mockIngestOpRepo) List(_ context.Context, _ domain.OperationFilter) ([]*domain.Operation, int, error) {
	return nil, 0, nil
}

func TestIngestor_OperationProjection_OutOfOrderIgnored(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRuleRouter{hasRule: false}
	opRepo := &mockIngestOpRepo{operations: make(map[uuid.UUID]*domain.Operation)}

	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	ing.SetOperationRepository(opRepo)

	opID := uuid.New()
	actorID := uuid.New()

	t1 := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 20, 10, 5, 0, 0, time.UTC)

	// 1. Ingest newer event: operation.failed at t2
	failPayload, _ := json.Marshal(map[string]any{
		"operation_id":   opID.String(),
		"operation_type": "sales_order.confirm",
		"state":          "failed",
		"actor_user_id":  actorID.String(),
		"error": map[string]any{
			"code":    "insufficient_stock",
			"message": "Only 6 available",
		},
	})
	failEnv := &domain.EventEnvelope{
		EventID:      uuid.New(),
		EventType:    "operation.failed",
		EventVersion: "1.0",
		OccurredAt:   t2,
		Producer:     "sales-service",
		Payload:      failPayload,
	}

	if err := ing.Handle(context.Background(), domain.EventSourceKafka, failEnv, IngestMeta{}); err != nil {
		t.Fatalf("unexpected error on failEnv: %v", err)
	}

	savedOp := opRepo.operations[opID]
	if savedOp == nil || savedOp.State != domain.OperationStateFailed {
		t.Fatalf("expected state failed, got %+v", savedOp)
	}

	// 2. Ingest older event: operation.started with occurred_at = t1 (out-of-order)
	startPayload, _ := json.Marshal(map[string]any{
		"operation_id":   opID.String(),
		"operation_type": "sales_order.confirm",
		"state":          "pending",
		"actor_user_id":  actorID.String(),
	})
	startEnv := &domain.EventEnvelope{
		EventID:      uuid.New(),
		EventType:    "operation.started",
		EventVersion: "1.0",
		OccurredAt:   t1,
		Producer:     "sales-service",
		Payload:      startPayload,
	}

	if err := ing.Handle(context.Background(), domain.EventSourceKafka, startEnv, IngestMeta{}); err != nil {
		t.Fatalf("unexpected error on startEnv: %v", err)
	}

	// State must remain "failed", not overwritten by older "pending"
	finalOp := opRepo.operations[opID]
	if finalOp.State != domain.OperationStateFailed {
		t.Fatalf("expected state to remain failed after older started event, got %s", finalOp.State)
	}
}
