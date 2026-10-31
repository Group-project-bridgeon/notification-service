package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
)

type mockReplayerDLRepo struct {
	mu          sync.Mutex
	deadLetters map[uuid.UUID]*domain.DeadLetter
}

func newMockReplayerDLRepo() *mockReplayerDLRepo {
	return &mockReplayerDLRepo{
		deadLetters: make(map[uuid.UUID]*domain.DeadLetter),
	}
}

func (m *mockReplayerDLRepo) Create(_ context.Context, dl *domain.DeadLetter) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadLetters[dl.ID] = dl
	return nil
}

func (m *mockReplayerDLRepo) List(_ context.Context, _, _ int) ([]*domain.DeadLetter, int, error) {
	return nil, 0, nil
}

func (m *mockReplayerDLRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.DeadLetter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dl, ok := m.deadLetters[id]
	if !ok {
		return nil, domain.NewNotFoundError("dead letter not found")
	}
	return dl, nil
}

func (m *mockReplayerDLRepo) MarkReplayed(_ context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dl, ok := m.deadLetters[id]
	if !ok {
		return domain.NewNotFoundError("dead letter not found")
	}
	now := time.Now().UTC()
	dl.ReplayedAt = &now
	return nil
}

func TestReplayer_Success(t *testing.T) {
	dlRepo := newMockReplayerDLRepo()
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRouter{}
	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)

	replayer := NewReplayer(dlRepo, ing, nil)

	dlID := uuid.New()
	eventID := uuid.New()
	topic := "erp.sales.events"
	part := 1
	offset := int64(100)

	payload := []byte(fmt.Sprintf(`{
		"event_id":       "%s",
		"event_type":     "order.created",
		"event_version":  "1",
		"occurred_at":    "2026-10-15T10:00:00Z",
		"producer":       "sales-service",
		"aggregate_type": "order",
		"aggregate_id":   "e81d8590-bfae-4d8b-9d41-e125032b4943",
		"payload":        {"total":"100"}
	}`, eventID.String()))

	dl := &domain.DeadLetter{
		ID:          dlID,
		Source:      domain.EventSourceKafka,
		Topic:       &topic,
		Partition:   &part,
		KafkaOffset: &offset,
		RawEvent:    json.RawMessage(payload),
		Error:       "temporary failure",
		FailedAt:    time.Now().UTC(),
	}
	_ = dlRepo.Create(context.Background(), dl)

	// First replay must succeed
	err := replayer.Replay(context.Background(), dlID)
	if err != nil {
		t.Fatalf("expected replay to succeed, got: %v", err)
	}

	// Verify router received event
	if len(router.routedEvents) != 1 || router.routedEvents[0].EventID != eventID {
		t.Fatalf("expected router to receive event %s", eventID)
	}

	// Verify replayed_at is set
	replayedDL, _ := dlRepo.GetByID(context.Background(), dlID)
	if replayedDL.ReplayedAt == nil {
		t.Fatal("expected replayed_at timestamp to be set")
	}
}

func TestReplayer_ReplayTwice_ReturnsInvalidStateTransition(t *testing.T) {
	dlRepo := newMockReplayerDLRepo()
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRouter{}
	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)

	replayer := NewReplayer(dlRepo, ing, nil)

	dlID := uuid.New()
	eventID := uuid.New()
	topic := "erp.sales.events"

	payload := []byte(fmt.Sprintf(`{
		"event_id":       "%s",
		"event_type":     "order.created",
		"event_version":  "1",
		"occurred_at":    "2026-10-15T10:00:00Z",
		"producer":       "sales-service",
		"aggregate_type": "order",
		"aggregate_id":   "e81d8590-bfae-4d8b-9d41-e125032b4943",
		"payload":        {}
	}`, eventID.String()))

	dl := &domain.DeadLetter{
		ID:       dlID,
		Source:   domain.EventSourceKafka,
		Topic:    &topic,
		RawEvent: json.RawMessage(payload),
		Error:    "temporary failure",
		FailedAt: time.Now().UTC(),
	}
	_ = dlRepo.Create(context.Background(), dl)

	// First replay
	err := replayer.Replay(context.Background(), dlID)
	if err != nil {
		t.Fatalf("first replay failed: %v", err)
	}

	// Second replay must return invalid_state_transition
	err = replayer.Replay(context.Background(), dlID)
	if err == nil {
		t.Fatal("expected error on second replay, got nil")
	}

	var appErr *domain.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected domain.AppError, got: %T (%v)", err, err)
	}
	if appErr.Code != domain.CodeInvalidStateTransition {
		t.Errorf("expected code %s, got %s", domain.CodeInvalidStateTransition, appErr.Code)
	}
}

func TestReplayer_NotFound(t *testing.T) {
	dlRepo := newMockReplayerDLRepo()
	ing := NewIngestor(&mockTxRunner{}, newMockInboxRepo(), newMockRecipientRepo(), &mockRouter{}, nil)
	replayer := NewReplayer(dlRepo, ing, nil)

	err := replayer.Replay(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("expected error for non-existent dead letter, got nil")
	}

	var appErr *domain.AppError
	if !errors.As(err, &appErr) || appErr.Code != domain.CodeNotFound {
		t.Errorf("expected not_found error code, got %v", err)
	}
}
