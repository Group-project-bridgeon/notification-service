package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
)

type mockTxRunner struct{}

func (m *mockTxRunner) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

type mockInboxRepo struct {
	events       map[uuid.UUID]*domain.InboxEvent
	insertErr    error
	statusSet    map[uuid.UUID]domain.InboxStatus
	lastErrorSet map[uuid.UUID]*string
}

func newMockInboxRepo() *mockInboxRepo {
	return &mockInboxRepo{
		events:       make(map[uuid.UUID]*domain.InboxEvent),
		statusSet:    make(map[uuid.UUID]domain.InboxStatus),
		lastErrorSet: make(map[uuid.UUID]*string),
	}
}

func (m *mockInboxRepo) InsertIfAbsent(_ context.Context, event *domain.InboxEvent) (bool, error) {
	if m.insertErr != nil {
		return false, m.insertErr
	}
	if _, exists := m.events[event.EventID]; exists {
		return false, nil
	}
	m.events[event.EventID] = event
	return true, nil
}

func (m *mockInboxRepo) SetStatus(_ context.Context, eventID uuid.UUID, status domain.InboxStatus, errStr *string) error {
	m.statusSet[eventID] = status
	m.lastErrorSet[eventID] = errStr
	return nil
}

type mockRecipientRepo struct {
	recipients map[uuid.UUID]*domain.Recipient
	upsertErr  error
}

func newMockRecipientRepo() *mockRecipientRepo {
	return &mockRecipientRepo{
		recipients: make(map[uuid.UUID]*domain.Recipient),
	}
}

func (m *mockRecipientRepo) UpsertGuarded(_ context.Context, recipient *domain.Recipient) error {
	if m.upsertErr != nil {
		return m.upsertErr
	}
	existing, found := m.recipients[recipient.UserID]
	if !found || recipient.SourceOccurredAt.After(existing.SourceOccurredAt) {
		m.recipients[recipient.UserID] = recipient
	}
	return nil
}

func (m *mockRecipientRepo) FindByRoles(_ context.Context, roles []string) ([]*domain.Recipient, error) {
	var res []*domain.Recipient
	for _, r := range m.recipients {
		for _, requiredRole := range roles {
			for _, userRole := range r.Roles {
				if userRole == requiredRole {
					res = append(res, r)
					break
				}
			}
		}
	}
	return res, nil
}

func (m *mockRecipientRepo) FindByID(_ context.Context, id uuid.UUID) (*domain.Recipient, error) {
	r, ok := m.recipients[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return r, nil
}

type mockRouter struct {
	routedEvents []*domain.EventEnvelope
	errToReturn  error
}

func (m *mockRouter) Route(_ context.Context, env *domain.EventEnvelope) error {
	m.routedEvents = append(m.routedEvents, env)
	return m.errToReturn
}

func TestIngestor_DuplicateIgnored(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRouter{}
	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)

	eventID := uuid.New()
	env := &domain.EventEnvelope{
		EventID:       eventID,
		EventType:     "order.created",
		EventVersion:  "1",
		OccurredAt:    time.Now().UTC(),
		Producer:      "order-service",
		AggregateType: "order",
		AggregateID:   uuid.New().String(),
		Payload:       json.RawMessage(`{}`),
	}

	// First time
	err := ing.Handle(context.Background(), domain.EventSourceKafka, env, IngestMeta{})
	if err != nil {
		t.Fatalf("first handle failed: %v", err)
	}
	if len(router.routedEvents) != 1 {
		t.Fatalf("expected 1 routed event, got %d", len(router.routedEvents))
	}
	if inbox.statusSet[eventID] != domain.InboxStatusProcessed {
		t.Errorf("expected processed status, got %s", inbox.statusSet[eventID])
	}

	// Second time (duplicate)
	err = ing.Handle(context.Background(), domain.EventSourceKafka, env, IngestMeta{})
	if err != nil {
		t.Fatalf("duplicate handle failed: %v", err)
	}
	if len(router.routedEvents) != 1 {
		t.Fatalf("expected router NOT to be called again for duplicate, got %d calls", len(router.routedEvents))
	}
}

func TestIngestor_NoRuleMatched_MarksIgnored(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRouter{errToReturn: ErrNoRuleMatched}
	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)

	eventID := uuid.New()
	env := &domain.EventEnvelope{
		EventID:       eventID,
		EventType:     "unknown.event",
		EventVersion:  "1",
		OccurredAt:    time.Now().UTC(),
		Producer:      "some-service",
		AggregateType: "entity",
		AggregateID:   uuid.New().String(),
		Payload:       json.RawMessage(`{}`),
	}

	err := ing.Handle(context.Background(), domain.EventSourceKafka, env, IngestMeta{})
	if err != nil {
		t.Fatalf("expected nil error on no rule matched, got: %v", err)
	}
	if inbox.statusSet[eventID] != domain.InboxStatusIgnored {
		t.Errorf("expected ignored status, got %s", inbox.statusSet[eventID])
	}
}

func TestIngestor_IdentityEvent_UpsertsRecipient(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &mockRouter{}
	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)

	userID := uuid.New()
	eventID := uuid.New()
	occurredAt := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)

	payloadBytes, _ := json.Marshal(map[string]any{
		"user_id":      userID.String(),
		"email":        "alice@example.com",
		"display_name": "Alice Smith",
		"roles":        []string{"admin", "manager"},
		"is_active":    true,
	})

	env := &domain.EventEnvelope{
		EventID:       eventID,
		EventType:     "user.created",
		EventVersion:  "1",
		OccurredAt:    occurredAt,
		Producer:      "auth-service",
		AggregateType: "user",
		AggregateID:   userID.String(),
		Payload:       payloadBytes,
	}

	err := ing.Handle(context.Background(), domain.EventSourceKafka, env, IngestMeta{})
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}

	// Identity events should NOT call router
	if len(router.routedEvents) != 0 {
		t.Fatalf("expected router NOT to be called for identity event")
	}

	// Status should be processed
	if inbox.statusSet[eventID] != domain.InboxStatusProcessed {
		t.Errorf("expected inbox status processed, got %s", inbox.statusSet[eventID])
	}

	// Check recipient
	rec, err := recipients.FindByID(context.Background(), userID)
	if err != nil {
		t.Fatalf("failed finding recipient: %v", err)
	}
	if rec.Email != "alice@example.com" {
		t.Errorf("expected alice@example.com, got %s", rec.Email)
	}
	if len(rec.Roles) != 2 || rec.Roles[0] != "admin" {
		t.Errorf("unexpected roles: %v", rec.Roles)
	}
}

func TestIngestor_IdentityEvent_OutOfOrderProtection(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	ing := NewIngestor(&mockTxRunner{}, inbox, recipients, &mockRouter{}, nil)

	userID := uuid.New()
	t1 := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC) // older event arriving later

	// Event 1 (newer)
	p1, _ := json.Marshal(map[string]any{
		"user_id":      userID.String(),
		"email":        "newer@example.com",
		"display_name": "Newer Name",
		"roles":        []string{"admin"},
		"is_active":    true,
	})
	env1 := &domain.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     "user.updated",
		EventVersion:  "1",
		OccurredAt:    t1,
		Producer:      "auth-service",
		AggregateType: "user",
		AggregateID:   userID.String(),
		Payload:       p1,
	}

	if err := ing.Handle(context.Background(), domain.EventSourceKafka, env1, IngestMeta{}); err != nil {
		t.Fatalf("failed handle env1: %v", err)
	}

	// Event 2 (older out-of-order event arriving after env1)
	p2, _ := json.Marshal(map[string]any{
		"user_id":      userID.String(),
		"email":        "older@example.com",
		"display_name": "Older Name",
		"roles":        []string{"user"},
		"is_active":    false,
	})
	env2 := &domain.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     "user.updated",
		EventVersion:  "1",
		OccurredAt:    t2,
		Producer:      "auth-service",
		AggregateType: "user",
		AggregateID:   userID.String(),
		Payload:       p2,
	}

	if err := ing.Handle(context.Background(), domain.EventSourceKafka, env2, IngestMeta{}); err != nil {
		t.Fatalf("failed handle env2: %v", err)
	}

	// Verify recipient still has the newer data
	rec, _ := recipients.FindByID(context.Background(), userID)
	if rec.Email != "newer@example.com" {
		t.Errorf("expected newer@example.com retained, got %s", rec.Email)
	}
	if rec.DisplayName != "Newer Name" {
		t.Errorf("expected 'Newer Name', got %s", rec.DisplayName)
	}
}
