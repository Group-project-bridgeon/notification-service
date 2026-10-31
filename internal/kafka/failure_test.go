package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"notification-service/internal/domain"
	"notification-service/internal/ingest"
)

type mockDeadLetterRepo struct {
	mu          sync.Mutex
	deadLetters map[uuid.UUID]*domain.DeadLetter
	createErr   error
}

func newMockDeadLetterRepo() *mockDeadLetterRepo {
	return &mockDeadLetterRepo{
		deadLetters: make(map[uuid.UUID]*domain.DeadLetter),
	}
}

func (m *mockDeadLetterRepo) Create(_ context.Context, dl *domain.DeadLetter) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createErr != nil {
		return m.createErr
	}
	m.deadLetters[dl.ID] = dl
	return nil
}

func (m *mockDeadLetterRepo) List(_ context.Context, _, _ int) ([]*domain.DeadLetter, int, error) {
	return nil, 0, nil
}

func (m *mockDeadLetterRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.DeadLetter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dl, ok := m.deadLetters[id]
	if !ok {
		return nil, domain.NewNotFoundError("dead letter not found")
	}
	return dl, nil
}

func (m *mockDeadLetterRepo) MarkReplayed(_ context.Context, id uuid.UUID) error {
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

func TestFailureHandler_TransientProgression(t *testing.T) {
	publisher := NewMockPublisher()
	dlRepo := newMockDeadLetterRepo()
	metrics := NewMetrics()
	startTime := time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	cfg := DefaultRetryConfig()

	handler := NewStandardFailureHandler(publisher, dlRepo, metrics, clock, cfg, nil)
	ctx := context.Background()

	originalVal := []byte(`{"event_id":"7b8e1460-63bf-4787-8e68-07df95a5f782"}`)
	record := &kgo.Record{
		Key:       []byte("order-123"),
		Value:     originalVal,
		Topic:     "erp.sales.events",
		Partition: 2,
		Offset:    105,
	}

	transientErr := &ingest.TransientError{Cause: errors.New("database connection refused")}

	// Stage 1: Initial failure on erp.sales.events -> erp.notification.retry.1m
	err := handler.Handle(ctx, record, transientErr)
	if err != nil {
		t.Fatalf("stage 1 handle failed: %v", err)
	}

	published := publisher.GetPublished()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	rec1 := published[0]
	if rec1.Topic != "erp.notification.retry.1m" {
		t.Errorf("stage 1 topic = %s, want erp.notification.retry.1m", rec1.Topic)
	}
	if getHeader(rec1.Headers, "x-attempt") != "1" {
		t.Errorf("stage 1 x-attempt = %s, want 1", getHeader(rec1.Headers, "x-attempt"))
	}
	if getHeader(rec1.Headers, "x-original-topic") != "erp.sales.events" {
		t.Errorf("stage 1 x-original-topic = %s", getHeader(rec1.Headers, "x-original-topic"))
	}
	if getHeader(rec1.Headers, "x-original-partition") != "2" {
		t.Errorf("stage 1 x-original-partition = %s", getHeader(rec1.Headers, "x-original-partition"))
	}
	if getHeader(rec1.Headers, "x-original-offset") != "105" {
		t.Errorf("stage 1 x-original-offset = %s", getHeader(rec1.Headers, "x-original-offset"))
	}
	expectedNotBefore1 := startTime.Add(1 * time.Minute).Format(time.RFC3339)
	if getHeader(rec1.Headers, "x-not-before") != expectedNotBefore1 {
		t.Errorf("stage 1 x-not-before = %s, want %s", getHeader(rec1.Headers, "x-not-before"), expectedNotBefore1)
	}

	// Stage 2: Failure on 1m retry topic -> erp.notification.retry.10m
	clock.Advance(1 * time.Minute)
	rec1.Offset = 50 // offset on retry topic
	err = handler.Handle(ctx, rec1, transientErr)
	if err != nil {
		t.Fatalf("stage 2 handle failed: %v", err)
	}

	published = publisher.GetPublished()
	if len(published) != 2 {
		t.Fatalf("expected 2 published messages, got %d", len(published))
	}
	rec2 := published[1]
	if rec2.Topic != "erp.notification.retry.10m" {
		t.Errorf("stage 2 topic = %s, want erp.notification.retry.10m", rec2.Topic)
	}
	if getHeader(rec2.Headers, "x-attempt") != "2" {
		t.Errorf("stage 2 x-attempt = %s, want 2", getHeader(rec2.Headers, "x-attempt"))
	}
	if getHeader(rec2.Headers, "x-original-topic") != "erp.sales.events" {
		t.Errorf("original topic preserved: got %s", getHeader(rec2.Headers, "x-original-topic"))
	}
	expectedNotBefore2 := startTime.Add(1*time.Minute + 10*time.Minute).Format(time.RFC3339)
	if getHeader(rec2.Headers, "x-not-before") != expectedNotBefore2 {
		t.Errorf("stage 2 x-not-before = %s, want %s", getHeader(rec2.Headers, "x-not-before"), expectedNotBefore2)
	}

	// Stage 3: Failure on 10m retry topic -> erp.notification.retry.1h
	clock.Advance(10 * time.Minute)
	rec2.Offset = 60
	err = handler.Handle(ctx, rec2, transientErr)
	if err != nil {
		t.Fatalf("stage 3 handle failed: %v", err)
	}

	published = publisher.GetPublished()
	if len(published) != 3 {
		t.Fatalf("expected 3 published messages, got %d", len(published))
	}
	rec3 := published[2]
	if rec3.Topic != "erp.notification.retry.1h" {
		t.Errorf("stage 3 topic = %s, want erp.notification.retry.1h", rec3.Topic)
	}
	if getHeader(rec3.Headers, "x-attempt") != "3" {
		t.Errorf("stage 3 x-attempt = %s, want 3", getHeader(rec3.Headers, "x-attempt"))
	}

	// Stage 4: Retries exhausted on 1h retry topic -> erp.notification.dlq + DB insert
	clock.Advance(1 * time.Hour)
	rec3.Offset = 70
	err = handler.Handle(ctx, rec3, transientErr)
	if err != nil {
		t.Fatalf("stage 4 handle failed: %v", err)
	}

	published = publisher.GetPublished()
	if len(published) != 4 {
		t.Fatalf("expected 4 published messages, got %d", len(published))
	}
	rec4 := published[3]
	if rec4.Topic != "erp.notification.dlq" {
		t.Errorf("stage 4 topic = %s, want erp.notification.dlq", rec4.Topic)
	}
	if getHeader(rec4.Headers, "x-attempt") != "4" {
		t.Errorf("stage 4 x-attempt = %s, want 4", getHeader(rec4.Headers, "x-attempt"))
	}
	// x-not-before must NOT be attached to DLQ records
	if nb := getHeader(rec4.Headers, "x-not-before"); nb != "" {
		t.Errorf("expected no x-not-before on DLQ record, got %s", nb)
	}

	// Verify database insert
	dlRepo.mu.Lock()
	defer dlRepo.mu.Unlock()
	if len(dlRepo.deadLetters) != 1 {
		t.Fatalf("expected 1 dead letter row, got %d", len(dlRepo.deadLetters))
	}
	for _, dl := range dlRepo.deadLetters {
		if *dl.Topic != "erp.sales.events" {
			t.Errorf("expected original topic erp.sales.events in DB, got %s", *dl.Topic)
		}
		if *dl.Partition != 2 {
			t.Errorf("expected original partition 2 in DB, got %d", *dl.Partition)
		}
		if *dl.KafkaOffset != 105 {
			t.Errorf("expected original offset 105 in DB, got %d", *dl.KafkaOffset)
		}
	}

	// Verify metrics
	snap := metrics.Snapshot()
	if snap.RetriedTotal != 3 {
		t.Errorf("expected 3 retried, got %d", snap.RetriedTotal)
	}
	if snap.DLQTotal != 1 {
		t.Errorf("expected 1 dlq, got %d", snap.DLQTotal)
	}
	if snap.FailedTotal != 4 {
		t.Errorf("expected 4 failed, got %d", snap.FailedTotal)
	}
}

func TestFailureHandler_PermanentDirectToDLQ(t *testing.T) {
	publisher := NewMockPublisher()
	dlRepo := newMockDeadLetterRepo()
	metrics := NewMetrics()
	handler := NewStandardFailureHandler(publisher, dlRepo, metrics, nil, DefaultRetryConfig(), nil)

	record := &kgo.Record{
		Key:       []byte("key-1"),
		Value:     []byte("not-valid-json"),
		Topic:     "erp.purchasing.events",
		Partition: 1,
		Offset:    55,
	}

	permErr := &ingest.PermanentError{Cause: errors.New("invalid JSON payload")}

	err := handler.Handle(context.Background(), record, permErr)
	if err != nil {
		t.Fatalf("permanent failure handle failed: %v", err)
	}

	// Must go directly to DLQ topic without going through retry topics
	published := publisher.GetPublished()
	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	if published[0].Topic != "erp.notification.dlq" {
		t.Errorf("expected direct to erp.notification.dlq, got %s", published[0].Topic)
	}

	// Check DB row has raw_value populated for non-JSON
	dlRepo.mu.Lock()
	defer dlRepo.mu.Unlock()
	if len(dlRepo.deadLetters) != 1 {
		t.Fatalf("expected 1 dead letter row, got %d", len(dlRepo.deadLetters))
	}
	for _, dl := range dlRepo.deadLetters {
		if dl.RawValue == nil || string(dl.RawValue) != "not-valid-json" {
			t.Errorf("expected raw_value to contain original bytes")
		}
		if dl.RawEvent != nil {
			t.Errorf("expected raw_event to be nil for invalid JSON")
		}
	}

	snap := metrics.Snapshot()
	if snap.RetriedTotal != 0 {
		t.Errorf("expected 0 retried for permanent, got %d", snap.RetriedTotal)
	}
	if snap.DLQTotal != 1 {
		t.Errorf("expected 1 dlq for permanent, got %d", snap.DLQTotal)
	}
}

func TestFailureHandler_SanitizeErrorSecretsAndTruncate(t *testing.T) {
	publisher := NewMockPublisher()
	dlRepo := newMockDeadLetterRepo()
	handler := NewStandardFailureHandler(publisher, dlRepo, NewMetrics(), nil, DefaultRetryConfig(), nil)

	longAndSecretErr := &ingest.PermanentError{
		Cause: fmt.Errorf("connection failed: postgres://user:super_secret_pw@localhost:5432/db?token=secret-token-123456789 and password=plaintext-pass %s", strings.Repeat("A", 300)),
	}

	record := &kgo.Record{
		Key:       []byte("k"),
		Value:     []byte(`{}`),
		Topic:     "test",
		Partition: 0,
		Offset:    1,
	}

	err := handler.Handle(context.Background(), record, longAndSecretErr)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}

	published := publisher.GetPublished()
	errHeader := getHeader(published[0].Headers, "x-error")

	// Secrets must be redacted
	if strings.Contains(errHeader, "super_secret_pw") {
		t.Error("error header exposed password")
	}
	if strings.Contains(errHeader, "secret-token") {
		t.Error("error header exposed token")
	}
	if strings.Contains(errHeader, "plaintext-pass") {
		t.Error("error header exposed password")
	}

	// Length must not exceed 200 chars
	if len(errHeader) > 200 {
		t.Errorf("error header length %d exceeds 200 char limit", len(errHeader))
	}
}

func TestFailureHandler_PublisherFailureReturnsError(t *testing.T) {
	publisher := NewMockPublisher()
	publisher.Err = errors.New("kafka broker unavailable")
	dlRepo := newMockDeadLetterRepo()

	handler := NewStandardFailureHandler(publisher, dlRepo, NewMetrics(), nil, DefaultRetryConfig(), nil)

	record := &kgo.Record{Topic: "test", Value: []byte(`{}`)}
	err := handler.Handle(context.Background(), record, &ingest.TransientError{Cause: errors.New("fail")})
	if err == nil {
		t.Fatal("expected error when publisher fails, got nil")
	}
}
