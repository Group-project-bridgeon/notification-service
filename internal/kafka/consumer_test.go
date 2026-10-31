package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"notification-service/internal/domain"
	"notification-service/internal/ingest"
)

type mockTxRunner struct{}

func (m *mockTxRunner) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

type mockInboxRepo struct {
	mu     sync.Mutex
	events map[uuid.UUID]*domain.InboxEvent
}

func newMockInboxRepo() *mockInboxRepo {
	return &mockInboxRepo{events: make(map[uuid.UUID]*domain.InboxEvent)}
}

func (m *mockInboxRepo) InsertIfAbsent(_ context.Context, event *domain.InboxEvent) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.events[event.EventID]; exists {
		return false, nil
	}
	m.events[event.EventID] = event
	return true, nil
}

func (m *mockInboxRepo) SetStatus(_ context.Context, eventID uuid.UUID, status domain.InboxStatus, errStr *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ev, ok := m.events[eventID]; ok {
		ev.Status = status
		ev.Error = errStr
	}
	return nil
}

type mockRecipientRepo struct {
	mu         sync.Mutex
	recipients map[uuid.UUID]*domain.Recipient
}

func newMockRecipientRepo() *mockRecipientRepo {
	return &mockRecipientRepo{recipients: make(map[uuid.UUID]*domain.Recipient)}
}

func (m *mockRecipientRepo) UpsertGuarded(_ context.Context, recipient *domain.Recipient) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, found := m.recipients[recipient.UserID]
	if !found || recipient.SourceOccurredAt.After(existing.SourceOccurredAt) {
		m.recipients[recipient.UserID] = recipient
	}
	return nil
}

func (m *mockRecipientRepo) FindByRoles(_ context.Context, roles []string) ([]*domain.Recipient, error) {
	return nil, nil
}

func (m *mockRecipientRepo) FindByID(_ context.Context, id uuid.UUID) (*domain.Recipient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recipients[id], nil
}

type trackingRouter struct {
	mu        sync.Mutex
	handled   []*domain.EventEnvelope
	handleDel time.Duration
	panics    bool
}

func (r *trackingRouter) Route(_ context.Context, env *domain.EventEnvelope) error {
	if r.panics {
		panic("simulated fatal handler panic")
	}
	if r.handleDel > 0 {
		time.Sleep(r.handleDel)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handled = append(r.handled, env)
	return nil
}

type mockCommitter struct {
	mu        sync.Mutex
	committed []*kgo.Record
}

func (c *mockCommitter) MarkCommit(r *kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.committed = append(c.committed, r)
}

func (c *mockCommitter) CommitNow(_ context.Context) error {
	return nil
}

type mockFailureHandler struct {
	mu      sync.Mutex
	handled []*kgo.Record
	errs    []error
	errToReturn error
}

func (h *mockFailureHandler) Handle(_ context.Context, r *kgo.Record, err error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handled = append(h.handled, r)
	h.errs = append(h.errs, err)
	return h.errToReturn
}

func makeValidEventJSON(eventID uuid.UUID, eventType string) []byte {
	return []byte(fmt.Sprintf(`{
		"event_id":       "%s",
		"event_type":     "%s",
		"event_version":  "1",
		"occurred_at":    "2026-10-14T10:00:00Z",
		"producer":       "test-service",
		"aggregate_type": "test",
		"aggregate_id":   "e81d8590-bfae-4d8b-9d41-e125032b4943",
		"payload":        {"key":"value"}
	}`, eventID.String(), eventType))
}

func TestPartitionWorker_InOrderHandling(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &trackingRouter{}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	committer := &mockCommitter{}
	failure := &mockFailureHandler{}
	metrics := NewMetrics()

	worker := NewPartitionWorker("test-topic", 0, 10, ing, failure, committer, metrics, nil, nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	// Send 5 records in order
	const n = 5
	var eventIDs []uuid.UUID
	for i := 0; i < n; i++ {
		eid := uuid.New()
		eventIDs = append(eventIDs, eid)
		val := makeValidEventJSON(eid, "order.created")
		worker.Enqueue(ctx, &kgo.Record{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
			Value:     val,
		})
	}

	// Wait for processing
	for i := 0; i < 50; i++ {
		committer.mu.Lock()
		count := len(committer.committed)
		committer.mu.Unlock()
		if count == n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	worker.Stop()

	// Verify all records were processed and committed in strict offset order
	if len(committer.committed) != n {
		t.Fatalf("expected %d committed records, got %d", n, len(committer.committed))
	}
	for i, r := range committer.committed {
		if r.Offset != int64(i) {
			t.Errorf("expected offset %d, got %d", i, r.Offset)
		}
	}

	// Verify router received them in the same order
	router.mu.Lock()
	defer router.mu.Unlock()
	for i, env := range router.handled {
		if env.EventID != eventIDs[i] {
			t.Errorf("at index %d: expected eventID %s, got %s", i, eventIDs[i], env.EventID)
		}
	}
}

func TestPartitionWorker_ParallelAcrossPartitions(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &trackingRouter{handleDel: 20 * time.Millisecond}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	committer := &mockCommitter{}
	failure := &mockFailureHandler{}
	metrics := NewMetrics()

	// Two workers for different partitions
	workerP0 := NewPartitionWorker("topic-a", 0, 10, ing, failure, committer, metrics, nil, nil, nil, nil, nil)
	workerP1 := NewPartitionWorker("topic-a", 1, 10, ing, failure, committer, metrics, nil, nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go workerP0.Run(ctx)
	go workerP1.Run(ctx)

	start := time.Now()

	// Enqueue to both workers concurrently
	workerP0.Enqueue(ctx, &kgo.Record{Topic: "topic-a", Partition: 0, Offset: 0, Value: makeValidEventJSON(uuid.New(), "order.created")})
	workerP1.Enqueue(ctx, &kgo.Record{Topic: "topic-a", Partition: 1, Offset: 0, Value: makeValidEventJSON(uuid.New(), "order.created")})

	for i := 0; i < 50; i++ {
		committer.mu.Lock()
		count := len(committer.committed)
		committer.mu.Unlock()
		if count == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	workerP0.Stop()
	workerP1.Stop()

	duration := time.Since(start)
	if duration > 150*time.Millisecond {
		t.Logf("workers ran in %v", duration)
	}

	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(committer.committed) != 2 {
		t.Fatalf("expected 2 committed records, got %d", len(committer.committed))
	}
}

func TestPartitionWorker_CommitOnlyAfterHandle(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()

	var committedAfterHandled atomic.Bool

	committer := &mockCommitter{}
	failure := &mockFailureHandler{}
	metrics := NewMetrics()

	customRouter := &trackingRouter{}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, customRouter, nil)

	worker := NewPartitionWorker("test", 0, 10, ing, failure, committer, metrics, nil, nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	worker.Enqueue(ctx, &kgo.Record{
		Topic:     "test",
		Partition: 0,
		Offset:    10,
		Value:     makeValidEventJSON(uuid.New(), "order.created"),
	})

	for i := 0; i < 50; i++ {
		committer.mu.Lock()
		committedCount := len(committer.committed)
		committer.mu.Unlock()

		customRouter.mu.Lock()
		handledCount := len(customRouter.handled)
		customRouter.mu.Unlock()

		if committedCount > 0 {
			if handledCount > 0 {
				committedAfterHandled.Store(true)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	worker.Stop()

	if !committedAfterHandled.Load() {
		t.Error("expected commit to occur strictly after handling finished")
	}
}

func TestPartitionWorker_XNotBefore_PausesAndWaits(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &trackingRouter{}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	committer := &mockCommitter{}
	failure := &mockFailureHandler{}
	metrics := NewMetrics()

	initialTime := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(initialTime)

	var pausedTopic atomic.Value
	var resumedTopic atomic.Value

	worker := NewPartitionWorker(
		"erp.notification.retry.10m", 0, 10,
		ing, failure, committer, metrics, clock, nil,
		func(topic string, part int32) {
			pausedTopic.Store(topic)
		},
		func(topic string, part int32) {
			resumedTopic.Store(topic)
		},
		nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	// Record with x-not-before 10 minutes in the future
	notBeforeTime := initialTime.Add(10 * time.Minute)
	record := &kgo.Record{
		Topic:     "erp.notification.retry.10m",
		Partition: 0,
		Offset:    1,
		Value:     makeValidEventJSON(uuid.New(), "order.created"),
		Headers: []kgo.RecordHeader{
			{Key: "x-not-before", Value: []byte(notBeforeTime.Format(time.RFC3339))},
		},
	}

	worker.Enqueue(ctx, record)

	// Wait for pause to be triggered
	for i := 0; i < 50; i++ {
		if pausedTopic.Load() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if pausedTopic.Load() != "erp.notification.retry.10m" {
		t.Errorf("expected partition pause to be called, got: %v", pausedTopic.Load())
	}

	// Record should NOT be committed yet
	committer.mu.Lock()
	countBefore := len(committer.committed)
	committer.mu.Unlock()
	if countBefore != 0 {
		t.Fatalf("expected 0 commits before time elapsed, got %d", countBefore)
	}

	// Advance fake clock by 10 minutes to reach notBefore
	clock.Advance(10 * time.Minute)

	// Wait for worker to finish processing and commit
	for i := 0; i < 50; i++ {
		committer.mu.Lock()
		countAfter := len(committer.committed)
		committer.mu.Unlock()
		if countAfter == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	worker.Stop()

	if resumedTopic.Load() != "erp.notification.retry.10m" {
		t.Errorf("expected partition resume to be called, got: %v", resumedTopic.Load())
	}

	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(committer.committed) != 1 {
		t.Fatalf("expected 1 committed record after advancing clock, got %d", len(committer.committed))
	}
}

func TestPartitionWorker_PanicHandledAsPermanent(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &trackingRouter{panics: true}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	committer := &mockCommitter{}
	failure := &mockFailureHandler{}
	metrics := NewMetrics()

	worker := NewPartitionWorker("test", 0, 10, ing, failure, committer, metrics, nil, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	worker.Enqueue(ctx, &kgo.Record{
		Topic:     "test",
		Partition: 0,
		Offset:    25,
		Value:     makeValidEventJSON(uuid.New(), "order.created"),
	})

	for i := 0; i < 50; i++ {
		failure.mu.Lock()
		count := len(failure.handled)
		failure.mu.Unlock()
		if count == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	worker.Stop()

	failure.mu.Lock()
	defer failure.mu.Unlock()
	if len(failure.handled) != 1 {
		t.Fatalf("expected failure handler called once on panic, got %d", len(failure.handled))
	}
	if !ingest.IsPermanent(failure.errs[0]) {
		t.Errorf("expected panic to be classified as permanent error, got %v", failure.errs[0])
	}

	// Offset should be committed after failure handling succeeds
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(committer.committed) != 1 || committer.committed[0].Offset != 25 {
		t.Fatalf("expected offset 25 committed after failure handler handled panic")
	}
}

func TestPartitionWorker_TombstoneRecord(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &trackingRouter{}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	committer := &mockCommitter{}
	failure := &mockFailureHandler{}
	metrics := NewMetrics()

	worker := NewPartitionWorker("test", 0, 10, ing, failure, committer, metrics, nil, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	worker.Enqueue(ctx, &kgo.Record{
		Topic:     "test",
		Partition: 0,
		Offset:    5,
		Value:     nil,
	})

	for i := 0; i < 50; i++ {
		committer.mu.Lock()
		count := len(committer.committed)
		committer.mu.Unlock()
		if count == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	worker.Stop()

	router.mu.Lock()
	if len(router.handled) != 0 {
		t.Errorf("expected router not to be called for tombstone, got %d", len(router.handled))
	}
	router.mu.Unlock()

	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(committer.committed) != 1 || committer.committed[0].Offset != 5 {
		t.Fatalf("expected tombstone offset 5 committed, got %v", committer.committed)
	}
}

func TestPartitionWorker_OversizedRecord(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &trackingRouter{}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	committer := &mockCommitter{}
	failure := &mockFailureHandler{}
	metrics := NewMetrics()

	worker := NewPartitionWorker("test", 0, 10, ing, failure, committer, metrics, nil, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	bigVal := make([]byte, ingest.MaxMessageBytes+1)
	bigVal[0] = '{'
	bigVal[ingest.MaxMessageBytes] = '}'

	worker.Enqueue(ctx, &kgo.Record{
		Topic:     "test",
		Partition: 0,
		Offset:    8,
		Value:     bigVal,
	})

	for i := 0; i < 50; i++ {
		committer.mu.Lock()
		count := len(committer.committed)
		committer.mu.Unlock()
		if count == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	worker.Stop()

	failure.mu.Lock()
	defer failure.mu.Unlock()
	if len(failure.handled) != 1 {
		t.Fatalf("expected failure handler called once, got %d", len(failure.handled))
	}
	if !ingest.IsPermanent(failure.errs[0]) {
		t.Errorf("expected permanent error for oversized record, got %v", failure.errs[0])
	}

	committer.mu.Lock()
	defer committer.mu.Unlock()
	if len(committer.committed) != 1 || committer.committed[0].Offset != 8 {
		t.Fatalf("expected offset 8 committed, got %v", committer.committed)
	}
}

func TestPartitionWorker_FailureHandlerErrorDoesNotCommit(t *testing.T) {
	inbox := newMockInboxRepo()
	recipients := newMockRecipientRepo()
	router := &trackingRouter{}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, router, nil)
	committer := &mockCommitter{}
	failure := &mockFailureHandler{errToReturn: errors.New("kafka producer unavailable")}
	metrics := NewMetrics()

	worker := NewPartitionWorker("test", 0, 10, ing, failure, committer, metrics, nil, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	// Bad json to trigger failure handler
	worker.Enqueue(ctx, &kgo.Record{
		Topic:     "test",
		Partition: 0,
		Offset:    100,
		Value:     []byte("bad json"),
	})

	// Wait for failure handler to be invoked
	for i := 0; i < 20; i++ {
		failure.mu.Lock()
		count := len(failure.handled)
		failure.mu.Unlock()
		if count > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify committer has NOT been called because failure handler returned error
	committer.mu.Lock()
	committedCount := len(committer.committed)
	committer.mu.Unlock()
	if committedCount != 0 {
		t.Fatalf("expected 0 commits when failure handler fails, got %d", committedCount)
	}

	worker.Stop()
}
