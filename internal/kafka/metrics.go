package kafka

import (
	"sync"
	"sync/atomic"
	"time"
)

// MetricsSnapshot is a thread-safe snapshot of consumer metrics for status/health APIs.
type MetricsSnapshot struct {
	ProcessedTotal int64              `json:"processed_total"`
	RetriedTotal   int64              `json:"retried_total"`
	DLQTotal       int64              `json:"dlq_total"`
	FailedTotal    int64              `json:"failed_total"`
	LastMessageAt  *time.Time         `json:"last_message_at"`
	Assignments    map[string][]int32 `json:"assignments"`
}

// MetricsReader provides read access to consumer metrics.
type MetricsReader interface {
	Snapshot() MetricsSnapshot
	IsHealthy() bool
}

// Metrics tracks consumer throughput and partition assignments in memory.
type Metrics struct {
	processedTotal atomic.Int64
	retriedTotal   atomic.Int64
	dlqTotal       atomic.Int64
	failedTotal    atomic.Int64
	lastMessageAt  atomic.Pointer[time.Time]

	mu          sync.RWMutex
	assignments map[string][]int32
}

// NewMetrics initializes consumer metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		assignments: make(map[string][]int32),
	}
}

// IncProcessed increments the count of successfully processed events.
func (m *Metrics) IncProcessed() {
	m.processedTotal.Add(1)
	now := time.Now().UTC()
	m.lastMessageAt.Store(&now)
}

// IncRetried increments retry attempts.
func (m *Metrics) IncRetried() {
	m.retriedTotal.Add(1)
}

// IncDLQ increments the dead-letter queue count.
func (m *Metrics) IncDLQ() {
	m.dlqTotal.Add(1)
}

// IncFailed increments failed message attempts.
func (m *Metrics) IncFailed() {
	m.failedTotal.Add(1)
}

// SetAssignments updates currently assigned topics and partitions from Kafka consumer callbacks.
func (m *Metrics) SetAssignments(assignments map[string][]int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make(map[string][]int32, len(assignments))
	for k, v := range assignments {
		parts := make([]int32, len(v))
		copy(parts, v)
		cp[k] = parts
	}
	m.assignments = cp
}

// Snapshot returns a copy of current metrics values.
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	assignmentsCopy := make(map[string][]int32, len(m.assignments))
	for k, v := range m.assignments {
		parts := make([]int32, len(v))
		copy(parts, v)
		assignmentsCopy[k] = parts
	}

	return MetricsSnapshot{
		ProcessedTotal: m.processedTotal.Load(),
		RetriedTotal:   m.retriedTotal.Load(),
		DLQTotal:       m.dlqTotal.Load(),
		FailedTotal:    m.failedTotal.Load(),
		LastMessageAt:  m.lastMessageAt.Load(),
		Assignments:    assignmentsCopy,
	}
}

// IsHealthy returns true if there is at least one active partition assignment.
func (m *Metrics) IsHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, parts := range m.assignments {
		if len(parts) > 0 {
			return true
		}
	}
	return false
}
