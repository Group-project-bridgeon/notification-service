package kafka

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"notification-service/internal/domain"
	"notification-service/internal/metrics"
)

// ConsumerStatusProvider provides consumer lag and execution metrics.
type ConsumerStatusProvider interface {
	GetStatus(ctx context.Context) *domain.ConsumerStatus
}

type lagCache struct {
	cachedAt time.Time
	state    string
	topics   []domain.ConsumerTopicStatus
}

// ConsumerLagTracker caches Kafka admin lag queries for 5 seconds.
type ConsumerLagTracker struct {
	consumer *Consumer
	mu       sync.Mutex
	cache    lagCache
}

// NewConsumerLagTracker creates a ConsumerLagTracker.
func NewConsumerLagTracker(consumer *Consumer) *ConsumerLagTracker {
	return &ConsumerLagTracker{
		consumer: consumer,
	}
}

// GetStatus returns the consumer group status with 5s cached lag details.
// If Kafka is unreachable, returns 200 with state=disconnected and empty topics, never failing.
func (t *ConsumerLagTracker) GetStatus(ctx context.Context) *domain.ConsumerStatus {
	snap := t.consumer.metrics.Snapshot()

	resp := &domain.ConsumerStatus{
		GroupID:        t.consumer.cfg.Kafka.GroupID,
		State:          "running",
		Topics:         []domain.ConsumerTopicStatus{},
		LastMessageAt:  snap.LastMessageAt,
		ProcessedTotal: snap.ProcessedTotal,
		RetriedTotal:   snap.RetriedTotal,
		DLQTotal:       snap.DLQTotal,
		FailedTotal:    snap.FailedTotal,
	}

	if !t.consumer.enabled || !t.consumer.running.Load() || t.consumer.client == nil {
		resp.State = "disconnected"
		return resp
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check 5-second cache
	if time.Since(t.cache.cachedAt) < 5*time.Second && t.cache.cachedAt.After(time.Time{}) {
		resp.State = t.cache.state
		resp.Topics = t.cache.topics
		return resp
	}

	queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	adm := kadm.NewClient(t.consumer.client)
	lags, err := adm.Lag(queryCtx, t.consumer.cfg.Kafka.GroupID)
	if err != nil {
		t.consumer.logger.WarnContext(ctx, "failed querying Kafka admin lag, returning disconnected", "error", err)
		resp.State = "disconnected"
		return resp
	}

	groupLag, ok := lags[t.consumer.cfg.Kafka.GroupID]
	if !ok || groupLag.Error() != nil {
		resp.State = "disconnected"
		return resp
	}

	topicMap := make(map[string][]domain.ConsumerPartitionStatus)
	for _, memberLag := range groupLag.Lag.Sorted() {
		metrics.Default().SetConsumerLag(memberLag.Topic, int(memberLag.Partition), memberLag.Lag)
		topicMap[memberLag.Topic] = append(topicMap[memberLag.Topic], domain.ConsumerPartitionStatus{
			Partition:       int(memberLag.Partition),
			CommittedOffset: memberLag.Commit.At,
			EndOffset:       memberLag.End.Offset,
			Lag:             memberLag.Lag,
		})
	}

	var topics []domain.ConsumerTopicStatus
	for topic, parts := range topicMap {
		topics = append(topics, domain.ConsumerTopicStatus{
			Topic:      topic,
			Partitions: parts,
		})
	}

	state := strings.ToLower(groupLag.State)
	if state == "" || state == "stable" {
		state = "running"
	}

	t.cache = lagCache{
		cachedAt: time.Now(),
		state:    state,
		topics:   topics,
	}

	resp.State = state
	resp.Topics = topics
	return resp
}
