package metrics

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultBuckets are standard histogram buckets for handler duration in seconds.
var DefaultBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}

// Registry maintains in-memory counters, gauges, and histograms for Prometheus scraping.
type Registry struct {
	mu sync.RWMutex

	// messages_consumed_total{topic,result}
	consumed map[string]*atomic.Uint64

	// handler_duration_seconds histogram
	buckets       []float64
	bucketCounts  []*atomic.Uint64
	durationSumBits atomic.Uint64
	durationCount atomic.Uint64

	// consumer_lag{topic,partition}
	consumerLag map[string]*atomic.Int64

	// retry_published_total{topic}
	retryPublished map[string]*atomic.Uint64

	// dlq_published_total{topic}
	dlqPublished map[string]*atomic.Uint64

	// inbox_duplicates_total
	inboxDuplicates atomic.Uint64

	// email_queue_depth provider
	emailQueueDepthProvider func() int64

	// sse_connections provider
	sseConnectionsProvider func() int64
}

// Global registry instance
var (
	defaultRegistry *Registry
	once            sync.Once
)

// Default returns the singleton global metrics registry.
func Default() *Registry {
	once.Do(func() {
		defaultRegistry = NewRegistry()
	})
	return defaultRegistry
}

// NewRegistry initializes a clean Registry.
func NewRegistry() *Registry {
	r := &Registry{
		consumed:       make(map[string]*atomic.Uint64),
		buckets:        DefaultBuckets,
		bucketCounts:   make([]*atomic.Uint64, len(DefaultBuckets)),
		consumerLag:    make(map[string]*atomic.Int64),
		retryPublished: make(map[string]*atomic.Uint64),
		dlqPublished:   make(map[string]*atomic.Uint64),
	}
	for i := range r.bucketCounts {
		r.bucketCounts[i] = &atomic.Uint64{}
	}
	return r
}

// SetEmailQueueDepthProvider configures the provider function for email_queue_depth.
func (r *Registry) SetEmailQueueDepthProvider(fn func() int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.emailQueueDepthProvider = fn
}

// SetSSEConnectionsProvider configures the provider function for sse_connections.
func (r *Registry) SetSSEConnectionsProvider(fn func() int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sseConnectionsProvider = fn
}

// RecordConsumed records a consumed message with topic, outcome result, and duration.
func (r *Registry) RecordConsumed(topic, result string, duration time.Duration) {
	key := fmt.Sprintf(`topic="%s",result="%s"`, topic, result)

	r.mu.RLock()
	counter, ok := r.consumed[key]
	r.mu.RUnlock()

	if !ok {
		r.mu.Lock()
		counter, ok = r.consumed[key]
		if !ok {
			counter = &atomic.Uint64{}
			r.consumed[key] = counter
		}
		r.mu.Unlock()
	}
	counter.Add(1)

	// Record duration in seconds
	durSec := duration.Seconds()
	r.durationCount.Add(1)

	// Atomic float addition for sum
	for {
		oldBits := r.durationSumBits.Load()
		newVal := math.Float64frombits(oldBits) + durSec
		newBits := math.Float64bits(newVal)
		if r.durationSumBits.CompareAndSwap(oldBits, newBits) {
			break
		}
	}

	for i, b := range r.buckets {
		if durSec <= b {
			r.bucketCounts[i].Add(1)
		}
	}
}

// SetConsumerLag updates the consumer_lag gauge for a topic and partition.
func (r *Registry) SetConsumerLag(topic string, partition int, lag int64) {
	key := fmt.Sprintf(`topic="%s",partition="%d"`, topic, partition)

	r.mu.RLock()
	gauge, ok := r.consumerLag[key]
	r.mu.RUnlock()

	if !ok {
		r.mu.Lock()
		gauge, ok = r.consumerLag[key]
		if !ok {
			gauge = &atomic.Int64{}
			r.consumerLag[key] = gauge
		}
		r.mu.Unlock()
	}
	gauge.Store(lag)
}

// IncRetryPublished increments retry_published_total for a topic.
func (r *Registry) IncRetryPublished(topic string) {
	r.mu.RLock()
	counter, ok := r.retryPublished[topic]
	r.mu.RUnlock()

	if !ok {
		r.mu.Lock()
		counter, ok = r.retryPublished[topic]
		if !ok {
			counter = &atomic.Uint64{}
			r.retryPublished[topic] = counter
		}
		r.mu.Unlock()
	}
	counter.Add(1)
}

// IncDLQPublished increments dlq_published_total for a topic.
func (r *Registry) IncDLQPublished(topic string) {
	r.mu.RLock()
	counter, ok := r.dlqPublished[topic]
	r.mu.RUnlock()

	if !ok {
		r.mu.Lock()
		counter, ok = r.dlqPublished[topic]
		if !ok {
			counter = &atomic.Uint64{}
			r.dlqPublished[topic] = counter
		}
		r.mu.Unlock()
	}
	counter.Add(1)
}

// IncInboxDuplicates increments inbox_duplicates_total.
func (r *Registry) IncInboxDuplicates() {
	r.inboxDuplicates.Add(1)
}

// RenderPrometheus formats all tracked metrics in standard Prometheus text exposition format (v0.0.4).
func (r *Registry) RenderPrometheus() []byte {
	var buf bytes.Buffer

	// 1. messages_consumed_total
	buf.WriteString("# HELP messages_consumed_total Total messages consumed by topic and result\n")
	buf.WriteString("# TYPE messages_consumed_total counter\n")
	r.mu.RLock()
	var consumedKeys []string
	for k := range r.consumed {
		consumedKeys = append(consumedKeys, k)
	}
	sort.Strings(consumedKeys)
	for _, k := range consumedKeys {
		buf.WriteString(fmt.Sprintf("messages_consumed_total{%s} %d\n", k, r.consumed[k].Load()))
	}
	r.mu.RUnlock()

	// 2. handler_duration_seconds
	buf.WriteString("# HELP handler_duration_seconds Ingestion handler execution duration in seconds\n")
	buf.WriteString("# TYPE handler_duration_seconds histogram\n")
	var cumulative uint64
	for i, b := range r.buckets {
		cumulative += r.bucketCounts[i].Load()
		buf.WriteString(fmt.Sprintf("handler_duration_seconds_bucket{le=\"%g\"} %d\n", b, cumulative))
	}
	totalCount := r.durationCount.Load()
	buf.WriteString(fmt.Sprintf("handler_duration_seconds_bucket{le=\"+Inf\"} %d\n", totalCount))
	sum := math.Float64frombits(r.durationSumBits.Load())
	buf.WriteString(fmt.Sprintf("handler_duration_seconds_sum %g\n", sum))
	buf.WriteString(fmt.Sprintf("handler_duration_seconds_count %d\n", totalCount))

	// 3. consumer_lag
	buf.WriteString("# HELP consumer_lag Consumer offset lag per topic and partition\n")
	buf.WriteString("# TYPE consumer_lag gauge\n")
	r.mu.RLock()
	var lagKeys []string
	for k := range r.consumerLag {
		lagKeys = append(lagKeys, k)
	}
	sort.Strings(lagKeys)
	for _, k := range lagKeys {
		buf.WriteString(fmt.Sprintf("consumer_lag{%s} %d\n", k, r.consumerLag[k].Load()))
	}
	r.mu.RUnlock()

	// 4. retry_published_total
	buf.WriteString("# HELP retry_published_total Total messages published to retry topics\n")
	buf.WriteString("# TYPE retry_published_total counter\n")
	r.mu.RLock()
	var retryKeys []string
	for k := range r.retryPublished {
		retryKeys = append(retryKeys, k)
	}
	sort.Strings(retryKeys)
	for _, k := range retryKeys {
		buf.WriteString(fmt.Sprintf("retry_published_total{topic=\"%s\"} %d\n", k, r.retryPublished[k].Load()))
	}
	r.mu.RUnlock()

	// 5. dlq_published_total
	buf.WriteString("# HELP dlq_published_total Total messages published to dead letter queue\n")
	buf.WriteString("# TYPE dlq_published_total counter\n")
	r.mu.RLock()
	var dlqKeys []string
	for k := range r.dlqPublished {
		dlqKeys = append(dlqKeys, k)
	}
	sort.Strings(dlqKeys)
	for _, k := range dlqKeys {
		buf.WriteString(fmt.Sprintf("dlq_published_total{topic=\"%s\"} %d\n", k, r.dlqPublished[k].Load()))
	}
	r.mu.RUnlock()

	// 6. inbox_duplicates_total
	buf.WriteString("# HELP inbox_duplicates_total Total duplicate inbox messages detected and skipped\n")
	buf.WriteString("# TYPE inbox_duplicates_total counter\n")
	buf.WriteString(fmt.Sprintf("inbox_duplicates_total %d\n", r.inboxDuplicates.Load()))

	// 7. email_queue_depth
	buf.WriteString("# HELP email_queue_depth Current number of queued transactional and business emails\n")
	buf.WriteString("# TYPE email_queue_depth gauge\n")
	var queueDepth int64
	r.mu.RLock()
	if r.emailQueueDepthProvider != nil {
		queueDepth = r.emailQueueDepthProvider()
	}
	r.mu.RUnlock()
	buf.WriteString(fmt.Sprintf("email_queue_depth %d\n", queueDepth))

	// 8. sse_connections
	buf.WriteString("# HELP sse_connections Current number of active SSE client connections\n")
	buf.WriteString("# TYPE sse_connections gauge\n")
	var sseConns int64
	r.mu.RLock()
	if r.sseConnectionsProvider != nil {
		sseConns = r.sseConnectionsProvider()
	}
	r.mu.RUnlock()
	buf.WriteString(fmt.Sprintf("sse_connections %d\n", sseConns))

	return buf.Bytes()
}
