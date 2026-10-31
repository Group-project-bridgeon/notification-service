package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	"notification-service/internal/ingest"
	"notification-service/internal/metrics"
)

// Committer defines the interface for marking and persisting Kafka offset commits.
type Committer interface {
	MarkCommit(record *kgo.Record)
	CommitNow(ctx context.Context) error
}

type partitionKey struct {
	topic     string
	partition int32
}

// PartitionWorker processes records strictly in-order for a single assigned partition.
type PartitionWorker struct {
	topic     string
	partition int32
	records   chan *kgo.Record
	quit      chan struct{}
	done      chan struct{}

	ingestor  *ingest.Ingestor
	failure   FailureHandler
	committer Committer
	metrics   *Metrics
	clock     Clock
	logger    *slog.Logger

	pauseFetch     func(topic string, partition int32)
	resumeFetch    func(topic string, partition int32)
	onLowWatermark func(topic string, partition int32)
}

// NewPartitionWorker creates a PartitionWorker.
func NewPartitionWorker(
	topic string,
	partition int32,
	bufferSize int,
	ingestor *ingest.Ingestor,
	failure FailureHandler,
	committer Committer,
	metrics *Metrics,
	clock Clock,
	logger *slog.Logger,
	pauseFetch func(topic string, partition int32),
	resumeFetch func(topic string, partition int32),
	onLowWatermark func(topic string, partition int32),
) *PartitionWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if clock == nil {
		clock = NewRealClock()
	}
	return &PartitionWorker{
		topic:          topic,
		partition:      partition,
		records:        make(chan *kgo.Record, bufferSize),
		quit:           make(chan struct{}),
		done:           make(chan struct{}),
		ingestor:       ingestor,
		failure:        failure,
		committer:      committer,
		metrics:        metrics,
		clock:          clock,
		logger:         logger,
		pauseFetch:     pauseFetch,
		resumeFetch:    resumeFetch,
		onLowWatermark: onLowWatermark,
	}
}

// Run starts the processing loop for this partition worker.
func (pw *PartitionWorker) Run(ctx context.Context) {
	defer close(pw.done)

	for {
		select {
		case <-ctx.Done():
			return
		case <-pw.quit:
			return
		case record, ok := <-pw.records:
			if !ok {
				return
			}
			pw.processRecord(ctx, record)

			if pw.onLowWatermark != nil && len(pw.records) <= 20 {
				pw.onLowWatermark(pw.topic, pw.partition)
			}
		}
	}
}

// Stop signals the worker to terminate after completing the current in-flight record.
func (pw *PartitionWorker) Stop() {
	close(pw.quit)
	<-pw.done
}

// Enqueue adds a record to the worker queue, respecting worker shutdown or context cancellation.
func (pw *PartitionWorker) Enqueue(ctx context.Context, record *kgo.Record) bool {
	select {
	case <-ctx.Done():
		return false
	case <-pw.quit:
		return false
	case pw.records <- record:
		return true
	}
}

// QueueLen returns the count of buffered records awaiting processing.
func (pw *PartitionWorker) QueueLen() int {
	return len(pw.records)
}

func (pw *PartitionWorker) processRecord(ctx context.Context, record *kgo.Record) {
	// Panic recovery: any handler panic is classified as a PermanentError
	defer func() {
		if r := recover(); r != nil {
			pw.logger.ErrorContext(ctx, "panic recovered in partition worker", "panic", r)
			panicErr := &ingest.PermanentError{Cause: fmt.Errorf("worker panic: %v", r)}
			if pw.handleFailureWithBackoff(ctx, record, panicErr) {
				pw.committer.MarkCommit(record)
			}
		}
	}()

	// 1. Delayed Retry Scheduling (x-not-before): pause partition without busy loop
	notBeforeStr := getHeader(record.Headers, "x-not-before")
	if notBeforeStr != "" {
		if notBefore, err := time.Parse(time.RFC3339, notBeforeStr); err == nil {
			now := pw.clock.Now()
			if now.Before(notBefore) {
				delay := notBefore.Sub(now)
				pw.logger.InfoContext(ctx, "record x-not-before is in the future, pausing partition fetch",
					"topic", record.Topic,
					"partition", record.Partition,
					"not_before", notBeforeStr,
					"delay", delay,
				)
				if pw.pauseFetch != nil {
					pw.pauseFetch(record.Topic, record.Partition)
				}
				select {
				case <-pw.clock.After(delay):
				case <-pw.quit:
					return
				case <-ctx.Done():
					return
				}
				if pw.resumeFetch != nil {
					pw.resumeFetch(record.Topic, record.Partition)
				}
			}
		}
	}

	start := time.Now()
	var (
		eventIDStr = "none"
		corrIDStr  = "none"
		result     = "success"
	)

	// 2. Tombstone: nil value is skipped and committed
	if record.Value == nil {
		result = "tombstone"
		dur := time.Since(start)
		metrics.Default().RecordConsumed(record.Topic, result, dur)
		pw.logger.InfoContext(ctx, "kafka message consumed",
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"event_id", eventIDStr,
			"correlation_id", corrIDStr,
			"result", result,
			"duration", dur,
		)
		pw.committer.MarkCommit(record)
		return
	}

	// 3. Decode and validate
	env, err := ingest.ParseAndValidate(record.Value)
	if err != nil {
		result = "poison"
		dur := time.Since(start)
		metrics.Default().RecordConsumed(record.Topic, result, dur)
		pw.logger.InfoContext(ctx, "kafka message consumed",
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"event_id", eventIDStr,
			"correlation_id", corrIDStr,
			"result", result,
			"duration", dur,
		)
		if pw.handleFailureWithBackoff(ctx, record, err) {
			pw.committer.MarkCommit(record)
		}
		return
	}

	eventIDStr = env.EventID.String()
	if env.CorrelationID != nil {
		corrIDStr = env.CorrelationID.String()
	}

	// 4. Process through Ingestor
	partInt := int(record.Partition)
	meta := ingest.IngestMeta{
		Topic:     &record.Topic,
		Partition: &partInt,
		Offset:    &record.Offset,
	}

	handleErr := pw.ingestor.Handle(ctx, domain.EventSourceKafka, env, meta)
	if handleErr != nil {
		if ingest.IsTransient(handleErr) {
			result = "retry"
		} else {
			result = "dlq"
		}
		dur := time.Since(start)
		metrics.Default().RecordConsumed(record.Topic, result, dur)
		pw.logger.InfoContext(ctx, "kafka message consumed",
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"event_id", eventIDStr,
			"correlation_id", corrIDStr,
			"result", result,
			"duration", dur,
		)
		if pw.handleFailureWithBackoff(ctx, record, handleErr) {
			pw.committer.MarkCommit(record)
		}
		return
	}

	dur := time.Since(start)
	metrics.Default().RecordConsumed(record.Topic, result, dur)
	pw.logger.InfoContext(ctx, "kafka message consumed",
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"event_id", eventIDStr,
		"correlation_id", corrIDStr,
		"result", result,
		"duration", dur,
	)

	if pw.metrics != nil {
		pw.metrics.IncProcessed()
	}

	// 5. Commit offset only after handling has finished
	pw.committer.MarkCommit(record)
}

func (pw *PartitionWorker) handleFailureWithBackoff(ctx context.Context, record *kgo.Record, err error) bool {
	if pw.failure == nil {
		return true
	}

	backoff := 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return false
		case <-pw.quit:
			return false
		default:
		}

		failErr := pw.failure.Handle(ctx, record, err)
		if failErr == nil {
			return true
		}

		pw.logger.ErrorContext(ctx, "failure handler returned error, backing off",
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"error", failErr,
			"retry_in", backoff,
		)

		select {
		case <-ctx.Done():
			return false
		case <-pw.quit:
			return false
		case <-pw.clock.After(backoff):
		}

		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

// franzCommitter batches offset commits every 100 records or 1 second.
type franzCommitter struct {
	client      *kgo.Client
	mu          sync.Mutex
	uncommitted int
	logger      *slog.Logger
}

func newFranzCommitter(client *kgo.Client, logger *slog.Logger) *franzCommitter {
	if logger == nil {
		logger = slog.Default()
	}
	return &franzCommitter{
		client: client,
		logger: logger,
	}
}

func (c *franzCommitter) MarkCommit(record *kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.client.MarkCommitRecords(record)
	c.uncommitted++

	if c.uncommitted >= 100 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.client.CommitMarkedOffsets(ctx); err != nil {
			c.logger.Error("failed batch commit marked offsets", "error", err)
		} else {
			c.uncommitted = 0
		}
	}
}

func (c *franzCommitter) CommitNow(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.client.CommitMarkedOffsets(ctx); err != nil {
		return err
	}
	c.uncommitted = 0
	return nil
}

// Consumer implements Kafka event ingestion using Franz-Go.
type Consumer struct {
	cfg       *config.Config
	ingestor  *ingest.Ingestor
	failure   FailureHandler
	metrics   *Metrics
	clock     Clock
	logger    *slog.Logger
	enabled   bool
	client    *kgo.Client
	committer *franzCommitter

	workersMu sync.Mutex
	workers   map[partitionKey]*PartitionWorker

	lastPollSuccess atomic.Bool
	running         atomic.Bool
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// NewConsumer initializes the Consumer.
func NewConsumer(
	cfg *config.Config,
	ingestor *ingest.Ingestor,
	failure FailureHandler,
	metrics *Metrics,
	logger *slog.Logger,
) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = NewMetrics()
	}

	return &Consumer{
		cfg:      cfg,
		ingestor: ingestor,
		failure:  failure,
		metrics:  metrics,
		clock:    NewRealClock(),
		logger:   logger,
		enabled:  cfg.Kafka.ConsumerEnabled,
		workers:  make(map[partitionKey]*PartitionWorker),
	}
}

// SetClock sets a custom Clock implementation (used for deterministic testing).
func (c *Consumer) SetClock(clock Clock) {
	c.clock = clock
}

// Start boots up the consumer client and poll loop if enabled.
func (c *Consumer) Start(ctx context.Context) error {
	if !c.enabled {
		c.logger.Info("Kafka consumer disabled by configuration")
		return nil
	}

	allTopics := append([]string{}, c.cfg.Kafka.Topics...)
	allTopics = append(allTopics, c.cfg.Kafka.RetryTopics...)

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.cfg.Kafka.Brokers...),
		kgo.ClientID(c.cfg.Kafka.ClientID),
		kgo.ConsumerGroup(c.cfg.Kafka.GroupID),
		kgo.ConsumeTopics(allTopics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(c.onPartitionsAssigned),
		kgo.OnPartitionsRevoked(c.onPartitionsRevoked),
		kgo.OnPartitionsLost(c.onPartitionsLost),
	}

	if strings.EqualFold(c.cfg.Kafka.StartOffset, "earliest") {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	// Security configuration
	if strings.EqualFold(c.cfg.Kafka.SecurityProtocol, "SASL_SSL") || strings.EqualFold(c.cfg.Kafka.SecurityProtocol, "SASL_PLAINTEXT") {
		switch strings.ToUpper(c.cfg.Kafka.SASLMechanism) {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: c.cfg.Kafka.SASLUsername,
				Pass: c.cfg.Kafka.SASLPassword,
			}.AsMechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: c.cfg.Kafka.SASLUsername,
				Pass: c.cfg.Kafka.SASLPassword,
			}.AsSha512Mechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: c.cfg.Kafka.SASLUsername,
				Pass: c.cfg.Kafka.SASLPassword,
			}.AsSha256Mechanism()))
		}
	}

	if strings.EqualFold(c.cfg.Kafka.SecurityProtocol, "SASL_SSL") || strings.EqualFold(c.cfg.Kafka.SecurityProtocol, "SSL") {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if c.cfg.Kafka.TLSCAFile != "" {
			caCert, err := os.ReadFile(c.cfg.Kafka.TLSCAFile)
			if err != nil {
				return fmt.Errorf("failed to read Kafka TLS CA file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsCfg.RootCAs = caCertPool
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed creating franz-go consumer client: %w", err)
	}

	c.client = cl
	c.committer = newFranzCommitter(cl, c.logger)

	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.running.Store(true)

	// Periodic commit ticker (1s commit interval)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				commitCtx, commitCancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = c.committer.CommitNow(commitCtx)
				commitCancel()
			}
		}
	}()

	// Poll loop
	c.wg.Add(1)
	go c.pollLoop(runCtx)

	c.logger.Info("Kafka consumer started",
		"group_id", c.cfg.Kafka.GroupID,
		"topics", c.cfg.Kafka.Topics,
	)
	return nil
}

func (c *Consumer) pollLoop(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			c.lastPollSuccess.Store(false)
			for _, err := range errs {
				c.logger.Error("kafka poll error", "topic", err.Topic, "partition", err.Partition, "error", err.Err)
			}
		} else {
			c.lastPollSuccess.Store(true)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			key := partitionKey{topic: record.Topic, partition: record.Partition}
			c.workersMu.Lock()
			worker, exists := c.workers[key]
			c.workersMu.Unlock()

			if !exists {
				return
			}

			// Backpressure pause when worker queue is high
			if worker.QueueLen() >= 80 {
				c.client.PauseFetchPartitions(map[string][]int32{record.Topic: {record.Partition}})
			}

			worker.Enqueue(ctx, record)
		})
	}
}

func (c *Consumer) onPartitionsAssigned(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	c.logger.InfoContext(ctx, "Kafka partitions assigned", "partitions", assigned)
	c.metrics.SetAssignments(assigned)

	for topic, parts := range assigned {
		for _, part := range parts {
			key := partitionKey{topic: topic, partition: part}
			if _, exists := c.workers[key]; exists {
				continue
			}

			pw := NewPartitionWorker(
				topic,
				part,
				100,
				c.ingestor,
				c.failure,
				c.committer,
				c.metrics,
				c.clock,
				c.logger,
				func(t string, p int32) {
					c.client.PauseFetchPartitions(map[string][]int32{t: {p}})
				},
				func(t string, p int32) {
					c.client.ResumeFetchPartitions(map[string][]int32{t: {p}})
				},
				func(t string, p int32) {
					c.client.ResumeFetchPartitions(map[string][]int32{t: {p}})
				},
			)
			c.workers[key] = pw
			c.wg.Add(1)
			go func(w *PartitionWorker) {
				defer c.wg.Done()
				w.Run(ctx)
			}(pw)
		}
	}
}

func (c *Consumer) onPartitionsRevoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	c.workersMu.Lock()

	c.logger.InfoContext(ctx, "Kafka partitions revoked", "partitions", revoked)

	var toStop []*PartitionWorker
	for topic, parts := range revoked {
		for _, part := range parts {
			key := partitionKey{topic: topic, partition: part}
			if worker, exists := c.workers[key]; exists {
				toStop = append(toStop, worker)
				delete(c.workers, key)
			}
		}
	}
	c.workersMu.Unlock()

	// Stop workers (they will finish their in-flight record)
	for _, w := range toStop {
		w.Stop()
	}

	// Commit offsets synchronously for finished records
	commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if c.committer != nil {
		_ = c.committer.CommitNow(commitCtx)
	}

	// Update assignments
	c.workersMu.Lock()
	remaining := make(map[string][]int32)
	for key := range c.workers {
		remaining[key.topic] = append(remaining[key.topic], key.partition)
	}
	c.metrics.SetAssignments(remaining)
	c.workersMu.Unlock()
}

func (c *Consumer) onPartitionsLost(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	c.logger.WarnContext(ctx, "Kafka partitions lost", "partitions", lost)

	for topic, parts := range lost {
		for _, part := range parts {
			key := partitionKey{topic: topic, partition: part}
			if worker, exists := c.workers[key]; exists {
				close(worker.quit)
				delete(c.workers, key)
			}
		}
	}

	remaining := make(map[string][]int32)
	for key := range c.workers {
		remaining[key.topic] = append(remaining[key.topic], key.partition)
	}
	c.metrics.SetAssignments(remaining)
}

// Stop cleanly shuts down the consumer, workers, and client.
func (c *Consumer) Stop() {
	if !c.enabled || !c.running.Load() {
		return
	}
	c.running.Store(false)

	if c.cancel != nil {
		c.cancel()
	}

	c.workersMu.Lock()
	workers := make([]*PartitionWorker, 0, len(c.workers))
	for _, w := range c.workers {
		workers = append(workers, w)
	}
	c.workersMu.Unlock()

	for _, w := range workers {
		w.Stop()
	}

	if c.client != nil {
		c.client.Close()
	}

	c.wg.Wait()
	c.logger.Info("Kafka consumer stopped cleanly")
}

// Check implements http.KafkaProber for /health/ready.
func (c *Consumer) Check(_ context.Context) string {
	if !c.enabled {
		return "disabled"
	}
	if !c.running.Load() {
		return "disconnected"
	}
	if c.metrics.IsHealthy() && c.lastPollSuccess.Load() {
		return "connected"
	}
	return "disconnected"
}

// MetricsReader returns the consumer metrics snapshot interface.
func (c *Consumer) MetricsReader() MetricsReader {
	return c.metrics
}
