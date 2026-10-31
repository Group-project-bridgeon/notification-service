package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"notification-service/internal/domain"
	"notification-service/internal/ingest"
	"notification-service/internal/metrics"
	"notification-service/internal/repository"
)

var (
	// regexes for scrubbing secrets in error strings
	passwordRegex = regexp.MustCompile(`(?i)(password|secret|token|authorization|apikey|otp)[=:\s]+[^\s,&]+`)
	dbConnRegex   = regexp.MustCompile(`postgres://[^@]+@`)
)

// SanitizeError scrubs potential credentials/secrets and truncates to maxLen characters.
func SanitizeError(err error, maxLen int) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	s = passwordRegex.ReplaceAllString(s, "$1=[REDACTED]")
	s = dbConnRegex.ReplaceAllString(s, "postgres://[REDACTED]@")

	if maxLen > 0 && len(s) > maxLen {
		s = s[:maxLen]
	}
	return s
}

// FailureHandler handles record processing failures by managing retries and dead-letter routing.
type FailureHandler interface {
	Handle(ctx context.Context, record *kgo.Record, err error) error
}

// RetryConfig specifies topics and delay intervals for Kafka consumer retries.
type RetryConfig struct {
	RetryTopics []string
	RetryDelays []time.Duration
	DLQTopic    string
}

// DefaultRetryConfig returns default retry settings per KAFKA_CONTRACT.md §2.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		RetryTopics: []string{
			"erp.notification.retry.1m",
			"erp.notification.retry.10m",
			"erp.notification.retry.1h",
		},
		RetryDelays: []time.Duration{
			1 * time.Minute,
			10 * time.Minute,
			1 * time.Hour,
		},
		DLQTopic: "erp.notification.dlq",
	}
}

// StandardFailureHandler classifies failures, publishes to delayed retry topics or DLQ,
// and persists dead letters to the database.
type StandardFailureHandler struct {
	publisher      Publisher
	deadLetterRepo repository.DeadLetterRepository
	metrics        *Metrics
	clock          Clock
	config         RetryConfig
	logger         *slog.Logger
}

// NewStandardFailureHandler constructs a StandardFailureHandler.
func NewStandardFailureHandler(
	publisher Publisher,
	deadLetterRepo repository.DeadLetterRepository,
	metrics *Metrics,
	clock Clock,
	config RetryConfig,
	logger *slog.Logger,
) *StandardFailureHandler {
	if clock == nil {
		clock = NewRealClock()
	}
	if logger == nil {
		logger = slog.Default()
	}
	if len(config.RetryTopics) == 0 {
		config = DefaultRetryConfig()
	}
	return &StandardFailureHandler{
		publisher:      publisher,
		deadLetterRepo: deadLetterRepo,
		metrics:        metrics,
		clock:          clock,
		config:         config,
		logger:         logger,
	}
}

// Handle classifies the processing error and routes the record to the appropriate retry topic or DLQ.
// Returns nil only after successful publish (and database persistence for DLQ records).
func (h *StandardFailureHandler) Handle(ctx context.Context, record *kgo.Record, procErr error) error {
	if record == nil {
		return nil
	}

	if h.metrics != nil {
		h.metrics.IncFailed()
	}

	// 1. Extract existing headers or fallback to current record metadata
	headers := record.Headers
	attemptStr := getHeader(headers, "x-attempt")
	attempt := 0
	if attemptStr != "" {
		if a, err := strconv.Atoi(attemptStr); err == nil {
			attempt = a
		}
	}

	origTopic := getHeader(headers, "x-original-topic")
	if origTopic == "" {
		origTopic = record.Topic
	}

	origPartStr := getHeader(headers, "x-original-partition")
	if origPartStr == "" {
		origPartStr = strconv.Itoa(int(record.Partition))
	}

	origOffsetStr := getHeader(headers, "x-original-offset")
	if origOffsetStr == "" {
		origOffsetStr = strconv.FormatInt(record.Offset, 10)
	}

	sanitizedErr := SanitizeError(procErr, 200)

	// 2. Classify: Permanent vs Transient
	isPermanent := ingest.IsPermanent(procErr)

	if isPermanent {
		h.logger.WarnContext(ctx, "routing permanent failure directly to DLQ",
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
			"error", sanitizedErr,
		)
		return h.routeToDLQ(ctx, record, attempt+1, origTopic, origPartStr, origOffsetStr, sanitizedErr)
	}

	// Transient failure: check retry stage
	if attempt < len(h.config.RetryTopics) {
		nextTopic := h.config.RetryTopics[attempt]
		delay := h.config.RetryDelays[attempt]
		nextAttempt := attempt + 1
		notBefore := h.clock.Now().Add(delay).UTC().Format(time.RFC3339)

		newHeaders := copyHeadersExcluding(headers, "x-attempt", "x-original-topic", "x-original-partition", "x-original-offset", "x-error", "x-not-before")
		newHeaders = append(newHeaders,
			kgo.RecordHeader{Key: "x-attempt", Value: []byte(strconv.Itoa(nextAttempt))},
			kgo.RecordHeader{Key: "x-original-topic", Value: []byte(origTopic)},
			kgo.RecordHeader{Key: "x-original-partition", Value: []byte(origPartStr)},
			kgo.RecordHeader{Key: "x-original-offset", Value: []byte(origOffsetStr)},
			kgo.RecordHeader{Key: "x-error", Value: []byte(sanitizedErr)},
			kgo.RecordHeader{Key: "x-not-before", Value: []byte(notBefore)},
		)

		retryRecord := &kgo.Record{
			Key:       record.Key,
			Value:     record.Value,
			Topic:     nextTopic,
			Headers:   newHeaders,
			Timestamp: h.clock.Now(),
		}

		if err := h.publisher.Publish(ctx, retryRecord); err != nil {
			h.logger.ErrorContext(ctx, "failed publishing to retry topic",
				"retry_topic", nextTopic,
				"attempt", nextAttempt,
				"error", err,
			)
			return fmt.Errorf("failed publishing to retry topic %s: %w", nextTopic, err)
		}

		if h.metrics != nil {
			h.metrics.IncRetried()
		}
		metrics.Default().IncRetryPublished(nextTopic)

		h.logger.InfoContext(ctx, "event routed to retry topic",
			"retry_topic", nextTopic,
			"attempt", nextAttempt,
			"not_before", notBefore,
		)
		return nil
	}

	// Transient retries exhausted: route to DLQ
	h.logger.WarnContext(ctx, "transient retries exhausted, routing to DLQ",
		"attempt", attempt,
		"topic", record.Topic,
		"error", sanitizedErr,
	)
	return h.routeToDLQ(ctx, record, attempt+1, origTopic, origPartStr, origOffsetStr, sanitizedErr)
}

func (h *StandardFailureHandler) routeToDLQ(
	ctx context.Context,
	record *kgo.Record,
	attempt int,
	origTopic, origPartStr, origOffsetStr, sanitizedErr string,
) error {
	newHeaders := copyHeadersExcluding(record.Headers, "x-attempt", "x-original-topic", "x-original-partition", "x-original-offset", "x-error", "x-not-before")
	newHeaders = append(newHeaders,
		kgo.RecordHeader{Key: "x-attempt", Value: []byte(strconv.Itoa(attempt))},
		kgo.RecordHeader{Key: "x-original-topic", Value: []byte(origTopic)},
		kgo.RecordHeader{Key: "x-original-partition", Value: []byte(origPartStr)},
		kgo.RecordHeader{Key: "x-original-offset", Value: []byte(origOffsetStr)},
		kgo.RecordHeader{Key: "x-error", Value: []byte(sanitizedErr)},
	)

	dlqRecord := &kgo.Record{
		Key:       record.Key,
		Value:     record.Value,
		Topic:     h.config.DLQTopic,
		Headers:   newHeaders,
		Timestamp: h.clock.Now(),
	}

	// 1. Publish to DLQ Kafka topic with acks=all
	if err := h.publisher.Publish(ctx, dlqRecord); err != nil {
		h.logger.ErrorContext(ctx, "failed publishing to DLQ topic",
			"dlq_topic", h.config.DLQTopic,
			"error", err,
		)
		return fmt.Errorf("failed publishing to DLQ: %w", err)
	}
	metrics.Default().IncDLQPublished(h.config.DLQTopic)

	// 2. Insert into notification_dead_letters database table
	partInt, _ := strconv.Atoi(origPartStr)
	offsetInt, _ := strconv.ParseInt(origOffsetStr, 10, 64)

	dl := &domain.DeadLetter{
		ID:          uuid.New(),
		Source:      domain.EventSourceKafka,
		Topic:       &origTopic,
		Partition:   &partInt,
		KafkaOffset: &offsetInt,
		Error:       sanitizedErr,
		FailedAt:    h.clock.Now().UTC(),
	}

	if json.Valid(record.Value) {
		dl.RawEvent = json.RawMessage(record.Value)
	} else {
		dl.RawValue = record.Value
	}

	if err := h.deadLetterRepo.Create(ctx, dl); err != nil {
		h.logger.ErrorContext(ctx, "failed inserting dead letter into database",
			"id", dl.ID,
			"error", err,
		)
		return fmt.Errorf("failed saving dead letter to database: %w", err)
	}

	if h.metrics != nil {
		h.metrics.IncDLQ()
	}

	h.logger.WarnContext(ctx, "record successfully committed to DLQ and database",
		"dlq_id", dl.ID,
		"topic", origTopic,
		"partition", partInt,
		"offset", offsetInt,
	)
	return nil
}

func getHeader(headers []kgo.RecordHeader, key string) string {
	for _, h := range headers {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func copyHeadersExcluding(headers []kgo.RecordHeader, excludeKeys ...string) []kgo.RecordHeader {
	ex := make(map[string]bool, len(excludeKeys))
	for _, k := range excludeKeys {
		ex[strings.ToLower(k)] = true
	}

	var res []kgo.RecordHeader
	for _, h := range headers {
		if !ex[strings.ToLower(h.Key)] {
			res = append(res, h)
		}
	}
	return res
}
