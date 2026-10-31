package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

// Replayer re-runs dead-letter events through the ingestion pipeline directly without Kafka.
type Replayer struct {
	repo     repository.DeadLetterRepository
	ingestor *Ingestor
	logger   *slog.Logger
}

// NewReplayer constructs a Replayer instance.
func NewReplayer(repo repository.DeadLetterRepository, ingestor *Ingestor, logger *slog.Logger) *Replayer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Replayer{
		repo:     repo,
		ingestor: ingestor,
		logger:   logger,
	}
}

// Replay executes the stored event against the Ingestor.
// Returns domain.CodeInvalidStateTransition if the dead letter has already been replayed.
func (r *Replayer) Replay(ctx context.Context, deadLetterID uuid.UUID) error {
	dl, err := r.repo.GetByID(ctx, deadLetterID)
	if err != nil {
		return err
	}

	if dl.ReplayedAt != nil {
		return &domain.AppError{
			Code:       domain.CodeInvalidStateTransition,
			Message:    "dead letter event has already been replayed",
			HTTPStatus: http.StatusConflict,
		}
	}

	var rawBytes []byte
	if len(dl.RawEvent) > 0 {
		rawBytes = dl.RawEvent
	} else if len(dl.RawValue) > 0 {
		rawBytes = dl.RawValue
	} else {
		return &PermanentError{Cause: fmt.Errorf("dead letter %s has empty raw payload", deadLetterID)}
	}

	env, err := ParseAndValidate(rawBytes)
	if err != nil {
		return &PermanentError{Cause: fmt.Errorf("failed parsing dead letter event envelope: %w", err)}
	}

	meta := IngestMeta{
		Topic:     dl.Topic,
		Partition: dl.Partition,
		Offset:    dl.KafkaOffset,
	}

	if err := r.ingestor.Handle(ctx, dl.Source, env, meta); err != nil {
		return fmt.Errorf("replaying event failed: %w", err)
	}

	if err := r.repo.MarkReplayed(ctx, deadLetterID); err != nil {
		return fmt.Errorf("failed marking dead letter as replayed: %w", err)
	}

	r.logger.InfoContext(ctx, "dead letter replayed successfully",
		"id", deadLetterID,
		"event_id", env.EventID,
		"event_type", env.EventType,
	)
	return nil
}
