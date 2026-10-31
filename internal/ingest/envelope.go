package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
)

// MaxMessageBytes is the hard 256 KB message size limit from KAFKA_CONTRACT.md §3.
const MaxMessageBytes = 256 * 1024

// eventTypeRe validates the domain.action pattern (lowercase letters, digits, underscores; at least one dot).
var eventTypeRe = regexp.MustCompile(`^[a-z0-9_]+(\.[a-z0-9_]+)+$`)

// rawEnvelope decodes JSON fields as plain strings before uuid/time parsing.
type rawEnvelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  string          `json:"event_version"`
	OccurredAt    string          `json:"occurred_at"`
	Producer      string          `json:"producer"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	ActorUserID   string          `json:"actor_user_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

// ParseAndValidate decodes raw JSON bytes into a validated EventEnvelope.
// Returns *PermanentError for any structural or semantic violation.
func ParseAndValidate(raw []byte) (*domain.EventEnvelope, error) {
	// 1. Size guard (checked first — before parsing costs are paid)
	if len(raw) > MaxMessageBytes {
		return nil, &PermanentError{
			Cause: fmt.Errorf("message size %d bytes exceeds 256 KB limit", len(raw)),
		}
	}

	// 2. Valid JSON object
	var r rawEnvelope
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, &PermanentError{Cause: fmt.Errorf("invalid JSON: %w", err)}
	}

	// 3. Required string fields
	var missing []string
	if r.EventID == "" {
		missing = append(missing, "event_id")
	}
	if r.EventType == "" {
		missing = append(missing, "event_type")
	}
	if r.EventVersion == "" {
		missing = append(missing, "event_version")
	}
	if r.OccurredAt == "" {
		missing = append(missing, "occurred_at")
	}
	if r.Producer == "" {
		missing = append(missing, "producer")
	}
	if r.AggregateType == "" {
		missing = append(missing, "aggregate_type")
	}
	if r.AggregateID == "" {
		missing = append(missing, "aggregate_id")
	}
	if len(missing) > 0 {
		return nil, &PermanentError{
			Cause: fmt.Errorf("missing required fields: %v", missing),
		}
	}

	// 4. event_id must be a valid UUID
	eventID, err := uuid.Parse(r.EventID)
	if err != nil {
		return nil, &PermanentError{
			Cause: fmt.Errorf("invalid event_id UUID %q: %w", r.EventID, err),
		}
	}

	// 5. event_type pattern: domain.action (lowercase, digits, underscores, at least one dot)
	if !eventTypeRe.MatchString(r.EventType) {
		return nil, &PermanentError{
			Cause: fmt.Errorf("invalid event_type %q: must match ^[a-z0-9_]+(\\.[a-z0-9_]+)+$", r.EventType),
		}
	}

	// 6. occurred_at: RFC 3339 parse, normalised to UTC
	occurredAt, err := time.Parse(time.RFC3339, r.OccurredAt)
	if err != nil {
		return nil, &PermanentError{
			Cause: fmt.Errorf("invalid occurred_at %q: %w", r.OccurredAt, err),
		}
	}
	occurredAt = occurredAt.UTC()

	// 7. payload must be a non-null JSON object
	trimmed := bytes.TrimSpace(r.Payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, &PermanentError{
			Cause: fmt.Errorf("payload must be a JSON object, got: %.20s", trimmed),
		}
	}

	env := &domain.EventEnvelope{
		EventID:       eventID,
		EventType:     r.EventType,
		EventVersion:  r.EventVersion,
		OccurredAt:    occurredAt,
		Producer:      r.Producer,
		AggregateType: r.AggregateType,
		AggregateID:   r.AggregateID,
		Payload:       r.Payload,
	}

	// 8. Optional UUID fields
	if r.CorrelationID != "" {
		cid, err := uuid.Parse(r.CorrelationID)
		if err != nil {
			return nil, &PermanentError{
				Cause: fmt.Errorf("invalid correlation_id UUID %q: %w", r.CorrelationID, err),
			}
		}
		env.CorrelationID = &cid
	}
	if r.ActorUserID != "" {
		aid, err := uuid.Parse(r.ActorUserID)
		if err != nil {
			return nil, &PermanentError{
				Cause: fmt.Errorf("invalid actor_user_id UUID %q: %w", r.ActorUserID, err),
			}
		}
		env.ActorUserID = &aid
	}

	return env, nil
}
