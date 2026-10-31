package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// EventEnvelope is the canonical event envelope (the JSON value of every Kafka message).
type EventEnvelope struct {
	EventID       uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  string          `json:"event_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Producer      string          `json:"producer"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	CorrelationID *uuid.UUID      `json:"correlation_id,omitempty"`
	ActorUserID   *uuid.UUID      `json:"actor_user_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

// EntityRef represents the related aggregate reference in responses.
type EntityRef struct {
	Type string    `json:"type"`
	ID   uuid.UUID `json:"id"`
}
