package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

// identityEventTypes are the auth events that maintain the local recipient projection.
// These events update notification_recipients and create no notifications of their own.
var identityEventTypes = map[string]bool{
	"user.created":       true,
	"user.updated":       true,
	"user.roles_changed": true,
	"user.deactivated":   true,
}

// IsIdentityEvent reports whether event_type is a recipient projection event.
func IsIdentityEvent(eventType string) bool {
	return identityEventTypes[eventType]
}

// identityPayload is the expected payload for identity events from erp.auth.events.
// See KAFKA_CONTRACT.md §5 (Identity Event Payload).
type identityPayload struct {
	UserID      string   `json:"user_id"`
	Email       string   `json:"email"`
	DisplayName string   `json:"display_name"`
	Roles       []string `json:"roles"`
	IsActive    bool     `json:"is_active"`
}

// handleIdentity upserts the notification_recipients projection from an identity event.
// Must be called inside an active database transaction (txCtx).
// Out-of-order protection is enforced by UpsertGuarded's SQL WHERE clause.
func handleIdentity(
	txCtx context.Context,
	env *domain.EventEnvelope,
	recipients repository.RecipientRepository,
) error {
	var p identityPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return &PermanentError{Cause: fmt.Errorf("failed decoding identity payload: %w", err)}
	}
	if p.UserID == "" {
		return &PermanentError{Cause: fmt.Errorf("identity payload missing user_id")}
	}

	userID, err := uuid.Parse(p.UserID)
	if err != nil {
		return &PermanentError{Cause: fmt.Errorf("identity payload invalid user_id UUID: %w", err)}
	}

	roles := p.Roles
	if roles == nil {
		roles = []string{}
	}

	rec := &domain.Recipient{
		UserID:           userID,
		Email:            p.Email,
		DisplayName:      p.DisplayName,
		Roles:            roles,
		IsActive:         p.IsActive,
		SourceOccurredAt: env.OccurredAt,
		UpdatedAt:        time.Now().UTC(),
	}

	if err := recipients.UpsertGuarded(txCtx, rec); err != nil {
		return &TransientError{Cause: fmt.Errorf("failed upserting recipient: %w", err)}
	}
	return nil
}
