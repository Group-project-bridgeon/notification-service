package email

import (
	"context"

	"github.com/google/uuid"
	"notification-service/internal/domain"
)

// QueuedEmail specifies metadata and payload to queue an email delivery.
type QueuedEmail struct {
	RecipientID     uuid.UUID
	ToAddresses     []string
	Subject         string
	TemplateKey     string
	TemplateVersion int
	Variables       map[string]any
	Priority        domain.EmailPriority
	RelatedType     *string
	RelatedID       *uuid.UUID
}

// EmailQueue abstracts enqueueing outbound email messages.
type EmailQueue interface {
	Enqueue(ctx context.Context, email QueuedEmail) error
}

// NoopEmailQueue is the default no-op EmailQueue implementation prior to Prompt 8.
type NoopEmailQueue struct{}

// Enqueue discards the queued email safely.
func (NoopEmailQueue) Enqueue(_ context.Context, _ QueuedEmail) error {
	return nil
}
