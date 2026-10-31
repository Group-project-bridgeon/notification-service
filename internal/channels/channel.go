package channels

import (
	"context"
	"notification-service/internal/domain"
)

// Channel defines an interface for delivering notifications across mediums (in_app, email).
type Channel interface {
	Name() string
	Deliver(ctx context.Context, notification *domain.Notification) error
}
