package notification

import (
	"context"

	"notification-service/internal/domain"
)

// UnreadCounts conveys recipient unread notification totals and per-category distributions.
type UnreadCounts struct {
	Total      int            `json:"total"`
	ByCategory map[string]int `json:"by_category"`
}

// NotificationPublisher defines an interface for real-time notification dispatch (e.g., SSE streaming hub).
type NotificationPublisher interface {
	PublishNotification(ctx context.Context, notification *domain.Notification, counts UnreadCounts) error
}

// NoopNotificationPublisher is the default no-op NotificationPublisher until SSE hub implementation in Prompt 7.
type NoopNotificationPublisher struct{}

// PublishNotification accepts the notification without action.
func (NoopNotificationPublisher) PublishNotification(_ context.Context, _ *domain.Notification, _ UnreadCounts) error {
	return nil
}
