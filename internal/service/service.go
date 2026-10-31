package service

import (
	"context"
	"notification-service/internal/domain"
)

// NotificationService defines core business logic methods for notification management.
type NotificationService interface {
	GetNotifications(ctx context.Context, userID string, filter any) ([]*domain.Notification, int, error)
	MarkAsRead(ctx context.Context, userID, notificationID string) error
	GetUnreadCount(ctx context.Context, userID string) (int, map[string]int, error)
}
