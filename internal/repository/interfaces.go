package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
)

type NotificationRepository interface {
	BatchInsert(ctx context.Context, notifications []*domain.Notification) error
	List(ctx context.Context, filter domain.NotificationFilter) ([]*domain.Notification, int, error)
	GetByID(ctx context.Context, recipientID, id uuid.UUID) (*domain.Notification, error)
	SetStatus(ctx context.Context, recipientID, id uuid.UUID, status domain.NotificationStatus) (*domain.Notification, error)
	ReadAll(ctx context.Context, recipientID uuid.UUID, category *domain.NotificationCategory) (int64, error)
	GetUnreadCounts(ctx context.Context, recipientID uuid.UUID) (int, map[string]int, error)
}

type DeliveryRepository interface {
	Create(ctx context.Context, delivery *domain.NotificationDelivery) error
	ClaimDue(ctx context.Context, limit int) ([]*domain.NotificationDelivery, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status domain.DeliveryStatus, attempts int, nextRetryAt *time.Time, lastError *string) error
	List(ctx context.Context, filter domain.DeliveryFilter) ([]*domain.NotificationDelivery, int, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.NotificationDelivery, error)
}

type NotificationTemplateRepository interface {
	GetActive(ctx context.Context, templateKey, locale string) (*domain.NotificationTemplate, error)
	List(ctx context.Context) ([]*domain.NotificationTemplate, error)
	Create(ctx context.Context, tmpl *domain.NotificationTemplate) error
	CreateNewVersion(ctx context.Context, tmpl *domain.NotificationTemplate) error
}

type NotificationRuleRepository interface {
	GetActiveByEventType(ctx context.Context, eventType string) ([]*domain.NotificationRule, error)
	List(ctx context.Context) ([]*domain.NotificationRule, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.NotificationRule, error)
	Create(ctx context.Context, rule *domain.NotificationRule) error
	Update(ctx context.Context, rule *domain.NotificationRule) error
	Delete(ctx context.Context, id uuid.UUID) error
	Deactivate(ctx context.Context, id uuid.UUID) error
}

type RecipientRepository interface {
	UpsertGuarded(ctx context.Context, recipient *domain.Recipient) error
	FindByRoles(ctx context.Context, roles []string) ([]*domain.Recipient, error)
	FindByID(ctx context.Context, id uuid.UUID) (*domain.Recipient, error)
}

type PreferenceRepository interface {
	GetByUser(ctx context.Context, userID uuid.UUID) ([]*domain.NotificationPreference, error)
	Upsert(ctx context.Context, pref *domain.NotificationPreference) error
}

type InboxRepository interface {
	InsertIfAbsent(ctx context.Context, event *domain.InboxEvent) (isNew bool, err error)
	SetStatus(ctx context.Context, eventID uuid.UUID, status domain.InboxStatus, errStr *string) error
}

type DeadLetterRepository interface {
	Create(ctx context.Context, dl *domain.DeadLetter) error
	List(ctx context.Context, limit, offset int) ([]*domain.DeadLetter, int, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.DeadLetter, error)
	MarkReplayed(ctx context.Context, id uuid.UUID) error
}

type StatsRepository interface {
	GetNotificationStats(ctx context.Context, since time.Time) (*domain.NotificationStats, error)
}

type EmailSettingsRepository interface {
	Get(ctx context.Context) (*domain.EmailSettings, error)
	Upsert(ctx context.Context, s *domain.EmailSettings) error
}

type EmailTemplateRepository interface {
	List(ctx context.Context) ([]*domain.EmailTemplate, error)
	ListVersions(ctx context.Context, templateKey string) ([]*domain.EmailTemplate, error)
	GetActive(ctx context.Context, templateKey string) (*domain.EmailTemplate, error)
	GetByVersion(ctx context.Context, templateKey string, version int) (*domain.EmailTemplate, error)
	CreateNewVersion(ctx context.Context, tmpl *domain.EmailTemplate) error
	Activate(ctx context.Context, templateKey string, version int) error
}

type EmailMessageRepository interface {
	Create(ctx context.Context, msg *domain.EmailMessage) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.EmailMessage, error)
	GetByProviderMessageID(ctx context.Context, providerMsgID string) (*domain.EmailMessage, error)
	List(ctx context.Context, filter domain.EmailMessageFilter) ([]*domain.EmailMessage, int, error)
	ClaimDue(ctx context.Context, limit int) ([]*domain.EmailMessage, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status domain.EmailMessageStatus, attempts int, nextAttemptAt, sentAt *time.Time, lastError, providerMessageID *string) error
	SetResendLink(ctx context.Context, id, resendOf uuid.UUID) error
	CountSentToday(ctx context.Context) (int, error)
	MarkLostOnRestart(ctx context.Context) (int64, error)
}

type SuppressionRepository interface {
	List(ctx context.Context) ([]*domain.EmailSuppression, error)
	GetByEmail(ctx context.Context, email string) (*domain.EmailSuppression, error)
	Add(ctx context.Context, supp *domain.EmailSuppression) error
	Delete(ctx context.Context, email string) error
}

type IdempotencyKeyRepository interface {
	Get(ctx context.Context, key, scope string) (*domain.IdempotencyKey, error)
	Create(ctx context.Context, item *domain.IdempotencyKey) error
	UpdateResponse(ctx context.Context, key, scope string, response []byte) error
}

type OperationRepository interface {
	Upsert(ctx context.Context, op *domain.Operation) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Operation, error)
	List(ctx context.Context, filter domain.OperationFilter) ([]*domain.Operation, int, error)
}

