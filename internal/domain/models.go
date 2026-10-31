package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Recipient is the local projection of user identity populated via Auth domain events.
type Recipient struct {
	UserID           uuid.UUID `json:"user_id"`
	Email            string    `json:"email"`
	DisplayName      string    `json:"display_name"`
	Roles            []string  `json:"roles"`
	IsActive         bool      `json:"is_active"`
	SourceOccurredAt time.Time `json:"source_occurred_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// NotificationTemplate defines in-app copy templates with versioning and localization.
type NotificationTemplate struct {
	ID            uuid.UUID `json:"id"`
	TemplateKey   string    `json:"template_key"`
	EventType     string    `json:"event_type"`
	Locale        string    `json:"locale"`
	TitleTemplate string    `json:"title_template"`
	BodyTemplate  string    `json:"body_template"`
	Version       int       `json:"version"`
	IsActive      bool      `json:"is_active"`
	CreatedAt     time.Time `json:"created_at"`
}

// NotificationRule determines routing from incoming event types to roles, categories, and channels.
type NotificationRule struct {
	ID               uuid.UUID            `json:"id"`
	EventType        string               `json:"event_type"`
	TargetRoles      []string             `json:"target_roles"`
	TargetUserPath   *string              `json:"target_user_path,omitempty"`
	Type             NotificationType     `json:"type"`
	Category         NotificationCategory `json:"category"`
	Priority         Priority             `json:"priority"`
	LinkTemplate     *string              `json:"link_template,omitempty"`
	TemplateKey      string               `json:"template_key"`
	SendEmail        bool                 `json:"send_email"`
	EmailTemplateKey *string              `json:"email_template_key,omitempty"`
	IsActive         bool                 `json:"is_active"`
	CreatedAt        time.Time            `json:"created_at"`
}

// Notification represents an in-app notification matching 000004_create_notifications.
type Notification struct {
	ID            uuid.UUID            `json:"id"`
	RecipientID   uuid.UUID            `json:"recipient_id"`
	EventID       uuid.UUID            `json:"event_id"`
	EventType     string               `json:"event_type"`
	Type          NotificationType     `json:"type"`
	Category      NotificationCategory `json:"category"`
	Priority      Priority             `json:"priority"`
	Title         string               `json:"title"`
	Body          string               `json:"body"`
	Link          *string              `json:"link,omitempty"`
	EntityType    *string              `json:"entity_type,omitempty"`
	EntityID      *uuid.UUID           `json:"entity_id,omitempty"`
	CorrelationID *uuid.UUID           `json:"correlation_id,omitempty"`
	Status        NotificationStatus   `json:"status"`
	ReadAt        *time.Time           `json:"read_at,omitempty"`
	IsTest        bool                 `json:"is_test"`
	CreatedAt     time.Time            `json:"created_at"`
}

// NotificationDelivery records a specific delivery attempt over in_app or email.
type NotificationDelivery struct {
	ID             uuid.UUID      `json:"id"`
	NotificationID uuid.UUID      `json:"notification_id"`
	Channel        Channel        `json:"channel"`
	Status         DeliveryStatus `json:"status"`
	Attempts       int            `json:"attempts"`
	NextRetryAt    *time.Time     `json:"next_retry_at,omitempty"`
	LastError      *string        `json:"last_error,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// NotificationPreference stores per-category channel toggles for a user.
type NotificationPreference struct {
	UserID    uuid.UUID            `json:"user_id"`
	Category  NotificationCategory `json:"category"`
	Channel   Channel              `json:"channel"`
	Enabled   bool                 `json:"enabled"`
	UpdatedAt time.Time            `json:"updated_at"`
}

// EmailSettings is the singleton configuration record for email provider / SMTP settings.
type EmailSettings struct {
	ID                int        `json:"id"`
	Provider          string     `json:"provider"`
	Host              string     `json:"host"`
	Port              int        `json:"port"`
	Username          string     `json:"username"`
	PasswordEncrypted []byte     `json:"-"`
	FromName          string     `json:"from_name"`
	FromAddress       string     `json:"from_address"`
	ReplyTo           *string    `json:"reply_to,omitempty"`
	TLSMode           TLSMode    `json:"tls_mode"`
	DailyLimit        int        `json:"daily_limit"`
	Enabled           bool       `json:"enabled"`
	UpdatedBy         *uuid.UUID `json:"updated_by,omitempty"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

// EmailTemplate defines versioned email templates.
type EmailTemplate struct {
	ID                 uuid.UUID  `json:"id"`
	TemplateKey        string     `json:"template_key"`
	Version            int        `json:"version"`
	SubjectTemplate    string     `json:"subject_template"`
	HTMLTemplate       string     `json:"html_template"`
	TextTemplate       string     `json:"text_template"`
	DeclaredVariables  []string   `json:"declared_variables"`
	SensitiveVariables []string   `json:"sensitive_variables"`
	IsActive           bool       `json:"is_active"`
	CreatedBy          *uuid.UUID `json:"created_by,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
}

// EmailMessage is an outbound email message record with sensitive variable redaction.
type EmailMessage struct {
	ID                uuid.UUID          `json:"id"`
	TemplateKey       string             `json:"template_key"`
	TemplateVersion   int                `json:"template_version"`
	ToAddresses       []string           `json:"to_addresses"`
	CcAddresses       []string           `json:"cc_addresses"`
	Subject           string             `json:"subject"`
	BodyHTMLRedacted  string             `json:"body_html_redacted"`
	BodyTextRedacted  string             `json:"body_text_redacted"`
	Status            EmailMessageStatus `json:"status"`
	Priority          EmailPriority      `json:"priority"`
	Attempts          int                `json:"attempts"`
	NextAttemptAt     *time.Time         `json:"next_attempt_at,omitempty"`
	ExpiresAt         *time.Time         `json:"expires_at,omitempty"`
	ProviderMessageID *string            `json:"provider_message_id,omitempty"`
	RelatedType       *string            `json:"related_type,omitempty"`
	RelatedID         *uuid.UUID         `json:"related_id,omitempty"`
	ResendOf          *uuid.UUID         `json:"resend_of,omitempty"`
	SentBy            *uuid.UUID         `json:"sent_by,omitempty"`
	LastError         *string            `json:"last_error,omitempty"`
	CreatedAt         time.Time          `json:"created_at"`
	SentAt            *time.Time         `json:"sent_at,omitempty"`
}

// EmailSuppression tracks blocked recipients.
type EmailSuppression struct {
	Email     string                 `json:"email"`
	Reason    EmailSuppressionReason `json:"reason"`
	CreatedAt time.Time              `json:"created_at"`
	CreatedBy *uuid.UUID             `json:"created_by,omitempty"`
}

// InboxEvent stores consumed events for deduplication and idempotent processing.
type InboxEvent struct {
	EventID     uuid.UUID   `json:"event_id"`
	EventType   string      `json:"event_type"`
	Status      InboxStatus `json:"status"`
	Source      EventSource `json:"source"`
	Topic       *string     `json:"topic,omitempty"`
	Partition   *int        `json:"partition,omitempty"`
	KafkaOffset *int64      `json:"kafka_offset,omitempty"`
	ReceivedAt  time.Time   `json:"received_at"`
	ProcessedAt *time.Time  `json:"processed_at,omitempty"`
	Error       *string     `json:"error,omitempty"`
}

// DeadLetter stores poisoned or unroutable notification events.
type DeadLetter struct {
	ID          uuid.UUID       `json:"id"`
	Source      EventSource     `json:"source"`
	Topic       *string         `json:"topic,omitempty"`
	Partition   *int            `json:"partition,omitempty"`
	KafkaOffset *int64          `json:"kafka_offset,omitempty"`
	RawEvent    json.RawMessage `json:"raw_event,omitempty"`
	RawValue    []byte          `json:"raw_value,omitempty"`
	Error       string          `json:"error"`
	FailedAt    time.Time       `json:"failed_at"`
	ReplayedAt  *time.Time      `json:"replayed_at,omitempty"`
}

// IdempotencyKey stores cached responses for idempotent operations.
type IdempotencyKey struct {
	ID          uuid.UUID       `json:"id"`
	Key         string          `json:"key"`
	Scope       string          `json:"scope"`
	RequestHash string          `json:"request_hash"`
	Response    json.RawMessage `json:"response,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

// NotificationFilter specifies query criteria for listing in-app notifications.
type NotificationFilter struct {
	RecipientID uuid.UUID
	Status      *NotificationStatus
	Category    *NotificationCategory
	Priority    *Priority
	Type        *NotificationType
	From        *time.Time
	To          *time.Time
	Sort        string
	Page        int
	PageSize    int
}

// EmailMessageFilter specifies query criteria for listing email messages.
type EmailMessageFilter struct {
	Status      *EmailMessageStatus
	TemplateKey *string
	ToAddress   *string
	RelatedType *string
	RelatedID   *uuid.UUID
	From        *time.Time
	ToDate      *time.Time
	SentBy      *uuid.UUID
	Sort        string
	Page        int
	PageSize    int
}

// DeliveryFilter specifies query criteria for listing deliveries.
type DeliveryFilter struct {
	Status   *DeliveryStatus
	Channel  *Channel
	From     *time.Time
	To       *time.Time
	Page     int
	PageSize int
}

// NotificationStats provides 24-hour delivery, failure, and category metrics.
type NotificationStats struct {
	ByCategory           map[string]int `json:"by_category"`
	ByPriority           map[string]int `json:"by_priority"`
	DeliverySuccessRate  float64        `json:"delivery_success_rate"`
	TotalDeliveries      int            `json:"total_deliveries"`
	SuccessfulDeliveries int            `json:"successful_deliveries"`
	DeadLettersCount     int            `json:"dead_letters_count"`
	EmailFailuresCount   int            `json:"email_failures_count"`
	Period               string         `json:"period"`
}

// ConsumerPartitionStatus tracks committed and end offsets per partition.
type ConsumerPartitionStatus struct {
	Partition       int   `json:"partition"`
	CommittedOffset int64 `json:"committed_offset"`
	EndOffset       int64 `json:"end_offset"`
	Lag             int64 `json:"lag"`
}

// ConsumerTopicStatus groups partition status per topic.
type ConsumerTopicStatus struct {
	Topic      string                    `json:"topic"`
	Partitions []ConsumerPartitionStatus `json:"partitions"`
}

// ConsumerStatus aggregates Kafka consumer group lag and execution metrics.
type ConsumerStatus struct {
	GroupID        string                `json:"group_id"`
	State          string                `json:"state"`
	Topics         []ConsumerTopicStatus `json:"topics"`
	LastMessageAt  *time.Time            `json:"last_message_at"`
	ProcessedTotal int64                 `json:"processed_total"`
	RetriedTotal   int64                 `json:"retried_total"`
	DLQTotal       int64                 `json:"dlq_total"`
	FailedTotal    int64                 `json:"failed_total"`
}

// OperationState represents the lifecycle state of an async distributed operation.
type OperationState string

const (
	OperationStatePending      OperationState = "pending"
	OperationStateSucceeded    OperationState = "succeeded"
	OperationStateFailed       OperationState = "failed"
	OperationStateCompensating OperationState = "compensating"
	OperationStateCompensated  OperationState = "compensated"
)

// OperationEntity identifies the target domain entity.
type OperationEntity struct {
	Type string    `json:"type"`
	ID   uuid.UUID `json:"id"`
}

// OperationError contains error code, message, and field-level details.
type OperationError struct {
	Code    string        `json:"code"`
	Message string        `json:"message"`
	Details []ErrorDetail `json:"details"`
}

// Operation represents an async operation projection.
type Operation struct {
	ID                  uuid.UUID       `json:"id"`
	Type                string          `json:"type"`
	Entity              OperationEntity `json:"entity"`
	ActorUserID         *uuid.UUID      `json:"actor_user_id,omitempty"`
	State               OperationState  `json:"state"`
	Error               *OperationError `json:"error,omitempty"`
	Compensation        *string         `json:"compensation,omitempty"`
	CorrelationID       uuid.UUID       `json:"correlation_id"`
	StartedAt           time.Time       `json:"started_at"`
	FinishedAt          *time.Time      `json:"finished_at,omitempty"`
	UpdatedAt           time.Time       `json:"updated_at"`
	LastEventOccurredAt time.Time       `json:"last_event_occurred_at"`
}

// OperationResponse represents the standardized JSON response for an operation.
type OperationResponse struct {
	ID            uuid.UUID       `json:"id"`
	Type          string          `json:"type"`
	Entity        OperationEntity `json:"entity"`
	State         OperationState  `json:"state"`
	Error         *OperationError `json:"error"`
	Compensation  *string         `json:"compensation"`
	CorrelationID uuid.UUID       `json:"correlation_id"`
	StartedAt     time.Time       `json:"started_at"`
	FinishedAt    *time.Time      `json:"finished_at"`
}

func (o *Operation) ToResponse() OperationResponse {
	return OperationResponse{
		ID:            o.ID,
		Type:          o.Type,
		Entity:        o.Entity,
		State:         o.State,
		Error:         o.Error,
		Compensation:  o.Compensation,
		CorrelationID: o.CorrelationID,
		StartedAt:     o.StartedAt,
		FinishedAt:    o.FinishedAt,
	}
}

// OperationFilter defines query criteria for GET /operations.
type OperationFilter struct {
	ActorUserID *uuid.UUID
	EntityType  string
	EntityID    *uuid.UUID
	Page        int
	PageSize    int
}

