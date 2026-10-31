package domain

// NotificationStatus represents in-app notification read/archive states.
type NotificationStatus string

const (
	NotificationStatusUnread   NotificationStatus = "unread"
	NotificationStatusRead     NotificationStatus = "read"
	NotificationStatusArchived NotificationStatus = "archived"
)

// Priority defines the urgency level of a notification or event.
type Priority string

const (
	PriorityLow      Priority = "low"
	PriorityNormal   Priority = "normal"
	PriorityHigh     Priority = "high"
	PriorityCritical Priority = "critical"
)

// NotificationCategory classifies notifications according to business domains.
type NotificationCategory string

const (
	CategoryApproval   NotificationCategory = "approval"
	CategoryPurchasing NotificationCategory = "purchasing"
	CategoryInventory  NotificationCategory = "inventory"
	CategoryWarehouse  NotificationCategory = "warehouse"
	CategorySales      NotificationCategory = "sales"
	CategoryDelivery   NotificationCategory = "delivery"
	CategoryReturns    NotificationCategory = "returns"
	CategoryFinance    NotificationCategory = "finance"
	CategorySecurity   NotificationCategory = "security"
	CategoryAccount    NotificationCategory = "account"
	CategorySaga       NotificationCategory = "saga"
	CategorySystem     NotificationCategory = "system"
)

// Channel defines delivery mediums.
type Channel string

const (
	ChannelInApp Channel = "in_app"
	ChannelEmail Channel = "email"
)

// NotificationType enumerates all recognized contract notification types.
type NotificationType string

const (
	TypeApprovalPending   NotificationType = "approval_pending"
	TypeApprovalDecided   NotificationType = "approval_decided"
	TypeLowStock          NotificationType = "low_stock"
	TypeReservationShort  NotificationType = "reservation_short"
	TypeDeliveryStatus    NotificationType = "delivery_status"
	TypeInvoiceOverdue    NotificationType = "invoice_overdue"
	TypePaymentReceived   NotificationType = "payment_received"
	TypeReceiptPosted     NotificationType = "receipt_posted"
	TypeOrderConfirmed    NotificationType = "order_confirmed"
	TypeOrderFailed       NotificationType = "order_failed"
	TypeReturnUpdate      NotificationType = "return_update"
	TypeAdjustmentPending NotificationType = "adjustment_pending"
	TypeCommentMention    NotificationType = "comment_mention"
	TypeSecurityAlert     NotificationType = "security_alert"
	TypeSystem            NotificationType = "system"
)

// DeliveryStatus tracks individual notification dispatch attempts.
type DeliveryStatus string

const (
	DeliveryStatusPending DeliveryStatus = "pending"
	DeliveryStatusSent    DeliveryStatus = "sent"
	DeliveryStatusFailed  DeliveryStatus = "failed"
	DeliveryStatusDead    DeliveryStatus = "dead"
)

// EmailMessageStatus represents the lifecycle state of an outbound email.
type EmailMessageStatus string

const (
	EmailStatusQueued        EmailMessageStatus = "queued"
	EmailMessageStatusQueued EmailMessageStatus = "queued"
	EmailStatusSending       EmailMessageStatus = "sending"
	EmailMessageStatusSending EmailMessageStatus = "sending"
	EmailStatusSent          EmailMessageStatus = "sent"
	EmailMessageStatusSent   EmailMessageStatus = "sent"
	EmailStatusDelivered     EmailMessageStatus = "delivered"
	EmailMessageStatusDelivered EmailMessageStatus = "delivered"
	EmailStatusFailed        EmailMessageStatus = "failed"
	EmailMessageStatusFailed EmailMessageStatus = "failed"
	EmailStatusBounced       EmailMessageStatus = "bounced"
	EmailMessageStatusBounced EmailMessageStatus = "bounced"
	EmailStatusSuppressed    EmailMessageStatus = "suppressed"
	EmailMessageStatusSuppressed EmailMessageStatus = "suppressed"
	EmailStatusCancelled     EmailMessageStatus = "cancelled"
	EmailMessageStatusCancelled EmailMessageStatus = "cancelled"
)

// EmailPriority defines the delivery urgency of an email message.
type EmailPriority string

const (
	EmailPriorityTransactional EmailPriority = "transactional"
	EmailPriorityBusiness      EmailPriority = "business"
)

// EmailSuppressionReason indicates why an email address is blocked.
type EmailSuppressionReason string

const (
	SuppressionReasonHardBounce      EmailSuppressionReason = "hard_bounce"
	EmailSuppressionReasonHardBounce EmailSuppressionReason = "hard_bounce"
	SuppressionReasonComplaint       EmailSuppressionReason = "complaint"
	EmailSuppressionReasonComplaint  EmailSuppressionReason = "complaint"
	SuppressionReasonManual          EmailSuppressionReason = "manual"
	EmailSuppressionReasonManual     EmailSuppressionReason = "manual"
)

// InboxStatus represents the processing outcome of an ingested event.
type InboxStatus string

const (
	InboxStatusReceived  InboxStatus = "received"
	InboxStatusProcessed InboxStatus = "processed"
	InboxStatusIgnored   InboxStatus = "ignored"
	InboxStatusFailed    InboxStatus = "failed"
)

// EventSource identifies how an event entered the service.
type EventSource string

const (
	EventSourceKafka EventSource = "kafka"
	EventSourceHTTP  EventSource = "http"
)

// TLSMode defines SMTP transport encryption.
type TLSMode string

const (
	TLSModeNone     TLSMode = "none"
	TLSModeStartTLS TLSMode = "starttls"
	TLSModeSSLTLS   TLSMode = "ssl_tls"
)
