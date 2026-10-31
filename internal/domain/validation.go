package domain

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

var allowedNotificationCategories = map[NotificationCategory]struct{}{
	CategoryApproval:   {},
	CategoryPurchasing: {},
	CategoryInventory:  {},
	CategoryWarehouse:  {},
	CategorySales:      {},
	CategoryDelivery:   {},
	CategoryReturns:    {},
	CategoryFinance:    {},
	CategorySecurity:   {},
	CategoryAccount:    {},
	CategorySaga:       {},
	CategorySystem:     {},
}

// IsValidCategory checks if the category is recognized.
func IsValidCategory(c NotificationCategory) bool {
	_, ok := allowedNotificationCategories[c]
	return ok
}

var allowedPriorities = map[Priority]struct{}{
	PriorityLow:      {},
	PriorityNormal:   {},
	PriorityHigh:     {},
	PriorityCritical: {},
}

// IsValidPriority checks if the priority is recognized.
func IsValidPriority(p Priority) bool {
	_, ok := allowedPriorities[p]
	return ok
}

var allowedNotificationTypes = map[NotificationType]struct{}{
	TypeApprovalPending:   {},
	TypeApprovalDecided:   {},
	TypeLowStock:          {},
	TypeReservationShort:  {},
	TypeDeliveryStatus:    {},
	TypeInvoiceOverdue:    {},
	TypePaymentReceived:   {},
	TypeReceiptPosted:     {},
	TypeOrderConfirmed:    {},
	TypeOrderFailed:       {},
	TypeReturnUpdate:      {},
	TypeAdjustmentPending: {},
	TypeCommentMention:    {},
	TypeSecurityAlert:     {},
	TypeSystem:            {},
}

// IsValidNotificationType checks if the type is recognized.
func IsValidNotificationType(t NotificationType) bool {
	_, ok := allowedNotificationTypes[t]
	return ok
}

var placeholderRegex = regexp.MustCompile(`\{\{([^{}]+)\}\}`)

// ValidateTemplatePlaceholders verifies that all placeholders in title and body templates
// conform strictly to the allowed list: payload.*, event_type, aggregate_id, actor_user_id, correlation_id.
func ValidateTemplatePlaceholders(titleTmpl, bodyTmpl string) error {
	var details []ErrorDetail

	checkField := func(field, content string) {
		matches := placeholderRegex.FindAllStringSubmatch(content, -1)
		for _, m := range matches {
			if len(m) < 2 {
				continue
			}
			token := strings.TrimSpace(m[1])
			if token == "event_type" || token == "aggregate_id" || token == "actor_user_id" || token == "correlation_id" {
				continue
			}
			if strings.HasPrefix(token, "payload.") && len(token) > len("payload.") {
				continue
			}

			details = append(details, ErrorDetail{
				Field: field,
				Issue: fmt.Sprintf("invalid_placeholder:%s", token),
			})
		}
	}

	checkField("title_template", titleTmpl)
	checkField("body_template", bodyTmpl)

	if len(details) > 0 {
		return &AppError{
			HTTPStatus: http.StatusBadRequest,
			Code:       CodeValidationFailed,
			Message:    "template contains disallowed placeholders; only payload.*, event_type, aggregate_id, actor_user_id, correlation_id are permitted",
			Details:    details,
		}
	}

	return nil
}
