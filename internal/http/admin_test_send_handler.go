package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/notification"
	"notification-service/internal/repository"
)

type AdminTestSendHandler struct {
	recipientRepo    repository.RecipientRepository
	templateRepo     repository.NotificationTemplateRepository
	notificationRepo repository.NotificationRepository
	deliveryRepo     repository.DeliveryRepository
	publisher        notification.NotificationPublisher
	logger           *slog.Logger
}

func NewAdminTestSendHandler(
	recipientRepo repository.RecipientRepository,
	templateRepo repository.NotificationTemplateRepository,
	notificationRepo repository.NotificationRepository,
	deliveryRepo repository.DeliveryRepository,
	publisher notification.NotificationPublisher,
	logger *slog.Logger,
) *AdminTestSendHandler {
	if publisher == nil {
		publisher = notification.NoopNotificationPublisher{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminTestSendHandler{
		recipientRepo:    recipientRepo,
		templateRepo:     templateRepo,
		notificationRepo: notificationRepo,
		deliveryRepo:     deliveryRepo,
		publisher:        publisher,
		logger:           logger,
	}
}

type testSendRequest struct {
	UserID        uuid.UUID      `json:"user_id"`
	TemplateKey   string         `json:"template_key"`
	SamplePayload map[string]any `json:"sample_payload"`
}

// TestSend handles POST /admin/notifications/test-send.
func (h *AdminTestSendHandler) TestSend(c *gin.Context) {
	var req testSendRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	if req.UserID == uuid.Nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "user_id is required", []domain.ErrorDetail{
			{Field: "user_id", Issue: "required"},
		})
		return
	}
	if req.TemplateKey == "" {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "template_key is required", []domain.ErrorDetail{
			{Field: "template_key", Issue: "required"},
		})
		return
	}

	// 1. Verify user exists in notification_recipients
	recipient, err := h.recipientRepo.FindByID(c.Request.Context(), req.UserID)
	if err != nil || recipient == nil {
		RespondError(c, http.StatusNotFound, domain.CodeNotFound, "User recipient not found", nil)
		return
	}

	// 2. Load active template
	tmpl, err := h.templateRepo.GetActive(c.Request.Context(), req.TemplateKey, "en")
	if err != nil || tmpl == nil {
		RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Notification template not found", nil)
		return
	}

	// 3. Render title and body
	payloadBytes, _ := json.Marshal(req.SamplePayload)
	eventID := uuid.New()
	env := &domain.EventEnvelope{
		EventID:       eventID,
		EventType:     tmpl.EventType,
		EventVersion: "1.0",
		OccurredAt:    time.Now().UTC(),
		Producer:      "admin-test-send",
		AggregateType: "test",
		AggregateID:   req.UserID.String(),
		Payload:       payloadBytes,
	}

	title := notification.RenderTemplate(tmpl.TitleTemplate, env, h.logger)
	body := notification.RenderTemplate(tmpl.BodyTemplate, env, h.logger)

	// 4. Create Notification record with is_test = true
	notifID := uuid.New()
	notif := &domain.Notification{
		ID:          notifID,
		RecipientID: req.UserID,
		EventID:     eventID,
		EventType:   tmpl.EventType,
		Type:        domain.TypeSystem,
		Category:    domain.CategorySystem,
		Priority:    domain.PriorityNormal,
		Title:       title,
		Body:        body,
		Status:      domain.NotificationStatusUnread,
		IsTest:      true,
		CreatedAt:   time.Now().UTC(),
	}

	if err := h.notificationRepo.BatchInsert(c.Request.Context(), []*domain.Notification{notif}); err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed creating test notification: "+err.Error(), nil)
		return
	}

	// 5. Create Delivery record (in_app, sent)
	deliv := &domain.NotificationDelivery{
		ID:             uuid.New(),
		NotificationID: notifID,
		Channel:        domain.ChannelInApp,
		Status:         domain.DeliveryStatusSent,
		Attempts:       1,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	if err := h.deliveryRepo.Create(c.Request.Context(), deliv); err != nil {
		h.logger.ErrorContext(c.Request.Context(), "failed recording test delivery", "error", err)
	}

	// 6. Push live via SSE publisher
	if h.publisher != nil {
		counts := notification.UnreadCounts{Total: 1}
		if h.notificationRepo != nil {
			if total, catMap, err := h.notificationRepo.GetUnreadCounts(c.Request.Context(), req.UserID); err == nil {
				counts.Total = total
				counts.ByCategory = catMap
			}
		}
		if err := h.publisher.PublishNotification(c.Request.Context(), notif, counts); err != nil {
			h.logger.WarnContext(c.Request.Context(), "failed publishing test notification to SSE", "error", err)
		}
	}

	adminID, _ := GetUserID(c)
	h.logger.Info("admin_audit",
		"action", "test_send_notification",
		"admin_id", adminID,
		"target_user_id", req.UserID,
		"template_key", req.TemplateKey,
		"notification_id", notifID,
	)

	RespondSuccess(c, http.StatusOK, gin.H{
		"delivered": true,
		"channels":  []string{"in_app"},
	})
}
