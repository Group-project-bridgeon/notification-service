package http

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

type AdminDeliveryHandler struct {
	deliveryRepo repository.DeliveryRepository
	logger       *slog.Logger
}

func NewAdminDeliveryHandler(
	deliveryRepo repository.DeliveryRepository,
	logger *slog.Logger,
) *AdminDeliveryHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminDeliveryHandler{
		deliveryRepo: deliveryRepo,
		logger:       logger,
	}
}

// List handles GET /admin/notification-deliveries.
func (h *AdminDeliveryHandler) List(c *gin.Context) {
	var filter domain.DeliveryFilter

	if s := c.Query("status"); s != "" {
		st := domain.DeliveryStatus(s)
		filter.Status = &st
	}
	if ch := c.Query("channel"); ch != "" {
		cChan := domain.Channel(ch)
		filter.Channel = &cChan
	}
	if fromStr := c.Query("from"); fromStr != "" {
		if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
			filter.From = &t
		}
	}
	if toStr := c.Query("to"); toStr != "" {
		if t, err := time.Parse(time.RFC3339, toStr); err == nil {
			filter.To = &t
		}
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	} else if pageSize > 100 {
		pageSize = 100
	}
	filter.Page = page
	filter.PageSize = pageSize

	deliveries, total, err := h.deliveryRepo.List(c.Request.Context(), filter)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}
	if deliveries == nil {
		deliveries = []*domain.NotificationDelivery{}
	}

	RespondList(c, http.StatusOK, deliveries, page, pageSize, total)
}

// Retry handles POST /admin/notification-deliveries/:id/retry.
// Allowed only when status is failed or dead.
func (h *AdminDeliveryHandler) Retry(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid delivery ID UUID", nil)
		return
	}

	delivery, err := h.deliveryRepo.GetByID(c.Request.Context(), id)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	if delivery.Status != domain.DeliveryStatusFailed && delivery.Status != domain.DeliveryStatusDead {
		RespondError(c, http.StatusConflict, domain.CodeInvalidStateTransition,
			"Only failed or dead deliveries can be retried", nil)
		return
	}

	now := time.Now().UTC()
	attempts := delivery.Attempts + 1
	if err := h.deliveryRepo.UpdateStatus(c.Request.Context(), id, domain.DeliveryStatusPending, attempts, &now, nil); err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	delivery.Status = domain.DeliveryStatusPending
	delivery.Attempts = attempts
	delivery.NextRetryAt = &now
	delivery.LastError = nil

	adminID, _ := GetUserID(c)
	h.logger.Info("admin_audit",
		"action", "retry_notification_delivery",
		"admin_id", adminID,
		"delivery_id", id,
		"attempts", attempts,
	)

	RespondSuccess(c, http.StatusOK, delivery)
}
