package http

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
)

// ConsumerStatusProvider provides consumer lag and execution metrics.
type ConsumerStatusProvider interface {
	GetStatus(ctx context.Context) *domain.ConsumerStatus
}

type AdminConsumerStatusHandler struct {
	provider ConsumerStatusProvider
	groupID  string
	logger   *slog.Logger
}

func NewAdminConsumerStatusHandler(
	provider ConsumerStatusProvider,
	groupID string,
	logger *slog.Logger,
) *AdminConsumerStatusHandler {
	if logger == nil {
		logger = slog.Default()
	}
	if groupID == "" {
		groupID = "notification-service"
	}
	return &AdminConsumerStatusHandler{
		provider: provider,
		groupID:  groupID,
		logger:   logger,
	}
}

// GetStatus handles GET /admin/notification-consumer-status.
// If Kafka is unreachable, returns 200 with state=disconnected and no lag values, never a 500.
func (h *AdminConsumerStatusHandler) GetStatus(c *gin.Context) {
	if h.provider == nil {
		RespondSuccess(c, http.StatusOK, &domain.ConsumerStatus{
			GroupID:        h.groupID,
			State:          "disconnected",
			Topics:         []domain.ConsumerTopicStatus{},
			ProcessedTotal: 0,
			RetriedTotal:   0,
			DLQTotal:       0,
			FailedTotal:    0,
		})
		return
	}

	status := h.provider.GetStatus(c.Request.Context())
	if status == nil {
		RespondSuccess(c, http.StatusOK, &domain.ConsumerStatus{
			GroupID:        h.groupID,
			State:          "disconnected",
			Topics:         []domain.ConsumerTopicStatus{},
			ProcessedTotal: 0,
			RetriedTotal:   0,
			DLQTotal:       0,
			FailedTotal:    0,
		})
		return
	}

	RespondSuccess(c, http.StatusOK, status)
}
