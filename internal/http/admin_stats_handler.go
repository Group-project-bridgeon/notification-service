package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

type AdminStatsHandler struct {
	statsRepo repository.StatsRepository
	logger    *slog.Logger
}

func NewAdminStatsHandler(
	statsRepo repository.StatsRepository,
	logger *slog.Logger,
) *AdminStatsHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminStatsHandler{
		statsRepo: statsRepo,
		logger:    logger,
	}
}

// GetStats handles GET /admin/notification-stats.
func (h *AdminStatsHandler) GetStats(c *gin.Context) {
	since := time.Now().UTC().Add(-24 * time.Hour)

	stats, err := h.statsRepo.GetNotificationStats(c.Request.Context(), since)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed querying notification stats: "+err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, stats)
}
