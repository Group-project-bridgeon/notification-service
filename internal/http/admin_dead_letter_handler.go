package http

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/ingest"
	"notification-service/internal/repository"
)

type AdminDeadLetterHandler struct {
	deadLetterRepo repository.DeadLetterRepository
	ingestor       *ingest.Ingestor
	logger         *slog.Logger
}

func NewAdminDeadLetterHandler(
	deadLetterRepo repository.DeadLetterRepository,
	ingestor *ingest.Ingestor,
	logger *slog.Logger,
) *AdminDeadLetterHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminDeadLetterHandler{
		deadLetterRepo: deadLetterRepo,
		ingestor:       ingestor,
		logger:         logger,
	}
}

// List handles GET /admin/notification-dead-letters.
func (h *AdminDeadLetterHandler) List(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	offset := (page - 1) * pageSize

	list, total, err := h.deadLetterRepo.List(c.Request.Context(), pageSize, offset)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}
	if list == nil {
		list = []*domain.DeadLetter{}
	}

	RespondList(c, http.StatusOK, list, page, pageSize, total)
}

// Replay handles POST /admin/notification-dead-letters/:id/replay.
func (h *AdminDeadLetterHandler) Replay(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid dead letter ID UUID", nil)
		return
	}

	dl, err := h.deadLetterRepo.GetByID(c.Request.Context(), id)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	if dl.ReplayedAt != nil {
		RespondError(c, http.StatusConflict, domain.CodeInvalidStateTransition, "Dead letter has already been replayed", nil)
		return
	}

	var rawBytes []byte
	if len(dl.RawEvent) > 0 {
		rawBytes = []byte(dl.RawEvent)
	} else if len(dl.RawValue) > 0 {
		rawBytes = dl.RawValue
	}

	if len(rawBytes) == 0 {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Dead letter contains no event data to replay", nil)
		return
	}

	env, err := ingest.ParseAndValidate(rawBytes)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Cannot replay dead letter: invalid event envelope: "+err.Error(), nil)
		return
	}

	if h.ingestor != nil {
		meta := ingest.IngestMeta{
			Topic:     dl.Topic,
			Partition: dl.Partition,
			Offset:    dl.KafkaOffset,
		}
		if err := h.ingestor.Handle(c.Request.Context(), dl.Source, env, meta); err != nil {
			RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Replay ingestion failed: "+err.Error(), nil)
			return
		}
	}

	if err := h.deadLetterRepo.MarkReplayed(c.Request.Context(), id); err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed updating dead letter replayed_at: "+err.Error(), nil)
		return
	}

	adminID, _ := GetUserID(c)
	h.logger.Info("admin_audit",
		"action", "replay_dead_letter",
		"admin_id", adminID,
		"dead_letter_id", id,
		"event_id", env.EventID,
	)

	RespondSuccess(c, http.StatusOK, gin.H{
		"replayed": true,
		"id":       id,
		"event_id": env.EventID,
	})
}
