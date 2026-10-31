package http

import (
	"io"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
	"notification-service/internal/ingest"
)

// InternalEventsHandler handles POST /internal/events for HTTP event ingestion.
type InternalEventsHandler struct {
	ingestor *ingest.Ingestor
	enabled  bool
}

// NewInternalEventsHandler constructs an InternalEventsHandler.
func NewInternalEventsHandler(ingestor *ingest.Ingestor, enabled bool) *InternalEventsHandler {
	return &InternalEventsHandler{
		ingestor: ingestor,
		enabled:  enabled,
	}
}

// HandleEvent processes an incoming HTTP canonical event envelope.
// Returns 202 Accepted, 400 validation_failed, or 404 if disabled.
func (h *InternalEventsHandler) HandleEvent(c *gin.Context) {
	if !h.enabled {
		RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Internal events endpoint is disabled", nil)
		return
	}

	// Read body (up to MaxMessageBytes + 1 so ParseAndValidate can detect oversized payloads)
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, ingest.MaxMessageBytes+2))
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Failed to read request body", nil)
		return
	}

	env, err := ingest.ParseAndValidate(body)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, err.Error(), nil)
		return
	}

	meta := ingest.IngestMeta{}
	if err := h.ingestor.Handle(c.Request.Context(), domain.EventSourceHTTP, env, meta); err != nil {
		if ingest.IsPermanent(err) {
			RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, err.Error(), nil)
			return
		}
		slog.Error("Internal events ingestion failed", slog.String("error", err.Error()))
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusAccepted, gin.H{
		"status":   "accepted",
		"event_id": env.EventID,
	})
}
