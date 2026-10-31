package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
	"notification-service/internal/email"
)

// EmailHandler handles internal email dispatch routes.
type EmailHandler struct {
	engine *email.Engine
}

// NewEmailHandler creates an EmailHandler instance.
func NewEmailHandler(engine *email.Engine) *EmailHandler {
	return &EmailHandler{engine: engine}
}

// Send handles POST /internal/email/send.
func (h *EmailHandler) Send(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey == "" {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Idempotency-Key header is required", []domain.ErrorDetail{
			{Field: "Idempotency-Key", Issue: "idempotency_key_required"},
		})
		return
	}

	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Failed reading request payload", nil)
		return
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(rawBody))

	var req email.SendEmailRequest
	if err := json.Unmarshal(rawBody, &req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed JSON request payload", nil)
		return
	}

	res, err := h.engine.SendEmail(c.Request.Context(), &req, idempotencyKey, rawBody)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusAccepted, res)
}
