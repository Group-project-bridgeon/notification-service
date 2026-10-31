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

// EmailWebhookHandler processes inbound email provider webhooks with HMAC-SHA256 signature verification.
type EmailWebhookHandler struct {
	engine        *email.Engine
	webhookSecret string
}

// NewEmailWebhookHandler creates an EmailWebhookHandler instance.
func NewEmailWebhookHandler(engine *email.Engine, webhookSecret string) *EmailWebhookHandler {
	return &EmailWebhookHandler{
		engine:        engine,
		webhookSecret: webhookSecret,
	}
}

// HandleProviderWebhook handles POST /webhooks/email/:provider.
func (h *EmailWebhookHandler) HandleProviderWebhook(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	provider := c.Param("provider")
	if provider == "" {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Provider parameter is required", nil)
		return
	}

	sigHeader := c.GetHeader("X-Signature")
	if sigHeader == "" {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Missing X-Signature header", nil)
		return
	}

	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Failed reading webhook body", nil)
		return
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(rawBody))

	var payload email.WebhookPayload
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed webhook JSON payload", nil)
		return
	}

	if err := h.engine.HandleWebhook(c.Request.Context(), provider, rawBody, sigHeader, h.webhookSecret, &payload); err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"processed": true,
		"provider":  provider,
		"event":     payload.Event,
	})
}
