package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
	"notification-service/internal/email"
)

// AdminEmailHandler manages email configuration and connectivity testing.
type AdminEmailHandler struct {
	engine *email.Engine
}

// NewAdminEmailHandler creates an AdminEmailHandler instance.
func NewAdminEmailHandler(engine *email.Engine) *AdminEmailHandler {
	return &AdminEmailHandler{engine: engine}
}

// GetSettings handles GET /admin/email-settings.
func (h *AdminEmailHandler) GetSettings(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	settings, err := h.engine.GetSettings(c.Request.Context())
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) || domain.IsNotFound(err) {
			RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Email settings not configured", nil)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, settings)
}

// UpdateSettings handles PUT /admin/email-settings.
func (h *AdminEmailHandler) UpdateSettings(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	var req email.UpdateSettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	if uid, ok := GetUserID(c); ok {
		req.UpdatedBy = &uid
	}

	if err := h.engine.UpdateSettings(c.Request.Context(), &req); err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	settings, err := h.engine.GetSettings(c.Request.Context())
	if err != nil {
		RespondSuccess(c, http.StatusOK, gin.H{"status": "updated"})
		return
	}

	RespondSuccess(c, http.StatusOK, settings)
}

type testSettingsRequest struct {
	To string `json:"to"`
}

// TestSettings handles POST /admin/email-settings/test.
func (h *AdminEmailHandler) TestSettings(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	var req testSettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	res, err := h.engine.TestSettings(c.Request.Context(), req.To)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusBadGateway, domain.CodeEmailSendFailed, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"delivered":           true,
		"provider_message_id": res.ProviderMessageID,
	})
}
