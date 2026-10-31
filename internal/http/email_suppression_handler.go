package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/email"
)

// EmailSuppressionHandler manages the recipient suppression list.
type EmailSuppressionHandler struct {
	engine *email.Engine
}

// NewEmailSuppressionHandler creates an EmailSuppressionHandler instance.
func NewEmailSuppressionHandler(engine *email.Engine) *EmailSuppressionHandler {
	return &EmailSuppressionHandler{engine: engine}
}

// List handles GET /email/suppressions.
func (h *EmailSuppressionHandler) List(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	suppressions, err := h.engine.ListSuppressions(c.Request.Context())
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, suppressions)
}

type addSuppressionRequest struct {
	Email  string                        `json:"email"`
	Reason domain.EmailSuppressionReason `json:"reason,omitempty"`
}

// Add handles POST /email/suppressions.
func (h *EmailSuppressionHandler) Add(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	var req addSuppressionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	if req.Email == "" {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "email is required", []domain.ErrorDetail{
			{Field: "email", Issue: "required"},
		})
		return
	}

	var createdByPtr *uuid.UUID
	if uid, ok := GetUserID(c); ok {
		createdByPtr = &uid
	}

	if err := h.engine.AddSuppression(c.Request.Context(), req.Email, req.Reason, createdByPtr); err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusCreated, gin.H{
		"email":  req.Email,
		"reason": req.Reason,
		"status": "suppressed",
	})
}

// Delete handles DELETE /email/suppressions/:email.
func (h *EmailSuppressionHandler) Delete(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	emailAddr := c.Param("email")
	if emailAddr == "" {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "email address parameter required", nil)
		return
	}

	if err := h.engine.DeleteSuppression(c.Request.Context(), emailAddr); err != nil {
		if errors.Is(err, domain.ErrNotFound) || domain.IsNotFound(err) {
			RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Email suppression not found", nil)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	c.Status(http.StatusNoContent)
}
