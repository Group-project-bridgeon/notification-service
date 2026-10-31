package http

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/email"
)

// EmailMessageHandler manages outbound email audit records, status inspections, and resend/cancellation actions.
type EmailMessageHandler struct {
	engine *email.Engine
}

// NewEmailMessageHandler creates an EmailMessageHandler instance.
func NewEmailMessageHandler(engine *email.Engine) *EmailMessageHandler {
	return &EmailMessageHandler{engine: engine}
}

// List handles GET /email/messages.
func (h *EmailMessageHandler) List(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	filter := domain.EmailMessageFilter{
		Page:     1,
		PageSize: 20,
	}

	if p := c.Query("page"); p != "" {
		if page, err := strconv.Atoi(p); err == nil && page > 0 {
			filter.Page = page
		}
	}
	if ps := c.Query("page_size"); ps != "" {
		if pageSize, err := strconv.Atoi(ps); err == nil && pageSize > 0 {
			filter.PageSize = pageSize
		}
	}

	if s := c.Query("status"); s != "" {
		st := domain.EmailMessageStatus(s)
		filter.Status = &st
	}
	if tk := c.Query("template_key"); tk != "" {
		filter.TemplateKey = &tk
	}
	if to := c.Query("to"); to != "" {
		filter.ToAddress = &to
	}
	if rt := c.Query("related_type"); rt != "" {
		filter.RelatedType = &rt
	}
	if rid := c.Query("related_id"); rid != "" {
		if parsed, err := uuid.Parse(rid); err == nil {
			filter.RelatedID = &parsed
		}
	}
	if f := c.Query("from"); f != "" {
		if t, err := time.Parse(time.RFC3339, f); err == nil {
			filter.From = &t
		}
	}
	if td := c.Query("to_date"); td != "" {
		if t, err := time.Parse(time.RFC3339, td); err == nil {
			filter.ToDate = &t
		}
	}
	if sort := c.Query("sort"); sort != "" {
		filter.Sort = sort
	}

	// Scoping: non-admin callers see only messages they sent
	callerID, hasCaller := GetUserID(c)
	if !hasAdminRole(c) {
		if hasCaller {
			filter.SentBy = &callerID
		} else {
			RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Caller identity required", nil)
			return
		}
	}

	messages, totalCount, err := h.engine.ListMessages(c.Request.Context(), filter)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondList(c, http.StatusOK, messages, filter.Page, filter.PageSize, totalCount)
}

// GetByID handles GET /email/messages/:id.
func (h *EmailMessageHandler) GetByID(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid message ID UUID format", nil)
		return
	}

	callerID, hasCaller := GetUserID(c)
	var callerUIDPtr *uuid.UUID
	if hasCaller {
		callerUIDPtr = &callerID
	}
	isAdmin := hasAdminRole(c)

	msg, err := h.engine.GetMessage(c.Request.Context(), id, callerUIDPtr, isAdmin)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) || domain.IsNotFound(err) {
			RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Email message not found", nil)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, msg)
}

// Resend handles POST /email/messages/:id/resend.
func (h *EmailMessageHandler) Resend(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid message ID UUID format", nil)
		return
	}

	callerID, hasCaller := GetUserID(c)
	var callerUIDPtr *uuid.UUID
	if hasCaller {
		callerUIDPtr = &callerID
	}
	isAdmin := hasAdminRole(c)

	newMsg, err := h.engine.ResendMessage(c.Request.Context(), id, callerUIDPtr, isAdmin)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		if errors.Is(err, domain.ErrNotFound) || domain.IsNotFound(err) {
			RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Email message not found", nil)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, newMsg)
}

// Cancel handles POST /email/messages/:id/cancel.
func (h *EmailMessageHandler) Cancel(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid message ID UUID format", nil)
		return
	}

	callerID, hasCaller := GetUserID(c)
	var callerUIDPtr *uuid.UUID
	if hasCaller {
		callerUIDPtr = &callerID
	}
	isAdmin := hasAdminRole(c)

	if err := h.engine.CancelMessage(c.Request.Context(), id, callerUIDPtr, isAdmin); err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		if errors.Is(err, domain.ErrNotFound) || domain.IsNotFound(err) {
			RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Email message not found", nil)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, gin.H{"status": "cancelled", "id": id})
}

func hasAdminRole(c *gin.Context) bool {
	for _, r := range GetUserRoles(c) {
		if strings.EqualFold(r, "admin") {
			return true
		}
	}
	return false
}
