package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
	"notification-service/internal/email"
)

// EmailTemplateHandler manages email template versions, preview, and activation.
type EmailTemplateHandler struct {
	engine *email.Engine
}

// NewEmailTemplateHandler creates an EmailTemplateHandler instance.
func NewEmailTemplateHandler(engine *email.Engine) *EmailTemplateHandler {
	return &EmailTemplateHandler{engine: engine}
}

// List handles GET /email/templates.
func (h *EmailTemplateHandler) List(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	templates, err := h.engine.ListTemplates(c.Request.Context())
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	// Distinct by template_key, picking active or highest version
	type TemplateSummary struct {
		TemplateKey        string   `json:"template_key"`
		ActiveVersion      int      `json:"active_version"`
		SubjectTemplate    string   `json:"subject_template"`
		DeclaredVariables  []string `json:"declared_variables"`
		SensitiveVariables []string `json:"sensitive_variables"`
		IsActive           bool     `json:"is_active"`
	}

	summaryMap := make(map[string]*TemplateSummary)
	for _, t := range templates {
		existing, ok := summaryMap[t.TemplateKey]
		if !ok {
			summaryMap[t.TemplateKey] = &TemplateSummary{
				TemplateKey:        t.TemplateKey,
				ActiveVersion:      t.Version,
				SubjectTemplate:    t.SubjectTemplate,
				DeclaredVariables:  t.DeclaredVariables,
				SensitiveVariables: t.SensitiveVariables,
				IsActive:           t.IsActive,
			}
		} else if t.IsActive {
			existing.ActiveVersion = t.Version
			existing.SubjectTemplate = t.SubjectTemplate
			existing.DeclaredVariables = t.DeclaredVariables
			existing.SensitiveVariables = t.SensitiveVariables
			existing.IsActive = true
		}
	}

	summaries := make([]*TemplateSummary, 0, len(summaryMap))
	for _, s := range summaryMap {
		summaries = append(summaries, s)
	}

	RespondSuccess(c, http.StatusOK, summaries)
}

// GetByKey handles GET /email/templates/:key.
func (h *EmailTemplateHandler) GetByKey(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	key := c.Param("key")
	details, err := h.engine.GetTemplateDetails(c.Request.Context(), key)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) || domain.IsNotFound(err) {
			RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Email template not found", nil)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, details)
}

// PutVersion handles PUT /email/templates/:key. Always creates a new version.
func (h *EmailTemplateHandler) PutVersion(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	key := c.Param("key")
	var req email.CreateTemplateVersionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	if uid, ok := GetUserID(c); ok {
		req.CreatedBy = &uid
	}

	created, err := h.engine.CreateTemplateVersion(c.Request.Context(), key, &req)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusCreated, created)
}

type previewRequest struct {
	Variables map[string]any `json:"variables"`
}

// Preview handles POST /email/templates/:key/preview. Renders with escaping, never sends.
func (h *EmailTemplateHandler) Preview(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	key := c.Param("key")
	var req previewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	res, err := h.engine.PreviewTemplate(c.Request.Context(), key, req.Variables)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusBadRequest, domain.CodeEmailTemplateInvalid, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"subject": res.Subject,
		"html":    res.HTMLBody,
		"text":    res.TextBody,
	})
}

type activateRequest struct {
	Version int `json:"version"`
}

// Activate handles POST /email/templates/:key/activate.
func (h *EmailTemplateHandler) Activate(c *gin.Context) {
	if h.engine == nil {
		RespondError(c, http.StatusConflict, domain.CodeEmailNotConfigured, "Email engine not configured", nil)
		return
	}

	key := c.Param("key")
	var req activateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	if err := h.engine.ActivateTemplate(c.Request.Context(), key, req.Version); err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"template_key": key,
		"version":      req.Version,
		"is_active":    true,
	})
}
