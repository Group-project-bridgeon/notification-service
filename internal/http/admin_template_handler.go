package http

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

type AdminTemplateHandler struct {
	templateRepo repository.NotificationTemplateRepository
	logger       *slog.Logger
}

func NewAdminTemplateHandler(
	templateRepo repository.NotificationTemplateRepository,
	logger *slog.Logger,
) *AdminTemplateHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminTemplateHandler{
		templateRepo: templateRepo,
		logger:       logger,
	}
}

func TemplateETag(tmpl *domain.NotificationTemplate) string {
	return fmt.Sprintf(`"%d"`, tmpl.Version)
}

type templateCreateRequest struct {
	TemplateKey   string `json:"template_key"`
	EventType     string `json:"event_type"`
	Locale        string `json:"locale,omitempty"`
	TitleTemplate string `json:"title_template"`
	BodyTemplate  string `json:"body_template"`
}

type templateUpdateRequest struct {
	EventType     string `json:"event_type"`
	Locale        string `json:"locale,omitempty"`
	TitleTemplate string `json:"title_template"`
	BodyTemplate  string `json:"body_template"`
}

// List handles GET /admin/notification-templates.
func (h *AdminTemplateHandler) List(c *gin.Context) {
	templates, err := h.templateRepo.List(c.Request.Context())
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}
	if templates == nil {
		templates = []*domain.NotificationTemplate{}
	}
	RespondSuccess(c, http.StatusOK, templates)
}

// Create handles POST /admin/notification-templates.
func (h *AdminTemplateHandler) Create(c *gin.Context) {
	var req templateCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	var details []domain.ErrorDetail
	if strings.TrimSpace(req.TemplateKey) == "" {
		details = append(details, domain.ErrorDetail{Field: "template_key", Issue: "required"})
	}
	if strings.TrimSpace(req.EventType) == "" {
		details = append(details, domain.ErrorDetail{Field: "event_type", Issue: "required"})
	}
	if strings.TrimSpace(req.TitleTemplate) == "" {
		details = append(details, domain.ErrorDetail{Field: "title_template", Issue: "required"})
	}
	if strings.TrimSpace(req.BodyTemplate) == "" {
		details = append(details, domain.ErrorDetail{Field: "body_template", Issue: "required"})
	}
	if len(details) > 0 {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Template validation failed", details)
		return
	}

	// Validate placeholders strictly
	if err := domain.ValidateTemplatePlaceholders(req.TitleTemplate, req.BodyTemplate); err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, err.Error(), nil)
		return
	}

	locale := req.Locale
	if locale == "" {
		locale = "en"
	}

	tmpl := &domain.NotificationTemplate{
		ID:            uuid.New(),
		TemplateKey:   req.TemplateKey,
		EventType:     req.EventType,
		Locale:        locale,
		TitleTemplate: req.TitleTemplate,
		BodyTemplate:  req.BodyTemplate,
		Version:       1,
		IsActive:      true,
	}

	if err := h.templateRepo.Create(c.Request.Context(), tmpl); err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	adminID, _ := GetUserID(c)
	h.logger.Info("admin_audit",
		"action", "create_notification_template",
		"admin_id", adminID,
		"template_key", tmpl.TemplateKey,
		"version", tmpl.Version,
	)

	c.Header("ETag", TemplateETag(tmpl))
	RespondSuccess(c, http.StatusCreated, tmpl)
}

// PutVersion handles PUT /admin/notification-templates/:key.
// Creates a new version and deactivates the previous one.
func (h *AdminTemplateHandler) PutVersion(c *gin.Context) {
	key := c.Param("key")
	if strings.TrimSpace(key) == "" {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Template key is required", nil)
		return
	}

	var req templateUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	var details []domain.ErrorDetail
	if strings.TrimSpace(req.TitleTemplate) == "" {
		details = append(details, domain.ErrorDetail{Field: "title_template", Issue: "required"})
	}
	if strings.TrimSpace(req.BodyTemplate) == "" {
		details = append(details, domain.ErrorDetail{Field: "body_template", Issue: "required"})
	}
	if len(details) > 0 {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Template validation failed", details)
		return
	}

	// Validate placeholders strictly
	if err := domain.ValidateTemplatePlaceholders(req.TitleTemplate, req.BodyTemplate); err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, err.Error(), nil)
		return
	}

	locale := req.Locale
	if locale == "" {
		locale = "en"
	}

	// Check active version for If-Match concurrency check
	activeTmpl, err := h.templateRepo.GetActive(c.Request.Context(), key, locale)
	if err == nil && activeTmpl != nil {
		if !CheckETagMatch(c, TemplateETag(activeTmpl)) {
			RespondError(c, http.StatusConflict, domain.CodeVersionConflict, "Template version conflict: If-Match ETag does not match active version", nil)
			return
		}
	}

	eventType := req.EventType
	if eventType == "" && activeTmpl != nil {
		eventType = activeTmpl.EventType
	}

	newTmpl := &domain.NotificationTemplate{
		ID:            uuid.New(),
		TemplateKey:   key,
		EventType:     eventType,
		Locale:        locale,
		TitleTemplate: req.TitleTemplate,
		BodyTemplate:  req.BodyTemplate,
	}

	if err := h.templateRepo.CreateNewVersion(c.Request.Context(), newTmpl); err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	adminID, _ := GetUserID(c)
	h.logger.Info("admin_audit",
		"action", "update_notification_template",
		"admin_id", adminID,
		"template_key", newTmpl.TemplateKey,
		"version", newTmpl.Version,
	)

	c.Header("ETag", TemplateETag(newTmpl))
	RespondSuccess(c, http.StatusOK, newTmpl)
}
