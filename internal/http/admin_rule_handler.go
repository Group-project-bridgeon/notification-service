package http

import (
	"crypto/sha256"
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

type AdminRuleHandler struct {
	ruleRepo          repository.NotificationRuleRepository
	emailTemplateRepo repository.EmailTemplateRepository
	allowedRoles      []string
	logger            *slog.Logger
}

func NewAdminRuleHandler(
	ruleRepo repository.NotificationRuleRepository,
	emailTemplateRepo repository.EmailTemplateRepository,
	allowedRoles []string,
	logger *slog.Logger,
) *AdminRuleHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminRuleHandler{
		ruleRepo:          ruleRepo,
		emailTemplateRepo: emailTemplateRepo,
		allowedRoles:      allowedRoles,
		logger:            logger,
	}
}

func RuleETag(rule *domain.NotificationRule) string {
	data := fmt.Sprintf("%s:%s:%s:%s:%t:%d", rule.ID, rule.EventType, rule.Category, rule.Priority, rule.IsActive, rule.CreatedAt.UnixNano())
	sum := sha256.Sum256([]byte(data))
	return fmt.Sprintf(`"%x"`, sum[:8])
}

func CheckETagMatch(c *gin.Context, currentETag string) bool {
	ifMatch := c.GetHeader("If-Match")
	if ifMatch == "" || ifMatch == "*" {
		return true
	}
	cleanMatch := strings.Trim(strings.TrimPrefix(strings.TrimSpace(ifMatch), "W/"), `"`)
	cleanCurrent := strings.Trim(strings.TrimPrefix(strings.TrimSpace(currentETag), "W/"), `"`)
	return cleanMatch == cleanCurrent
}

type ruleRequest struct {
	EventType        string   `json:"event_type"`
	TargetRoles      []string `json:"target_roles"`
	TargetUserPath   *string  `json:"target_user_path,omitempty"`
	Type             string   `json:"type"`
	Category         string   `json:"category"`
	Priority         string   `json:"priority"`
	LinkTemplate     *string  `json:"link_template,omitempty"`
	TemplateKey      string   `json:"template_key"`
	SendEmail        bool     `json:"send_email"`
	EmailTemplateKey *string  `json:"email_template_key,omitempty"`
	IsActive         *bool    `json:"is_active,omitempty"`
}

func (h *AdminRuleHandler) validateRuleRequest(c *gin.Context, req *ruleRequest) ([]domain.ErrorDetail, bool) {
	var details []domain.ErrorDetail

	if strings.TrimSpace(req.EventType) == "" {
		details = append(details, domain.ErrorDetail{Field: "event_type", Issue: "required"})
	}

	// Validate roles against configured list
	roleMap := make(map[string]struct{}, len(h.allowedRoles))
	for _, r := range h.allowedRoles {
		roleMap[strings.ToLower(r)] = struct{}{}
	}
	for _, r := range req.TargetRoles {
		if _, ok := roleMap[strings.ToLower(r)]; !ok {
			details = append(details, domain.ErrorDetail{
				Field: "target_roles",
				Issue: fmt.Sprintf("invalid_role:%s", r),
			})
		}
	}

	// Validate Category
	if !domain.IsValidCategory(domain.NotificationCategory(req.Category)) {
		details = append(details, domain.ErrorDetail{Field: "category", Issue: "invalid_category"})
	}

	// Validate Priority
	if req.Priority == "" {
		req.Priority = string(domain.PriorityNormal)
	}
	if !domain.IsValidPriority(domain.Priority(req.Priority)) {
		details = append(details, domain.ErrorDetail{Field: "priority", Issue: "invalid_priority"})
	}

	// Validate Type
	if !domain.IsValidNotificationType(domain.NotificationType(req.Type)) {
		details = append(details, domain.ErrorDetail{Field: "type", Issue: "invalid_type"})
	}

	// Validate TemplateKey
	if strings.TrimSpace(req.TemplateKey) == "" {
		details = append(details, domain.ErrorDetail{Field: "template_key", Issue: "required"})
	}

	// Validate send_email requirements
	if req.SendEmail {
		if req.EmailTemplateKey == nil || strings.TrimSpace(*req.EmailTemplateKey) == "" {
			details = append(details, domain.ErrorDetail{Field: "email_template_key", Issue: "required_when_send_email_true"})
		} else if h.emailTemplateRepo != nil {
			tmpl, err := h.emailTemplateRepo.GetActive(c.Request.Context(), *req.EmailTemplateKey)
			if err != nil || tmpl == nil {
				details = append(details, domain.ErrorDetail{Field: "email_template_key", Issue: "email_template_not_found"})
			}
		}
	}

	return details, len(details) == 0
}

// List handles GET /admin/notification-rules.
func (h *AdminRuleHandler) List(c *gin.Context) {
	rules, err := h.ruleRepo.List(c.Request.Context())
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}
	if rules == nil {
		rules = []*domain.NotificationRule{}
	}
	RespondSuccess(c, http.StatusOK, rules)
}

// GetByID handles GET /admin/notification-rules/:id.
func (h *AdminRuleHandler) GetByID(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid rule ID UUID", nil)
		return
	}

	rule, err := h.ruleRepo.GetByID(c.Request.Context(), id)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	c.Header("ETag", RuleETag(rule))
	RespondSuccess(c, http.StatusOK, rule)
}

// Create handles POST /admin/notification-rules.
func (h *AdminRuleHandler) Create(c *gin.Context) {
	var req ruleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	if details, ok := h.validateRuleRequest(c, &req); !ok {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Rule validation failed", details)
		return
	}

	isActive := true
	if req.IsActive != nil {
		isActive = *req.IsActive
	}

	rule := &domain.NotificationRule{
		ID:               uuid.New(),
		EventType:        req.EventType,
		TargetRoles:      req.TargetRoles,
		TargetUserPath:   req.TargetUserPath,
		Type:             domain.NotificationType(req.Type),
		Category:         domain.NotificationCategory(req.Category),
		Priority:         domain.Priority(req.Priority),
		LinkTemplate:     req.LinkTemplate,
		TemplateKey:      req.TemplateKey,
		SendEmail:        req.SendEmail,
		EmailTemplateKey: req.EmailTemplateKey,
		IsActive:         isActive,
	}

	if err := h.ruleRepo.Create(c.Request.Context(), rule); err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	adminID, _ := GetUserID(c)
	h.logger.Info("admin_audit",
		"action", "create_notification_rule",
		"admin_id", adminID,
		"rule_id", rule.ID,
		"event_type", rule.EventType,
	)

	c.Header("ETag", RuleETag(rule))
	RespondSuccess(c, http.StatusCreated, rule)
}

// Update handles PUT /admin/notification-rules/:id.
func (h *AdminRuleHandler) Update(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid rule ID UUID", nil)
		return
	}

	existing, err := h.ruleRepo.GetByID(c.Request.Context(), id)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	currentETag := RuleETag(existing)
	if !CheckETagMatch(c, currentETag) {
		RespondError(c, http.StatusConflict, domain.CodeVersionConflict, "Rule version conflict: If-Match ETag does not match current version", nil)
		return
	}

	var req ruleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Malformed request body: "+err.Error(), nil)
		return
	}

	if details, ok := h.validateRuleRequest(c, &req); !ok {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Rule validation failed", details)
		return
	}

	isActive := existing.IsActive
	if req.IsActive != nil {
		isActive = *req.IsActive
	}

	existing.EventType = req.EventType
	existing.TargetRoles = req.TargetRoles
	existing.TargetUserPath = req.TargetUserPath
	existing.Type = domain.NotificationType(req.Type)
	existing.Category = domain.NotificationCategory(req.Category)
	existing.Priority = domain.Priority(req.Priority)
	existing.LinkTemplate = req.LinkTemplate
	existing.TemplateKey = req.TemplateKey
	existing.SendEmail = req.SendEmail
	existing.EmailTemplateKey = req.EmailTemplateKey
	existing.IsActive = isActive

	if err := h.ruleRepo.Update(c.Request.Context(), existing); err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	adminID, _ := GetUserID(c)
	h.logger.Info("admin_audit",
		"action", "update_notification_rule",
		"admin_id", adminID,
		"rule_id", existing.ID,
		"event_type", existing.EventType,
	)

	c.Header("ETag", RuleETag(existing))
	RespondSuccess(c, http.StatusOK, existing)
}

// Deactivate handles DELETE /admin/notification-rules/:id and POST /admin/notification-rules/:id/deactivate.
func (h *AdminRuleHandler) Deactivate(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid rule ID UUID", nil)
		return
	}

	if err := h.ruleRepo.Deactivate(c.Request.Context(), id); err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, err.Error(), nil)
		return
	}

	adminID, _ := GetUserID(c)
	h.logger.Info("admin_audit",
		"action", "deactivate_notification_rule",
		"admin_id", adminID,
		"rule_id", id,
	)

	RespondSuccess(c, http.StatusOK, gin.H{"id": id, "is_active": false})
}
