package http

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

// NotificationEntityResponse formats the entity reference according to API_CONTRACT.md.
type NotificationEntityResponse struct {
	Type string     `json:"type"`
	ID   *uuid.UUID `json:"id,omitempty"`
}

// NotificationResponse represents the user-facing Notification JSON object.
type NotificationResponse struct {
	ID            uuid.UUID                   `json:"id"`
	Type          string                      `json:"type"`
	Category      string                      `json:"category"`
	Priority      string                      `json:"priority"`
	Title         string                      `json:"title"`
	Body          string                      `json:"body"`
	Entity        *NotificationEntityResponse `json:"entity,omitempty"`
	Link          *string                     `json:"link,omitempty"`
	CorrelationID *uuid.UUID                  `json:"correlation_id,omitempty"`
	Status        string                      `json:"status"`
	CreatedAt     string                      `json:"created_at"`
	ReadAt        *string                     `json:"read_at"`
}
// ToNotificationResponse converts a domain.Notification to NotificationResponse.
func ToNotificationResponse(n *domain.Notification) NotificationResponse {
	var ent *NotificationEntityResponse
	if n.EntityType != nil && *n.EntityType != "" {
		ent = &NotificationEntityResponse{
			Type: *n.EntityType,
			ID:   n.EntityID,
		}
	}

	var readAtStr *string
	if n.ReadAt != nil {
		s := n.ReadAt.UTC().Format(time.RFC3339)
		readAtStr = &s
	}

	return NotificationResponse{
		ID:            n.ID,
		Type:          string(n.Type),
		Category:      string(n.Category),
		Priority:      string(n.Priority),
		Title:         n.Title,
		Body:          n.Body,
		Entity:        ent,
		Link:          n.Link,
		CorrelationID: n.CorrelationID,
		Status:        string(n.Status),
		CreatedAt:     n.CreatedAt.UTC().Format(time.RFC3339),
		ReadAt:        readAtStr,
	}
}

// NotificationHandler handles public in-app notification routes.
type NotificationHandler struct {
	repo repository.NotificationRepository
}

// NewNotificationHandler creates a NotificationHandler.
func NewNotificationHandler(repo repository.NotificationRepository) *NotificationHandler {
	return &NotificationHandler{repo: repo}
}

// List handles GET /notifications with pagination, sorting, and filtering.
func (h *NotificationHandler) List(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	var validationDetails []domain.ErrorDetail

	page := 1
	if pageStr := c.Query("page"); pageStr != "" {
		p, err := strconv.Atoi(pageStr)
		if err != nil || p < 1 {
			validationDetails = append(validationDetails, domain.ErrorDetail{
				Field: "page",
				Issue: "must_be_positive_integer",
			})
		} else {
			page = p
		}
	}

	pageSize := 20
	if pageSizeStr := c.Query("page_size"); pageSizeStr != "" {
		ps, err := strconv.Atoi(pageSizeStr)
		if err != nil || ps < 1 || ps > 100 {
			validationDetails = append(validationDetails, domain.ErrorDetail{
				Field: "page_size",
				Issue: "must_be_between_1_and_100",
			})
		} else {
			pageSize = ps
		}
	}

	sort := c.DefaultQuery("sort", "-created_at")

	var statusFilter *domain.NotificationStatus
	if s := c.Query("status"); s != "" {
		switch domain.NotificationStatus(s) {
		case domain.NotificationStatusUnread, domain.NotificationStatusRead, domain.NotificationStatusArchived:
			val := domain.NotificationStatus(s)
			statusFilter = &val
		default:
			validationDetails = append(validationDetails, domain.ErrorDetail{
				Field: "status",
				Issue: "invalid_status_value",
			})
		}
	}

	var categoryFilter *domain.NotificationCategory
	if cat := c.Query("category"); cat != "" {
		val := domain.NotificationCategory(cat)
		categoryFilter = &val
	}

	var priorityFilter *domain.Priority
	if p := c.Query("priority"); p != "" {
		switch domain.Priority(p) {
		case domain.PriorityLow, domain.PriorityNormal, domain.PriorityHigh, domain.PriorityCritical:
			val := domain.Priority(p)
			priorityFilter = &val
		default:
			validationDetails = append(validationDetails, domain.ErrorDetail{
				Field: "priority",
				Issue: "invalid_priority_value",
			})
		}
	}

	var typeFilter *domain.NotificationType
	if t := c.Query("type"); t != "" {
		val := domain.NotificationType(t)
		typeFilter = &val
	}

	var fromTime *time.Time
	if fromStr := c.Query("from"); fromStr != "" {
		t, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			validationDetails = append(validationDetails, domain.ErrorDetail{
				Field: "from",
				Issue: "invalid_rfc3339_timestamp",
			})
		} else {
			fromTime = &t
		}
	}

	var toTime *time.Time
	if toStr := c.Query("to"); toStr != "" {
		t, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			validationDetails = append(validationDetails, domain.ErrorDetail{
				Field: "to",
				Issue: "invalid_rfc3339_timestamp",
			})
		} else {
			toTime = &t
		}
	}

	if len(validationDetails) > 0 {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Request parameter validation failed", validationDetails)
		return
	}

	filter := domain.NotificationFilter{
		RecipientID: userID,
		Status:      statusFilter,
		Category:    categoryFilter,
		Priority:    priorityFilter,
		Type:        typeFilter,
		From:        fromTime,
		To:          toTime,
		Sort:        sort,
		Page:        page,
		PageSize:    pageSize,
	}

	notifications, totalCount, err := h.repo.List(c.Request.Context(), filter)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed querying notifications", nil)
		return
	}

	responseList := make([]NotificationResponse, 0, len(notifications))
	for _, n := range notifications {
		responseList = append(responseList, ToNotificationResponse(n))
	}

	RespondList(c, http.StatusOK, responseList, page, pageSize, totalCount)
}

// UnreadCount handles GET /notifications/unread-count.
func (h *NotificationHandler) UnreadCount(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	total, byCategory, err := h.repo.GetUnreadCounts(c.Request.Context(), userID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed retrieving unread counts", nil)
		return
	}

	if byCategory == nil {
		byCategory = make(map[string]int)
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"total":       total,
		"by_category": byCategory,
	})
}

// GetByID handles GET /notifications/{id}.
func (h *NotificationHandler) GetByID(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	idStr := c.Param("id")
	notifID, err := uuid.Parse(idStr)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid notification UUID format", []domain.ErrorDetail{
			{Field: "id", Issue: "invalid_uuid"},
		})
		return
	}

	notif, err := h.repo.GetByID(c.Request.Context(), userID, notifID)
	if err != nil {
		RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Notification not found", nil)
		return
	}

	RespondSuccess(c, http.StatusOK, ToNotificationResponse(notif))
}

// MarkRead handles POST /notifications/{id}/read.
func (h *NotificationHandler) MarkRead(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	idStr := c.Param("id")
	notifID, err := uuid.Parse(idStr)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid notification UUID format", []domain.ErrorDetail{
			{Field: "id", Issue: "invalid_uuid"},
		})
		return
	}

	notif, err := h.repo.GetByID(c.Request.Context(), userID, notifID)
	if err != nil {
		RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Notification not found", nil)
		return
	}

	if notif.Status == domain.NotificationStatusArchived {
		RespondError(c, http.StatusConflict, domain.CodeInvalidStateTransition, "Cannot mark archived notification as read", nil)
		return
	}

	updated, err := h.repo.SetStatus(c.Request.Context(), userID, notifID, domain.NotificationStatusRead)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed updating notification status", nil)
		return
	}

	var readAtStr *string
	if updated.ReadAt != nil {
		s := updated.ReadAt.UTC().Format(time.RFC3339)
		readAtStr = &s
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"id":      updated.ID,
		"status":  string(updated.Status),
		"read_at": readAtStr,
	})
}

// MarkUnread handles POST /notifications/{id}/unread.
func (h *NotificationHandler) MarkUnread(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	idStr := c.Param("id")
	notifID, err := uuid.Parse(idStr)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid notification UUID format", []domain.ErrorDetail{
			{Field: "id", Issue: "invalid_uuid"},
		})
		return
	}

	notif, err := h.repo.GetByID(c.Request.Context(), userID, notifID)
	if err != nil {
		RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Notification not found", nil)
		return
	}

	if notif.Status == domain.NotificationStatusArchived {
		RespondError(c, http.StatusConflict, domain.CodeInvalidStateTransition, "Cannot mark archived notification as unread", nil)
		return
	}

	updated, err := h.repo.SetStatus(c.Request.Context(), userID, notifID, domain.NotificationStatusUnread)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed updating notification status", nil)
		return
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"id":      updated.ID,
		"status":  string(updated.Status),
		"read_at": nil,
	})
}

// Archive handles POST /notifications/{id}/archive.
func (h *NotificationHandler) Archive(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	idStr := c.Param("id")
	notifID, err := uuid.Parse(idStr)
	if err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid notification UUID format", []domain.ErrorDetail{
			{Field: "id", Issue: "invalid_uuid"},
		})
		return
	}

	// Scoped check: must exist for this recipient
	_, err = h.repo.GetByID(c.Request.Context(), userID, notifID)
	if err != nil {
		RespondError(c, http.StatusNotFound, domain.CodeNotFound, "Notification not found", nil)
		return
	}

	updated, err := h.repo.SetStatus(c.Request.Context(), userID, notifID, domain.NotificationStatusArchived)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed archiving notification", nil)
		return
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"id":     updated.ID,
		"status": string(updated.Status),
	})
}

type readAllRequest struct {
	Category *string `json:"category"`
}

// ReadAll handles POST /notifications/read-all.
func (h *NotificationHandler) ReadAll(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	var req readAllRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, c.Request.Body.Close()) && c.Request.ContentLength > 0 {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid request body JSON", nil)
		return
	}

	var category *domain.NotificationCategory
	if req.Category != nil && *req.Category != "" {
		cat := domain.NotificationCategory(*req.Category)
		category = &cat
	}

	updatedCount, err := h.repo.ReadAll(c.Request.Context(), userID, category)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed marking notifications as read", nil)
		return
	}

	RespondSuccess(c, http.StatusOK, gin.H{
		"updated_count": updatedCount,
	})
}
