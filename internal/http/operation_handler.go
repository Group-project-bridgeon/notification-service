package http

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

// OperationHandler serves endpoints for querying asynchronous distributed operations.
type OperationHandler struct {
	repo   repository.OperationRepository
	logger *slog.Logger
}

// NewOperationHandler initializes a new OperationHandler.
func NewOperationHandler(repo repository.OperationRepository, logger *slog.Logger) *OperationHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &OperationHandler{
		repo:   repo,
		logger: logger,
	}
}

// GetByID handles GET /operations/:id.
// Caller must be the actor, or hold role "admin" or "manager"; otherwise returns 404 not_found.
func (h *OperationHandler) GetByID(c *gin.Context) {
	idStr := c.Param("id")
	opID, err := uuid.Parse(idStr)
	if err != nil {
		RespondAppError(c, domain.NewNotFoundError("operation not found"))
		return
	}

	op, err := h.repo.GetByID(c.Request.Context(), opID)
	if err != nil {
		var appErr *domain.AppError
		if errors.As(err, &appErr) {
			RespondAppError(c, appErr)
			return
		}
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed querying operation", nil)
		return
	}

	callerID, ok := GetUserID(c)
	roles := GetUserRoles(c)
	isAdminOrManager := hasRole(roles, "admin") || hasRole(roles, "manager")

	if !isAdminOrManager {
		if !ok || op.ActorUserID == nil || *op.ActorUserID != callerID {
			RespondAppError(c, domain.NewNotFoundError("operation not found"))
			return
		}
	}

	RespondSuccess(c, http.StatusOK, op.ToResponse())
}

// List handles GET /operations?entity_type=&entity_id=&page=&page_size=.
// If caller is admin or manager, returns operations matching filters across all users.
// Otherwise, returns operations restricted to the caller's user ID.
func (h *OperationHandler) List(c *gin.Context) {
	callerID, _ := GetUserID(c)
	roles := GetUserRoles(c)
	isAdminOrManager := hasRole(roles, "admin") || hasRole(roles, "manager")

	var filterActorID *uuid.UUID
	if !isAdminOrManager {
		filterActorID = &callerID
	}

	entityType := strings.TrimSpace(c.Query("entity_type"))
	var entityID *uuid.UUID
	if idStr := strings.TrimSpace(c.Query("entity_id")); idStr != "" {
		parsed, err := uuid.Parse(idStr)
		if err != nil {
			RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid entity_id format", []domain.ErrorDetail{
				{Field: "entity_id", Issue: "must be a valid UUID"},
			})
			return
		}
		entityID = &parsed
	}

	page := 1
	if pStr := c.Query("page"); pStr != "" {
		if parsed, err := strconv.Atoi(pStr); err == nil && parsed > 0 {
			page = parsed
		}
	}

	pageSize := 20
	if psStr := c.Query("page_size"); psStr != "" {
		if parsed, err := strconv.Atoi(psStr); err == nil && parsed > 0 {
			pageSize = parsed
			if pageSize > 100 {
				pageSize = 100
			}
		}
	}

	filter := domain.OperationFilter{
		ActorUserID: filterActorID,
		EntityType:  entityType,
		EntityID:    entityID,
		Page:        page,
		PageSize:    pageSize,
	}

	ops, totalCount, err := h.repo.List(c.Request.Context(), filter)
	if err != nil {
		h.logger.ErrorContext(c.Request.Context(), "failed listing operations", "error", err)
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed querying operations", nil)
		return
	}

	respList := make([]domain.OperationResponse, 0, len(ops))
	for _, o := range ops {
		respList = append(respList, o.ToResponse())
	}

	RespondList(c, http.StatusOK, respList, page, pageSize, totalCount)
}

func hasRole(roles []string, target string) bool {
	for _, r := range roles {
		if strings.EqualFold(r, target) {
			return true
		}
	}
	return false
}
