package http

import (
	"math"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
)

// ListMeta provides standard pagination attributes according to API conventions v0.2.
type ListMeta struct {
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalCount int `json:"total_count"`
	TotalPages int `json:"total_pages"`
}

// SuccessEnvelope wraps a standard data response.
type SuccessEnvelope struct {
	Data any `json:"data"`
}

// ListEnvelope wraps list data and pagination metadata.
type ListEnvelope struct {
	Data any      `json:"data"`
	Meta ListMeta `json:"meta"`
}

// ErrorDetail defines field-level validation or domain issues.
type ErrorDetail struct {
	Field string `json:"field,omitempty"`
	Issue string `json:"issue"`
}

// ErrorBody defines the inner payload of the API error envelope.
type ErrorBody struct {
	Code      string               `json:"code"`
	Message   string               `json:"message"`
	Details   []domain.ErrorDetail `json:"details,omitempty"`
	RequestID string               `json:"request_id"`
}

// ErrorEnvelope wraps an error response conforming to API v0.2.
type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
}

// RespondSuccess renders a 2xx data envelope.
func RespondSuccess(c *gin.Context, statusCode int, data any) {
	c.JSON(statusCode, SuccessEnvelope{
		Data: data,
	})
}

// RespondList renders a paginated list envelope.
func RespondList(c *gin.Context, statusCode int, data any, page, pageSize, totalCount int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	totalPages := 0
	if totalCount > 0 {
		totalPages = int(math.Ceil(float64(totalCount) / float64(pageSize)))
	}

	c.JSON(statusCode, ListEnvelope{
		Data: data,
		Meta: ListMeta{
			Page:       page,
			PageSize:   pageSize,
			TotalCount: totalCount,
			TotalPages: totalPages,
		},
	})
}

// RespondError renders a standardized error envelope.
func RespondError(c *gin.Context, statusCode int, code, message string, details []domain.ErrorDetail) {
	requestID := GetRequestID(c)
	c.JSON(statusCode, ErrorEnvelope{
		Error: ErrorBody{
			Code:      code,
			Message:   message,
			Details:   details,
			RequestID: requestID,
		},
	})
}

// RespondAppError maps a domain.AppError to the standardized error envelope.
func RespondAppError(c *gin.Context, err *domain.AppError) {
	RespondError(c, err.HTTPStatus, err.Code, err.Message, err.Details)
}
