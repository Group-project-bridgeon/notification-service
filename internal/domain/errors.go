package domain

import (
	"fmt"
	"net/http"
	"errors"
)

// Standard Error Codes from CONTEXT.md Rule 8
const (
	CodeValidationFailed       = "validation_failed"
	CodeUnauthenticated        = "unauthenticated"
	CodeForbiddenRole          = "forbidden_role"
	CodeNotFound               = "not_found"
	CodeInvalidStateTransition = "invalid_state_transition"
	CodeVersionConflict        = "version_conflict"
	CodeIdempotencyConflict    = "idempotency_key_conflict"
	CodeRateLimited            = "rate_limited"
	CodeEmailNotConfigured     = "email_not_configured"
	CodeEmailSuppressed        = "email_suppressed"
	CodeEmailTemplateInvalid   = "email_template_invalid"
	CodeEmailSendFailed        = "email_send_failed"
	CodeInternalServerError    = "internal_server_error"
)

// ErrNotFound represents generic record not found
var ErrNotFound = errors.New("not found")

// ErrorDetail describes an issue on a specific request field.
type ErrorDetail struct {
	Field string `json:"field,omitempty"`
	Issue string `json:"issue"`
}

// AppError is the centralized application error conforming to the API v0.2 error envelope.
type AppError struct {
	Code       string        `json:"code"`
	Message    string        `json:"message"`
	Details    []ErrorDetail `json:"details,omitempty"`
	HTTPStatus int           `json:"-"`
}

func (e *AppError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// MapCodeToStatus returns the standard HTTP status code for a given error code.
func MapCodeToStatus(code string) int {
	switch code {
	case CodeValidationFailed, CodeEmailTemplateInvalid:
		return http.StatusBadRequest
	case CodeUnauthenticated:
		return http.StatusUnauthorized
	case CodeForbiddenRole:
		return http.StatusForbidden
	case CodeNotFound:
		return http.StatusNotFound
	case CodeInvalidStateTransition, CodeVersionConflict, CodeIdempotencyConflict,
		CodeEmailNotConfigured, CodeEmailSuppressed:
		return http.StatusConflict
	case CodeRateLimited:
		return http.StatusTooManyRequests
	case CodeEmailSendFailed:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

// NewAppError creates an AppError with automatic HTTP status mapping.
func NewAppError(code, message string) *AppError {
	return &AppError{
		Code:       code,
		Message:    message,
		HTTPStatus: MapCodeToStatus(code),
	}
}

// NewValidationError creates a validation_failed (400) AppError with optional details.
func NewValidationError(message string, details ...ErrorDetail) *AppError {
	return &AppError{
		Code:       CodeValidationFailed,
		Message:    message,
		Details:    details,
		HTTPStatus: http.StatusBadRequest,
	}
}

// NewNotFoundError creates a not_found (404) AppError.
func NewNotFoundError(message string) *AppError {
	return &AppError{
		Code:       CodeNotFound,
		Message:    message,
		HTTPStatus: http.StatusNotFound,
	}
}

// IsNotFound checks if an error is an AppError with CodeNotFound.
func IsNotFound(err error) bool {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.Code == CodeNotFound
	}
	return false
}

// NewInternalError creates an internal_server_error (500) AppError.
func NewInternalError(message string) *AppError {
	return &AppError{
		Code:       CodeInternalServerError,
		Message:    message,
		HTTPStatus: http.StatusInternalServerError,
	}
}

