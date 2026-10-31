package auth

import (
	"errors"
)

// Claims represents the expected user claims from a JWT token.
type Claims struct {
	Sub   string   `json:"sub"`
	Roles []string `json:"roles"`
	Email string   `json:"email"`
	Exp   int64    `json:"exp"`
}

// Validator validates authorization tokens.
type Validator interface {
	ValidateToken(tokenString string) (*Claims, error)
	ValidateInternalToken(tokenString string) error
}

var (
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrForbiddenRole   = errors.New("forbidden_role")
)
