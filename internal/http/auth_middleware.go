package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/config"
	"notification-service/internal/domain"
)

const (
	CtxKeyUserID  = "auth_user_id"
	CtxKeyRoles   = "auth_roles"
	CtxKeyEmail   = "auth_email"
	CtxKeySubject = "auth_subject"
)

// AuthMiddleware creates the authentication middleware depending on AUTH_MODE (jwt or dev).
func AuthMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.ToLower(cfg.AuthMode) == "dev" {
			devUserID := c.GetHeader("X-Dev-User-Id")
			if devUserID != "" {
				parsedUID, err := uuid.Parse(devUserID)
				if err != nil {
					RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Invalid X-Dev-User-Id header format", nil)
					c.Abort()
					return
				}

				var roles []string
				devRoles := c.GetHeader("X-Dev-Roles")
				if devRoles != "" {
					parts := strings.Split(devRoles, ",")
					for _, p := range parts {
						trimmed := strings.TrimSpace(p)
						if trimmed != "" {
							roles = append(roles, trimmed)
						}
					}
				}

				devEmail := c.GetHeader("X-Dev-Email")
				if devEmail == "" {
					devEmail = "dev@example.com"
				}

				c.Set(CtxKeyUserID, parsedUID)
				c.Set(CtxKeySubject, parsedUID.String())
				c.Set(CtxKeyRoles, roles)
				c.Set(CtxKeyEmail, devEmail)
				c.Next()
				return
			}
		}

		// Bearer token authentication
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Missing Authorization header", nil)
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Invalid Authorization header format; must be 'Bearer <token>'", nil)
			c.Abort()
			return
		}

		rawToken := strings.TrimSpace(parts[1])
		if rawToken == "" {
			RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Empty Bearer token provided", nil)
			c.Abort()
			return
		}

		claims, err := ParseAndValidateJWT(rawToken, cfg.JWTSecret, cfg.JWTPublicKey)
		if err != nil {
			RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Invalid or expired authentication token: "+err.Error(), nil)
			c.Abort()
			return
		}

		parsedUID, err := uuid.Parse(claims.Sub)
		if err != nil {
			RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Invalid token subject UUID", nil)
			c.Abort()
			return
		}

		c.Set(CtxKeyUserID, parsedUID)
		c.Set(CtxKeySubject, claims.Sub)
		c.Set(CtxKeyRoles, []string(claims.Roles))
		c.Set(CtxKeyEmail, claims.Email)
		c.Next()
	}
}

// RequireRole checks if the authenticated user has at least one of the specified roles.
func RequireRole(roles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		userRoles := GetUserRoles(c)
		roleSet := make(map[string]bool, len(userRoles))
		for _, r := range userRoles {
			roleSet[strings.ToLower(r)] = true
		}

		for _, required := range roles {
			if roleSet[strings.ToLower(required)] {
				c.Next()
				return
			}
		}

		RespondError(c, http.StatusForbidden, domain.CodeForbiddenRole, "User lacks required role permission", nil)
		c.Abort()
	}
}

// GetUserID retrieves the authenticated user UUID from context.
func GetUserID(c *gin.Context) (uuid.UUID, bool) {
	if val, exists := c.Get(CtxKeyUserID); exists {
		if uid, ok := val.(uuid.UUID); ok {
			return uid, true
		}
	}
	return uuid.Nil, false
}

// GetUserRoles retrieves the authenticated user's assigned roles from context.
func GetUserRoles(c *gin.Context) []string {
	if val, exists := c.Get(CtxKeyRoles); exists {
		if roles, ok := val.([]string); ok {
			return roles
		}
	}
	return nil
}

// GetUserEmail retrieves the authenticated user's email from context.
func GetUserEmail(c *gin.Context) string {
	if val, exists := c.Get(CtxKeyEmail); exists {
		if email, ok := val.(string); ok {
			return email
		}
	}
	return ""
}
