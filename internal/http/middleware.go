package http

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
)

const HeaderRequestID = "X-Request-ID"
const CtxKeyRequestID = "request_id"

// RequestIDMiddleware extracts or creates an X-Request-ID header and stores it in context.
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.GetHeader(HeaderRequestID)
		if reqID == "" {
			reqID = uuid.NewString()
		}

		c.Set(CtxKeyRequestID, reqID)
		c.Writer.Header().Set(HeaderRequestID, reqID)
		c.Next()
	}
}

// GetRequestID retrieves the request ID from Gin context.
func GetRequestID(c *gin.Context) string {
	if val, exists := c.Get(CtxKeyRequestID); exists {
		if s, ok := val.(string); ok {
			return s
		}
	}
	// Fallback to header or generate new
	reqID := c.GetHeader(HeaderRequestID)
	if reqID == "" {
		reqID = uuid.NewString()
	}
	return reqID
}

// StructuredLoggerMiddleware logs each HTTP request using slog in JSON format.
func StructuredLoggerMiddleware(logger *slog.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		rawQuery := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		statusCode := c.Writer.Status()
		clientIP := c.ClientIP()
		method := c.Request.Method
		reqID := GetRequestID(c)

		if rawQuery != "" {
			queryVals := c.Request.URL.Query()
			if queryVals.Has("ticket") {
				queryVals.Set("ticket", "[REDACTED]")
			}
			if enc := queryVals.Encode(); enc != "" {
				path = path + "?" + enc
			}
		}

		logAttrs := []slog.Attr{
			slog.String("request_id", reqID),
			slog.String("method", method),
			slog.String("path", path),
			slog.Int("status", statusCode),
			slog.Duration("latency", latency),
			slog.String("client_ip", clientIP),
		}

		if len(c.Errors) > 0 {
			logAttrs = append(logAttrs, slog.String("error", c.Errors.String()))
		}

		level := slog.LevelInfo
		if statusCode >= 500 {
			level = slog.LevelError
		} else if statusCode >= 400 {
			level = slog.LevelWarn
		}

		logger.LogAttrs(c.Request.Context(), level, "HTTP request handled", logAttrs...)
	}
}

// RecoveryMiddleware catches panics and formats standard 500 JSON error envelopes.
func RecoveryMiddleware(logger *slog.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				reqID := GetRequestID(c)
				logger.Error("panic recovered during HTTP request",
					slog.String("request_id", reqID),
					slog.Any("panic", r),
				)

				RespondError(
					c,
					http.StatusInternalServerError,
					domain.CodeInternalServerError,
					"An unexpected internal error occurred",
					nil,
				)
				c.Abort()
			}
		}()
		c.Next()
	}
}

// CORSMiddleware manages Cross-Origin Resource Sharing against allowed origins.
func CORSMiddleware(allowedOrigins []string) gin.HandlerFunc {
	originsMap := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		originsMap[strings.TrimSpace(o)] = true
	}

	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if origin != "" {
			if originsMap["*"] || originsMap[origin] {
				c.Header("Access-Control-Allow-Origin", origin)
				c.Header("Access-Control-Allow-Credentials", "true")
				c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, X-Request-ID, Idempotency-Key, X-Internal-Token")
				c.Header("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE, PATCH")
				c.Header("Access-Control-Expose-Headers", "X-Request-ID")
			}
		}

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// InternalAuthMiddleware verifies the X-Internal-Token header for service-to-service internal routes.
func InternalAuthMiddleware(internalToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("X-Internal-Token")
		if internalToken == "" || token != internalToken {
			RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Invalid or missing internal token", nil)
			c.Abort()
			return
		}
		c.Next()
	}
}
