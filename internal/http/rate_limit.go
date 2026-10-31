package http

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
)

type userBucket struct {
	tokens     float64
	lastRefill time.Time
}

// UserRateLimiter provides in-memory token-bucket rate limiting per user ID.
type UserRateLimiter struct {
	ratePerSec float64
	burst      float64
	mu         sync.Mutex
	buckets    map[string]*userBucket
}

// NewUserRateLimiter constructs a UserRateLimiter.
// ratePerSec is the refill rate in tokens per second, burst is the maximum bucket size.
func NewUserRateLimiter(ratePerSec float64, burst float64) *UserRateLimiter {
	return &UserRateLimiter{
		ratePerSec: ratePerSec,
		burst:      burst,
		buckets:    make(map[string]*userBucket),
	}
}

// Allow checks if a request is permitted for the given key. Returns true if allowed, false if rate limited.
func (rl *UserRateLimiter) Allow(key string) (bool, time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	bucket, exists := rl.buckets[key]
	if !exists {
		bucket = &userBucket{
			tokens:     rl.burst,
			lastRefill: now,
		}
		rl.buckets[key] = bucket
	}

	// Refill tokens based on elapsed time
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	bucket.tokens += elapsed * rl.ratePerSec
	if bucket.tokens > rl.burst {
		bucket.tokens = rl.burst
	}
	bucket.lastRefill = now

	if bucket.tokens >= 1.0 {
		bucket.tokens -= 1.0
		return true, 0
	}

	// Calculate wait time until 1 token is available
	missing := 1.0 - bucket.tokens
	retryAfter := time.Duration((missing / rl.ratePerSec) * float64(time.Second))
	if retryAfter < time.Second {
		retryAfter = time.Second
	}

	return false, retryAfter
}

// WriteRateLimitMiddleware applies per-user rate limiting exclusively on write HTTP methods (POST, PUT, DELETE, PATCH).
func WriteRateLimitMiddleware(limiter *UserRateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		if method != http.MethodPost && method != http.MethodPut && method != http.MethodDelete && method != http.MethodPatch {
			c.Next()
			return
		}

		// Identify by user ID or fallback to client IP
		key := c.ClientIP()
		if uid, ok := GetUserID(c); ok {
			key = uid.String()
		}

		allowed, retryAfter := limiter.Allow(key)
		if !allowed {
			retrySeconds := int(retryAfter.Seconds())
			if retrySeconds < 1 {
				retrySeconds = 1
			}
			c.Header("Retry-After", fmt.Sprintf("%d", retrySeconds))
			RespondError(c, http.StatusTooManyRequests, domain.CodeRateLimited, "Rate limit threshold exceeded", nil)
			c.Abort()
			return
		}

		c.Next()
	}
}
