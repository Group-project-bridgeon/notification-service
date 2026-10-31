package http

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Pinger defines an interface for components that can report health readiness.
type Pinger interface {
	Ping(ctx context.Context) error
}

// KafkaProber defines an interface for querying Kafka connectivity state.
type KafkaProber interface {
	Check(ctx context.Context) string
}

// ReadyResponse is the JSON payload for GET /health/ready.
type ReadyResponse struct {
	Status   string `json:"status"`
	Database string `json:"database"`
	Kafka    string `json:"kafka"`
}

// HealthHandler provides health check routes.
type HealthHandler struct {
	db    Pinger
	kafka KafkaProber
}

// NewHealthHandler creates a HealthHandler with DB and Kafka dependencies.
func NewHealthHandler(db Pinger, kafka KafkaProber) *HealthHandler {
	return &HealthHandler{
		db:    db,
		kafka: kafka,
	}
}

// Live handles GET /health/live.
func (h *HealthHandler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "alive",
	})
}

// Ready handles GET /health/ready.
// 200 only when the database ping works, else 503; Kafka alone never makes the service unready.
func (h *HealthHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	dbStatus := "connected"
	dbErr := h.db.Ping(ctx)
	if dbErr != nil {
		dbStatus = "disconnected"
	}

	kafkaStatus := "disabled"
	if h.kafka != nil {
		kafkaStatus = h.kafka.Check(ctx)
	}

	resp := ReadyResponse{
		Database: dbStatus,
		Kafka:    kafkaStatus,
	}

	if dbErr != nil {
		resp.Status = "unready"
		c.JSON(http.StatusServiceUnavailable, resp)
		return
	}

	resp.Status = "ready"
	c.JSON(http.StatusOK, resp)
}

// Version handles GET /health/version.
func (h *HealthHandler) Version(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"version":    "0.2.0",
		"service":    "notification-service",
		"build_time": time.Now().UTC().Format(time.RFC3339),
	})
}
