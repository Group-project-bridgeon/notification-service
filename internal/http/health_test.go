package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"notification-service/internal/config"
)

type mockPinger struct {
	err error
}

func (m *mockPinger) Ping(ctx context.Context) error {
	return m.err
}

type mockKafkaProber struct {
	state string
}

func (m *mockKafkaProber) Check(ctx context.Context) string {
	return m.state
}

func TestHealthHandler_Live(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHealthHandler(&mockPinger{err: nil}, &mockKafkaProber{state: "connected"})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/health/live", nil)

	h.Live(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var res map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res["status"] != "alive" {
		t.Errorf("expected status alive, got %s", res["status"])
	}
}

func TestHealthHandler_Ready_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHealthHandler(&mockPinger{err: nil}, &mockKafkaProber{state: "connected"})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/health/ready", nil)

	h.Ready(c)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var res ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Status != "ready" || res.Database != "connected" || res.Kafka != "connected" {
		t.Errorf("unexpected ready response: %+v", res)
	}
}

func TestHealthHandler_Ready_DBFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHealthHandler(&mockPinger{err: errors.New("db down")}, &mockKafkaProber{state: "connected"})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/health/ready", nil)

	h.Ready(c)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}

	var res ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Status != "unready" || res.Database != "disconnected" {
		t.Errorf("unexpected ready response: %+v", res)
	}
}

func TestHealthHandler_Ready_KafkaAloneDoesNotDegradeReady(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// Database is UP, but Kafka is disconnected
	h := NewHealthHandler(&mockPinger{err: nil}, &mockKafkaProber{state: "disconnected"})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodGet, "/health/ready", nil)

	h.Ready(c)

	// Must be 200 OK because Kafka alone never makes the service unready
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK even when Kafka is disconnected, got %d", w.Code)
	}

	var res ReadyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Status != "ready" || res.Database != "connected" || res.Kafka != "disconnected" {
		t.Errorf("unexpected ready response: %+v", res)
	}
}

func TestMetrics_Endpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{
		Env:                  "test",
		InternalServiceToken: "secret-token-123",
	}

	router := NewRouter(RouterConfig{
		Config: cfg,
	})

	// 1. Missing or invalid token -> 401
	{
		req, _ := http.NewRequest(http.MethodGet, "/metrics", nil)
		req.Header.Set("X-Internal-Token", "wrong-token")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized, got %d", w.Code)
		}
	}

	// 2. Valid token -> 200 text/plain with prometheus metrics
	{
		req, _ := http.NewRequest(http.MethodGet, "/metrics", nil)
		req.Header.Set("X-Internal-Token", "secret-token-123")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}

		body := w.Body.String()
		if !strings.Contains(body, "messages_consumed_total") || !strings.Contains(body, "handler_duration_seconds") {
			t.Fatalf("expected prometheus metrics in response, got:\n%s", body)
		}
	}
}

