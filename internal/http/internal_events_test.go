package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	"notification-service/internal/ingest"
)

type mockTxRunner struct{}

func (m *mockTxRunner) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

type mockInboxRepo struct {
	events map[uuid.UUID]*domain.InboxEvent
}

func (m *mockInboxRepo) InsertIfAbsent(_ context.Context, event *domain.InboxEvent) (bool, error) {
	if _, exists := m.events[event.EventID]; exists {
		return false, nil
	}
	m.events[event.EventID] = event
	return true, nil
}

func (m *mockInboxRepo) SetStatus(_ context.Context, eventID uuid.UUID, status domain.InboxStatus, errStr *string) error {
	if ev, ok := m.events[eventID]; ok {
		ev.Status = status
		ev.Error = errStr
	}
	return nil
}

type mockRecipientRepo struct {
	recipients map[uuid.UUID]*domain.Recipient
}

func (m *mockRecipientRepo) UpsertGuarded(_ context.Context, recipient *domain.Recipient) error {
	m.recipients[recipient.UserID] = recipient
	return nil
}

func (m *mockRecipientRepo) FindByRoles(_ context.Context, roles []string) ([]*domain.Recipient, error) {
	return nil, nil
}

func (m *mockRecipientRepo) FindByID(_ context.Context, id uuid.UUID) (*domain.Recipient, error) {
	return m.recipients[id], nil
}

func setupTestRouter(internalEnabled bool, token string) (*gin.Engine, *ingest.Ingestor) {
	gin.SetMode(gin.TestMode)

	inbox := &mockInboxRepo{events: make(map[uuid.UUID]*domain.InboxEvent)}
	recipients := &mockRecipientRepo{recipients: make(map[uuid.UUID]*domain.Recipient)}
	ing := ingest.NewIngestor(&mockTxRunner{}, inbox, recipients, ingest.NoopRouter{}, nil)

	cfg := &config.Config{
		InternalServiceToken:  token,
		InternalEventsEnabled: internalEnabled,
		CORSAllowedOrigins:    []string{"*"},
	}

	router := NewRouter(RouterConfig{
		Config:   cfg,
		Logger:   nil,
		DB:       nil,
		Kafka:    nil,
		Ingestor: ing,
	})

	return router, ing
}

func makeValidEventJSON() []byte {
	return []byte(`{
		"event_id":       "7b8e1460-63bf-4787-8e68-07df95a5f782",
		"event_type":     "sales_order.created",
		"event_version":  "1",
		"occurred_at":    "2026-10-14T10:00:00Z",
		"producer":       "sales-service",
		"aggregate_type": "sales_order",
		"aggregate_id":   "9d1c08be-2415-46aa-bd08-8e6ca6f0e495",
		"payload":        {"order_number":"SO-001"}
	}`)
}

func TestInternalEvents_ValidTokenAndEnvelope(t *testing.T) {
	router, _ := setupTestRouter(true, "test-secret-token")

	req, _ := http.NewRequest(http.MethodPost, "/internal/events", bytes.NewBuffer(makeValidEventJSON()))
	req.Header.Set("X-Internal-Token", "test-secret-token")
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Data struct {
			Status  string `json:"status"`
			EventID string `json:"event_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}
	if resp.Data.Status != "accepted" {
		t.Errorf("expected status accepted, got %s", resp.Data.Status)
	}
}

func TestInternalEvents_MissingOrInvalidToken(t *testing.T) {
	router, _ := setupTestRouter(true, "test-secret-token")

	cases := []struct {
		name  string
		token string
	}{
		{"missing token", ""},
		{"wrong token", "wrong-token"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, "/internal/events", bytes.NewBuffer(makeValidEventJSON()))
			if tc.token != "" {
				req.Header.Set("X-Internal-Token", tc.token)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401 Unauthorized, got %d: %s", w.Code, w.Body.String())
			}

			var errResp ErrorEnvelope
			if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("failed decoding error response: %v", err)
			}
			if errResp.Error.Code != domain.CodeUnauthenticated {
				t.Errorf("expected code unauthenticated, got %s", errResp.Error.Code)
			}
		})
	}
}

func TestInternalEvents_InvalidEnvelope(t *testing.T) {
	router, _ := setupTestRouter(true, "test-secret-token")

	cases := []struct {
		name string
		body []byte
	}{
		{"invalid json", []byte(`not-json`)},
		{"missing required event_id", []byte(`{"event_type":"order.created"}`)},
		{"bad uuid event_id", []byte(`{"event_id":"not-uuid","event_type":"order.created","occurred_at":"2026-10-14T10:00:00Z","producer":"s","aggregate_type":"a","aggregate_id":"e81d8590-bfae-4d8b-9d41-e125032b4943","payload":{}}`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, "/internal/events", bytes.NewBuffer(tc.body))
			req.Header.Set("X-Internal-Token", "test-secret-token")
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 Bad Request, got %d: %s", w.Code, w.Body.String())
			}

			var errResp ErrorEnvelope
			if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("failed decoding error response: %v", err)
			}
			if errResp.Error.Code != domain.CodeValidationFailed {
				t.Errorf("expected code validation_failed, got %s", errResp.Error.Code)
			}
		})
	}
}

func TestInternalEvents_DisabledEndpoint(t *testing.T) {
	router, _ := setupTestRouter(false, "test-secret-token")

	req, _ := http.NewRequest(http.MethodPost, "/internal/events", bytes.NewBuffer(makeValidEventJSON()))
	req.Header.Set("X-Internal-Token", "test-secret-token")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found when disabled, got %d: %s", w.Code, w.Body.String())
	}
}

func TestInternalEvents_DuplicateYields202(t *testing.T) {
	router, _ := setupTestRouter(true, "test-secret-token")

	body := makeValidEventJSON()

	// First post
	req1, _ := http.NewRequest(http.MethodPost, "/internal/events", bytes.NewBuffer(body))
	req1.Header.Set("X-Internal-Token", "test-secret-token")
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	if w1.Code != http.StatusAccepted {
		t.Fatalf("first post expected 202, got %d", w1.Code)
	}

	// Second post (duplicate)
	req2, _ := http.NewRequest(http.MethodPost, "/internal/events", bytes.NewBuffer(body))
	req2.Header.Set("X-Internal-Token", "test-secret-token")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("second post (duplicate) expected 202, got %d", w2.Code)
	}
}
