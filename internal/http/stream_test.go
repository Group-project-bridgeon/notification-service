package http

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	"notification-service/internal/notification"
	"notification-service/internal/sse"
)

func setupStreamTestRouter(notifRepo *mockHTTPNotificationRepo) (*StreamHandler, *sse.Hub, *sse.TicketStore, http.Handler) {
	hub := sse.NewHub(nil)
	ticketStore := sse.NewTicketStore()

	cfg := &config.Config{
		Env:      "test",
		AuthMode: "dev",
	}

	r := NewRouter(RouterConfig{
		Config:           cfg,
		NotificationRepo: notifRepo,
		SSEHub:           hub,
		TicketStore:      ticketStore,
	})

	handler := NewStreamHandler(hub, ticketStore, notifRepo, nil)
	return handler, hub, ticketStore, r
}

func TestStreamTicket_UnauthorizedWithoutAuth(t *testing.T) {
	cfg := &config.Config{
		Env:       "test",
		AuthMode:  "jwt",
		JWTSecret: "test-secret-12345",
	}
	hub := sse.NewHub(nil)
	ticketStore := sse.NewTicketStore()
	defer ticketStore.Close()
	defer hub.Close()

	router := NewRouter(RouterConfig{
		Config:      cfg,
		SSEHub:      hub,
		TicketStore: ticketStore,
	})

	req, _ := http.NewRequest(http.MethodPost, "/notifications/stream-ticket", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", w.Code)
	}
}

func TestStreamTicket_SuccessWithDevAuth(t *testing.T) {
	_, hub, ticketStore, router := setupStreamTestRouter(newMockHTTPNotificationRepo())
	defer ticketStore.Close()
	defer hub.Close()

	userID := uuid.New()
	req, _ := http.NewRequest(http.MethodPost, "/notifications/stream-ticket", nil)
	req.Header.Set("X-Dev-User-Id", userID.String())

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Data struct {
			Ticket    string `json:"ticket"`
			ExpiresIn int    `json:"expires_in"`
		} `json:"data"`
	}

	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed unmarshaling ticket response: %v", err)
	}

	if !strings.HasPrefix(resp.Data.Ticket, "st_") {
		t.Fatalf("expected ticket starting with st_, got %s", resp.Data.Ticket)
	}
	if resp.Data.ExpiresIn != 30 {
		t.Fatalf("expected expires_in 30, got %d", resp.Data.ExpiresIn)
	}
}

func TestStream_UnauthorizedScenarios(t *testing.T) {
	_, hub, ticketStore, router := setupStreamTestRouter(newMockHTTPNotificationRepo())
	defer ticketStore.Close()
	defer hub.Close()

	// 1. Missing ticket parameter
	req1, _ := http.NewRequest(http.MethodGet, "/notifications/stream", nil)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on missing ticket, got %d", w1.Code)
	}

	// 2. Invalid ticket parameter
	req2, _ := http.NewRequest(http.MethodGet, "/notifications/stream?ticket=st_invalid", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on invalid ticket, got %d", w2.Code)
	}
}

func TestStream_SingleUseTicketEnforced(t *testing.T) {
	_, hub, ticketStore, router := setupStreamTestRouter(newMockHTTPNotificationRepo())
	defer ticketStore.Close()
	defer hub.Close()

	userID := uuid.New()
	ticket, _ := ticketStore.Generate(userID, 30*time.Second)

	// First redemption via HTTP request
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/notifications/stream?ticket="+ticket, nil)
	w := httptest.NewRecorder()

	go func() {
		// Cancel shortly after stream starts
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected first stream to succeed with 200, got %d", w.Code)
	}

	// Second redemption with the exact same ticket MUST fail
	req2, _ := http.NewRequest(http.MethodGet, "/notifications/stream?ticket="+ticket, nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("expected second stream attempt with same ticket to fail with 401, got %d", w2.Code)
	}
}

func TestStream_LivePushNotification(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	_, hub, ticketStore, router := setupStreamTestRouter(notifRepo)
	defer ticketStore.Close()
	defer hub.Close()

	userID := uuid.New()
	ticket, _ := ticketStore.Generate(userID, 30*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/notifications/stream?ticket="+ticket, nil)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		router.ServeHTTP(w, req)
		close(done)
	}()

	// Wait for client to register in hub
	deadline := time.Now().Add(1 * time.Second)
	for hub.UserClientCount(userID) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hub.UserClientCount(userID) == 0 {
		t.Fatalf("client did not register in hub")
	}

	// Push a notification
	notifID := uuid.New()
	notif := &domain.Notification{
		ID:          notifID,
		RecipientID: userID,
		Type:        domain.TypeApprovalPending,
		Category:    domain.CategoryApproval,
		Priority:    domain.PriorityNormal,
		Title:       "PO-2026-0042 Approval",
		Body:        "Requires approval",
		Status:      domain.NotificationStatusUnread,
		CreatedAt:   time.Now().UTC(),
	}
	counts := notification.UnreadCounts{Total: 1, ByCategory: map[string]int{"approval": 1}}

	if err := hub.PublishNotification(context.Background(), notif, counts); err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// Wait a moment then cancel stream context
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	body := w.Body.String()
	if !strings.Contains(body, "event: notification") {
		t.Fatalf("expected 'event: notification' in SSE stream, got:\n%s", body)
	}
	if !strings.Contains(body, notifID.String()) {
		t.Fatalf("expected notification id %s in stream, got:\n%s", notifID.String(), body)
	}
	if !strings.Contains(body, "event: unread_count") {
		t.Fatalf("expected 'event: unread_count' in stream, got:\n%s", body)
	}
}

func TestStream_ReplayOnLastEventID(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	_, hub, ticketStore, router := setupStreamTestRouter(notifRepo)
	defer ticketStore.Close()
	defer hub.Close()

	userID := uuid.New()
	id1 := uuid.New()
	id2 := uuid.New()

	n1 := &domain.Notification{
		ID:          id1,
		RecipientID: userID,
		Title:       "First notification",
		Status:      domain.NotificationStatusUnread,
		CreatedAt:   time.Now().UTC().Add(-2 * time.Minute),
	}
	n2 := &domain.Notification{
		ID:          id2,
		RecipientID: userID,
		Title:       "Second notification",
		Status:      domain.NotificationStatusUnread,
		CreatedAt:   time.Now().UTC().Add(-1 * time.Minute),
	}

	_ = notifRepo.BatchInsert(context.Background(), []*domain.Notification{n1, n2})

	ticket, _ := ticketStore.Generate(userID, 30*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/notifications/stream?ticket="+ticket, nil)
	req.Header.Set("Last-Event-ID", id1.String())

	w := httptest.NewRecorder()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	router.ServeHTTP(w, req)

	body := w.Body.String()

	// Should replay n2 (newer than id1)
	if !strings.Contains(body, id2.String()) {
		t.Fatalf("expected replayed notification id %s in stream, got:\n%s", id2.String(), body)
	}
	// Should NOT replay n1 itself
	if strings.Contains(body, fmt.Sprintf("id: %s", id1.String())) {
		t.Fatalf("did not expect last seen id %s to be replayed in stream, got:\n%s", id1.String(), body)
	}
	// Should include unread_count event after replay
	if !strings.Contains(body, "event: unread_count") {
		t.Fatalf("expected 'event: unread_count' after replay, got:\n%s", body)
	}
}

func parseSSEEvents(raw string) []map[string]string {
	scanner := bufio.NewScanner(strings.NewReader(raw))
	var events []map[string]string
	current := make(map[string]string)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(current) > 0 {
				events = append(events, current)
				current = make(map[string]string)
			}
			continue
		}
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) == 2 {
			current[parts[0]] = parts[1]
		}
	}
	if len(current) > 0 {
		events = append(events, current)
	}
	return events
}
