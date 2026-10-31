package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/config"
	"notification-service/internal/domain"
)

// --- Mock Repositories for HTTP Tests ---

type mockHTTPNotificationRepo struct {
	notifications map[uuid.UUID]*domain.Notification
}

func newMockHTTPNotificationRepo() *mockHTTPNotificationRepo {
	return &mockHTTPNotificationRepo{
		notifications: make(map[uuid.UUID]*domain.Notification),
	}
}

func (m *mockHTTPNotificationRepo) BatchInsert(ctx context.Context, notifications []*domain.Notification) error {
	for _, n := range notifications {
		m.notifications[n.ID] = n
	}
	return nil
}

func (m *mockHTTPNotificationRepo) List(ctx context.Context, filter domain.NotificationFilter) ([]*domain.Notification, int, error) {
	var userNotifs []*domain.Notification
	for _, n := range m.notifications {
		if n.RecipientID == filter.RecipientID {
			userNotifs = append(userNotifs, n)
		}
	}

	total := len(userNotifs)
	page := filter.Page
	if page < 1 {
		page = 1
	}
	pageSize := filter.PageSize
	if pageSize < 1 {
		pageSize = 20
	}

	start := (page - 1) * pageSize
	if start >= total {
		return []*domain.Notification{}, total, nil
	}
	end := start + pageSize
	if end > total {
		end = total
	}

	return userNotifs[start:end], total, nil
}

func (m *mockHTTPNotificationRepo) GetByID(ctx context.Context, recipientID, id uuid.UUID) (*domain.Notification, error) {
	n, exists := m.notifications[id]
	if !exists || n.RecipientID != recipientID {
		return nil, domain.NewNotFoundError("notification not found")
	}
	return n, nil
}

func (m *mockHTTPNotificationRepo) SetStatus(ctx context.Context, recipientID, id uuid.UUID, status domain.NotificationStatus) (*domain.Notification, error) {
	n, err := m.GetByID(ctx, recipientID, id)
	if err != nil {
		return nil, err
	}
	n.Status = status
	if status == domain.NotificationStatusRead {
		now := time.Now().UTC()
		n.ReadAt = &now
	} else if status == domain.NotificationStatusUnread {
		n.ReadAt = nil
	}
	return n, nil
}

func (m *mockHTTPNotificationRepo) ReadAll(ctx context.Context, recipientID uuid.UUID, category *domain.NotificationCategory) (int64, error) {
	var count int64
	now := time.Now().UTC()
	for _, n := range m.notifications {
		if n.RecipientID == recipientID && n.Status == domain.NotificationStatusUnread {
			if category == nil || n.Category == *category {
				n.Status = domain.NotificationStatusRead
				n.ReadAt = &now
				count++
			}
		}
	}
	return count, nil
}

func (m *mockHTTPNotificationRepo) GetUnreadCounts(ctx context.Context, recipientID uuid.UUID) (int, map[string]int, error) {
	total := 0
	byCat := make(map[string]int)
	for _, n := range m.notifications {
		if n.RecipientID == recipientID && n.Status == domain.NotificationStatusUnread {
			total++
			byCat[string(n.Category)]++
		}
	}
	return total, byCat, nil
}

type mockHTTPPreferenceRepo struct {
	prefs map[string]*domain.NotificationPreference
}

func newMockHTTPPreferenceRepo() *mockHTTPPreferenceRepo {
	return &mockHTTPPreferenceRepo{
		prefs: make(map[string]*domain.NotificationPreference),
	}
}

func (m *mockHTTPPreferenceRepo) GetByUser(ctx context.Context, userID uuid.UUID) ([]*domain.NotificationPreference, error) {
	var res []*domain.NotificationPreference
	for _, p := range m.prefs {
		if p.UserID == userID {
			res = append(res, p)
		}
	}
	return res, nil
}

func (m *mockHTTPPreferenceRepo) Upsert(ctx context.Context, pref *domain.NotificationPreference) error {
	key := pref.UserID.String() + ":" + string(pref.Category) + ":" + string(pref.Channel)
	m.prefs[key] = pref
	return nil
}

type mockHTTPRuleRepo struct{}

func (m *mockHTTPRuleRepo) GetActiveByEventType(ctx context.Context, eventType string) ([]*domain.NotificationRule, error) {
	return nil, nil
}
func (m *mockHTTPRuleRepo) List(ctx context.Context) ([]*domain.NotificationRule, error) {
	return []*domain.NotificationRule{}, nil
}
func (m *mockHTTPRuleRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.NotificationRule, error) {
	return nil, nil
}
func (m *mockHTTPRuleRepo) Create(ctx context.Context, rule *domain.NotificationRule) error {
	return nil
}
func (m *mockHTTPRuleRepo) Update(ctx context.Context, rule *domain.NotificationRule) error {
	return nil
}
func (m *mockHTTPRuleRepo) Delete(ctx context.Context, id uuid.UUID) error {
	return nil
}
func (m *mockHTTPRuleRepo) Deactivate(ctx context.Context, id uuid.UUID) error {
	return nil
}

// Helper to build test Gin router
func setupAPITestRouter(notifRepo *mockHTTPNotificationRepo, prefRepo *mockHTTPPreferenceRepo, authMode string) (*gin.Engine, string) {
	jwtSecret := "test-secret-key-12345"
	cfg := &config.Config{
		Env:                  "test",
		AuthMode:             authMode,
		JWTSecret:            jwtSecret,
		InternalServiceToken: "internal-secret",
	}

	router := NewRouter(RouterConfig{
		Config:           cfg,
		NotificationRepo: notifRepo,
		PreferenceRepo:   prefRepo,
		RuleRepo:         &mockHTTPRuleRepo{},
	})

	return router, jwtSecret
}

// --- Test Cases ---

func TestAuth_401WithoutToken(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	prefRepo := newMockHTTPPreferenceRepo()
	router, _ := setupAPITestRouter(notifRepo, prefRepo, "jwt")

	req, _ := http.NewRequest(http.MethodGet, "/notifications", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", w.Code)
	}

	var errResp ErrorEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed decoding error envelope: %v", err)
	}

	if errResp.Error.Code != domain.CodeUnauthenticated {
		t.Errorf("expected error code %q, got %q", domain.CodeUnauthenticated, errResp.Error.Code)
	}
	if errResp.Error.RequestID == "" {
		t.Errorf("expected request_id in error envelope, got empty")
	}
}

func TestAuth_403ForWrongRole(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	prefRepo := newMockHTTPPreferenceRepo()
	router, secret := setupAPITestRouter(notifRepo, prefRepo, "jwt")

	userID := uuid.New().String()
	token, err := GenerateTestHS256Token(userID, []string{"employee"}, "user@example.com", secret, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed generating test token: %v", err)
	}

	// /admin/notification-rules requires "admin" role
	req, _ := http.NewRequest(http.MethodGet, "/admin/notification-rules", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d (body: %s)", w.Code, w.Body.String())
	}

	var errResp ErrorEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed decoding error envelope: %v", err)
	}

	if errResp.Error.Code != domain.CodeForbiddenRole {
		t.Errorf("expected error code %q, got %q", domain.CodeForbiddenRole, errResp.Error.Code)
	}
}

func TestNotification_404ForAnotherUsersNotification(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	prefRepo := newMockHTTPPreferenceRepo()
	router, secret := setupAPITestRouter(notifRepo, prefRepo, "jwt")

	userA := uuid.New()
	userB := uuid.New()
	notifID := uuid.New()

	// Notification belongs to User B
	notifRepo.notifications[notifID] = &domain.Notification{
		ID:          notifID,
		RecipientID: userB,
		Type:        domain.TypeOrderConfirmed,
		Category:    domain.CategorySales,
		Priority:    domain.PriorityNormal,
		Title:       "User B Order Confirmed",
		Body:        "Order confirmed",
		Status:      domain.NotificationStatusUnread,
		CreatedAt:   time.Now().UTC(),
	}

	// User A requests it
	tokenA, _ := GenerateTestHS256Token(userA.String(), []string{"employee"}, "a@example.com", secret, 1*time.Hour)

	req, _ := http.NewRequest(http.MethodGet, "/notifications/"+notifID.String(), nil)
	req.Header.Set("Authorization", "Bearer "+tokenA)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", w.Code)
	}

	var errResp ErrorEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed decoding error envelope: %v", err)
	}
	if errResp.Error.Code != domain.CodeNotFound {
		t.Errorf("expected error code %q, got %q", domain.CodeNotFound, errResp.Error.Code)
	}
}

func TestNotification_TwoPagesOfResultsWithCorrectMeta(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	prefRepo := newMockHTTPPreferenceRepo()
	router, secret := setupAPITestRouter(notifRepo, prefRepo, "jwt")

	userID := uuid.New()
	token, _ := GenerateTestHS256Token(userID.String(), []string{"employee"}, "user@example.com", secret, 1*time.Hour)

	// Seed 3 notifications for this user
	for i := 1; i <= 3; i++ {
		id := uuid.New()
		notifRepo.notifications[id] = &domain.Notification{
			ID:          id,
			RecipientID: userID,
			Type:        domain.TypeApprovalPending,
			Category:    domain.CategoryApproval,
			Priority:    domain.PriorityNormal,
			Title:       "Approval Request",
			Body:        "Please approve",
			Status:      domain.NotificationStatusUnread,
			CreatedAt:   time.Now().UTC().Add(time.Duration(i) * time.Minute),
		}
	}

	// Page 1: page=1, page_size=2
	req1, _ := http.NewRequest(http.MethodGet, "/notifications?page=1&page_size=2", nil)
	req1.Header.Set("Authorization", "Bearer "+token)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("expected status 200 on page 1, got %d", w1.Code)
	}

	var resp1 struct {
		Data []NotificationResponse `json:"data"`
		Meta ListMeta               `json:"meta"`
	}
	if err := json.Unmarshal(w1.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("failed decoding page 1 response: %v", err)
	}

	if len(resp1.Data) != 2 {
		t.Errorf("expected 2 items on page 1, got %d", len(resp1.Data))
	}
	if resp1.Meta.Page != 1 || resp1.Meta.PageSize != 2 || resp1.Meta.TotalCount != 3 || resp1.Meta.TotalPages != 2 {
		t.Errorf("unexpected meta on page 1: %+v", resp1.Meta)
	}

	// Page 2: page=2, page_size=2
	req2, _ := http.NewRequest(http.MethodGet, "/notifications?page=2&page_size=2", nil)
	req2.Header.Set("Authorization", "Bearer "+token)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected status 200 on page 2, got %d", w2.Code)
	}

	var resp2 struct {
		Data []NotificationResponse `json:"data"`
		Meta ListMeta               `json:"meta"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("failed decoding page 2 response: %v", err)
	}

	if len(resp2.Data) != 1 {
		t.Errorf("expected 1 item on page 2, got %d", len(resp2.Data))
	}
	if resp2.Meta.Page != 2 || resp2.Meta.PageSize != 2 || resp2.Meta.TotalCount != 3 || resp2.Meta.TotalPages != 2 {
		t.Errorf("unexpected meta on page 2: %+v", resp2.Meta)
	}
}

func TestNotification_ReadAll(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	prefRepo := newMockHTTPPreferenceRepo()
	router, secret := setupAPITestRouter(notifRepo, prefRepo, "jwt")

	userID := uuid.New()
	token, _ := GenerateTestHS256Token(userID.String(), []string{"employee"}, "user@example.com", secret, 1*time.Hour)

	// Seed 2 unread approval notifications and 1 unread inventory notification
	for i := 0; i < 2; i++ {
		id := uuid.New()
		notifRepo.notifications[id] = &domain.Notification{
			ID:          id,
			RecipientID: userID,
			Type:        domain.TypeApprovalPending,
			Category:    domain.CategoryApproval,
			Status:      domain.NotificationStatusUnread,
		}
	}
	invID := uuid.New()
	notifRepo.notifications[invID] = &domain.Notification{
		ID:          invID,
		RecipientID: userID,
		Type:        domain.TypeLowStock,
		Category:    domain.CategoryInventory,
		Status:      domain.NotificationStatusUnread,
	}

	// Read all in category approval
	body := []byte(`{"category":"approval"}`)
	req, _ := http.NewRequest(http.MethodPost, "/notifications/read-all", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp struct {
		Data struct {
			UpdatedCount int `json:"updated_count"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed decoding read-all response: %v", err)
	}

	if resp.Data.UpdatedCount != 2 {
		t.Errorf("expected updated_count 2, got %d", resp.Data.UpdatedCount)
	}

	// Verify inventory notification remains unread
	if notifRepo.notifications[invID].Status != domain.NotificationStatusUnread {
		t.Errorf("expected inventory notification to remain unread, got %s", notifRepo.notifications[invID].Status)
	}
}

func TestNotification_ArchiveAndStateTransitions(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	prefRepo := newMockHTTPPreferenceRepo()
	router, secret := setupAPITestRouter(notifRepo, prefRepo, "jwt")

	userID := uuid.New()
	notifID := uuid.New()
	token, _ := GenerateTestHS256Token(userID.String(), []string{"employee"}, "user@example.com", secret, 1*time.Hour)

	notifRepo.notifications[notifID] = &domain.Notification{
		ID:          notifID,
		RecipientID: userID,
		Type:        domain.TypeOrderConfirmed,
		Category:    domain.CategorySales,
		Status:      domain.NotificationStatusUnread,
	}

	// 1. Read
	reqRead, _ := http.NewRequest(http.MethodPost, "/notifications/"+notifID.String()+"/read", nil)
	reqRead.Header.Set("Authorization", "Bearer "+token)
	wRead := httptest.NewRecorder()
	router.ServeHTTP(wRead, reqRead)
	if wRead.Code != http.StatusOK {
		t.Fatalf("expected status 200 for read, got %d", wRead.Code)
	}

	// 2. Archive
	reqArch, _ := http.NewRequest(http.MethodPost, "/notifications/"+notifID.String()+"/archive", nil)
	reqArch.Header.Set("Authorization", "Bearer "+token)
	wArch := httptest.NewRecorder()
	router.ServeHTTP(wArch, reqArch)
	if wArch.Code != http.StatusOK {
		t.Fatalf("expected status 200 for archive, got %d", wArch.Code)
	}

	// 3. Attempting to mark archived notification as read should return 409 invalid_state_transition
	reqBadRead, _ := http.NewRequest(http.MethodPost, "/notifications/"+notifID.String()+"/read", nil)
	reqBadRead.Header.Set("Authorization", "Bearer "+token)
	wBadRead := httptest.NewRecorder()
	router.ServeHTTP(wBadRead, reqBadRead)

	if wBadRead.Code != http.StatusConflict {
		t.Fatalf("expected status 409 for read on archived notification, got %d", wBadRead.Code)
	}

	var errResp ErrorEnvelope
	if err := json.Unmarshal(wBadRead.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed decoding error response: %v", err)
	}
	if errResp.Error.Code != domain.CodeInvalidStateTransition {
		t.Errorf("expected error code %q, got %q", domain.CodeInvalidStateTransition, errResp.Error.Code)
	}
}

func TestPreferences_LockedPreferenceReturns422(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	prefRepo := newMockHTTPPreferenceRepo()
	router, secret := setupAPITestRouter(notifRepo, prefRepo, "jwt")

	userID := uuid.New()
	token, _ := GenerateTestHS256Token(userID.String(), []string{"employee"}, "user@example.com", secret, 1*time.Hour)

	// Attempt to disable channels.in_app.security (locked!)
	badPayload := []byte(`{
		"channels": {
			"in_app": {
				"security": false,
				"purchasing": true
			},
			"email": {
				"security": true
			}
		}
	}`)

	req, _ := http.NewRequest(http.MethodPut, "/me/notification-preferences", bytes.NewReader(badPayload))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected status 422 for locked preference, got %d (body: %s)", w.Code, w.Body.String())
	}

	var errResp ErrorEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed decoding error envelope: %v", err)
	}

	if errResp.Error.Code != domain.CodeValidationFailed {
		t.Errorf("expected error code %q, got %q", domain.CodeValidationFailed, errResp.Error.Code)
	}

	foundIssue := false
	for _, d := range errResp.Error.Details {
		if d.Field == "channels.in_app.security" && d.Issue == "preference_locked" {
			foundIssue = true
			break
		}
	}
	if !foundIssue {
		t.Errorf("expected detail issue 'preference_locked' on 'channels.in_app.security', got details: %+v", errResp.Error.Details)
	}
}

func TestDevAuthMode(t *testing.T) {
	notifRepo := newMockHTTPNotificationRepo()
	prefRepo := newMockHTTPPreferenceRepo()
	router, _ := setupAPITestRouter(notifRepo, prefRepo, "dev")

	adminUID := uuid.New()

	// In dev mode, X-Dev-User-Id and X-Dev-Roles grant access
	req, _ := http.NewRequest(http.MethodGet, "/admin/notification-rules", nil)
	req.Header.Set("X-Dev-User-Id", adminUID.String())
	req.Header.Set("X-Dev-Roles", "admin,finance")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200 in dev mode, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestWriteRateLimiter(t *testing.T) {
	limiter := NewUserRateLimiter(1.0, 1.0) // 1 token max, 1 token/sec
	key := "user-123"

	// 1st request succeeds
	allowed, _ := limiter.Allow(key)
	if !allowed {
		t.Fatalf("first request should be allowed")
	}

	// 2nd immediate request must be blocked
	allowed, retryAfter := limiter.Allow(key)
	if allowed {
		t.Fatalf("second immediate request should be rate limited")
	}
	if retryAfter <= 0 {
		t.Fatalf("expected positive retry-after duration, got %v", retryAfter)
	}
}
