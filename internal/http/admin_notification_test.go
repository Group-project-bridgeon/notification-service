package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	apphttp "notification-service/internal/http"
	"notification-service/internal/notification"
)

// --- Mock Repositories ---

type mockRuleRepo struct {
	rules map[uuid.UUID]*domain.NotificationRule
}

func newMockRuleRepo() *mockRuleRepo {
	return &mockRuleRepo{rules: make(map[uuid.UUID]*domain.NotificationRule)}
}

func (m *mockRuleRepo) GetActiveByEventType(_ context.Context, eventType string) ([]*domain.NotificationRule, error) {
	var list []*domain.NotificationRule
	for _, r := range m.rules {
		if r.EventType == eventType && r.IsActive {
			list = append(list, r)
		}
	}
	return list, nil
}

func (m *mockRuleRepo) List(_ context.Context) ([]*domain.NotificationRule, error) {
	var list []*domain.NotificationRule
	for _, r := range m.rules {
		list = append(list, r)
	}
	return list, nil
}

func (m *mockRuleRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.NotificationRule, error) {
	r, ok := m.rules[id]
	if !ok {
		return nil, domain.NewNotFoundError("rule not found")
	}
	return r, nil
}

func (m *mockRuleRepo) Create(_ context.Context, rule *domain.NotificationRule) error {
	m.rules[rule.ID] = rule
	return nil
}

func (m *mockRuleRepo) Update(_ context.Context, rule *domain.NotificationRule) error {
	if _, ok := m.rules[rule.ID]; !ok {
		return domain.NewNotFoundError("rule not found")
	}
	m.rules[rule.ID] = rule
	return nil
}

func (m *mockRuleRepo) Delete(_ context.Context, id uuid.UUID) error {
	delete(m.rules, id)
	return nil
}

func (m *mockRuleRepo) Deactivate(_ context.Context, id uuid.UUID) error {
	r, ok := m.rules[id]
	if !ok {
		return domain.NewNotFoundError("rule not found")
	}
	r.IsActive = false
	return nil
}

type mockTemplateRepo struct {
	templates map[string][]*domain.NotificationTemplate
}

func newMockTemplateRepo() *mockTemplateRepo {
	return &mockTemplateRepo{templates: make(map[string][]*domain.NotificationTemplate)}
}

func (m *mockTemplateRepo) GetActive(_ context.Context, templateKey, locale string) (*domain.NotificationTemplate, error) {
	list, ok := m.templates[templateKey]
	if !ok || len(list) == 0 {
		return nil, domain.NewNotFoundError("template not found")
	}
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].IsActive && (locale == "" || list[i].Locale == locale) {
			return list[i], nil
		}
	}
	return nil, domain.NewNotFoundError("active template not found")
}

func (m *mockTemplateRepo) List(_ context.Context) ([]*domain.NotificationTemplate, error) {
	var all []*domain.NotificationTemplate
	for _, tmpls := range m.templates {
		all = append(all, tmpls...)
	}
	return all, nil
}

func (m *mockTemplateRepo) Create(_ context.Context, tmpl *domain.NotificationTemplate) error {
	m.templates[tmpl.TemplateKey] = append(m.templates[tmpl.TemplateKey], tmpl)
	return nil
}

func (m *mockTemplateRepo) CreateNewVersion(_ context.Context, tmpl *domain.NotificationTemplate) error {
	list := m.templates[tmpl.TemplateKey]
	maxVer := 0
	for _, old := range list {
		old.IsActive = false
		if old.Version > maxVer {
			maxVer = old.Version
		}
	}
	tmpl.Version = maxVer + 1
	tmpl.IsActive = true
	m.templates[tmpl.TemplateKey] = append(m.templates[tmpl.TemplateKey], tmpl)
	return nil
}

type mockDeliveryRepo struct {
	deliveries map[uuid.UUID]*domain.NotificationDelivery
}

func newMockDeliveryRepo() *mockDeliveryRepo {
	return &mockDeliveryRepo{deliveries: make(map[uuid.UUID]*domain.NotificationDelivery)}
}

func (m *mockDeliveryRepo) Create(_ context.Context, delivery *domain.NotificationDelivery) error {
	m.deliveries[delivery.ID] = delivery
	return nil
}

func (m *mockDeliveryRepo) ClaimDue(_ context.Context, limit int) ([]*domain.NotificationDelivery, error) {
	return nil, nil
}

func (m *mockDeliveryRepo) UpdateStatus(_ context.Context, id uuid.UUID, status domain.DeliveryStatus, attempts int, nextRetryAt *time.Time, lastError *string) error {
	d, ok := m.deliveries[id]
	if !ok {
		return domain.NewNotFoundError("delivery not found")
	}
	d.Status = status
	d.Attempts = attempts
	d.NextRetryAt = nextRetryAt
	d.LastError = lastError
	return nil
}

func (m *mockDeliveryRepo) List(_ context.Context, filter domain.DeliveryFilter) ([]*domain.NotificationDelivery, int, error) {
	var list []*domain.NotificationDelivery
	for _, d := range m.deliveries {
		if filter.Status != nil && d.Status != *filter.Status {
			continue
		}
		if filter.Channel != nil && d.Channel != *filter.Channel {
			continue
		}
		list = append(list, d)
	}
	return list, len(list), nil
}

func (m *mockDeliveryRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.NotificationDelivery, error) {
	d, ok := m.deliveries[id]
	if !ok {
		return nil, domain.NewNotFoundError("delivery not found")
	}
	return d, nil
}

type mockDeadLetterRepo struct {
	deadLetters map[uuid.UUID]*domain.DeadLetter
}

func newMockDeadLetterRepo() *mockDeadLetterRepo {
	return &mockDeadLetterRepo{deadLetters: make(map[uuid.UUID]*domain.DeadLetter)}
}

func (m *mockDeadLetterRepo) Create(_ context.Context, dl *domain.DeadLetter) error {
	m.deadLetters[dl.ID] = dl
	return nil
}

func (m *mockDeadLetterRepo) List(_ context.Context, limit, offset int) ([]*domain.DeadLetter, int, error) {
	var list []*domain.DeadLetter
	for _, dl := range m.deadLetters {
		list = append(list, dl)
	}
	return list, len(list), nil
}

func (m *mockDeadLetterRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.DeadLetter, error) {
	dl, ok := m.deadLetters[id]
	if !ok {
		return nil, domain.NewNotFoundError("dead letter not found")
	}
	return dl, nil
}

func (m *mockDeadLetterRepo) MarkReplayed(_ context.Context, id uuid.UUID) error {
	dl, ok := m.deadLetters[id]
	if !ok {
		return domain.NewNotFoundError("dead letter not found")
	}
	now := time.Now().UTC()
	dl.ReplayedAt = &now
	return nil
}

type mockStatsRepo struct {
	stats *domain.NotificationStats
}

func (m *mockStatsRepo) GetNotificationStats(_ context.Context, _ time.Time) (*domain.NotificationStats, error) {
	if m.stats != nil {
		return m.stats, nil
	}
	return &domain.NotificationStats{
		ByCategory:           map[string]int{"approval": 10, "security": 2},
		ByPriority:           map[string]int{"normal": 10, "high": 2},
		DeliverySuccessRate:  0.95,
		TotalDeliveries:      20,
		SuccessfulDeliveries: 19,
		DeadLettersCount:     1,
		EmailFailuresCount:   0,
		Period:               "24h",
	}, nil
}

type mockConsumerStatusProvider struct {
	status *domain.ConsumerStatus
}

func (m *mockConsumerStatusProvider) GetStatus(_ context.Context) *domain.ConsumerStatus {
	return m.status
}

type mockRecipientRepo struct {
	recipients map[uuid.UUID]*domain.Recipient
}

func (m *mockRecipientRepo) UpsertGuarded(_ context.Context, r *domain.Recipient) error {
	m.recipients[r.UserID] = r
	return nil
}

func (m *mockRecipientRepo) FindByRoles(_ context.Context, roles []string) ([]*domain.Recipient, error) {
	return nil, nil
}

func (m *mockRecipientRepo) FindByID(_ context.Context, id uuid.UUID) (*domain.Recipient, error) {
	r, ok := m.recipients[id]
	if !ok {
		return nil, domain.NewNotFoundError("recipient not found")
	}
	return r, nil
}

type mockNotificationRepo struct {
	notifs map[uuid.UUID]*domain.Notification
}

func (m *mockNotificationRepo) BatchInsert(_ context.Context, notifs []*domain.Notification) error {
	for _, n := range notifs {
		m.notifs[n.ID] = n
	}
	return nil
}

func (m *mockNotificationRepo) List(_ context.Context, _ domain.NotificationFilter) ([]*domain.Notification, int, error) {
	return nil, 0, nil
}

func (m *mockNotificationRepo) GetByID(_ context.Context, _, id uuid.UUID) (*domain.Notification, error) {
	n, ok := m.notifs[id]
	if !ok {
		return nil, domain.NewNotFoundError("not found")
	}
	return n, nil
}

func (m *mockNotificationRepo) SetStatus(_ context.Context, _, _ uuid.UUID, _ domain.NotificationStatus) (*domain.Notification, error) {
	return nil, nil
}

func (m *mockNotificationRepo) ReadAll(_ context.Context, _ uuid.UUID, _ *domain.NotificationCategory) (int64, error) {
	return 0, nil
}

func (m *mockNotificationRepo) GetUnreadCounts(_ context.Context, _ uuid.UUID) (int, map[string]int, error) {
	return 0, map[string]int{}, nil
}

type mockPublisher struct{}

func (mockPublisher) PublishNotification(_ context.Context, _ *domain.Notification, _ notification.UnreadCounts) error {
	return nil
}

// --- Test Fixture ---

type adminFixture struct {
	router           http.Handler
	ruleRepo         *mockRuleRepo
	tmplRepo         *mockTemplateRepo
	deliveryRepo     *mockDeliveryRepo
	deadLetterRepo   *mockDeadLetterRepo
	recipientRepo    *mockRecipientRepo
	notificationRepo *mockNotificationRepo
	consumerProvider *mockConsumerStatusProvider
	statsRepo        *mockStatsRepo
	emailTmplRepo    *mockHttpTemplateRepo
}

func setupAdminTest(t *testing.T) *adminFixture {
	t.Helper()

	cfg := &config.Config{
		Env:          "test",
		AuthMode:     "dev",
		AllowedRoles: []string{"admin", "manager", "purchasing", "warehouse", "sales", "finance"},
		Kafka: config.KafkaConfig{
			GroupID: "notification-service",
		},
	}

	ruleRepo := newMockRuleRepo()
	tmplRepo := newMockTemplateRepo()
	deliveryRepo := newMockDeliveryRepo()
	deadLetterRepo := newMockDeadLetterRepo()
	recipientRepo := &mockRecipientRepo{recipients: make(map[uuid.UUID]*domain.Recipient)}
	notificationRepo := &mockNotificationRepo{notifs: make(map[uuid.UUID]*domain.Notification)}
	consumerProvider := &mockConsumerStatusProvider{
		status: &domain.ConsumerStatus{
			GroupID:        "notification-service",
			State:          "running",
			Topics:         []domain.ConsumerTopicStatus{},
			ProcessedTotal: 50,
			RetriedTotal:   2,
			DLQTotal:       1,
			FailedTotal:    0,
		},
	}
	statsRepo := &mockStatsRepo{}
	emailTmplRepo := &mockHttpTemplateRepo{templates: make(map[string]*domain.EmailTemplate)}

	// Preload an active email template
	emailTmplRepo.templates["email_po_approved"] = &domain.EmailTemplate{
		ID:                uuid.New(),
		TemplateKey:       "email_po_approved",
		Version:           1,
		SubjectTemplate:   "PO Approved",
		DeclaredVariables: []string{"po_number"},
		IsActive:          true,
	}

	router := apphttp.NewRouter(apphttp.RouterConfig{
		Config:                   cfg,
		Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		RuleRepo:                 ruleRepo,
		NotificationTemplateRepo: tmplRepo,
		EmailTemplateRepo:        emailTmplRepo,
		RecipientRepo:            recipientRepo,
		DeliveryRepo:             deliveryRepo,
		DeadLetterRepo:           deadLetterRepo,
		StatsRepo:                statsRepo,
		ConsumerStatusProvider:   consumerProvider,
		NotificationRepo:         notificationRepo,
	})

	return &adminFixture{
		router:           router,
		ruleRepo:         ruleRepo,
		tmplRepo:         tmplRepo,
		deliveryRepo:     deliveryRepo,
		deadLetterRepo:   deadLetterRepo,
		recipientRepo:    recipientRepo,
		notificationRepo: notificationRepo,
		consumerProvider: consumerProvider,
		statsRepo:        statsRepo,
		emailTmplRepo:    emailTmplRepo,
	}
}

// 1. Non-admin routes reject with 403 forbidden_role
func TestAdmin_NonAdminForbidden_403(t *testing.T) {
	f := setupAdminTest(t)

	routes := []struct {
		method string
		path   string
	}{
		{"GET", "/admin/notification-rules"},
		{"POST", "/admin/notification-rules"},
		{"GET", "/admin/notification-templates"},
		{"POST", "/admin/notification-templates"},
		{"POST", "/admin/notifications/test-send"},
		{"GET", "/admin/notification-deliveries"},
		{"GET", "/admin/notification-dead-letters"},
		{"GET", "/admin/notification-stats"},
		{"GET", "/admin/notification-consumer-status"},
	}

	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			req := httptest.NewRequest(route.method, route.path, nil)
			req.Header.Set("X-Dev-User-Id", uuid.New().String())
			req.Header.Set("X-Dev-Roles", "employee,manager") // Non-admin

			rec := httptest.NewRecorder()
			f.router.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf("expected 403 Forbidden, got %d for %s %s", rec.Code, route.method, route.path)
			}
		})
	}
}

// 2. Notification Rules CRUD, Validation, and Concurrency
func TestAdmin_NotificationRules_Validation_And_Concurrency(t *testing.T) {
	f := setupAdminTest(t)
	adminID := uuid.New().String()

	// A. Validation failure: invalid role
	invalidRoleBody := map[string]any{
		"event_type":   "purchasing.created",
		"target_roles": []string{"super_wizard"}, // invalid role
		"type":         "order_confirmed",
		"category":     "purchasing",
		"priority":     "normal",
		"template_key": "tmpl_po",
	}
	b, _ := json.Marshal(invalidRoleBody)
	req := httptest.NewRequest("POST", "/admin/notification-rules", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for invalid role, got %d", rec.Code)
	}

	// B. Validation failure: send_email true without email_template_key
	missingEmailTmpl := map[string]any{
		"event_type":   "purchasing.created",
		"target_roles": []string{"purchasing"},
		"type":         "order_confirmed",
		"category":     "purchasing",
		"priority":     "normal",
		"template_key": "tmpl_po",
		"send_email":   true,
		// email_template_key missing
	}
	b, _ = json.Marshal(missingEmailTmpl)
	req = httptest.NewRequest("POST", "/admin/notification-rules", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for missing email_template_key, got %d", rec.Code)
	}

	// C. Successful Creation with ETag
	validRuleBody := map[string]any{
		"event_type":         "purchasing.created",
		"target_roles":       []string{"purchasing", "finance"},
		"type":               "order_confirmed",
		"category":           "purchasing",
		"priority":           "high",
		"template_key":       "tmpl_po",
		"send_email":         true,
		"email_template_key": "email_po_approved",
	}
	b, _ = json.Marshal(validRuleBody)
	req = httptest.NewRequest("POST", "/admin/notification-rules", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header on created rule")
	}

	var createdResp struct {
		Data domain.NotificationRule `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &createdResp)
	ruleID := createdResp.Data.ID

	// D. Get rule by ID
	req = httptest.NewRequest("GET", fmt.Sprintf("/admin/notification-rules/%s", ruleID), nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if rec.Header().Get("ETag") != etag {
		t.Fatalf("expected matching ETag %s, got %s", etag, rec.Header().Get("ETag"))
	}

	// E. Update rule with mismatched If-Match -> 409 Conflict
	updateBody := validRuleBody
	updateBody["priority"] = "critical"
	b, _ = json.Marshal(updateBody)

	req = httptest.NewRequest("PUT", fmt.Sprintf("/admin/notification-rules/%s", ruleID), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	req.Header.Set("If-Match", `"wrong-etag"`)
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on mismatched If-Match, got %d", rec.Code)
	}

	// F. Update rule with valid If-Match -> 200 OK
	req = httptest.NewRequest("PUT", fmt.Sprintf("/admin/notification-rules/%s", ruleID), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	req.Header.Set("If-Match", etag)
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on valid If-Match, got %d: %s", rec.Code, rec.Body.String())
	}

	// G. Deactivate rule
	req = httptest.NewRequest("DELETE", fmt.Sprintf("/admin/notification-rules/%s", ruleID), nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on delete, got %d", rec.Code)
	}
	if f.ruleRepo.rules[ruleID].IsActive {
		t.Fatal("expected rule to be inactive")
	}
}

// 3. Template Placeholder Validation & Versioning
func TestAdmin_NotificationTemplates_Placeholders_And_Versioning(t *testing.T) {
	f := setupAdminTest(t)
	adminID := uuid.New().String()

	// A. Reject disallowed placeholders (anything not in payload.*, event_type, aggregate_id, actor_user_id, correlation_id)
	invalidTmplBody := map[string]any{
		"template_key":   "tmpl_test",
		"event_type":     "test.event",
		"title_template": "Notice for {{disallowed_field}}",
		"body_template":  "User {{payload.name}} made a request.",
	}
	b, _ := json.Marshal(invalidTmplBody)
	req := httptest.NewRequest("POST", "/admin/notification-templates", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for disallowed placeholder, got %d", rec.Code)
	}

	// B. Create template with valid placeholders
	validTmplBody := map[string]any{
		"template_key":   "tmpl_po_approval",
		"event_type":     "purchasing.po_approval",
		"locale":         "en",
		"title_template": "Order {{payload.po_number}} requires review",
		"body_template":  "Event {{event_type}} occurred for entity {{aggregate_id}} by {{actor_user_id}} with correlation {{correlation_id}}.",
	}
	b, _ = json.Marshal(validTmplBody)
	req = httptest.NewRequest("POST", "/admin/notification-templates", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for valid template, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("ETag") != `"1"` {
		t.Fatalf("expected ETag \"1\", got %s", rec.Header().Get("ETag"))
	}

	// C. Update template creating version 2 with mismatched If-Match -> 409 Conflict
	updateBody := map[string]any{
		"title_template": "Order {{payload.po_number}} updated",
		"body_template":  "Updated by {{actor_user_id}}.",
	}
	b, _ = json.Marshal(updateBody)
	req = httptest.NewRequest("PUT", "/admin/notification-templates/tmpl_po_approval", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	req.Header.Set("If-Match", `"99"`) // Wrong version
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on mismatched template If-Match, got %d", rec.Code)
	}

	// D. Update template creating version 2 with valid If-Match: "1"
	req = httptest.NewRequest("PUT", "/admin/notification-templates/tmpl_po_approval", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	req.Header.Set("If-Match", `"1"`)
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on template update, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("ETag") != `"2"` {
		t.Fatalf("expected ETag \"2\", got %s", rec.Header().Get("ETag"))
	}

	// Check active version in repo
	active, err := f.tmplRepo.GetActive(context.Background(), "tmpl_po_approval", "en")
	if err != nil || active == nil {
		t.Fatal("expected active template version 2")
	}
	if active.Version != 2 {
		t.Fatalf("expected active version 2, got %d", active.Version)
	}
}

// 4. Test Send
func TestAdmin_TestSend(t *testing.T) {
	f := setupAdminTest(t)
	adminID := uuid.New().String()
	knownUserID := uuid.New()

	f.recipientRepo.recipients[knownUserID] = &domain.Recipient{
		UserID:   knownUserID,
		Email:    "test@example.com",
		IsActive: true,
	}

	_ = f.tmplRepo.Create(context.Background(), &domain.NotificationTemplate{
		ID:            uuid.New(),
		TemplateKey:   "tmpl_test_send",
		EventType:     "test.event",
		Locale:        "en",
		TitleTemplate: "Hello {{payload.name}}",
		BodyTemplate:  "Your code is {{payload.code}}",
		Version:       1,
		IsActive:      true,
	})

	// A. Unknown user returns 404
	testSendUnknown := map[string]any{
		"user_id":      uuid.New().String(),
		"template_key": "tmpl_test_send",
		"sample_payload": map[string]any{
			"name": "Alex",
			"code": "123",
		},
	}
	b, _ := json.Marshal(testSendUnknown)
	req := httptest.NewRequest("POST", "/admin/notifications/test-send", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for unknown user, got %d", rec.Code)
	}

	// B. Known user returns 200 with is_test: true
	testSendKnown := map[string]any{
		"user_id":      knownUserID.String(),
		"template_key": "tmpl_test_send",
		"sample_payload": map[string]any{
			"name": "Jane",
			"code": "456",
		},
	}
	b, _ = json.Marshal(testSendKnown)
	req = httptest.NewRequest("POST", "/admin/notifications/test-send", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify notification has is_test = true
	var foundTestNotif bool
	for _, n := range f.notificationRepo.notifs {
		if n.RecipientID == knownUserID && n.IsTest {
			foundTestNotif = true
			if n.Title != "Hello Jane" {
				t.Fatalf("expected title 'Hello Jane', got '%s'", n.Title)
			}
		}
	}
	if !foundTestNotif {
		t.Fatal("expected notification with is_test: true to be created")
	}
}

// 5. Delivery retry constraints
func TestAdmin_DeliveryRetry(t *testing.T) {
	f := setupAdminTest(t)
	adminID := uuid.New().String()

	sentDelivID := uuid.New()
	f.deliveryRepo.deliveries[sentDelivID] = &domain.NotificationDelivery{
		ID:             sentDelivID,
		NotificationID: uuid.New(),
		Channel:        domain.ChannelInApp,
		Status:         domain.DeliveryStatusSent,
	}

	failedDelivID := uuid.New()
	f.deliveryRepo.deliveries[failedDelivID] = &domain.NotificationDelivery{
		ID:             failedDelivID,
		NotificationID: uuid.New(),
		Channel:        domain.ChannelEmail,
		Status:         domain.DeliveryStatusFailed,
		Attempts:       1,
	}

	// A. Retrying 'sent' delivery returns 409
	req := httptest.NewRequest("POST", fmt.Sprintf("/admin/notification-deliveries/%s/retry", sentDelivID), nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when retrying sent delivery, got %d", rec.Code)
	}

	// B. Retrying 'failed' delivery succeeds and sets status to pending
	req = httptest.NewRequest("POST", fmt.Sprintf("/admin/notification-deliveries/%s/retry", failedDelivID), nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK when retrying failed delivery, got %d: %s", rec.Code, rec.Body.String())
	}
	if f.deliveryRepo.deliveries[failedDelivID].Status != domain.DeliveryStatusPending {
		t.Fatalf("expected delivery status to become pending, got %s", f.deliveryRepo.deliveries[failedDelivID].Status)
	}
}

// 6. Dead Letter Replay
func TestAdmin_DeadLetterReplay(t *testing.T) {
	f := setupAdminTest(t)
	adminID := uuid.New().String()

	dlID := uuid.New()
	rawEnvelope := fmt.Sprintf(`{
		"event_id": "%s",
		"event_type": "user.created",
		"event_version": "1.0",
		"occurred_at": "%s",
		"producer": "auth-service",
		"aggregate_type": "user",
		"aggregate_id": "%s",
		"payload": {"email": "replay@example.com"}
	}`, uuid.New().String(), time.Now().UTC().Format(time.RFC3339), uuid.New().String())

	f.deadLetterRepo.deadLetters[dlID] = &domain.DeadLetter{
		ID:       dlID,
		Source:   domain.EventSourceKafka,
		RawEvent: []byte(rawEnvelope),
		Error:    "temporary failure",
		FailedAt: time.Now().UTC(),
	}

	// A. First replay succeeds
	req := httptest.NewRequest("POST", fmt.Sprintf("/admin/notification-dead-letters/%s/replay", dlID), nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on dead letter replay, got %d: %s", rec.Code, rec.Body.String())
	}
	if f.deadLetterRepo.deadLetters[dlID].ReplayedAt == nil {
		t.Fatal("expected replayed_at timestamp to be set")
	}

	// B. Replaying the same dead letter a second time returns 409 Conflict
	req = httptest.NewRequest("POST", fmt.Sprintf("/admin/notification-dead-letters/%s/replay", dlID), nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on duplicate dead letter replay, got %d", rec.Code)
	}
}

// 7. Notification Stats & Consumer Status
func TestAdmin_StatsAndConsumerStatus(t *testing.T) {
	f := setupAdminTest(t)
	adminID := uuid.New().String()

	// A. Stats endpoint
	req := httptest.NewRequest("GET", "/admin/notification-stats", nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from stats endpoint, got %d", rec.Code)
	}

	var statsResp struct {
		Data domain.NotificationStats `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &statsResp)
	if statsResp.Data.TotalDeliveries != 20 {
		t.Fatalf("expected 20 total deliveries, got %d", statsResp.Data.TotalDeliveries)
	}

	// B. Consumer Status running
	req = httptest.NewRequest("GET", "/admin/notification-consumer-status", nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from consumer status, got %d", rec.Code)
	}

	// C. Consumer Status fallback when disconnected
	f.consumerProvider.status = &domain.ConsumerStatus{
		GroupID:        "notification-service",
		State:          "disconnected",
		Topics:         []domain.ConsumerTopicStatus{},
		ProcessedTotal: 50,
	}

	req = httptest.NewRequest("GET", "/admin/notification-consumer-status", nil)
	req.Header.Set("X-Dev-User-Id", adminID)
	req.Header.Set("X-Dev-Roles", "admin")
	rec = httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK even when disconnected, got %d", rec.Code)
	}
	var consResp struct {
		Data domain.ConsumerStatus `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &consResp)
	if consResp.Data.State != "disconnected" {
		t.Fatalf("expected state disconnected, got %s", consResp.Data.State)
	}
}
