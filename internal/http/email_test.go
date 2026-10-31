package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	"notification-service/internal/email"
	apphttp "notification-service/internal/http"
)

// --- Mocks for Email HTTP Tests ---

type mockHttpSettingsRepo struct {
	settings *domain.EmailSettings
}

func (m *mockHttpSettingsRepo) Get(ctx context.Context) (*domain.EmailSettings, error) {
	if m.settings == nil {
		return nil, domain.NewNotFoundError("settings not found")
	}
	return m.settings, nil
}

func (m *mockHttpSettingsRepo) Upsert(ctx context.Context, s *domain.EmailSettings) error {
	m.settings = s
	return nil
}

type mockHttpTemplateRepo struct {
	templates map[string]*domain.EmailTemplate
}

func (m *mockHttpTemplateRepo) List(ctx context.Context) ([]*domain.EmailTemplate, error) {
	var list []*domain.EmailTemplate
	for _, t := range m.templates {
		list = append(list, t)
	}
	return list, nil
}

func (m *mockHttpTemplateRepo) GetActive(ctx context.Context, templateKey string) (*domain.EmailTemplate, error) {
	tmpl, ok := m.templates[templateKey]
	if !ok || !tmpl.IsActive {
		return nil, domain.NewNotFoundError("active template not found")
	}
	return tmpl, nil
}

func (m *mockHttpTemplateRepo) GetByVersion(ctx context.Context, templateKey string, version int) (*domain.EmailTemplate, error) {
	tmpl, ok := m.templates[templateKey]
	if !ok || tmpl.Version != version {
		return nil, domain.NewNotFoundError("template not found")
	}
	return tmpl, nil
}

func (m *mockHttpTemplateRepo) ListVersions(ctx context.Context, templateKey string) ([]*domain.EmailTemplate, error) {
	var list []*domain.EmailTemplate
	for _, t := range m.templates {
		if t.TemplateKey == templateKey {
			list = append(list, t)
		}
	}
	return list, nil
}

func (m *mockHttpTemplateRepo) CreateNewVersion(ctx context.Context, tmpl *domain.EmailTemplate) error {
	m.templates[tmpl.TemplateKey] = tmpl
	return nil
}

func (m *mockHttpTemplateRepo) Activate(ctx context.Context, templateKey string, version int) error {
	if t, ok := m.templates[templateKey]; ok {
		t.IsActive = true
	}
	return nil
}

type mockHttpMessageRepo struct {
	messages map[uuid.UUID]*domain.EmailMessage
}

func (m *mockHttpMessageRepo) Create(ctx context.Context, msg *domain.EmailMessage) error {
	m.messages[msg.ID] = msg
	return nil
}

func (m *mockHttpMessageRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.EmailMessage, error) {
	msg, ok := m.messages[id]
	if !ok {
		return nil, domain.NewNotFoundError("message not found")
	}
	return msg, nil
}

func (m *mockHttpMessageRepo) GetByProviderMessageID(ctx context.Context, providerMsgID string) (*domain.EmailMessage, error) {
	for _, msg := range m.messages {
		if msg.ProviderMessageID != nil && *msg.ProviderMessageID == providerMsgID {
			return msg, nil
		}
	}
	return nil, domain.NewNotFoundError("message not found")
}

func (m *mockHttpMessageRepo) List(ctx context.Context, filter domain.EmailMessageFilter) ([]*domain.EmailMessage, int, error) {
	var list []*domain.EmailMessage
	for _, msg := range m.messages {
		list = append(list, msg)
	}
	return list, len(list), nil
}

func (m *mockHttpMessageRepo) ClaimDue(ctx context.Context, limit int) ([]*domain.EmailMessage, error) {
	return nil, nil
}

func (m *mockHttpMessageRepo) UpdateStatus(
	ctx context.Context,
	id uuid.UUID,
	status domain.EmailMessageStatus,
	attempts int,
	nextAttemptAt, sentAt *time.Time,
	lastError, providerMessageID *string,
) error {
	if msg, ok := m.messages[id]; ok {
		msg.Status = status
		msg.Attempts = attempts
		msg.NextAttemptAt = nextAttemptAt
		msg.SentAt = sentAt
		msg.LastError = lastError
		msg.ProviderMessageID = providerMessageID
	}
	return nil
}

func (m *mockHttpMessageRepo) SetResendLink(ctx context.Context, id, resendOf uuid.UUID) error {
	if msg, ok := m.messages[id]; ok {
		msg.ResendOf = &resendOf
	}
	return nil
}

func (m *mockHttpMessageRepo) CountSentToday(ctx context.Context) (int, error) {
	return 0, nil
}

func (m *mockHttpMessageRepo) MarkLostOnRestart(ctx context.Context) (int64, error) {
	return 0, nil
}

type mockHttpSuppressionRepo struct {
	suppressions map[string]*domain.EmailSuppression
}

func (m *mockHttpSuppressionRepo) List(ctx context.Context) ([]*domain.EmailSuppression, error) {
	var list []*domain.EmailSuppression
	for _, s := range m.suppressions {
		list = append(list, s)
	}
	return list, nil
}

func (m *mockHttpSuppressionRepo) GetByEmail(ctx context.Context, email string) (*domain.EmailSuppression, error) {
	s, ok := m.suppressions[email]
	if !ok {
		return nil, domain.NewNotFoundError("not suppressed")
	}
	return s, nil
}

func (m *mockHttpSuppressionRepo) Add(ctx context.Context, s *domain.EmailSuppression) error {
	m.suppressions[s.Email] = s
	return nil
}

func (m *mockHttpSuppressionRepo) Delete(ctx context.Context, email string) error {
	delete(m.suppressions, email)
	return nil
}

type mockHttpIdempotencyRepo struct {
	records map[string]*domain.IdempotencyKey
}

func (m *mockHttpIdempotencyRepo) Create(ctx context.Context, record *domain.IdempotencyKey) error {
	k := record.Scope + ":" + record.Key
	m.records[k] = record
	return nil
}

func (m *mockHttpIdempotencyRepo) Get(ctx context.Context, key, scope string) (*domain.IdempotencyKey, error) {
	k := scope + ":" + key
	rec, ok := m.records[k]
	if !ok {
		return nil, domain.NewNotFoundError("idempotency record not found")
	}
	return rec, nil
}

func (m *mockHttpIdempotencyRepo) UpdateResponse(ctx context.Context, key, scope string, response []byte) error {
	k := scope + ":" + key
	if rec, ok := m.records[k]; ok {
		rec.Response = response
	}
	return nil
}

type mockHttpSender struct {
	sent []*email.OutboundEmail
}

func (m *mockHttpSender) Send(ctx context.Context, msg *email.OutboundEmail) (*email.SendResult, error) {
	m.sent = append(m.sent, msg)
	return &email.SendResult{ProviderMessageID: "msg-" + uuid.NewString()}, nil
}

func setupEmailHttpTestRouter(t *testing.T) (*email.Engine, http.Handler, string) {
	t.Helper()
	internalToken := "test-internal-service-secret"

	cfg := &config.Config{
		Env:                  "test",
		InternalServiceToken: internalToken,
	}

	settingsRepo := &mockHttpSettingsRepo{
		settings: &domain.EmailSettings{
			Enabled:           true,
			Provider:          "smtp",
			Host:              "smtp.example.com",
			Port:              587,
			Username:          "mailer",
			TLSMode:           "starttls",
			FromName:          "ERP Test",
			FromAddress:       "noreply@example.com",
			DailyLimit:        1000,
			PasswordEncrypted: []byte("mock-encrypted"),
		},
	}

	tmplRepo := &mockHttpTemplateRepo{
		templates: map[string]*domain.EmailTemplate{
			"auth.verify_email_otp": {
				ID:                uuid.New(),
				TemplateKey:       "auth.verify_email_otp",
				Version:           1,
				SubjectTemplate:   "Your OTP: {{otp_code}}",
				HTMLTemplate:      "<p>Hello {{name}}, your code is <b>{{otp_code}}</b></p>",
				TextTemplate:      "Hello {{name}}, your code is {{otp_code}}",
				DeclaredVariables: []string{"name", "otp_code"},
				IsActive:          true,
			},
		},
	}

	msgRepo := &mockHttpMessageRepo{messages: make(map[uuid.UUID]*domain.EmailMessage)}
	suppRepo := &mockHttpSuppressionRepo{suppressions: make(map[string]*domain.EmailSuppression)}
	idempRepo := &mockHttpIdempotencyRepo{records: make(map[string]*domain.IdempotencyKey)}
	sender := &mockHttpSender{}
	clock := email.NewFakeClock(time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC))

	key, err := email.ParseEncryptionKey("MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	if err != nil {
		t.Fatalf("parse encryption key: %v", err)
	}

	txWorker := email.NewTransactionalWorker(msgRepo, sender, clock, nil)
	queueWorker := email.NewBusinessQueueWorker(settingsRepo, msgRepo, sender, clock, nil)

	eng := email.NewEngine(
		settingsRepo,
		tmplRepo,
		msgRepo,
		suppRepo,
		idempRepo,
		sender,
		txWorker,
		queueWorker,
		key,
		clock,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	router := apphttp.NewRouter(apphttp.RouterConfig{
		Config:      cfg,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		EmailEngine: eng,
	})

	return eng, router, internalToken
}

// --- Test Cases ---

func TestEmailHTTP_AuthRequired(t *testing.T) {
	_, router, _ := setupEmailHttpTestRouter(t)

	reqBody := `{"template_key":"auth.verify_email_otp","to":["user@example.com"]}`
	req := httptest.NewRequest(http.MethodPost, "/internal/email/send", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "idemp-auth-1")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 unauthenticated, got %d", rec.Code)
	}
}

func TestEmailHTTP_MissingIdempotencyKey(t *testing.T) {
	_, router, token := setupEmailHttpTestRouter(t)

	reqBody := `{"template_key":"auth.verify_email_otp","to":["user@example.com"]}`
	req := httptest.NewRequest(http.MethodPost, "/internal/email/send", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", token)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 validation_failed, got %d", rec.Code)
	}

	var errResp apphttp.ErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed decoding error response: %v", err)
	}
	if errResp.Error.Code != domain.CodeValidationFailed {
		t.Fatalf("expected code validation_failed, got %s", errResp.Error.Code)
	}
}

func TestEmailHTTP_AttachmentsNotSupported(t *testing.T) {
	_, router, token := setupEmailHttpTestRouter(t)

	reqBody := `{
		"template_key": "auth.verify_email_otp",
		"to": ["user@example.com"],
		"variables": {"name": "Bob", "otp_code": "123456"},
		"attachments": [{"filename": "doc.pdf", "content": "base64"}]
	}`
	req := httptest.NewRequest(http.MethodPost, "/internal/email/send", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", token)
	req.Header.Set("Idempotency-Key", "idemp-attach-1")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for attachments, got %d", rec.Code)
	}

	var errResp apphttp.ErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed unmarshaling error response: %v", err)
	}
	if errResp.Error.Code != domain.CodeValidationFailed {
		t.Fatalf("expected code validation_failed, got %s", errResp.Error.Code)
	}
	foundIssue := false
	for _, d := range errResp.Error.Details {
		if d.Field == "attachments" && d.Issue == "attachments_not_supported" {
			foundIssue = true
			break
		}
	}
	if !foundIssue {
		t.Fatalf("expected detail attachments_not_supported, got %v", errResp.Error.Details)
	}
}

func TestEmailHTTP_SuccessAndIdempotency(t *testing.T) {
	eng, router, token := setupEmailHttpTestRouter(t)

	reqPayload := `{
		"template_key": "auth.verify_email_otp",
		"to": ["user@example.com"],
		"variables": {"name": "Bob", "otp_code": "654321"},
		"sensitive_variables": ["otp_code"],
		"priority": "transactional"
	}`

	// 1. Initial Send -> 202 Accepted
	req1 := httptest.NewRequest(http.MethodPost, "/internal/email/send", bytes.NewBufferString(reqPayload))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("X-Internal-Token", token)
	req1.Header.Set("Idempotency-Key", "idemp-key-send-1")

	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 accepted, got %d, body: %s", rec1.Code, rec1.Body.String())
	}

	var resp1 struct {
		Data email.SendEmailResponse `json:"data"`
	}
	if err := json.Unmarshal(rec1.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("failed unmarshaling response: %v", err)
	}
	if resp1.Data.MessageID == uuid.Nil || resp1.Data.Status != "queued" {
		t.Fatalf("unexpected response payload: %+v", resp1.Data)
	}

	eng.TxWorker().Wait()

	// 2. Replay identical request with same key -> Returns identical 202 response
	req2 := httptest.NewRequest(http.MethodPost, "/internal/email/send", bytes.NewBufferString(reqPayload))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Internal-Token", token)
	req2.Header.Set("Idempotency-Key", "idemp-key-send-1")

	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Fatalf("expected 202 accepted on replay, got %d", rec2.Code)
	}

	var resp2 struct {
		Data email.SendEmailResponse `json:"data"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("failed unmarshaling replay response: %v", err)
	}
	if resp2.Data.MessageID != resp1.Data.MessageID {
		t.Fatalf("expected same message_id %v, got %v", resp1.Data.MessageID, resp2.Data.MessageID)
	}

	// 3. Different payload with same Idempotency-Key -> 409 idempotency_key_conflict
	conflictPayload := `{
		"template_key": "auth.verify_email_otp",
		"to": ["different@example.com"],
		"variables": {"name": "Charlie", "otp_code": "999999"}
	}`
	req3 := httptest.NewRequest(http.MethodPost, "/internal/email/send", bytes.NewBufferString(conflictPayload))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("X-Internal-Token", token)
	req3.Header.Set("Idempotency-Key", "idemp-key-send-1")

	rec3 := httptest.NewRecorder()
	router.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusConflict {
		t.Fatalf("expected 409 conflict on payload mismatch, got %d, body: %s", rec3.Code, rec3.Body.String())
	}

	var errResp apphttp.ErrorEnvelope
	if err := json.Unmarshal(rec3.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed decoding conflict response: %v", err)
	}
	if errResp.Error.Code != domain.CodeIdempotencyConflict {
		t.Fatalf("expected code idempotency_key_conflict, got %s", errResp.Error.Code)
	}
}
