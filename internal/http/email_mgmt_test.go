package http_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	"notification-service/internal/email"
	apphttp "notification-service/internal/http"
)

type emailMgmtFixture struct {
	router        http.Handler
	engine        *email.Engine
	settingsRepo  *mockHttpSettingsRepo
	tmplRepo      *mockHttpTemplateRepo
	msgRepo       *mockHttpMessageRepo
	suppRepo      *mockHttpSuppressionRepo
	sender        *mockHttpSender
	clock         *email.FakeClock
	webhookSecret string
}

func setupEmailMgmtTest(t *testing.T) *emailMgmtFixture {
	t.Helper()
	webhookSecret := "mgmt-webhook-secret-123"

	cfg := &config.Config{
		Env:           "test",
		AuthMode:      "dev",
		WebhookSecret: webhookSecret,
	}

	settingsRepo := &mockHttpSettingsRepo{
		settings: &domain.EmailSettings{
			ID:                1,
			Enabled:           true,
			Provider:          "smtp",
			Host:              "smtp.example.com",
			Port:              587,
			Username:          "mailer",
			TLSMode:           "starttls",
			FromName:          "ERP Test",
			FromAddress:       "noreply@example.com",
			DailyLimit:        1000,
			PasswordEncrypted: []byte("encrypted-pass"),
		},
	}

	tmplRepo := &mockHttpTemplateRepo{
		templates: map[string]*domain.EmailTemplate{
			"auth.verify_email_otp": {
				ID:                 uuid.New(),
				TemplateKey:        "auth.verify_email_otp",
				Version:            1,
				SubjectTemplate:    "Your OTP: {{otp_code}}",
				HTMLTemplate:       "<p>Hello {{name}}, your code is <b>{{otp_code}}</b></p>",
				TextTemplate:       "Hello {{name}}, your code is {{otp_code}}",
				DeclaredVariables:  []string{"name", "otp_code"},
				SensitiveVariables: []string{"otp_code"},
				IsActive:           true,
			},
			"order.confirmed": {
				ID:                 uuid.New(),
				TemplateKey:        "order.confirmed",
				Version:            1,
				SubjectTemplate:    "Order {{order_no}} Confirmed",
				HTMLTemplate:       "<p>Thank you for order {{order_no}}</p>",
				TextTemplate:       "Thank you for order {{order_no}}",
				DeclaredVariables:  []string{"order_no"},
				SensitiveVariables: []string{},
				IsActive:           true,
			},
		},
	}

	msgRepo := &mockHttpMessageRepo{messages: make(map[uuid.UUID]*domain.EmailMessage)}
	suppRepo := &mockHttpSuppressionRepo{suppressions: make(map[string]*domain.EmailSuppression)}
	idempRepo := &mockHttpIdempotencyRepo{records: make(map[string]*domain.IdempotencyKey)}
	sender := &mockHttpSender{}
	clock := email.NewFakeClock(time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC))

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

	return &emailMgmtFixture{
		router:        router,
		engine:        eng,
		settingsRepo:  settingsRepo,
		tmplRepo:      tmplRepo,
		msgRepo:       msgRepo,
		suppRepo:      suppRepo,
		sender:        sender,
		clock:         clock,
		webhookSecret: webhookSecret,
	}
}

// 1. Acceptance Test: 403 for non-admin on admin-only routes
func TestEmailHTTP_ForbiddenForNonAdmin(t *testing.T) {
	f := setupEmailMgmtTest(t)

	nonAdminHeaders := map[string]string{
		"X-Dev-User-Id": "11111111-1111-1111-1111-111111111111",
		"X-Dev-Roles":   "employee", // Not admin
	}

	endpoints := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/admin/email-settings", ""},
		{http.MethodPut, "/admin/email-settings", `{"provider":"smtp"}`},
		{http.MethodPost, "/admin/email-settings/test", `{"to":"admin@example.com"}`},
		{http.MethodPut, "/email/templates/auth.verify_email_otp", `{"subject_template":"Sub","html_template":"<p>H</p>","text_template":"T","declared_variables":[]}`},
		{http.MethodPost, "/email/templates/auth.verify_email_otp/activate", `{"version":2}`},
		{http.MethodPost, "/email/suppressions", `{"email":"test@example.com"}`},
		{http.MethodDelete, "/email/suppressions/test@example.com", ""},
	}

	for _, ep := range endpoints {
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.path, bytes.NewBufferString(ep.body))
			req.Header.Set("Content-Type", "application/json")
			for k, v := range nonAdminHeaders {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			f.router.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf("expected 403 Forbidden for endpoint %s %s, got %d", ep.method, ep.path, rec.Code)
			}

			var errEnv apphttp.ErrorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &errEnv); err != nil {
				t.Fatalf("failed decoding error envelope: %v", err)
			}
			if errEnv.Error.Code != domain.CodeForbiddenRole {
				t.Fatalf("expected code forbidden_role, got %s", errEnv.Error.Code)
			}
		})
	}
}

// 2. Acceptance Test: Manager can read templates and suppressions
func TestEmailHTTP_ManagerAccessAllowed(t *testing.T) {
	f := setupEmailMgmtTest(t)

	managerHeaders := map[string]string{
		"X-Dev-User-Id": "22222222-2222-2222-2222-222222222222",
		"X-Dev-Roles":   "manager",
	}

	// GET /email/templates
	req1 := httptest.NewRequest(http.MethodGet, "/email/templates", nil)
	for k, v := range managerHeaders {
		req1.Header.Set(k, v)
	}
	rec1 := httptest.NewRecorder()
	f.router.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 for manager reading templates, got %d", rec1.Code)
	}

	// GET /email/templates/auth.verify_email_otp
	req2 := httptest.NewRequest(http.MethodGet, "/email/templates/auth.verify_email_otp", nil)
	for k, v := range managerHeaders {
		req2.Header.Set(k, v)
	}
	rec2 := httptest.NewRecorder()
	f.router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 for manager reading template details, got %d", rec2.Code)
	}

	// GET /email/suppressions
	req3 := httptest.NewRequest(http.MethodGet, "/email/suppressions", nil)
	for k, v := range managerHeaders {
		req3.Header.Set(k, v)
	}
	rec3 := httptest.NewRecorder()
	f.router.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200 for manager reading suppressions, got %d", rec3.Code)
	}
}

// 3. Acceptance Test: Template versioning and undeclared variable rejected
func TestEmailHTTP_TemplateVersioningAndUndeclaredVariableRejected(t *testing.T) {
	f := setupEmailMgmtTest(t)

	adminHeaders := map[string]string{
		"X-Dev-User-Id": "99999999-9999-9999-9999-999999999999",
		"X-Dev-Roles":   "admin",
	}

	// 1. Undeclared variable -> 400 validation_failed
	invalidBody := `{
		"subject_template": "Order {{order_no}}",
		"html_template": "<p>Total: {{total}} for {{order_no}}</p>",
		"text_template": "Total: {{total}}",
		"declared_variables": ["order_no"]
	}` // "total" is undeclared!
	req1 := httptest.NewRequest(http.MethodPut, "/email/templates/order.confirmed", bytes.NewBufferString(invalidBody))
	req1.Header.Set("Content-Type", "application/json")
	for k, v := range adminHeaders {
		req1.Header.Set(k, v)
	}
	rec1 := httptest.NewRecorder()
	f.router.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for undeclared variable, got %d", rec1.Code)
	}
	var errEnv apphttp.ErrorEnvelope
	_ = json.Unmarshal(rec1.Body.Bytes(), &errEnv)
	if errEnv.Error.Code != domain.CodeValidationFailed {
		t.Fatalf("expected validation_failed, got %s", errEnv.Error.Code)
	}

	// 2. Valid version creation -> 201 Created
	validBody := `{
		"subject_template": "Order {{order_no}} - Updated",
		"html_template": "<p>Order {{order_no}} total: {{total}}</p>",
		"text_template": "Order {{order_no}} total: {{total}}",
		"declared_variables": ["order_no", "total"]
	}`
	req2 := httptest.NewRequest(http.MethodPut, "/email/templates/order.confirmed", bytes.NewBufferString(validBody))
	req2.Header.Set("Content-Type", "application/json")
	for k, v := range adminHeaders {
		req2.Header.Set(k, v)
	}
	rec2 := httptest.NewRecorder()
	f.router.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for valid template version, got %d, body: %s", rec2.Code, rec2.Body.String())
	}
}

// 4. Acceptance Test: Preview renders with escaping and does NOT send
func TestEmailHTTP_PreviewDoesNotSend(t *testing.T) {
	f := setupEmailMgmtTest(t)

	managerHeaders := map[string]string{
		"X-Dev-User-Id": "22222222-2222-2222-2222-222222222222",
		"X-Dev-Roles":   "manager",
	}

	body := `{"variables":{"name":"Bob <b>bold</b>","otp_code":"778899"}}`
	req := httptest.NewRequest(http.MethodPost, "/email/templates/auth.verify_email_otp/preview", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range managerHeaders {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for preview, got %d", rec.Code)
	}

	var resp struct {
		Data struct {
			Subject string `json:"subject"`
			HTML    string `json:"html"`
			Text    string `json:"text"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed decoding preview response: %v", err)
	}

	// Verify HTML escaping
	if !bytes.Contains([]byte(resp.Data.HTML), []byte("&lt;b&gt;bold&lt;/b&gt;")) {
		t.Errorf("expected HTML escaping in preview output, got: %s", resp.Data.HTML)
	}

	// CRITICAL SECURITY ASSERTION: Preview MUST NEVER dispatch emails
	if len(f.sender.sent) != 0 {
		t.Fatalf("expected 0 emails sent during preview, got %d", len(f.sender.sent))
	}
}

// 5. Acceptance Test: Resend refused for an OTP message
func TestEmailHTTP_ResendRefusedForOTPMessage(t *testing.T) {
	f := setupEmailMgmtTest(t)

	msgID := uuid.New()
	failedMsg := &domain.EmailMessage{
		ID:               msgID,
		TemplateKey:      "auth.verify_email_otp", // Declares sensitive_variables: ["otp_code"]
		TemplateVersion:  1,
		ToAddresses:      []string{"user@example.com"},
		Subject:          "Your OTP",
		BodyHTMLRedacted: "<p>Code: ******</p>",
		BodyTextRedacted: "Code: ******",
		Status:           domain.EmailMessageStatusFailed,
		Priority:         domain.EmailPriorityTransactional,
		CreatedAt:        f.clock.Now(),
	}
	_ = f.msgRepo.Create(context.Background(), failedMsg)

	adminHeaders := map[string]string{
		"X-Dev-User-Id": "99999999-9999-9999-9999-999999999999",
		"X-Dev-Roles":   "admin",
	}

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/email/messages/%s/resend", msgID), nil)
	for k, v := range adminHeaders {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict resending message with sensitive variables, got %d", rec.Code)
	}

	var errEnv apphttp.ErrorEnvelope
	_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
	if errEnv.Error.Code != domain.CodeInvalidStateTransition {
		t.Fatalf("expected code invalid_state_transition, got %s", errEnv.Error.Code)
	}
}

// 6. Acceptance Test: Webhook signature accepted and rejected
func TestEmailHTTP_WebhookSignatureVerification(t *testing.T) {
	f := setupEmailMgmtTest(t)

	rawPayload := []byte(`{"event":"delivered","email":"alex@example.com","provider_message_id":"msg-deliver-001"}`)

	// 1. Missing / invalid signature -> 401 unauthenticated
	req1 := httptest.NewRequest(http.MethodPost, "/webhooks/email/sendgrid", bytes.NewBuffer(rawPayload))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("X-Signature", "invalid-hex-sig")

	rec1 := httptest.NewRecorder()
	f.router.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 unauthenticated on bad signature, got %d", rec1.Code)
	}

	// 2. Correct HMAC-SHA256 signature -> 200 OK
	mac := hmac.New(sha256.New, []byte(f.webhookSecret))
	mac.Write(rawPayload)
	validSig := hex.EncodeToString(mac.Sum(nil))

	req2 := httptest.NewRequest(http.MethodPost, "/webhooks/email/sendgrid", bytes.NewBuffer(rawPayload))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Signature", validSig)

	rec2 := httptest.NewRecorder()
	f.router.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on valid signature, got %d, body: %s", rec2.Code, rec2.Body.String())
	}
}

// 7. Acceptance Test: Hard bounce creates a suppression and sets status bounced
func TestEmailHTTP_HardBounceCreatesSuppression(t *testing.T) {
	f := setupEmailMgmtTest(t)

	provMsgID := "prov-bounce-999"
	msg := &domain.EmailMessage{
		ID:                uuid.New(),
		ProviderMessageID: &provMsgID,
		Status:            domain.EmailMessageStatusSent,
		CreatedAt:         f.clock.Now(),
	}
	_ = f.msgRepo.Create(context.Background(), msg)

	bounceEmail := "hard.bounce@example.com"
	payload := []byte(fmt.Sprintf(`{
		"event": "bounce",
		"email": "%s",
		"provider_message_id": "%s",
		"hard": true,
		"reason": "550 No such user"
	}`, bounceEmail, provMsgID))

	mac := hmac.New(sha256.New, []byte(f.webhookSecret))
	mac.Write(payload)
	sig := hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/email/generic_adapter", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature", sig)

	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for hard bounce webhook, got %d", rec.Code)
	}

	// 1. Verify address added to suppression list
	supp, err := f.suppRepo.GetByEmail(context.Background(), bounceEmail)
	if err != nil || supp == nil {
		t.Fatalf("expected email %s to be suppressed after hard bounce", bounceEmail)
	}
	if supp.Reason != domain.EmailSuppressionReasonHardBounce {
		t.Fatalf("expected reason hard_bounce, got %s", supp.Reason)
	}

	// 2. Verify message status updated to bounced
	if msg.Status != domain.EmailMessageStatusBounced {
		t.Fatalf("expected message status bounced, got %s", msg.Status)
	}
}
