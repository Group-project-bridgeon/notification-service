package email_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/email"
)

// --- Mocks ---

type mockEmailSettingsRepo struct {
	settings *domain.EmailSettings
}

func (m *mockEmailSettingsRepo) Get(ctx context.Context) (*domain.EmailSettings, error) {
	if m.settings == nil {
		return nil, domain.NewNotFoundError("email settings not configured")
	}
	return m.settings, nil
}

func (m *mockEmailSettingsRepo) Upsert(ctx context.Context, s *domain.EmailSettings) error {
	m.settings = s
	return nil
}

type mockEmailTemplateRepo struct {
	templates map[string]*domain.EmailTemplate
}

func newMockEmailTemplateRepo() *mockEmailTemplateRepo {
	return &mockEmailTemplateRepo{
		templates: make(map[string]*domain.EmailTemplate),
	}
}

func (m *mockEmailTemplateRepo) List(ctx context.Context) ([]*domain.EmailTemplate, error) {
	var list []*domain.EmailTemplate
	for _, t := range m.templates {
		list = append(list, t)
	}
	return list, nil
}

func (m *mockEmailTemplateRepo) GetActive(ctx context.Context, templateKey string) (*domain.EmailTemplate, error) {
	t, ok := m.templates[templateKey]
	if !ok || !t.IsActive {
		return nil, domain.NewNotFoundError("active template not found")
	}
	return t, nil
}

func (m *mockEmailTemplateRepo) GetByVersion(ctx context.Context, templateKey string, version int) (*domain.EmailTemplate, error) {
	t, ok := m.templates[templateKey]
	if !ok || t.Version != version {
		return nil, domain.NewNotFoundError("template version not found")
	}
	return t, nil
}

func (m *mockEmailTemplateRepo) ListVersions(ctx context.Context, templateKey string) ([]*domain.EmailTemplate, error) {
	var versions []*domain.EmailTemplate
	for _, t := range m.templates {
		if t.TemplateKey == templateKey {
			versions = append(versions, t)
		}
	}
	return versions, nil
}

func (m *mockEmailTemplateRepo) CreateNewVersion(ctx context.Context, tmpl *domain.EmailTemplate) error {
	m.templates[tmpl.TemplateKey] = tmpl
	return nil
}

func (m *mockEmailTemplateRepo) Activate(ctx context.Context, templateKey string, version int) error {
	t, ok := m.templates[templateKey]
	if !ok {
		return domain.NewNotFoundError("template not found")
	}
	t.IsActive = true
	return nil
}

type mockEmailMessageRepo struct {
	messages map[uuid.UUID]*domain.EmailMessage
	clock    email.Clock
}

func newMockEmailMessageRepo(clock email.Clock) *mockEmailMessageRepo {
	return &mockEmailMessageRepo{
		messages: make(map[uuid.UUID]*domain.EmailMessage),
		clock:    clock,
	}
}

func (m *mockEmailMessageRepo) Create(ctx context.Context, msg *domain.EmailMessage) error {
	m.messages[msg.ID] = msg
	return nil
}

func (m *mockEmailMessageRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.EmailMessage, error) {
	msg, ok := m.messages[id]
	if !ok {
		return nil, domain.NewNotFoundError("message not found")
	}
	return msg, nil
}

func (m *mockEmailMessageRepo) GetByProviderMessageID(ctx context.Context, providerMsgID string) (*domain.EmailMessage, error) {
	for _, msg := range m.messages {
		if msg.ProviderMessageID != nil && *msg.ProviderMessageID == providerMsgID {
			return msg, nil
		}
	}
	return nil, domain.NewNotFoundError("message not found")
}

func (m *mockEmailMessageRepo) List(ctx context.Context, filter domain.EmailMessageFilter) ([]*domain.EmailMessage, int, error) {
	var list []*domain.EmailMessage
	for _, msg := range m.messages {
		list = append(list, msg)
	}
	return list, len(list), nil
}

func (m *mockEmailMessageRepo) ClaimDue(ctx context.Context, limit int) ([]*domain.EmailMessage, error) {
	var due []*domain.EmailMessage
	var now time.Time
	if m.clock != nil {
		now = m.clock.Now()
	} else {
		now = time.Now().UTC()
	}
	for _, msg := range m.messages {
		if msg.Status == domain.EmailMessageStatusQueued {
			if msg.NextAttemptAt == nil || !msg.NextAttemptAt.After(now) {
				if msg.ExpiresAt == nil || msg.ExpiresAt.After(now) {
					due = append(due, msg)
					if len(due) >= limit {
						break
					}
				}
			}
		}
	}
	return due, nil
}

func (m *mockEmailMessageRepo) UpdateStatus(
	ctx context.Context,
	id uuid.UUID,
	status domain.EmailMessageStatus,
	attempts int,
	nextAttemptAt, sentAt *time.Time,
	lastError, providerMessageID *string,
) error {
	msg, ok := m.messages[id]
	if !ok {
		return domain.NewNotFoundError("message not found")
	}
	msg.Status = status
	msg.Attempts = attempts
	if nextAttemptAt != nil {
		msg.NextAttemptAt = nextAttemptAt
	}
	if sentAt != nil {
		msg.SentAt = sentAt
	}
	if lastError != nil {
		msg.LastError = lastError
	}
	if providerMessageID != nil {
		msg.ProviderMessageID = providerMessageID
	}
	return nil
}

func (m *mockEmailMessageRepo) SetResendLink(ctx context.Context, id, resendOf uuid.UUID) error {
	msg, ok := m.messages[id]
	if !ok {
		return domain.NewNotFoundError("message not found")
	}
	msg.ResendOf = &resendOf
	return nil
}

func (m *mockEmailMessageRepo) CountSentToday(ctx context.Context) (int, error) {
	count := 0
	for _, msg := range m.messages {
		if msg.Status == domain.EmailMessageStatusSent || msg.Status == domain.EmailMessageStatusQueued {
			count++
		}
	}
	return count, nil
}

func (m *mockEmailMessageRepo) MarkLostOnRestart(ctx context.Context) (int64, error) {
	var count int64
	errStr := "lost_on_restart"
	for _, msg := range m.messages {
		if msg.Priority == domain.EmailPriorityTransactional &&
			(msg.Status == domain.EmailMessageStatusQueued || msg.Status == domain.EmailMessageStatusSending) {
			msg.Status = domain.EmailMessageStatusFailed
			msg.LastError = &errStr
			count++
		}
	}
	return count, nil
}

type mockSuppressionRepo struct {
	suppressions map[string]*domain.EmailSuppression
}

func newMockSuppressionRepo() *mockSuppressionRepo {
	return &mockSuppressionRepo{suppressions: make(map[string]*domain.EmailSuppression)}
}

func (m *mockSuppressionRepo) List(ctx context.Context) ([]*domain.EmailSuppression, error) {
	var list []*domain.EmailSuppression
	for _, s := range m.suppressions {
		list = append(list, s)
	}
	return list, nil
}

func (m *mockSuppressionRepo) GetByEmail(ctx context.Context, email string) (*domain.EmailSuppression, error) {
	s, ok := m.suppressions[email]
	if !ok {
		return nil, nil
	}
	return s, nil
}

func (m *mockSuppressionRepo) Add(ctx context.Context, supp *domain.EmailSuppression) error {
	m.suppressions[supp.Email] = supp
	return nil
}

func (m *mockSuppressionRepo) Delete(ctx context.Context, email string) error {
	delete(m.suppressions, email)
	return nil
}

type mockIdempotencyRepo struct {
	keys map[string]*domain.IdempotencyKey
}

func newMockIdempotencyRepo() *mockIdempotencyRepo {
	return &mockIdempotencyRepo{keys: make(map[string]*domain.IdempotencyKey)}
}

func (m *mockIdempotencyRepo) Get(ctx context.Context, key, scope string) (*domain.IdempotencyKey, error) {
	composite := scope + ":" + key
	item, ok := m.keys[composite]
	if !ok {
		return nil, domain.NewNotFoundError("key not found")
	}
	return item, nil
}

func (m *mockIdempotencyRepo) Create(ctx context.Context, item *domain.IdempotencyKey) error {
	composite := item.Scope + ":" + item.Key
	m.keys[composite] = item
	return nil
}

func (m *mockIdempotencyRepo) UpdateResponse(ctx context.Context, key, scope string, response []byte) error {
	composite := scope + ":" + key
	item, ok := m.keys[composite]
	if !ok {
		return domain.NewNotFoundError("key not found")
	}
	item.Response = response
	return nil
}

type mockSender struct {
	sentEmails []*email.OutboundEmail
	sendErr    error
}

func (m *mockSender) Send(_ context.Context, msg *email.OutboundEmail) (*email.SendResult, error) {
	if m.sendErr != nil {
		return nil, m.sendErr
	}
	m.sentEmails = append(m.sentEmails, msg)
	return &email.SendResult{ProviderMessageID: fmt.Sprintf("mock-%s", uuid.New())}, nil
}

// --- Test Fixtures ---

func setupTestEnvironment() (
	*email.Engine,
	*mockEmailSettingsRepo,
	*mockEmailTemplateRepo,
	*mockEmailMessageRepo,
	*mockSuppressionRepo,
	*mockIdempotencyRepo,
	*mockSender,
	*email.FakeClock,
) {
	clock := email.NewFakeClock(time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC))
	settingsRepo := &mockEmailSettingsRepo{
		settings: &domain.EmailSettings{
			ID:          1,
			Provider:    "smtp",
			Host:        "localhost",
			Port:        1025,
			FromName:    "Fashion ERP",
			FromAddress: "no-reply@fashionerp.com",
			DailyLimit:  500,
			Enabled:     true,
			TLSMode:     domain.TLSModeStartTLS,
		},
	}

	tmplRepo := newMockEmailTemplateRepo()
	tmplRepo.templates["auth.verify_email_otp"] = &domain.EmailTemplate{
		ID:                 uuid.New(),
		TemplateKey:        "auth.verify_email_otp",
		Version:            1,
		SubjectTemplate:    "Verify your email",
		HTMLTemplate:       "<p>Hello {{name}}, code: <strong>{{otp_code}}</strong></p>",
		TextTemplate:       "Hello {{name}}, code: {{otp_code}}",
		DeclaredVariables:  []string{"name", "otp_code"},
		SensitiveVariables: []string{"otp_code"},
		IsActive:           true,
	}

	tmplRepo.templates["order.confirmed"] = &domain.EmailTemplate{
		ID:                uuid.New(),
		TemplateKey:       "order.confirmed",
		Version:           1,
		SubjectTemplate:   "Order {{order_no}} Confirmed",
		HTMLTemplate:      "<p>Thank you for order {{order_no}}</p>",
		TextTemplate:      "Thank you for order {{order_no}}",
		DeclaredVariables: []string{"order_no"},
		IsActive:          true,
	}

	msgRepo := newMockEmailMessageRepo(clock)
	suppRepo := newMockSuppressionRepo()
	idempRepo := newMockIdempotencyRepo()
	sender := &mockSender{}

	key, _ := email.ParseEncryptionKey("MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
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
		nil,
	)

	return eng, settingsRepo, tmplRepo, msgRepo, suppRepo, idempRepo, sender, clock
}

// --- Test Cases ---

func TestEmail_Redaction_SensitiveVariablesNeverStoredInDB(t *testing.T) {
	eng, _, _, msgRepo, _, _, sender, _ := setupTestEnvironment()

	otpSecret := "847192"
	req := &email.SendEmailRequest{
		TemplateKey:        "auth.verify_email_otp",
		To:                 []string{"alex@example.com"},
		Variables:          map[string]any{"name": "Alex", "otp_code": otpSecret},
		SensitiveVariables: []string{"otp_code"},
		Priority:           "transactional",
	}

	res, err := eng.SendEmail(context.Background(), req, "idemp-redaction-1", nil)
	if err != nil {
		t.Fatalf("unexpected SendEmail error: %v", err)
	}

	eng.TxWorker().Wait()

	// 1. Verify unredacted code WAS passed to sender
	if len(sender.sentEmails) != 1 {
		t.Fatalf("expected 1 email passed to sender, got %d", len(sender.sentEmails))
	}
	sent := sender.sentEmails[0]
	if !strings.Contains(sent.HTMLBody, otpSecret) || !strings.Contains(sent.TextBody, otpSecret) {
		t.Errorf("expected sender to receive unredacted OTP in memory, HTML: %s", sent.HTMLBody)
	}

	// 2. CRITICAL SECURITY ASSERTION: Verify the OTP secret NEVER appears in the database row
	storedMsg, ok := msgRepo.messages[res.MessageID]
	if !ok {
		t.Fatalf("stored message not found in repo")
	}

	if strings.Contains(storedMsg.BodyHTMLRedacted, otpSecret) {
		t.Fatalf("SECURITY VIOLATION: plain OTP %q found in body_html_redacted: %s", otpSecret, storedMsg.BodyHTMLRedacted)
	}
	if strings.Contains(storedMsg.BodyTextRedacted, otpSecret) {
		t.Fatalf("SECURITY VIOLATION: plain OTP %q found in body_text_redacted: %s", otpSecret, storedMsg.BodyTextRedacted)
	}
	if strings.Contains(storedMsg.Subject, otpSecret) {
		t.Fatalf("SECURITY VIOLATION: plain OTP %q found in subject: %s", otpSecret, storedMsg.Subject)
	}

	// 3. Verify that the placeholder ****** was correctly applied
	if !strings.Contains(storedMsg.BodyHTMLRedacted, "******") {
		t.Errorf("expected ****** in body_html_redacted, got: %s", storedMsg.BodyHTMLRedacted)
	}
	if !strings.Contains(storedMsg.BodyTextRedacted, "******") {
		t.Errorf("expected ****** in body_text_redacted, got: %s", storedMsg.BodyTextRedacted)
	}
}

func TestEmail_LogSender_OutputsFullCodeToConsole(t *testing.T) {
	var buf bytes.Buffer
	logSender, err := email.NewLogSender("development", nil, &buf)
	if err != nil {
		t.Fatalf("failed creating LogSender: %v", err)
	}

	otpSecret := "938214"
	outbound := &email.OutboundEmail{
		FromName:    "Fashion ERP",
		FromAddress: "auth@fashionerp.com",
		To:          []string{"user@example.com"},
		Subject:     "Verify your email",
		HTMLBody:    fmt.Sprintf("<p>Your code is %s</p>", otpSecret),
		TextBody:    fmt.Sprintf("Your code is %s", otpSecret),
	}

	res, err := logSender.Send(context.Background(), outbound)
	if err != nil {
		t.Fatalf("LogSender.Send failed: %v", err)
	}
	if !strings.HasPrefix(res.ProviderMessageID, "log-") {
		t.Errorf("expected ProviderMessageID starting with log-, got %s", res.ProviderMessageID)
	}

	output := buf.String()
	if !strings.Contains(output, otpSecret) {
		t.Fatalf("expected LogSender output to display full code %s, got:\n%s", otpSecret, output)
	}
}

func TestEmail_LogSender_ForbiddenInProduction(t *testing.T) {
	_, err := email.NewLogSender("production", nil)
	if err == nil {
		t.Fatalf("expected LogSender creation to fail in production environment")
	}
}

func TestEmail_IdempotencyReplayAndConflict(t *testing.T) {
	eng, _, _, _, _, _, _, _ := setupTestEnvironment()

	req1 := &email.SendEmailRequest{
		TemplateKey: "order.confirmed",
		To:          []string{"customer@example.com"},
		Variables:   map[string]any{"order_no": "SO-1001"},
		Priority:    "business",
	}
	bodyBytes1, _ := json.Marshal(req1)

	// 1. First call: 202 accepted
	res1, err := eng.SendEmail(context.Background(), req1, "idemp-key-999", bodyBytes1)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	// 2. Replay with identical payload and key: returns identical message_id
	res2, err := eng.SendEmail(context.Background(), req1, "idemp-key-999", bodyBytes1)
	if err != nil {
		t.Fatalf("replay call failed: %v", err)
	}
	if res2.MessageID != res1.MessageID {
		t.Fatalf("expected same message_id %s, got %s", res1.MessageID, res2.MessageID)
	}

	// 3. Conflict with different payload and same key: 409 conflict
	reqDifferent := &email.SendEmailRequest{
		TemplateKey: "order.confirmed",
		To:          []string{"different@example.com"},
		Variables:   map[string]any{"order_no": "SO-9999"},
		Priority:    "business",
	}
	bodyBytesDiff, _ := json.Marshal(reqDifferent)

	_, errConflict := eng.SendEmail(context.Background(), reqDifferent, "idemp-key-999", bodyBytesDiff)
	if errConflict == nil {
		t.Fatalf("expected 409 conflict for different payload on same key")
	}
	var appErr *domain.AppError
	if !errors.As(errConflict, &appErr) || appErr.Code != domain.CodeIdempotencyConflict {
		t.Fatalf("expected CodeIdempotencyConflict, got %v", errConflict)
	}
}

func TestEmail_SuppressionHandling(t *testing.T) {
	eng, _, _, msgRepo, suppRepo, _, _, _ := setupTestEnvironment()

	// Add suppressions
	_ = suppRepo.Add(context.Background(), &domain.EmailSuppression{
		Email:  "hardbounce@example.com",
		Reason: domain.EmailSuppressionReasonHardBounce,
	})
	_ = suppRepo.Add(context.Background(), &domain.EmailSuppression{
		Email:  "complaint@example.com",
		Reason: domain.EmailSuppressionReasonComplaint,
	})

	// 1. Business mail to suppressed address is skipped, recorded as suppressed, returns 409
	bReq := &email.SendEmailRequest{
		TemplateKey: "order.confirmed",
		To:          []string{"complaint@example.com"},
		Variables:   map[string]any{"order_no": "SO-2001"},
		Priority:    "business",
	}
	_, bErr := eng.SendEmail(context.Background(), bReq, "idemp-supp-1", nil)
	if bErr == nil {
		t.Fatalf("expected 409 email_suppressed for business mail to complaint recipient")
	}
	var appErr *domain.AppError
	if !errors.As(bErr, &appErr) || appErr.Code != domain.CodeEmailSuppressed {
		t.Fatalf("expected CodeEmailSuppressed, got %v", bErr)
	}

	// Verify suppressed row created in DB
	foundSuppressed := false
	for _, m := range msgRepo.messages {
		if m.Status == domain.EmailMessageStatusSuppressed {
			foundSuppressed = true
			break
		}
	}
	if !foundSuppressed {
		t.Fatalf("expected suppressed email_messages row to be persisted for business mail")
	}

	// 2. Transactional email to hard bounce: rejected with 409
	txHardReq := &email.SendEmailRequest{
		TemplateKey: "auth.verify_email_otp",
		To:          []string{"hardbounce@example.com"},
		Variables:   map[string]any{"name": "Bob", "otp_code": "112233"},
		Priority:    "transactional",
	}
	_, txHardErr := eng.SendEmail(context.Background(), txHardReq, "idemp-supp-2", nil)
	if txHardErr == nil {
		t.Fatalf("expected 409 email_suppressed for transactional mail to hard bounce")
	}

	// 3. Transactional security mail to non-hardbounce (e.g. complaint): ATTEMPTED & SENT!
	txComplaintReq := &email.SendEmailRequest{
		TemplateKey: "auth.verify_email_otp",
		To:          []string{"complaint@example.com"},
		Variables:   map[string]any{"name": "Carol", "otp_code": "445566"},
		Priority:    "transactional",
	}
	resTx, txErr := eng.SendEmail(context.Background(), txComplaintReq, "idemp-supp-3", nil)
	if txErr != nil {
		t.Fatalf("expected transactional email to be attempted for complaint suppression, got: %v", txErr)
	}
	if resTx.Status != "queued" {
		t.Fatalf("expected status queued, got %s", resTx.Status)
	}
}

func TestEmail_AttachmentsNotSupported(t *testing.T) {
	eng, _, _, _, _, _, _, _ := setupTestEnvironment()

	req := &email.SendEmailRequest{
		TemplateKey: "order.confirmed",
		To:          []string{"customer@example.com"},
		Variables:   map[string]any{"order_no": "SO-1002"},
		Attachments: []any{map[string]string{"filename": "invoice.pdf"}},
	}

	_, err := eng.SendEmail(context.Background(), req, "idemp-attach", nil)
	if err == nil {
		t.Fatalf("expected validation_failed error for attachments")
	}
	var appErr *domain.AppError
	if !errors.As(err, &appErr) || appErr.Code != domain.CodeValidationFailed {
		t.Fatalf("expected CodeValidationFailed, got %v", err)
	}
	if len(appErr.Details) == 0 || appErr.Details[0].Issue != "attachments_not_supported" {
		t.Fatalf("expected issue attachments_not_supported, got %+v", appErr.Details)
	}
}

func TestEmail_UnknownVariableRejected(t *testing.T) {
	eng, _, _, _, _, _, _, _ := setupTestEnvironment()

	req := &email.SendEmailRequest{
		TemplateKey: "order.confirmed",
		To:          []string{"customer@example.com"},
		Variables:   map[string]any{"order_no": "SO-1003", "unrecognized_var": "evil_payload"},
	}

	_, err := eng.SendEmail(context.Background(), req, "idemp-unknown-var", nil)
	if err == nil {
		t.Fatalf("expected 400 email_template_invalid for unknown variable")
	}
	var appErr *domain.AppError
	if !errors.As(err, &appErr) || appErr.Code != domain.CodeEmailTemplateInvalid {
		t.Fatalf("expected CodeEmailTemplateInvalid, got %v", err)
	}
}

func TestEmail_DailyLimitExceeded(t *testing.T) {
	eng, settingsRepo, _, _, _, _, _, _ := setupTestEnvironment()
	settingsRepo.settings.DailyLimit = 0 // Limit already reached

	req := &email.SendEmailRequest{
		TemplateKey: "order.confirmed",
		To:          []string{"customer@example.com"},
		Variables:   map[string]any{"order_no": "SO-1004"},
	}

	_, err := eng.SendEmail(context.Background(), req, "idemp-limit", nil)
	if err == nil {
		t.Fatalf("expected 429 rate_limited when daily limit exceeded")
	}
	var appErr *domain.AppError
	if !errors.As(err, &appErr) || appErr.Code != domain.CodeRateLimited {
		t.Fatalf("expected CodeRateLimited, got %v", err)
	}
}

func TestEmail_EmailNotConfiguredOrDisabled(t *testing.T) {
	eng, settingsRepo, _, _, _, _, _, _ := setupTestEnvironment()
	settingsRepo.settings.Enabled = false

	req := &email.SendEmailRequest{
		TemplateKey: "order.confirmed",
		To:          []string{"customer@example.com"},
		Variables:   map[string]any{"order_no": "SO-1005"},
	}

	_, err := eng.SendEmail(context.Background(), req, "idemp-disabled", nil)
	if err == nil {
		t.Fatalf("expected 409 email_not_configured when email is disabled")
	}
	var appErr *domain.AppError
	if !errors.As(err, &appErr) || appErr.Code != domain.CodeEmailNotConfigured {
		t.Fatalf("expected CodeEmailNotConfigured, got %v", err)
	}
}

func TestEmail_BusinessWorker_RetrySchedule(t *testing.T) {
	_, settingsRepo, _, msgRepo, _, _, sender, clock := setupTestEnvironment()

	// Simulate sender failure
	sender.sendErr = errors.New("temporary SMTP connection timeout")

	worker := email.NewBusinessQueueWorker(settingsRepo, msgRepo, sender, clock, nil)

	msgID := uuid.New()
	msg := &domain.EmailMessage{
		ID:               msgID,
		TemplateKey:      "order.confirmed",
		ToAddresses:      []string{"customer@example.com"},
		Subject:          "Order Confirmed",
		BodyHTMLRedacted: "<p>Confirmed</p>",
		BodyTextRedacted: "Confirmed",
		Status:           domain.EmailMessageStatusQueued,
		Priority:         domain.EmailPriorityBusiness,
		Attempts:         0,
		CreatedAt:        clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), msg)

	// Attempt 1 fails -> delay 1m
	worker.ProcessBatch(context.Background(), 10)
	if msg.Attempts != 1 || msg.Status != domain.EmailMessageStatusQueued {
		t.Fatalf("expected attempts 1 and queued status, got %d, %s", msg.Attempts, msg.Status)
	}
	expectedNext := clock.Now().Add(1 * time.Minute)
	if !msg.NextAttemptAt.Equal(expectedNext) {
		t.Errorf("expected next attempt at +1m, got %v", msg.NextAttemptAt)
	}

	// Attempt 2 fails -> delay 5m
	clock.Advance(1 * time.Minute)
	worker.ProcessBatch(context.Background(), 10)
	if msg.Attempts != 2 {
		t.Fatalf("expected attempts 2, got %d", msg.Attempts)
	}

	// Attempt 3 fails -> delay 15m
	clock.Advance(5 * time.Minute)
	worker.ProcessBatch(context.Background(), 10)
	if msg.Attempts != 3 {
		t.Fatalf("expected attempts 3, got %d", msg.Attempts)
	}

	// Attempt 4 fails -> delay 1h
	clock.Advance(15 * time.Minute)
	worker.ProcessBatch(context.Background(), 10)
	if msg.Attempts != 4 {
		t.Fatalf("expected attempts 4, got %d", msg.Attempts)
	}

	// Attempt 5 fails -> delay 6h
	clock.Advance(1 * time.Hour)
	worker.ProcessBatch(context.Background(), 10)
	if msg.Attempts != 5 {
		t.Fatalf("expected attempts 5, got %d", msg.Attempts)
	}

	// After 5 attempts, next failure transitions to failed
	clock.Advance(6 * time.Hour)
	worker.ProcessBatch(context.Background(), 10)
	if msg.Status != domain.EmailMessageStatusFailed {
		t.Fatalf("expected status failed after exhausting retry schedule, got %s", msg.Status)
	}
}

func TestEmail_TransactionalWorker_InFlightRetriesAndExpiry(t *testing.T) {
	_, _, _, msgRepo, _, _, sender, clock := setupTestEnvironment()

	// Sender fails first 2 attempts then succeeds
	sender.sendErr = errors.New("transient network drop")

	txWorker := email.NewTransactionalWorker(msgRepo, sender, clock, nil)
	defer txWorker.Stop()

	msgID := uuid.New()
	msg := &domain.EmailMessage{
		ID:        msgID,
		Status:    domain.EmailMessageStatusQueued,
		Priority:  domain.EmailPriorityTransactional,
		CreatedAt: clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), msg)

	txWorker.Submit(&email.TransactionalJob{
		MessageID: msgID,
		OutboundEmail: &email.OutboundEmail{
			Subject:  "Your OTP",
			HTMLBody: "OTP 123",
			TextBody: "OTP 123",
		},
	})

	// 0s: 1st attempt fails
	time.Sleep(20 * time.Millisecond)
	if msg.Attempts != 1 {
		t.Fatalf("expected 1st attempt at 0s, got %d", msg.Attempts)
	}

	// 5s: 2nd attempt
	clock.Advance(5 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if msg.Attempts != 2 {
		t.Fatalf("expected 2nd attempt at 5s, got %d", msg.Attempts)
	}

	// Fix sender for 3rd attempt
	sender.sendErr = nil

	// 15s (20s total): 3rd attempt succeeds
	clock.Advance(15 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if msg.Status != domain.EmailMessageStatusSent {
		t.Fatalf("expected 3rd attempt to succeed and set status sent, got %s", msg.Status)
	}
}

func TestEmail_OnStartup_LostOnRestartMarkedFailed(t *testing.T) {
	eng, _, _, msgRepo, _, _, _, clock := setupTestEnvironment()

	// Create an in-flight transactional message that was queued before process crashed/restarted
	txMsg := &domain.EmailMessage{
		ID:        uuid.New(),
		Status:    domain.EmailMessageStatusQueued,
		Priority:  domain.EmailPriorityTransactional,
		CreatedAt: clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), txMsg)

	// Create a business message (should NOT be marked lost on restart)
	bizMsg := &domain.EmailMessage{
		ID:        uuid.New(),
		Status:    domain.EmailMessageStatusQueued,
		Priority:  domain.EmailPriorityBusiness,
		CreatedAt: clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), bizMsg)

	// Execute OnStartup sweep
	if err := eng.OnStartup(context.Background()); err != nil {
		t.Fatalf("OnStartup failed: %v", err)
	}

	// Transactional message MUST be marked failed with lost_on_restart
	if txMsg.Status != domain.EmailMessageStatusFailed {
		t.Fatalf("expected transactional message status failed, got %s", txMsg.Status)
	}
	if txMsg.LastError == nil || *txMsg.LastError != "lost_on_restart" {
		t.Fatalf("expected last_error lost_on_restart, got %v", txMsg.LastError)
	}

	// Business message MUST remain queued
	if bizMsg.Status != domain.EmailMessageStatusQueued {
		t.Fatalf("expected business message to remain queued, got %s", bizMsg.Status)
	}
}

func TestEmail_TemplateVersioningAndUndeclaredVariable(t *testing.T) {
	eng, _, _, _, _, _, _, _ := setupTestEnvironment()

	// 1. Attempt creating a version that uses an undeclared variable -> MUST be rejected with 400 validation_failed
	invalidReq := &email.CreateTemplateVersionRequest{
		SubjectTemplate:   "Welcome {{name}}",
		HTMLTemplate:      "<p>Your code is {{otp_code}}</p>",
		TextTemplate:      "Your code is {{otp_code}}",
		DeclaredVariables: []string{"name"}, // "otp_code" is NOT declared!
	}
	_, err := eng.CreateTemplateVersion(context.Background(), "auth.sample", invalidReq)
	if err == nil {
		t.Fatalf("expected error for undeclared variable, got nil")
	}
	var appErr *domain.AppError
	if !errors.As(err, &appErr) || appErr.Code != domain.CodeValidationFailed {
		t.Fatalf("expected validation_failed, got %v", err)
	}

	// 2. Creating a version with all declared variables succeeds
	validReq := &email.CreateTemplateVersionRequest{
		SubjectTemplate:   "Welcome {{name}}",
		HTMLTemplate:      "<p>Your code is {{otp_code}}</p>",
		TextTemplate:      "Your code is {{otp_code}}",
		DeclaredVariables: []string{"name", "otp_code"},
	}
	tmpl, err := eng.CreateTemplateVersion(context.Background(), "auth.sample", validReq)
	if err != nil {
		t.Fatalf("failed creating valid template version: %v", err)
	}
	if tmpl.TemplateKey != "auth.sample" {
		t.Fatalf("expected template_key auth.sample, got %s", tmpl.TemplateKey)
	}
}

func TestEmail_PreviewDoesNotSend(t *testing.T) {
	eng, _, _, _, _, _, sender, _ := setupTestEnvironment()

	prev, err := eng.PreviewTemplate(context.Background(), "auth.verify_email_otp", map[string]any{
		"name":     "Alice <script>alert(1)</script>",
		"otp_code": "123456",
	})
	if err != nil {
		t.Fatalf("unexpected error previewing template: %v", err)
	}

	// HTML must be escaped
	if strings.Contains(prev.HTMLBody, "<script>") {
		t.Errorf("expected HTML escaping in preview, got: %s", prev.HTMLBody)
	}
	if !strings.Contains(prev.HTMLBody, "&lt;script&gt;") {
		t.Errorf("expected escaped script tag in preview, got: %s", prev.HTMLBody)
	}

	// CRITICAL ASSERTION: Preview MUST NEVER dispatch email via sender
	if len(sender.sentEmails) != 0 {
		t.Fatalf("expected 0 emails sent during preview, got %d", len(sender.sentEmails))
	}
}

func TestEmail_ResendRefusedForOTPMessage(t *testing.T) {
	eng, _, _, msgRepo, _, _, _, clock := setupTestEnvironment()

	// 1. Message using a template with sensitive_variables ("otp_code")
	otpMsg := &domain.EmailMessage{
		ID:               uuid.New(),
		TemplateKey:      "auth.verify_email_otp",
		TemplateVersion:  1,
		ToAddresses:      []string{"user@example.com"},
		Subject:          "Your OTP",
		BodyHTMLRedacted: "<p>Code: ******</p>",
		BodyTextRedacted: "Code: ******",
		Status:           domain.EmailMessageStatusFailed,
		Priority:         domain.EmailPriorityTransactional,
		CreatedAt:        clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), otpMsg)

	// Resending must be REFUSED with 409 invalid_state_transition
	_, err := eng.ResendMessage(context.Background(), otpMsg.ID, nil, true)
	if err == nil {
		t.Fatalf("expected 409 error resending message with sensitive variables, got nil")
	}
	var appErr *domain.AppError
	if !errors.As(err, &appErr) || appErr.Code != domain.CodeInvalidStateTransition {
		t.Fatalf("expected CodeInvalidStateTransition, got %v", err)
	}
}

func TestEmail_ResendSuccessForNonSensitiveFailedMessage(t *testing.T) {
	eng, _, _, msgRepo, _, _, _, clock := setupTestEnvironment()

	// Order confirmed template has NO sensitive variables
	bizMsg := &domain.EmailMessage{
		ID:               uuid.New(),
		TemplateKey:      "order.confirmed",
		TemplateVersion:  1,
		ToAddresses:      []string{"customer@example.com"},
		Subject:          "Order Confirmed",
		BodyHTMLRedacted: "<p>Order confirmed</p>",
		BodyTextRedacted: "Order confirmed",
		Status:           domain.EmailMessageStatusFailed,
		Priority:         domain.EmailPriorityBusiness,
		CreatedAt:        clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), bizMsg)

	newMsg, err := eng.ResendMessage(context.Background(), bizMsg.ID, nil, true)
	if err != nil {
		t.Fatalf("unexpected error resending non-sensitive message: %v", err)
	}
	if newMsg.ResendOf == nil || *newMsg.ResendOf != bizMsg.ID {
		t.Fatalf("expected new message resend_of to be linked to original, got %v", newMsg.ResendOf)
	}
	if newMsg.Status != domain.EmailMessageStatusQueued {
		t.Fatalf("expected new message status queued, got %s", newMsg.Status)
	}
}

func TestEmail_CancelQueuedOnly(t *testing.T) {
	eng, _, _, msgRepo, _, _, _, clock := setupTestEnvironment()

	// 1. Queued message can be cancelled
	queuedMsg := &domain.EmailMessage{
		ID:        uuid.New(),
		Status:    domain.EmailMessageStatusQueued,
		CreatedAt: clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), queuedMsg)

	if err := eng.CancelMessage(context.Background(), queuedMsg.ID, nil, true); err != nil {
		t.Fatalf("failed cancelling queued message: %v", err)
	}
	if queuedMsg.Status != domain.EmailMessageStatusCancelled {
		t.Fatalf("expected status cancelled, got %s", queuedMsg.Status)
	}

	// 2. Sent message cannot be cancelled -> 409 invalid_state_transition
	sentMsg := &domain.EmailMessage{
		ID:        uuid.New(),
		Status:    domain.EmailMessageStatusSent,
		CreatedAt: clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), sentMsg)

	err := eng.CancelMessage(context.Background(), sentMsg.ID, nil, true)
	if err == nil {
		t.Fatalf("expected error cancelling sent message, got nil")
	}
	var appErr *domain.AppError
	if !errors.As(err, &appErr) || appErr.Code != domain.CodeInvalidStateTransition {
		t.Fatalf("expected CodeInvalidStateTransition, got %v", err)
	}
}

func TestEmail_WebhookSignatureAndBounces(t *testing.T) {
	eng, _, _, msgRepo, suppRepo, _, _, clock := setupTestEnvironment()
	webhookSecret := "test-webhook-secret"

	provMsgID := "prov-msg-12345"
	msg := &domain.EmailMessage{
		ID:                uuid.New(),
		ProviderMessageID: &provMsgID,
		Status:            domain.EmailMessageStatusSent,
		CreatedAt:         clock.Now(),
	}
	_ = msgRepo.Create(context.Background(), msg)

	rawBody := []byte(`{"event":"bounce","email":"bad@example.com","provider_message_id":"prov-msg-12345","hard":true,"reason":"550 mailbox unavailable"}`)

	// 1. Invalid signature -> 401 unauthenticated
	badSigErr := eng.HandleWebhook(context.Background(), "sendgrid", rawBody, "invalid-sig", webhookSecret, nil)
	if badSigErr == nil {
		t.Fatalf("expected error for invalid signature, got nil")
	}
	var appErr *domain.AppError
	if !errors.As(badSigErr, &appErr) || appErr.Code != domain.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated, got %v", badSigErr)
	}

	// 2. Valid signature computation
	mac := hmac.New(sha256.New, []byte(webhookSecret))
	mac.Write(rawBody)
	validSig := hex.EncodeToString(mac.Sum(nil))

	var payload email.WebhookPayload
	_ = json.Unmarshal(rawBody, &payload)

	err := eng.HandleWebhook(context.Background(), "sendgrid", rawBody, validSig, webhookSecret, &payload)
	if err != nil {
		t.Fatalf("unexpected webhook error: %v", err)
	}

	// 3. Hard bounce MUST add address to suppressions
	supp, err := suppRepo.GetByEmail(context.Background(), "bad@example.com")
	if err != nil || supp == nil {
		t.Fatalf("expected address in suppression repo after hard bounce")
	}
	if supp.Reason != domain.EmailSuppressionReasonHardBounce {
		t.Fatalf("expected suppression reason hard_bounce, got %s", supp.Reason)
	}

	// 4. Message status MUST be set to bounced
	if msg.Status != domain.EmailMessageStatusBounced {
		t.Fatalf("expected message status bounced, got %s", msg.Status)
	}
}

