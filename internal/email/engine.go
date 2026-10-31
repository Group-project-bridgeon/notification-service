package email

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

// SendEmailRequest models the payload for POST /internal/email/send.
type SendEmailRequest struct {
	TemplateKey        string            `json:"template_key"`
	To                 []string          `json:"to"`
	Cc                 []string          `json:"cc"`
	Variables          map[string]any    `json:"variables"`
	SensitiveVariables []string          `json:"sensitive_variables"`
	Related            *domain.EntityRef `json:"related,omitempty"`
	Priority           string            `json:"priority"` // transactional | business
	ExpiresAt          *time.Time        `json:"expires_at,omitempty"`
	Attachments        []any             `json:"attachments,omitempty"`
}

// SendEmailResponse carries the message ID and queuing status.
type SendEmailResponse struct {
	MessageID uuid.UUID `json:"message_id"`
	Status    string    `json:"status"`
}

// EmailSettingsResponse exposes settings without revealing the secret password.
type EmailSettingsResponse struct {
	ID          int              `json:"id"`
	Provider    string           `json:"provider"`
	Host        string           `json:"host"`
	Port        int              `json:"port"`
	Username    string           `json:"username"`
	PasswordSet bool             `json:"password_set"`
	FromName    string           `json:"from_name"`
	FromAddress string           `json:"from_address"`
	ReplyTo     *string          `json:"reply_to,omitempty"`
	TLSMode     domain.TLSMode   `json:"tls_mode"`
	DailyLimit  int              `json:"daily_limit"`
	Enabled     bool             `json:"enabled"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// UpdateSettingsRequest models updating email provider configuration.
type UpdateSettingsRequest struct {
	Provider    string          `json:"provider"`
	Host        string          `json:"host"`
	Port        int             `json:"port"`
	Username    string          `json:"username"`
	Password    *string         `json:"password,omitempty"`
	FromName    string          `json:"from_name"`
	FromAddress string          `json:"from_address"`
	ReplyTo     *string         `json:"reply_to,omitempty"`
	TLSMode     *domain.TLSMode `json:"tls_mode,omitempty"`
	DailyLimit  *int            `json:"daily_limit,omitempty"`
	Enabled     *bool           `json:"enabled,omitempty"`
	UpdatedBy   *uuid.UUID      `json:"updated_by,omitempty"`
}

// CreateTemplateVersionRequest models payload for PUT /email/templates/{key}.
type CreateTemplateVersionRequest struct {
	SubjectTemplate    string     `json:"subject_template"`
	HTMLTemplate       string     `json:"html_template"`
	TextTemplate       string     `json:"text_template"`
	DeclaredVariables  []string   `json:"declared_variables"`
	SensitiveVariables []string   `json:"sensitive_variables"`
	CreatedBy          *uuid.UUID `json:"-"`
}

// TemplateVersionSummary models version metadata for template details.
type TemplateVersionSummary struct {
	Version   int       `json:"version"`
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
}

// TemplateDetailsResponse models GET /email/templates/{key}.
type TemplateDetailsResponse struct {
	TemplateKey        string                   `json:"template_key"`
	Version            int                      `json:"version"`
	SubjectTemplate    string                   `json:"subject_template"`
	HTMLTemplate       string                   `json:"html_template"`
	TextTemplate       string                   `json:"text_template"`
	DeclaredVariables  []string                 `json:"declared_variables"`
	SensitiveVariables []string                 `json:"sensitive_variables"`
	IsActive           bool                     `json:"is_active"`
	Versions           []TemplateVersionSummary `json:"versions"`
}

// WebhookPayload models POST /webhooks/email/{provider}.
type WebhookPayload struct {
	Event             string `json:"event"` // "bounce", "complaint", "delivered"
	Email             string `json:"email"`
	ProviderMessageID string `json:"provider_message_id"`
	Reason            string `json:"reason,omitempty"`
	Hard              bool   `json:"hard,omitempty"`
}

// Engine coordinates email template rendering, redaction, queue persistence, and dispatch.
type Engine struct {
	settingsRepo   repository.EmailSettingsRepository
	templateRepo   repository.EmailTemplateRepository
	messageRepo    repository.EmailMessageRepository
	suppressionRepo repository.SuppressionRepository
	idempotencyRepo repository.IdempotencyKeyRepository
	sender         EmailSender
	txWorker       *TransactionalWorker
	queueWorker    *BusinessQueueWorker
	encryptionKey  []byte
	clock          Clock
	logger         *slog.Logger
}

// NewEngine constructs the primary email engine.
func NewEngine(
	settingsRepo repository.EmailSettingsRepository,
	templateRepo repository.EmailTemplateRepository,
	messageRepo repository.EmailMessageRepository,
	suppressionRepo repository.SuppressionRepository,
	idempotencyRepo repository.IdempotencyKeyRepository,
	sender EmailSender,
	txWorker *TransactionalWorker,
	queueWorker *BusinessQueueWorker,
	encryptionKey []byte,
	clock Clock,
	logger *slog.Logger,
) *Engine {
	if clock == nil {
		clock = NewRealClock()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		settingsRepo:    settingsRepo,
		templateRepo:    templateRepo,
		messageRepo:     messageRepo,
		suppressionRepo: suppressionRepo,
		idempotencyRepo: idempotencyRepo,
		sender:          sender,
		txWorker:        txWorker,
		queueWorker:     queueWorker,
		encryptionKey:   encryptionKey,
		clock:           clock,
		logger:          logger,
	}
}

// OnStartup runs initialization routines including failing in-flight transactional emails lost across restarts.
func (e *Engine) OnStartup(ctx context.Context) error {
	count, err := e.messageRepo.MarkLostOnRestart(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "failed executing on-startup sweep of lost transactional emails", "error", err)
		return err
	}
	if count > 0 {
		e.logger.InfoContext(ctx, "marked in-flight transactional emails lost on restart", "count", count)
	}
	return nil
}

// Enqueue implements the EmailQueue interface from Prompt 5.
// Routing rules with send_email = true invoke this method to queue business emails.
func (e *Engine) Enqueue(ctx context.Context, q QueuedEmail) error {
	req := &SendEmailRequest{
		TemplateKey: q.TemplateKey,
		To:          q.ToAddresses,
		Variables:   q.Variables,
		Priority:    string(q.Priority),
		Related: func() *domain.EntityRef {
			if q.RelatedType != nil && q.RelatedID != nil {
				return &domain.EntityRef{Type: *q.RelatedType, ID: *q.RelatedID}
			}
			return nil
		}(),
	}

	idempotencyKey := fmt.Sprintf("rule_%s_%s", q.RecipientID, uuid.New())
	rawBody, _ := json.Marshal(req)
	_, err := e.SendEmail(ctx, req, idempotencyKey, rawBody)
	return err
}

// SendEmail processes POST /internal/email/send with all constraints:
// 1. Validation (attachments not supported -> 400).
// 2. Idempotency (replay or conflict).
// 3. Configuration check (enabled & configured -> 409 email_not_configured).
// 4. Suppressions (409 email_suppressed when nothing can be sent).
// 5. Daily limit check (429 rate_limited).
// 6. Template rendering & variable validation (unknown variable -> 400 email_template_invalid).
// 7. Redaction (sensitive values replaced by ****** in DB, never stored).
// 8. Queue persistence & dispatch.
func (e *Engine) SendEmail(
	ctx context.Context,
	req *SendEmailRequest,
	idempotencyKey string,
	rawBody []byte,
) (*SendEmailResponse, error) {
	// 1. Validate payload schema
	if req.TemplateKey == "" {
		return nil, domain.NewValidationError("template_key is required", domain.ErrorDetail{Field: "template_key", Issue: "required"})
	}
	if len(req.To) == 0 {
		return nil, domain.NewValidationError("to is required", domain.ErrorDetail{Field: "to", Issue: "required"})
	}
	for _, addr := range req.To {
		if addr == "" {
			return nil, domain.NewValidationError("recipient address cannot be empty", domain.ErrorDetail{Field: "to", Issue: "invalid_email"})
		}
	}
	if len(req.Attachments) > 0 {
		return nil, domain.NewValidationError("attachments are not supported", domain.ErrorDetail{
			Field: "attachments",
			Issue: "attachments_not_supported",
		})
	}

	priority := req.Priority
	if priority == "" {
		priority = "business"
	}
	if priority != "transactional" && priority != "business" {
		priority = "business"
	}

	// 2. Idempotency check
	var reqHash string
	if len(rawBody) > 0 {
		h := sha256.Sum256(rawBody)
		reqHash = hex.EncodeToString(h[:])
	} else {
		b, _ := json.Marshal(req)
		h := sha256.Sum256(b)
		reqHash = hex.EncodeToString(h[:])
	}

	if idempotencyKey != "" {
		existing, err := e.idempotencyRepo.Get(ctx, idempotencyKey, "email_send")
		if err == nil && existing != nil {
			if existing.RequestHash == reqHash {
				var cachedResp struct {
					Data SendEmailResponse `json:"data"`
				}
				if err := json.Unmarshal(existing.Response, &cachedResp); err == nil {
					return &cachedResp.Data, nil
				}
			}
			return nil, domain.NewAppError(domain.CodeIdempotencyConflict, "idempotency key already used with different payload")
		}
	}

	// 3. Email configured & enabled check
	settings, err := e.settingsRepo.Get(ctx)
	if err != nil || settings == nil || !settings.Enabled {
		return nil, domain.NewAppError(domain.CodeEmailNotConfigured, "email service is not configured or disabled")
	}

	// 4. Suppressions check
	allowedRecipients := make([]string, 0, len(req.To))
	for _, addr := range req.To {
		supp, _ := e.suppressionRepo.GetByEmail(ctx, addr)
		if supp != nil {
			if priority == "transactional" {
				// Transactional security mail is still attempted unless the address is a hard bounce
				if supp.Reason == domain.EmailSuppressionReasonHardBounce {
					continue
				}
				allowedRecipients = append(allowedRecipients, addr)
			} else {
				// Business mail is skipped
				continue
			}
		} else {
			allowedRecipients = append(allowedRecipients, addr)
		}
	}

	if len(allowedRecipients) == 0 {
		// If business mail, record row as suppressed
		if priority == "business" {
			suppressedMsg := &domain.EmailMessage{
				ID:                 uuid.New(),
				TemplateKey:        req.TemplateKey,
				ToAddresses:        req.To,
				CcAddresses:        req.Cc,
				Subject:            fmt.Sprintf("[%s] Suppressed", req.TemplateKey),
				BodyHTMLRedacted:   "<p>[SUPPRESSED]</p>",
				BodyTextRedacted:   "[SUPPRESSED]",
				Status:             domain.EmailMessageStatusSuppressed,
				Priority:           domain.EmailPriority(priority),
				CreatedAt:          e.clock.Now(),
			}
			_ = e.messageRepo.Create(ctx, suppressedMsg)
		}
		return nil, domain.NewAppError(domain.CodeEmailSuppressed, "all recipient email addresses are suppressed")
	}

	// 5. Daily limit check
	countToday, err := e.messageRepo.CountSentToday(ctx)
	if err == nil && countToday >= settings.DailyLimit {
		return nil, domain.NewAppError(domain.CodeRateLimited, "daily email dispatch limit exceeded")
	}

	// 6. Template rendering & variable validation
	tmpl, err := e.templateRepo.GetActive(ctx, req.TemplateKey)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) || domain.IsNotFound(err) {
			return nil, domain.NewAppError(domain.CodeEmailTemplateInvalid, fmt.Sprintf("active email template '%s' not found", req.TemplateKey))
		}
		return nil, domain.NewAppError(domain.CodeEmailTemplateInvalid, fmt.Sprintf("failed loading active template: %v", err))
	}

	renderRes, err := RenderEmailTemplate(tmpl, req.Variables, req.SensitiveVariables)
	if err != nil {
		return nil, err
	}

	// 7. Persist email_messages record
	messageID := uuid.New()
	msg := &domain.EmailMessage{
		ID:                 messageID,
		TemplateKey:        tmpl.TemplateKey,
		TemplateVersion:    tmpl.Version,
		ToAddresses:        allowedRecipients,
		CcAddresses:        req.Cc,
		Subject:            renderRes.Subject,
		BodyHTMLRedacted:   renderRes.HTMLBodyRedacted,
		BodyTextRedacted:   renderRes.TextBodyRedacted,
		Status:             domain.EmailMessageStatusQueued,
		Priority:           domain.EmailPriority(priority),
		ExpiresAt:          req.ExpiresAt,
		CreatedAt:          e.clock.Now(),
	}

	if req.Related != nil {
		relType := req.Related.Type
		relID := req.Related.ID
		msg.RelatedType = &relType
		msg.RelatedID = &relID
	}

	if err := e.messageRepo.Create(ctx, msg); err != nil {
		return nil, fmt.Errorf("failed persisting email message: %w", err)
	}

	// 8. Dispatch based on priority
	outbound := &OutboundEmail{
		FromName:    settings.FromName,
		FromAddress: settings.FromAddress,
		To:          allowedRecipients,
		Cc:          req.Cc,
		ReplyTo:     settings.ReplyTo,
		Subject:     renderRes.Subject,
		HTMLBody:    renderRes.HTMLBody, // Unredacted in memory for transmission
		TextBody:    renderRes.TextBody, // Unredacted in memory for transmission
	}

	if priority == "transactional" && e.txWorker != nil {
		e.txWorker.Submit(&TransactionalJob{
			MessageID:     messageID,
			OutboundEmail: outbound,
			ExpiresAt:     req.ExpiresAt,
		})
	}

	resp := &SendEmailResponse{
		MessageID: messageID,
		Status:    "queued",
	}

	// 9. Store Idempotency key
	if idempotencyKey != "" {
		respJSON, _ := json.Marshal(ginH{"data": resp})
		_ = e.idempotencyRepo.Create(ctx, &domain.IdempotencyKey{
			Key:         idempotencyKey,
			Scope:       "email_send",
			RequestHash: reqHash,
			Response:    respJSON,
			CreatedAt:   e.clock.Now(),
		})
	}

	return resp, nil
}

type ginH map[string]any

// GetSettings retrieves current email settings with password_set indicator.
func (e *Engine) GetSettings(ctx context.Context) (*EmailSettingsResponse, error) {
	s, err := e.settingsRepo.Get(ctx)
	if err != nil {
		return nil, err
	}

	return &EmailSettingsResponse{
		ID:          s.ID,
		Provider:    s.Provider,
		Host:        s.Host,
		Port:        s.Port,
		Username:    s.Username,
		PasswordSet: len(s.PasswordEncrypted) > 0,
		FromName:    s.FromName,
		FromAddress: s.FromAddress,
		ReplyTo:     s.ReplyTo,
		TLSMode:     s.TLSMode,
		DailyLimit:  s.DailyLimit,
		Enabled:     s.Enabled,
		UpdatedAt:   s.UpdatedAt,
	}, nil
}

// UpdateSettings updates email configuration, encrypting password when provided.
func (e *Engine) UpdateSettings(ctx context.Context, req *UpdateSettingsRequest) error {
	existing, _ := e.settingsRepo.Get(ctx)

	s := &domain.EmailSettings{
		ID:        1,
		Provider:  req.Provider,
		Host:      req.Host,
		Port:      req.Port,
		Username:  req.Username,
		FromName:  req.FromName,
		FromAddress: req.FromAddress,
		ReplyTo:   req.ReplyTo,
		UpdatedAt: e.clock.Now(),
	}

	if req.TLSMode != nil {
		s.TLSMode = *req.TLSMode
	} else if existing != nil {
		s.TLSMode = existing.TLSMode
	} else {
		s.TLSMode = domain.TLSModeStartTLS
	}

	if req.DailyLimit != nil {
		s.DailyLimit = *req.DailyLimit
	} else if existing != nil {
		s.DailyLimit = existing.DailyLimit
	} else {
		s.DailyLimit = 10000
	}

	if req.Enabled != nil {
		s.Enabled = *req.Enabled
	} else if existing != nil {
		s.Enabled = existing.Enabled
	} else {
		s.Enabled = true
	}

	s.UpdatedBy = req.UpdatedBy

	if req.Password != nil && *req.Password != "" {
		enc, err := EncryptPassword(*req.Password, e.encryptionKey)
		if err != nil {
			return fmt.Errorf("failed encrypting SMTP password: %w", err)
		}
		s.PasswordEncrypted = enc
	} else if existing != nil {
		s.PasswordEncrypted = existing.PasswordEncrypted
	}

	return e.settingsRepo.Upsert(ctx, s)
}

// TestSettings sends a real test email using the currently saved email settings.
func (e *Engine) TestSettings(ctx context.Context, to string) (*SendResult, error) {
	if to == "" {
		return nil, domain.NewValidationError("to address is required", domain.ErrorDetail{Field: "to", Issue: "required"})
	}

	settings, err := e.settingsRepo.Get(ctx)
	if err != nil || settings == nil || !settings.Enabled {
		return nil, domain.NewAppError(domain.CodeEmailNotConfigured, "email service is not configured or disabled")
	}

	outbound := &OutboundEmail{
		FromName:    settings.FromName,
		FromAddress: settings.FromAddress,
		To:          []string{to},
		Subject:     "Test Email - ERP Notification Service",
		HTMLBody:    "<p>This is a test email confirming that your email configuration is active and functional.</p>",
		TextBody:    "This is a test email confirming that your email configuration is active and functional.",
	}

	if e.sender == nil {
		return nil, domain.NewAppError(domain.CodeEmailNotConfigured, "email sender is not configured")
	}

	res, err := e.sender.Send(ctx, outbound)
	if err != nil {
		return nil, domain.NewAppError(domain.CodeEmailSendFailed, fmt.Sprintf("failed sending test email: %v", err))
	}

	return res, nil
}

// ListTemplates returns all email templates.
func (e *Engine) ListTemplates(ctx context.Context) ([]*domain.EmailTemplate, error) {
	return e.templateRepo.List(ctx)
}

// GetTemplateDetails retrieves template details and version history.
func (e *Engine) GetTemplateDetails(ctx context.Context, key string) (*TemplateDetailsResponse, error) {
	tmpl, err := e.templateRepo.GetActive(ctx, key)
	if err != nil {
		versions, lErr := e.templateRepo.ListVersions(ctx, key)
		if lErr != nil || len(versions) == 0 {
			return nil, domain.NewNotFoundError(fmt.Sprintf("email template '%s' not found", key))
		}
		tmpl = versions[0]
	}

	allVersions, err := e.templateRepo.ListVersions(ctx, key)
	if err != nil {
		return nil, err
	}

	versionSummaries := make([]TemplateVersionSummary, 0, len(allVersions))
	for _, v := range allVersions {
		versionSummaries = append(versionSummaries, TemplateVersionSummary{
			Version:   v.Version,
			IsActive:  v.IsActive,
			CreatedAt: v.CreatedAt,
		})
	}

	return &TemplateDetailsResponse{
		TemplateKey:        tmpl.TemplateKey,
		Version:            tmpl.Version,
		SubjectTemplate:    tmpl.SubjectTemplate,
		HTMLTemplate:       tmpl.HTMLTemplate,
		TextTemplate:       tmpl.TextTemplate,
		DeclaredVariables:  tmpl.DeclaredVariables,
		SensitiveVariables: tmpl.SensitiveVariables,
		IsActive:           tmpl.IsActive,
		Versions:           versionSummaries,
	}, nil
}

// CreateTemplateVersion creates an incremented version of an email template with undeclared variable validation.
func (e *Engine) CreateTemplateVersion(ctx context.Context, key string, req *CreateTemplateVersionRequest) (*domain.EmailTemplate, error) {
	if key == "" {
		return nil, domain.NewValidationError("template key is required", domain.ErrorDetail{Field: "key", Issue: "required"})
	}

	// Validate that every {{variable}} used in subject, html, and text is declared
	if err := ValidateTemplateVariables(req.DeclaredVariables, req.SubjectTemplate, req.HTMLTemplate, req.TextTemplate); err != nil {
		return nil, err
	}

	if req.DeclaredVariables == nil {
		req.DeclaredVariables = []string{}
	}
	if req.SensitiveVariables == nil {
		req.SensitiveVariables = []string{}
	}

	tmpl := &domain.EmailTemplate{
		ID:                 uuid.New(),
		TemplateKey:        key,
		Version:            0, // Auto-increment in repository
		SubjectTemplate:    req.SubjectTemplate,
		HTMLTemplate:       req.HTMLTemplate,
		TextTemplate:       req.TextTemplate,
		DeclaredVariables:  req.DeclaredVariables,
		SensitiveVariables: req.SensitiveVariables,
		IsActive:           false,
		CreatedBy:          req.CreatedBy,
		CreatedAt:          e.clock.Now(),
	}

	if err := e.templateRepo.CreateNewVersion(ctx, tmpl); err != nil {
		return nil, err
	}

	return tmpl, nil
}

// PreviewTemplate renders a preview with variables and escaping without sending.
func (e *Engine) PreviewTemplate(ctx context.Context, key string, variables map[string]any) (*RenderResult, error) {
	tmpl, err := e.templateRepo.GetActive(ctx, key)
	if err != nil {
		versions, lErr := e.templateRepo.ListVersions(ctx, key)
		if lErr != nil || len(versions) == 0 {
			return nil, domain.NewNotFoundError(fmt.Sprintf("email template '%s' not found", key))
		}
		tmpl = versions[0]
	}

	return RenderEmailTemplate(tmpl, variables, nil)
}

// ActivateTemplate promotes a specific version of a template to active.
func (e *Engine) ActivateTemplate(ctx context.Context, key string, version int) error {
	if version <= 0 {
		return domain.NewValidationError("version must be greater than 0", domain.ErrorDetail{Field: "version", Issue: "invalid"})
	}
	return e.templateRepo.Activate(ctx, key, version)
}

// ListMessages returns paginated email message records matching filter criteria.
func (e *Engine) ListMessages(ctx context.Context, filter domain.EmailMessageFilter) ([]*domain.EmailMessage, int, error) {
	return e.messageRepo.List(ctx, filter)
}

// GetMessage retrieves a specific email message, enforcing sent_by ownership for non-admin callers.
func (e *Engine) GetMessage(ctx context.Context, id uuid.UUID, callerID *uuid.UUID, isAdmin bool) (*domain.EmailMessage, error) {
	msg, err := e.messageRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if !isAdmin {
		if msg.SentBy == nil || callerID == nil || *msg.SentBy != *callerID {
			return nil, domain.NewNotFoundError("email message not found")
		}
	}

	return msg, nil
}

// ResendMessage re-queues a failed or bounced email message, refusing messages containing sensitive variables.
func (e *Engine) ResendMessage(ctx context.Context, id uuid.UUID, callerID *uuid.UUID, isAdmin bool) (*domain.EmailMessage, error) {
	msg, err := e.messageRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if !isAdmin {
		if msg.SentBy == nil || callerID == nil || *msg.SentBy != *callerID {
			return nil, domain.NewNotFoundError("email message not found")
		}
	}

	// Only status failed or bounced may be resent
	if msg.Status != domain.EmailMessageStatusFailed && msg.Status != domain.EmailMessageStatusBounced {
		return nil, domain.NewAppError(domain.CodeInvalidStateTransition, fmt.Sprintf("cannot resend email with status '%s' (only failed or bounced permitted)", msg.Status))
	}

	// Refuse with 409 invalid_state_transition when the original had sensitive variables or is an OTP
	if strings.Contains(strings.ToLower(msg.TemplateKey), "otp") ||
		strings.Contains(msg.BodyHTMLRedacted, "[REDACTED]") ||
		strings.Contains(msg.BodyTextRedacted, "[REDACTED]") {
		return nil, domain.NewAppError(domain.CodeInvalidStateTransition, "cannot resend email containing sensitive variables (OTP/tokens cannot be reconstructed)")
	}

	tmpl, _ := e.templateRepo.GetByVersion(ctx, msg.TemplateKey, msg.TemplateVersion)
	if tmpl != nil && len(tmpl.SensitiveVariables) > 0 {
		return nil, domain.NewAppError(domain.CodeInvalidStateTransition, "cannot resend email containing sensitive variables (OTP/tokens cannot be reconstructed)")
	}

	newID := uuid.New()
	newMsg := &domain.EmailMessage{
		ID:               newID,
		TemplateKey:      msg.TemplateKey,
		TemplateVersion:  msg.TemplateVersion,
		ToAddresses:      msg.ToAddresses,
		CcAddresses:      msg.CcAddresses,
		Subject:          msg.Subject,
		BodyHTMLRedacted: msg.BodyHTMLRedacted,
		BodyTextRedacted: msg.BodyTextRedacted,
		Status:           domain.EmailMessageStatusQueued,
		Priority:         msg.Priority,
		RelatedType:      msg.RelatedType,
		RelatedID:        msg.RelatedID,
		ResendOf:         &msg.ID,
		SentBy:           callerID,
		CreatedAt:        e.clock.Now(),
	}

	if err := e.messageRepo.Create(ctx, newMsg); err != nil {
		return nil, fmt.Errorf("failed creating resend message: %w", err)
	}

	_ = e.messageRepo.SetResendLink(ctx, msg.ID, newID)

	// If transactional and txWorker present, dispatch in-memory job
	if newMsg.Priority == domain.EmailPriorityTransactional && e.txWorker != nil {
		settings, _ := e.settingsRepo.Get(ctx)
		fromName := ""
		fromAddr := ""
		if settings != nil {
			fromName = settings.FromName
			fromAddr = settings.FromAddress
		}
		e.txWorker.Submit(&TransactionalJob{
			MessageID: newID,
			OutboundEmail: &OutboundEmail{
				FromName:    fromName,
				FromAddress: fromAddr,
				To:          newMsg.ToAddresses,
				Cc:          newMsg.CcAddresses,
				Subject:     newMsg.Subject,
				HTMLBody:    newMsg.BodyHTMLRedacted,
				TextBody:    newMsg.BodyTextRedacted,
			},
		})
	}

	return newMsg, nil
}

// CancelMessage cancels delivery for an email message currently in queued state.
func (e *Engine) CancelMessage(ctx context.Context, id uuid.UUID, callerID *uuid.UUID, isAdmin bool) error {
	msg, err := e.messageRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if !isAdmin {
		if msg.SentBy == nil || callerID == nil || *msg.SentBy != *callerID {
			return domain.NewNotFoundError("email message not found")
		}
	}

	if msg.Status != domain.EmailMessageStatusQueued {
		return domain.NewAppError(domain.CodeInvalidStateTransition, fmt.Sprintf("cannot cancel email with status '%s' (only queued permitted)", msg.Status))
	}

	reason := "cancelled_by_user"
	return e.messageRepo.UpdateStatus(ctx, id, domain.EmailMessageStatusCancelled, msg.Attempts, nil, nil, &reason, nil)
}

// ListSuppressions returns all suppressed email recipients.
func (e *Engine) ListSuppressions(ctx context.Context) ([]*domain.EmailSuppression, error) {
	return e.suppressionRepo.List(ctx)
}

// AddSuppression adds an address to the email suppression list.
func (e *Engine) AddSuppression(ctx context.Context, email string, reason domain.EmailSuppressionReason, createdBy *uuid.UUID) error {
	if email == "" {
		return domain.NewValidationError("email address is required", domain.ErrorDetail{Field: "email", Issue: "required"})
	}
	if reason == "" {
		reason = domain.EmailSuppressionReasonManual
	}

	supp := &domain.EmailSuppression{
		Email:     email,
		Reason:    reason,
		CreatedBy: createdBy,
		CreatedAt: e.clock.Now(),
	}

	return e.suppressionRepo.Add(ctx, supp)
}

// DeleteSuppression removes an address from the suppression list.
func (e *Engine) DeleteSuppression(ctx context.Context, email string) error {
	if email == "" {
		return domain.NewValidationError("email address is required", domain.ErrorDetail{Field: "email", Issue: "required"})
	}
	return e.suppressionRepo.Delete(ctx, email)
}

// HandleWebhook verifies HMAC-SHA256 signature and applies bounce, complaint, or delivery status updates.
func (e *Engine) HandleWebhook(
	ctx context.Context,
	provider string,
	rawBody []byte,
	signatureHeader string,
	webhookSecret string,
	payload *WebhookPayload,
) error {
	if signatureHeader == "" {
		return domain.NewAppError(domain.CodeUnauthenticated, "missing signature header")
	}

	// Verify HMAC-SHA256 signature over the raw body
	mac := hmac.New(sha256.New, []byte(webhookSecret))
	mac.Write(rawBody)
	expectedMAC := hex.EncodeToString(mac.Sum(nil))

	sigClean := strings.TrimPrefix(strings.ToLower(signatureHeader), "sha256=")
	if !hmac.Equal([]byte(sigClean), []byte(strings.ToLower(expectedMAC))) {
		return domain.NewAppError(domain.CodeUnauthenticated, "invalid webhook signature")
	}

	if payload == nil {
		return domain.NewValidationError("webhook payload is required", domain.ErrorDetail{Field: "body", Issue: "empty"})
	}

	switch strings.ToLower(payload.Event) {
	case "bounce":
		if payload.Hard {
			// Hard bounce: add address to email_suppressions
			if payload.Email != "" {
				_ = e.suppressionRepo.Add(ctx, &domain.EmailSuppression{
					Email:     payload.Email,
					Reason:    domain.EmailSuppressionReasonHardBounce,
					CreatedAt: e.clock.Now(),
				})
			}
		}
		if payload.ProviderMessageID != "" {
			if msg, err := e.messageRepo.GetByProviderMessageID(ctx, payload.ProviderMessageID); err == nil && msg != nil {
				errText := payload.Reason
				if errText == "" {
					if payload.Hard {
						errText = "hard_bounce"
					} else {
						errText = "soft_bounce"
					}
				}
				_ = e.messageRepo.UpdateStatus(ctx, msg.ID, domain.EmailMessageStatusBounced, msg.Attempts, nil, nil, &errText, &payload.ProviderMessageID)
			}
		}

	case "complaint":
		// Spam complaint: add address to email_suppressions and set message bounced
		if payload.Email != "" {
			_ = e.suppressionRepo.Add(ctx, &domain.EmailSuppression{
				Email:     payload.Email,
				Reason:    domain.EmailSuppressionReasonComplaint,
				CreatedAt: e.clock.Now(),
			})
		}
		if payload.ProviderMessageID != "" {
			if msg, err := e.messageRepo.GetByProviderMessageID(ctx, payload.ProviderMessageID); err == nil && msg != nil {
				errText := "spam_complaint"
				if payload.Reason != "" {
					errText = payload.Reason
				}
				_ = e.messageRepo.UpdateStatus(ctx, msg.ID, domain.EmailMessageStatusBounced, msg.Attempts, nil, nil, &errText, &payload.ProviderMessageID)
			}
		}

	case "delivered":
		if payload.ProviderMessageID != "" {
			if msg, err := e.messageRepo.GetByProviderMessageID(ctx, payload.ProviderMessageID); err == nil && msg != nil {
				now := e.clock.Now()
				_ = e.messageRepo.UpdateStatus(ctx, msg.ID, domain.EmailMessageStatusDelivered, msg.Attempts, nil, &now, nil, &payload.ProviderMessageID)
			}
		}
	}

	return nil
}

// TxWorker returns the transactional worker.
func (e *Engine) TxWorker() *TransactionalWorker {
	return e.txWorker
}

// QueueWorker returns the business queue worker.
func (e *Engine) QueueWorker() *BusinessQueueWorker {
	return e.queueWorker
}
