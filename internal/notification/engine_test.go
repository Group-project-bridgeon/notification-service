package notification

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/email"
	"notification-service/internal/ingest"
	"notification-service/internal/repository"
)

// --- Mock Implementations ---

type mockRuleRepo struct {
	rules []*domain.NotificationRule
	err   error
}

func (m *mockRuleRepo) GetActiveByEventType(ctx context.Context, eventType string) ([]*domain.NotificationRule, error) {
	if m.err != nil {
		return nil, m.err
	}
	var matched []*domain.NotificationRule
	for _, r := range m.rules {
		if r.EventType == eventType && r.IsActive {
			matched = append(matched, r)
		}
	}
	return matched, nil
}
func (m *mockRuleRepo) List(ctx context.Context) ([]*domain.NotificationRule, error) {
	return m.rules, m.err
}
func (m *mockRuleRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.NotificationRule, error) {
	return nil, nil
}
func (m *mockRuleRepo) Create(ctx context.Context, rule *domain.NotificationRule) error { return nil }
func (m *mockRuleRepo) Update(ctx context.Context, rule *domain.NotificationRule) error { return nil }
func (m *mockRuleRepo) Delete(ctx context.Context, id uuid.UUID) error                  { return nil }
func (m *mockRuleRepo) Deactivate(ctx context.Context, id uuid.UUID) error              { return nil }

type mockTemplateRepo struct {
	templates map[string]*domain.NotificationTemplate
	err       error
}

func (m *mockTemplateRepo) GetActive(ctx context.Context, templateKey, locale string) (*domain.NotificationTemplate, error) {
	if m.err != nil {
		return nil, m.err
	}
	if tmpl, ok := m.templates[templateKey]; ok {
		return tmpl, nil
	}
	return nil, domain.NewNotFoundError("template not found")
}
func (m *mockTemplateRepo) List(ctx context.Context) ([]*domain.NotificationTemplate, error) {
	return nil, nil
}
func (m *mockTemplateRepo) Create(ctx context.Context, tmpl *domain.NotificationTemplate) error {
	return nil
}
func (m *mockTemplateRepo) CreateNewVersion(ctx context.Context, tmpl *domain.NotificationTemplate) error {
	return nil
}

type mockRecipientRepo struct {
	recipients []*domain.Recipient
}

func (m *mockRecipientRepo) UpsertGuarded(ctx context.Context, recipient *domain.Recipient) error {
	return nil
}
func (m *mockRecipientRepo) FindByRoles(ctx context.Context, roles []string) ([]*domain.Recipient, error) {
	var result []*domain.Recipient
	roleSet := make(map[string]bool)
	for _, r := range roles {
		roleSet[r] = true
	}

	for _, rec := range m.recipients {
		for _, userRole := range rec.Roles {
			if roleSet[userRole] {
				result = append(result, rec)
				break
			}
		}
	}
	return result, nil
}
func (m *mockRecipientRepo) FindByID(ctx context.Context, id uuid.UUID) (*domain.Recipient, error) {
	for _, rec := range m.recipients {
		if rec.UserID == id {
			return rec, nil
		}
	}
	return nil, domain.NewNotFoundError("recipient not found")
}

type mockPreferenceRepo struct {
	preferences map[uuid.UUID][]*domain.NotificationPreference
}

func (m *mockPreferenceRepo) GetByUser(ctx context.Context, userID uuid.UUID) ([]*domain.NotificationPreference, error) {
	if prefs, ok := m.preferences[userID]; ok {
		return prefs, nil
	}
	return nil, nil
}
func (m *mockPreferenceRepo) Upsert(ctx context.Context, pref *domain.NotificationPreference) error {
	return nil
}

type mockNotificationRepo struct {
	insertedNotifications []*domain.Notification
	unreadCounts          int
	unreadByCategory      map[string]int
}

func (m *mockNotificationRepo) BatchInsert(ctx context.Context, notifications []*domain.Notification) error {
	m.insertedNotifications = append(m.insertedNotifications, notifications...)
	return nil
}
func (m *mockNotificationRepo) List(ctx context.Context, filter domain.NotificationFilter) ([]*domain.Notification, int, error) {
	return nil, 0, nil
}
func (m *mockNotificationRepo) GetByID(ctx context.Context, recipientID, id uuid.UUID) (*domain.Notification, error) {
	return nil, nil
}
func (m *mockNotificationRepo) SetStatus(ctx context.Context, recipientID, id uuid.UUID, status domain.NotificationStatus) (*domain.Notification, error) {
	return nil, nil
}
func (m *mockNotificationRepo) ReadAll(ctx context.Context, recipientID uuid.UUID, category *domain.NotificationCategory) (int64, error) {
	return 0, nil
}
func (m *mockNotificationRepo) GetUnreadCounts(ctx context.Context, recipientID uuid.UUID) (int, map[string]int, error) {
	return m.unreadCounts, m.unreadByCategory, nil
}

type mockDeliveryRepo struct {
	insertedDeliveries []*domain.NotificationDelivery
}

func (m *mockDeliveryRepo) Create(ctx context.Context, delivery *domain.NotificationDelivery) error {
	m.insertedDeliveries = append(m.insertedDeliveries, delivery)
	return nil
}
func (m *mockDeliveryRepo) ClaimDue(ctx context.Context, limit int) ([]*domain.NotificationDelivery, error) {
	return nil, nil
}
func (m *mockDeliveryRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.DeliveryStatus, attempts int, nextRetryAt *time.Time, lastError *string) error {
	return nil
}
func (m *mockDeliveryRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.NotificationDelivery, error) {
	return nil, nil
}
func (m *mockDeliveryRepo) List(ctx context.Context, filter domain.DeliveryFilter) ([]*domain.NotificationDelivery, int, error) {
	return nil, 0, nil
}

type mockEmailQueue struct {
	enqueued []email.QueuedEmail
}

func (m *mockEmailQueue) Enqueue(ctx context.Context, msg email.QueuedEmail) error {
	m.enqueued = append(m.enqueued, msg)
	return nil
}

type mockPublisher struct {
	published []*domain.Notification
	fail      bool
}

func (m *mockPublisher) PublishNotification(ctx context.Context, n *domain.Notification, counts UnreadCounts) error {
	if m.fail {
		return errors.New("simulated publisher network failure")
	}
	m.published = append(m.published, n)
	return nil
}

// Helper context runner with post-commit hook execution
type testTxContext struct{}

func runWithTestTx(fn func(ctx context.Context) error) error {
	ctx := repository.WithNewPostCommitHooks(context.Background())
	if err := fn(ctx); err != nil {
		return err
	}
	repository.ExecutePostCommitHooks(ctx, context.Background())
	return nil
}

// --- Tests ---

func TestEngine_RoleFanOutAndRecipientDeduplication(t *testing.T) {
	managerID := uuid.New()
	financeID := uuid.New()
	actorID := uuid.New()

	recipients := []*domain.Recipient{
		{UserID: managerID, Email: "manager@example.com", DisplayName: "Manager", Roles: []string{"manager"}, IsActive: true},
		{UserID: financeID, Email: "finance@example.com", DisplayName: "Finance", Roles: []string{"finance"}, IsActive: true},
	}

	rules := []*domain.NotificationRule{
		{
			ID:          uuid.New(),
			EventType:   "sales_order.held_for_credit_approval",
			TargetRoles: []string{"manager", "finance"},
			Type:        domain.TypeApprovalPending,
			Category:    domain.CategoryApproval,
			Priority:    domain.PriorityHigh,
			TemplateKey: "tmpl_so_held_credit",
			IsActive:    true,
		},
	}

	templates := map[string]*domain.NotificationTemplate{
		"tmpl_so_held_credit": {
			TemplateKey:   "tmpl_so_held_credit",
			TitleTemplate: "Credit approval needed",
			BodyTemplate:  "Order {{payload.order_no}} is on hold.",
			Version:       1,
			IsActive:      true,
		},
	}

	ruleRepo := &mockRuleRepo{rules: rules}
	templateRepo := &mockTemplateRepo{templates: templates}
	recipientRepo := &mockRecipientRepo{recipients: recipients}
	preferenceRepo := &mockPreferenceRepo{}
	notificationRepo := &mockNotificationRepo{unreadCounts: 1}
	deliveryRepo := &mockDeliveryRepo{}
	pub := &mockPublisher{}

	engine := NewEngine(ruleRepo, templateRepo, recipientRepo, preferenceRepo, notificationRepo, deliveryRepo, nil, pub, false, nil)

	env := &domain.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     "sales_order.held_for_credit_approval",
		EventVersion:  "1.0",
		OccurredAt:    time.Now().UTC(),
		Producer:      "sales-service",
		AggregateType: "sales_order",
		AggregateID:   uuid.New().String(),
		ActorUserID:   &actorID,
		Payload:       json.RawMessage(`{"order_no":"SO-999"}`),
	}

	err := runWithTestTx(func(ctx context.Context) error {
		return engine.Route(ctx, env)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should create 2 notifications (manager & finance)
	if len(notificationRepo.insertedNotifications) != 2 {
		t.Fatalf("expected 2 notifications, got %d", len(notificationRepo.insertedNotifications))
	}
	if len(deliveryRepo.insertedDeliveries) != 2 {
		t.Fatalf("expected 2 in_app deliveries, got %d", len(deliveryRepo.insertedDeliveries))
	}
	for _, d := range deliveryRepo.insertedDeliveries {
		if d.Status != domain.DeliveryStatusSent {
			t.Errorf("expected delivery status 'sent', got %s", d.Status)
		}
		if d.Channel != domain.ChannelInApp {
			t.Errorf("expected channel 'in_app', got %s", d.Channel)
		}
	}
	if len(pub.published) != 2 {
		t.Fatalf("expected 2 published notifications via SSE hook, got %d", len(pub.published))
	}
}

func TestEngine_ActorExclusion(t *testing.T) {
	actorID := uuid.New()
	colleagueID := uuid.New()

	recipients := []*domain.Recipient{
		{UserID: actorID, Email: "actor@example.com", Roles: []string{"warehouse"}, IsActive: true},
		{UserID: colleagueID, Email: "colleague@example.com", Roles: []string{"warehouse"}, IsActive: true},
	}

	templates := map[string]*domain.NotificationTemplate{
		"tmpl_so_confirmed": {
			TemplateKey:   "tmpl_so_confirmed",
			TitleTemplate: "Order confirmed",
			BodyTemplate:  "Order confirmed",
			Version:       1,
			IsActive:      true,
		},
	}

	t.Run("Actor excluded on non-security category", func(t *testing.T) {
		rules := []*domain.NotificationRule{
			{
				ID:          uuid.New(),
				EventType:   "sales_order.confirmed",
				TargetRoles: []string{"warehouse"},
				Type:        domain.TypeOrderConfirmed,
				Category:    domain.CategorySales, // non-security
				Priority:    domain.PriorityNormal,
				TemplateKey: "tmpl_so_confirmed",
				IsActive:    true,
			},
		}

		notifRepo := &mockNotificationRepo{}
		delivRepo := &mockDeliveryRepo{}
		engine := NewEngine(&mockRuleRepo{rules: rules}, &mockTemplateRepo{templates: templates}, &mockRecipientRepo{recipients: recipients}, &mockPreferenceRepo{}, notifRepo, delivRepo, nil, nil, false, nil)

		env := &domain.EventEnvelope{
			EventID:       uuid.New(),
			EventType:     "sales_order.confirmed",
			AggregateID:   uuid.New().String(),
			ActorUserID:   &actorID,
			Payload:       json.RawMessage(`{}`),
		}

		err := runWithTestTx(func(ctx context.Context) error {
			return engine.Route(ctx, env)
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Only colleague receives notification, actor is excluded
		if len(notifRepo.insertedNotifications) != 1 {
			t.Fatalf("expected 1 notification, got %d", len(notifRepo.insertedNotifications))
		}
		if notifRepo.insertedNotifications[0].RecipientID != colleagueID {
			t.Fatalf("expected recipient %s, got %s", colleagueID, notifRepo.insertedNotifications[0].RecipientID)
		}
	})

	t.Run("Actor NOT excluded on security category", func(t *testing.T) {
		secTemplates := map[string]*domain.NotificationTemplate{
			"tmpl_user_locked": {
				TemplateKey:   "tmpl_user_locked",
				TitleTemplate: "Locked",
				BodyTemplate:  "Your account was locked",
				Version:       1,
				IsActive:      true,
			},
		}
		rules := []*domain.NotificationRule{
			{
				ID:             uuid.New(),
				EventType:      "user.locked",
				TargetRoles:    []string{"warehouse"},
				Type:           domain.TypeSecurityAlert,
				Category:       domain.CategorySecurity, // security category!
				Priority:       domain.PriorityHigh,
				TemplateKey:    "tmpl_user_locked",
				IsActive:       true,
			},
		}

		notifRepo := &mockNotificationRepo{}
		delivRepo := &mockDeliveryRepo{}
		engine := NewEngine(&mockRuleRepo{rules: rules}, &mockTemplateRepo{templates: secTemplates}, &mockRecipientRepo{recipients: recipients}, &mockPreferenceRepo{}, notifRepo, delivRepo, nil, nil, false, nil)

		env := &domain.EventEnvelope{
			EventID:       uuid.New(),
			EventType:     "user.locked",
			AggregateID:   uuid.New().String(),
			ActorUserID:   &actorID,
			Payload:       json.RawMessage(`{}`),
		}

		err := runWithTestTx(func(ctx context.Context) error {
			return engine.Route(ctx, env)
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Both recipients, including actor, must receive security notification
		if len(notifRepo.insertedNotifications) != 2 {
			t.Fatalf("expected 2 notifications on security event, got %d", len(notifRepo.insertedNotifications))
		}
	})
}

func TestEngine_PreferencesAndOverrides(t *testing.T) {
	userID := uuid.New()
	recipients := []*domain.Recipient{
		{UserID: userID, Email: "user@example.com", Roles: []string{"purchasing"}, IsActive: true},
	}

	templates := map[string]*domain.NotificationTemplate{
		"tmpl_test": {
			TemplateKey:   "tmpl_test",
			TitleTemplate: "Test Title",
			BodyTemplate:  "Test Body",
			Version:       1,
			IsActive:      true,
		},
	}

	t.Run("Preference skips disabled in_app channel", func(t *testing.T) {
		rules := []*domain.NotificationRule{
			{
				ID:          uuid.New(),
				EventType:   "sales_order.confirmed",
				TargetRoles: []string{"purchasing"},
				Type:        domain.TypeOrderConfirmed,
				Category:    domain.CategorySales,
				Priority:    domain.PriorityNormal,
				TemplateKey: "tmpl_test",
				IsActive:    true,
			},
		}

		prefs := map[uuid.UUID][]*domain.NotificationPreference{
			userID: {
				{UserID: userID, Category: domain.CategorySales, Channel: domain.ChannelInApp, Enabled: false},
			},
		}

		notifRepo := &mockNotificationRepo{}
		delivRepo := &mockDeliveryRepo{}
		engine := NewEngine(&mockRuleRepo{rules: rules}, &mockTemplateRepo{templates: templates}, &mockRecipientRepo{recipients: recipients}, &mockPreferenceRepo{preferences: prefs}, notifRepo, delivRepo, nil, nil, false, nil)

		env := &domain.EventEnvelope{
			EventID:     uuid.New(),
			EventType:   "sales_order.confirmed",
			AggregateID: uuid.New().String(),
		}

		err := runWithTestTx(func(ctx context.Context) error {
			return engine.Route(ctx, env)
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Plan skipped entirely since user disabled in_app and email is not enabled
		if len(notifRepo.insertedNotifications) != 0 {
			t.Fatalf("expected 0 notifications when channel is disabled, got %d", len(notifRepo.insertedNotifications))
		}
	})

	t.Run("Critical priority overrides disabled preferences", func(t *testing.T) {
		rules := []*domain.NotificationRule{
			{
				ID:          uuid.New(),
				EventType:   "goods_receipt.failed",
				TargetRoles: []string{"purchasing"},
				Type:        domain.TypeOrderFailed,
				Category:    domain.CategorySaga,
				Priority:    domain.PriorityCritical, // CRITICAL
				TemplateKey: "tmpl_test",
				IsActive:    true,
			},
		}

		prefs := map[uuid.UUID][]*domain.NotificationPreference{
			userID: {
				{UserID: userID, Category: domain.CategorySaga, Channel: domain.ChannelInApp, Enabled: false},
			},
		}

		notifRepo := &mockNotificationRepo{}
		delivRepo := &mockDeliveryRepo{}
		engine := NewEngine(&mockRuleRepo{rules: rules}, &mockTemplateRepo{templates: templates}, &mockRecipientRepo{recipients: recipients}, &mockPreferenceRepo{preferences: prefs}, notifRepo, delivRepo, nil, nil, false, nil)

		env := &domain.EventEnvelope{
			EventID:     uuid.New(),
			EventType:   "goods_receipt.failed",
			AggregateID: uuid.New().String(),
		}

		err := runWithTestTx(func(ctx context.Context) error {
			return engine.Route(ctx, env)
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Critical priority cannot be disabled!
		if len(notifRepo.insertedNotifications) != 1 {
			t.Fatalf("expected 1 notification for critical priority despite disabled pref, got %d", len(notifRepo.insertedNotifications))
		}
		if len(delivRepo.insertedDeliveries) != 1 {
			t.Fatalf("expected 1 delivery for critical priority, got %d", len(delivRepo.insertedDeliveries))
		}
	})

	t.Run("Approval in_app cannot be disabled", func(t *testing.T) {
		rules := []*domain.NotificationRule{
			{
				ID:          uuid.New(),
				EventType:   "purchase_order.approval_required",
				TargetRoles: []string{"purchasing"},
				Type:        domain.TypeApprovalPending,
				Category:    domain.CategoryApproval, // APPROVAL category
				Priority:    domain.PriorityNormal,
				TemplateKey: "tmpl_test",
				IsActive:    true,
			},
		}

		prefs := map[uuid.UUID][]*domain.NotificationPreference{
			userID: {
				{UserID: userID, Category: domain.CategoryApproval, Channel: domain.ChannelInApp, Enabled: false},
			},
		}

		notifRepo := &mockNotificationRepo{}
		delivRepo := &mockDeliveryRepo{}
		engine := NewEngine(&mockRuleRepo{rules: rules}, &mockTemplateRepo{templates: templates}, &mockRecipientRepo{recipients: recipients}, &mockPreferenceRepo{preferences: prefs}, notifRepo, delivRepo, nil, nil, false, nil)

		env := &domain.EventEnvelope{
			EventID:     uuid.New(),
			EventType:   "purchase_order.approval_required",
			AggregateID: uuid.New().String(),
		}

		err := runWithTestTx(func(ctx context.Context) error {
			return engine.Route(ctx, env)
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Approval in-app is locked and cannot be disabled
		if len(notifRepo.insertedNotifications) != 1 {
			t.Fatalf("expected 1 notification for approval category, got %d", len(notifRepo.insertedNotifications))
		}
	})
}

func TestEngine_NoRuleMatched(t *testing.T) {
	ruleRepo := &mockRuleRepo{rules: nil}
	engine := NewEngine(ruleRepo, &mockTemplateRepo{}, &mockRecipientRepo{}, &mockPreferenceRepo{}, &mockNotificationRepo{}, &mockDeliveryRepo{}, nil, nil, false, nil)

	env := &domain.EventEnvelope{
		EventID:     uuid.New(),
		EventType:   "unknown.event.type",
		AggregateID: uuid.New().String(),
	}

	err := runWithTestTx(func(ctx context.Context) error {
		return engine.Route(ctx, env)
	})

	if !errors.Is(err, ingest.ErrNoRuleMatched) {
		t.Fatalf("expected ErrNoRuleMatched, got %v", err)
	}
}

func TestEngine_PublishFailureDoesNotFailEvent(t *testing.T) {
	userID := uuid.New()
	recipients := []*domain.Recipient{
		{UserID: userID, Email: "user@example.com", Roles: []string{"admin"}, IsActive: true},
	}
	rules := []*domain.NotificationRule{
		{
			ID:          uuid.New(),
			EventType:   "user.registered",
			TargetRoles: []string{"admin"},
			Type:        domain.TypeSystem,
			Category:    domain.CategoryAccount,
			Priority:    domain.PriorityNormal,
			TemplateKey: "tmpl_user_registered",
			IsActive:    true,
		},
	}
	templates := map[string]*domain.NotificationTemplate{
		"tmpl_user_registered": {
			TemplateKey:   "tmpl_user_registered",
			TitleTemplate: "New user registered",
			BodyTemplate:  "User registered",
			Version:       1,
			IsActive:      true,
		},
	}

	pub := &mockPublisher{fail: true} // Publisher fails
	notifRepo := &mockNotificationRepo{}
	delivRepo := &mockDeliveryRepo{}

	engine := NewEngine(&mockRuleRepo{rules: rules}, &mockTemplateRepo{templates: templates}, &mockRecipientRepo{recipients: recipients}, &mockPreferenceRepo{}, notifRepo, delivRepo, nil, pub, false, nil)

	env := &domain.EventEnvelope{
		EventID:     uuid.New(),
		EventType:   "user.registered",
		AggregateID: uuid.New().String(),
	}

	err := runWithTestTx(func(ctx context.Context) error {
		return engine.Route(ctx, env)
	})

	// Post-commit publisher failure must NEVER fail the event ingestion
	if err != nil {
		t.Fatalf("expected no error despite publisher failure, got: %v", err)
	}
	if len(notifRepo.insertedNotifications) != 1 {
		t.Fatalf("expected 1 notification inserted, got %d", len(notifRepo.insertedNotifications))
	}
}

func TestEngine_TargetUserPathAndDeduplicationAcrossRules(t *testing.T) {
	targetUID := uuid.New()
	userPath := "payload.requester_user_id"

	recipients := []*domain.Recipient{
		{UserID: targetUID, Email: "requester@example.com", Roles: []string{"employee"}, IsActive: true},
	}

	// Two rules that both match the target user
	rules := []*domain.NotificationRule{
		{
			ID:             uuid.New(),
			EventType:      "purchase_request.approved",
			TargetRoles:    []string{"employee"},
			TargetUserPath: &userPath,
			Type:           domain.TypeApprovalDecided,
			Category:       domain.CategoryApproval,
			Priority:       domain.PriorityNormal,
			TemplateKey:    "tmpl_pr_approved",
			IsActive:       true,
		},
		{
			ID:             uuid.New(),
			EventType:      "purchase_request.approved",
			TargetRoles:    []string{},
			TargetUserPath: &userPath,
			Type:           domain.TypeApprovalDecided,
			Category:       domain.CategoryApproval,
			Priority:       domain.PriorityNormal,
			TemplateKey:    "tmpl_pr_approved",
			IsActive:       true,
		},
	}

	templates := map[string]*domain.NotificationTemplate{
		"tmpl_pr_approved": {
			TemplateKey:   "tmpl_pr_approved",
			TitleTemplate: "PR Approved",
			BodyTemplate:  "Your PR {{payload.pr_no}} is approved.",
			Version:       1,
			IsActive:      true,
		},
	}

	notifRepo := &mockNotificationRepo{}
	delivRepo := &mockDeliveryRepo{}
	engine := NewEngine(&mockRuleRepo{rules: rules}, &mockTemplateRepo{templates: templates}, &mockRecipientRepo{recipients: recipients}, &mockPreferenceRepo{}, notifRepo, delivRepo, nil, nil, false, nil)

	env := &domain.EventEnvelope{
		EventID:     uuid.New(),
		EventType:   "purchase_request.approved",
		AggregateID: uuid.New().String(),
		Payload:     json.RawMessage(`{"requester_user_id":"` + targetUID.String() + `","pr_no":"PR-100"}`),
	}

	err := runWithTestTx(func(ctx context.Context) error {
		return engine.Route(ctx, env)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must deduplicate across rules: exactly 1 notification created for targetUID
	if len(notifRepo.insertedNotifications) != 1 {
		t.Fatalf("expected 1 notification deduplicated across rules, got %d", len(notifRepo.insertedNotifications))
	}
	if notifRepo.insertedNotifications[0].RecipientID != targetUID {
		t.Fatalf("expected recipient %s, got %s", targetUID, notifRepo.insertedNotifications[0].RecipientID)
	}
	if notifRepo.insertedNotifications[0].Body != "Your PR PR-100 is approved." {
		t.Fatalf("unexpected body rendered: %s", notifRepo.insertedNotifications[0].Body)
	}
}

func TestEngine_EmailEnqueuing(t *testing.T) {
	userID := uuid.New()
	recipients := []*domain.Recipient{
		{UserID: userID, Email: "admin@example.com", Roles: []string{"admin"}, IsActive: true},
	}

	rules := []*domain.NotificationRule{
		{
			ID:          uuid.New(),
			EventType:   "user.locked",
			TargetRoles: []string{"admin"},
			Type:        domain.TypeSecurityAlert,
			Category:    domain.CategorySecurity,
			Priority:    domain.PriorityHigh,
			TemplateKey: "tmpl_user_locked",
			SendEmail:   true,
			IsActive:    true,
		},
	}

	templates := map[string]*domain.NotificationTemplate{
		"tmpl_user_locked": {
			TemplateKey:   "tmpl_user_locked",
			TitleTemplate: "Account locked",
			BodyTemplate:  "Account locked",
			Version:       1,
			IsActive:      true,
		},
	}

	emailQueue := &mockEmailQueue{}
	notifRepo := &mockNotificationRepo{}
	delivRepo := &mockDeliveryRepo{}

	// Email enabled = true
	engine := NewEngine(&mockRuleRepo{rules: rules}, &mockTemplateRepo{templates: templates}, &mockRecipientRepo{recipients: recipients}, &mockPreferenceRepo{}, notifRepo, delivRepo, emailQueue, nil, true, nil)

	env := &domain.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     "user.locked",
		AggregateType: "user",
		AggregateID:   userID.String(),
	}

	err := runWithTestTx(func(ctx context.Context) error {
		return engine.Route(ctx, env)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Deliveries should have 2: in_app and email
	if len(delivRepo.insertedDeliveries) != 2 {
		t.Fatalf("expected 2 deliveries (in_app and email), got %d", len(delivRepo.insertedDeliveries))
	}

	// Email queue should have 1 enqueued email
	if len(emailQueue.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued email, got %d", len(emailQueue.enqueued))
	}
	if emailQueue.enqueued[0].RecipientID != userID {
		t.Fatalf("expected recipient %s, got %s", userID, emailQueue.enqueued[0].RecipientID)
	}
	if emailQueue.enqueued[0].Subject != "Account locked" {
		t.Fatalf("expected subject 'Account locked', got %s", emailQueue.enqueued[0].Subject)
	}
}
