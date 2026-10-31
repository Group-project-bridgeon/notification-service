package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/email"
	"notification-service/internal/ingest"
	"notification-service/internal/repository"
)

// Engine implements ingest.Router to transform incoming domain events into
// in-app notifications and email dispatch deliveries.
type Engine struct {
	ruleRepo         repository.NotificationRuleRepository
	templateRepo     repository.NotificationTemplateRepository
	recipientRepo    repository.RecipientRepository
	preferenceRepo   repository.PreferenceRepository
	notificationRepo repository.NotificationRepository
	deliveryRepo     repository.DeliveryRepository
	emailQueue       email.EmailQueue
	publisher        NotificationPublisher
	emailEnabled     bool
	logger           *slog.Logger
}

// NewEngine constructs a Notification Routing Engine.
func NewEngine(
	ruleRepo repository.NotificationRuleRepository,
	templateRepo repository.NotificationTemplateRepository,
	recipientRepo repository.RecipientRepository,
	preferenceRepo repository.PreferenceRepository,
	notificationRepo repository.NotificationRepository,
	deliveryRepo repository.DeliveryRepository,
	emailQueue email.EmailQueue,
	publisher NotificationPublisher,
	emailEnabled bool,
	logger *slog.Logger,
) *Engine {
	if emailQueue == nil {
		emailQueue = email.NoopEmailQueue{}
	}
	if publisher == nil {
		publisher = NoopNotificationPublisher{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		ruleRepo:         ruleRepo,
		templateRepo:     templateRepo,
		recipientRepo:    recipientRepo,
		preferenceRepo:   preferenceRepo,
		notificationRepo: notificationRepo,
		deliveryRepo:     deliveryRepo,
		emailQueue:       emailQueue,
		publisher:        publisher,
		emailEnabled:     emailEnabled,
		logger:           logger,
	}
}

type recipientPlan struct {
	recipient    *domain.Recipient
	rule         *domain.NotificationRule
	allowInApp   bool
	allowEmail   bool
	title        string
	body         string
	link         *string
}

// Route executes the full fan-out logic inside the active transaction propagated in ctx:
// 1. Loads active rules for event_type (returns ErrNoRuleMatched if none exist).
// 2. Resolves recipients by target_roles and target_user_path.
// 3. Excludes the actor (unless category is security) and de-duplicates per recipient.
// 4. Renders title, body, and link from the highest active version of the template.
// 5. Enforces channel preferences (locking security, saga, approval in-app & critical priority).
// 6. Persists notification and delivery rows, enqueuing emails where configured.
// 7. Registers a post-commit hook for real-time SSE notification publishing.
func (e *Engine) Route(ctx context.Context, env *domain.EventEnvelope) error {
	rules, err := e.ruleRepo.GetActiveByEventType(ctx, env.EventType)
	if err != nil {
		return fmt.Errorf("failed fetching rules for event %s: %w", env.EventType, err)
	}
	if len(rules) == 0 {
		return ingest.ErrNoRuleMatched
	}

	var payloadMap map[string]any
	if len(env.Payload) > 0 {
		_ = json.Unmarshal(env.Payload, &payloadMap)
	}

	plans := make(map[uuid.UUID]*recipientPlan)

	for _, rule := range rules {
		recipients, err := e.resolveRecipients(ctx, rule, env, payloadMap)
		if err != nil {
			return fmt.Errorf("failed resolving recipients for rule %s: %w", rule.ID, err)
		}

		for _, rec := range recipients {
			if _, exists := plans[rec.UserID]; exists {
				continue // De-duplicate across rules: one notification per recipient per event
			}

			// Actor exclusion: do not notify actor about their own action unless security
			if env.ActorUserID != nil && rec.UserID == *env.ActorUserID && rule.Category != domain.CategorySecurity {
				continue
			}

			// Preferences evaluation
			allowInApp, allowEmail, err := e.evaluatePreferences(ctx, rec.UserID, rule)
			if err != nil {
				return fmt.Errorf("failed evaluating preferences for user %s: %w", rec.UserID, err)
			}

			if !allowInApp && !allowEmail {
				continue
			}

			// Render template (highest active version)
			tmpl, err := e.templateRepo.GetActive(ctx, rule.TemplateKey, "en")
			if err != nil {
				e.logger.WarnContext(ctx, "failed loading notification template for rule",
					"template_key", rule.TemplateKey,
					"error", err,
				)
				continue
			}

			renderedTitle := RenderTemplate(tmpl.TitleTemplate, env, e.logger)
			renderedBody := RenderTemplate(tmpl.BodyTemplate, env, e.logger)
			var renderedLink *string
			if rule.LinkTemplate != nil && *rule.LinkTemplate != "" {
				l := RenderTemplate(*rule.LinkTemplate, env, e.logger)
				renderedLink = &l
			}

			plans[rec.UserID] = &recipientPlan{
				recipient:  rec,
				rule:       rule,
				allowInApp: allowInApp,
				allowEmail: allowEmail,
				title:      renderedTitle,
				body:       renderedBody,
				link:       renderedLink,
			}
		}
	}

	if len(plans) == 0 {
		return nil
	}

	// Persist notifications and delivery rows
	var createdNotifications []*domain.Notification
	var deliveries []*domain.NotificationDelivery

	var entityUUID *uuid.UUID
	if u, err := uuid.Parse(env.AggregateID); err == nil {
		entityUUID = &u
	}

	for _, plan := range plans {
		notifID := uuid.New()
		createdAt := time.Now().UTC()

		notif := &domain.Notification{
			ID:            notifID,
			RecipientID:   plan.recipient.UserID,
			EventID:       env.EventID,
			EventType:     env.EventType,
			Type:          plan.rule.Type,
			Category:      plan.rule.Category,
			Priority:      plan.rule.Priority,
			Title:         plan.title,
			Body:          plan.body,
			Link:          plan.link,
			EntityType:    &env.AggregateType,
			EntityID:      entityUUID,
			CorrelationID: env.CorrelationID,
			Status:        domain.NotificationStatusUnread,
			CreatedAt:     createdAt,
		}
		createdNotifications = append(createdNotifications, notif)

		// In-App delivery (created as sent)
		if plan.allowInApp {
			deliveries = append(deliveries, &domain.NotificationDelivery{
				ID:             uuid.New(),
				NotificationID: notifID,
				Channel:        domain.ChannelInApp,
				Status:         domain.DeliveryStatusSent,
				CreatedAt:      createdAt,
				UpdatedAt:      createdAt,
			})
		}

		// Email delivery (queued)
		if plan.allowEmail {
			deliveries = append(deliveries, &domain.NotificationDelivery{
				ID:             uuid.New(),
				NotificationID: notifID,
				Channel:        domain.ChannelEmail,
				Status:         domain.DeliveryStatusPending,
				CreatedAt:      createdAt,
				UpdatedAt:      createdAt,
			})

			emailTmplKey := plan.rule.TemplateKey
			if plan.rule.EmailTemplateKey != nil && *plan.rule.EmailTemplateKey != "" {
				emailTmplKey = *plan.rule.EmailTemplateKey
			}

			queuedEmail := email.QueuedEmail{
				RecipientID:     plan.recipient.UserID,
				ToAddresses:     []string{plan.recipient.Email},
				Subject:         plan.title,
				TemplateKey:     emailTmplKey,
				TemplateVersion: 1,
				Variables:       payloadMap,
				Priority:        mapPriorityToEmailPriority(plan.rule.Priority),
				RelatedType:     &env.AggregateType,
				RelatedID:       entityUUID,
			}
			if err := e.emailQueue.Enqueue(ctx, queuedEmail); err != nil {
				e.logger.ErrorContext(ctx, "failed enqueuing email",
					"recipient_id", plan.recipient.UserID,
					"error", err,
				)
			}
		}
	}

	if err := e.notificationRepo.BatchInsert(ctx, createdNotifications); err != nil {
		return fmt.Errorf("failed batch inserting notifications: %w", err)
	}

	for _, d := range deliveries {
		if err := e.deliveryRepo.Create(ctx, d); err != nil {
			return fmt.Errorf("failed inserting delivery row: %w", err)
		}
	}

	// Register post-commit SSE real-time publishing
	repository.AddPostCommitHook(ctx, func(postCommitCtx context.Context) {
		for _, notif := range createdNotifications {
			total, byCat, err := e.notificationRepo.GetUnreadCounts(postCommitCtx, notif.RecipientID)
			if err != nil {
				e.logger.WarnContext(postCommitCtx, "failed querying unread counts for post-commit publish",
					"recipient_id", notif.RecipientID,
					"error", err,
				)
				continue
			}

			counts := UnreadCounts{
				Total:      total,
				ByCategory: byCat,
			}
			if err := e.publisher.PublishNotification(postCommitCtx, notif, counts); err != nil {
				e.logger.WarnContext(postCommitCtx, "notification post-commit publish failed (non-fatal)",
					"notification_id", notif.ID,
					"error", err,
				)
			}
		}
	})

	return nil
}

func (e *Engine) resolveRecipients(
	ctx context.Context,
	rule *domain.NotificationRule,
	env *domain.EventEnvelope,
	payloadMap map[string]any,
) ([]*domain.Recipient, error) {
	recMap := make(map[uuid.UUID]*domain.Recipient)

	// 1. Target roles
	if len(rule.TargetRoles) > 0 {
		roleRecs, err := e.recipientRepo.FindByRoles(ctx, rule.TargetRoles)
		if err != nil {
			return nil, err
		}
		for _, r := range roleRecs {
			if r.IsActive {
				recMap[r.UserID] = r
			}
		}
	}

	// 2. Target user path
	if rule.TargetUserPath != nil && *rule.TargetUserPath != "" {
		targetUID := resolveUserPath(*rule.TargetUserPath, env, payloadMap)
		if targetUID != nil {
			userRec, err := e.recipientRepo.FindByID(ctx, *targetUID)
			if err == nil && userRec != nil && userRec.IsActive {
				recMap[userRec.UserID] = userRec
			}
		}
	}

	result := make([]*domain.Recipient, 0, len(recMap))
	for _, r := range recMap {
		result = append(result, r)
	}
	return result, nil
}

func resolveUserPath(path string, env *domain.EventEnvelope, payloadMap map[string]any) *uuid.UUID {
	path = strings.TrimSpace(path)
	if path == "actor_user_id" {
		return env.ActorUserID
	}
	if path == "aggregate_id" {
		if u, err := uuid.Parse(env.AggregateID); err == nil {
			return &u
		}
		return nil
	}
	if strings.HasPrefix(path, "payload.") {
		subPath := path[len("payload."):]
		val, found := extractPayloadPath(payloadMap, subPath)
		if found && val != nil {
			if s, ok := val.(string); ok {
				if parsed, err := uuid.Parse(s); err == nil {
					return &parsed
				}
			}
		}
	}
	return nil
}

func (e *Engine) evaluatePreferences(
	ctx context.Context,
	userID uuid.UUID,
	rule *domain.NotificationRule,
) (allowInApp, allowEmail bool, err error) {
	allowInApp = true
	allowEmail = rule.SendEmail && e.emailEnabled

	// Critical priority notifications can NEVER be disabled across any channel
	if rule.Priority == domain.PriorityCritical {
		return true, allowEmail, nil
	}

	// In-app notifications for security, saga, and approval can NEVER be disabled
	isLockedInApp := rule.Category == domain.CategorySecurity ||
		rule.Category == domain.CategorySaga ||
		rule.Category == domain.CategoryApproval

	prefs, err := e.preferenceRepo.GetByUser(ctx, userID)
	if err != nil {
		return false, false, err
	}

	for _, p := range prefs {
		if p.Category != rule.Category {
			continue
		}
		if p.Channel == domain.ChannelInApp && !isLockedInApp {
			allowInApp = p.Enabled
		}
		if p.Channel == domain.ChannelEmail && rule.SendEmail {
			allowEmail = p.Enabled && e.emailEnabled
		}
	}

	return allowInApp, allowEmail, nil
}

func mapPriorityToEmailPriority(p domain.Priority) domain.EmailPriority {
	switch p {
	case domain.PriorityHigh, domain.PriorityCritical:
		return domain.EmailPriorityTransactional
	default:
		return domain.EmailPriorityBusiness
	}
}
