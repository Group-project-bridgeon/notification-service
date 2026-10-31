package http

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	"notification-service/internal/email"
	"notification-service/internal/ingest"
	"notification-service/internal/metrics"
	"notification-service/internal/notification"
	"notification-service/internal/repository"
	"notification-service/internal/sse"
)

// RouterConfig contains dependencies required to build the Gin engine.
type RouterConfig struct {
	Config                 *config.Config
	Logger                 *slog.Logger
	DB                     Pinger
	Kafka                  KafkaProber
	Ingestor               *ingest.Ingestor
	NotificationRepo       repository.NotificationRepository
	PreferenceRepo         repository.PreferenceRepository
	RuleRepo               repository.NotificationRuleRepository
	NotificationTemplateRepo repository.NotificationTemplateRepository
	EmailTemplateRepo      repository.EmailTemplateRepository
	RecipientRepo          repository.RecipientRepository
	DeliveryRepo           repository.DeliveryRepository
	DeadLetterRepo         repository.DeadLetterRepository
	StatsRepo              repository.StatsRepository
	OperationRepo          repository.OperationRepository
	ConsumerStatusProvider ConsumerStatusProvider
	SSEHub                 *sse.Hub
	TicketStore            *sse.TicketStore
	EmailEngine            *email.Engine
}

// NewRouter constructs and configures the Gin engine with all middlewares and health endpoints.
func NewRouter(rc RouterConfig) *gin.Engine {
	if rc.Config.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	r := gin.New()

	// Global middlewares
	r.Use(RequestIDMiddleware())
	r.Use(StructuredLoggerMiddleware(rc.Logger))
	r.Use(RecoveryMiddleware(rc.Logger))
	r.Use(CORSMiddleware(rc.Config.CORSAllowedOrigins))

	// Health handler (unauthenticated)
	healthHandler := NewHealthHandler(rc.DB, rc.Kafka)
	r.GET("/health/live", healthHandler.Live)
	r.GET("/health/ready", healthHandler.Ready)
	r.GET("/health/version", healthHandler.Version)

	// Prometheus metrics endpoint (Protected by X-Internal-Token)
	r.GET("/metrics", func(c *gin.Context) {
		token := c.GetHeader("X-Internal-Token")
		if rc.Config != nil && rc.Config.InternalServiceToken != "" && token != rc.Config.InternalServiceToken {
			RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthorized access to internal metrics", nil)
			return
		}
		c.Data(http.StatusOK, "text/plain; version=0.0.4; charset=utf-8", metrics.Default().RenderPrometheus())
	})

	// Auth and rate limiting middlewares
	authMiddleware := AuthMiddleware(rc.Config)
	writeRateLimiter := NewUserRateLimiter(10.0, 60.0) // 10 ops/sec, burst of 60
	rateLimitMiddleware := WriteRateLimitMiddleware(writeRateLimiter)

	// Live Delivery SSE endpoints (Ticket issuance & SSE Stream)
	if rc.SSEHub != nil && rc.TicketStore != nil {
		streamHandler := NewStreamHandler(rc.SSEHub, rc.TicketStore, rc.NotificationRepo, rc.Logger)
		r.POST("/notifications/stream-ticket", authMiddleware, rateLimitMiddleware, streamHandler.IssueTicket)
		r.GET("/notifications/stream", streamHandler.Stream)
	}

	// User-facing Notification endpoints (JWT/Dev Auth + Write Rate Limiting)
	if rc.NotificationRepo != nil {
		notifHandler := NewNotificationHandler(rc.NotificationRepo)
		notifGroup := r.Group("/notifications")
		notifGroup.Use(authMiddleware)
		notifGroup.Use(rateLimitMiddleware)
		{
			notifGroup.GET("", notifHandler.List)
			notifGroup.GET("/unread-count", notifHandler.UnreadCount)
			notifGroup.GET("/:id", notifHandler.GetByID)
			notifGroup.POST("/:id/read", notifHandler.MarkRead)
			notifGroup.POST("/:id/unread", notifHandler.MarkUnread)
			notifGroup.POST("/:id/archive", notifHandler.Archive)
			notifGroup.POST("/read-all", notifHandler.ReadAll)
		}
	}

	// User-facing Preference endpoints
	if rc.PreferenceRepo != nil {
		prefHandler := NewPreferenceHandler(rc.PreferenceRepo)
		meGroup := r.Group("/me")
		meGroup.Use(authMiddleware)
		meGroup.Use(rateLimitMiddleware)
		{
			meGroup.GET("/notification-preferences", prefHandler.Get)
			meGroup.PUT("/notification-preferences", prefHandler.Update)
		}
	}

	// User-facing Operation endpoints (JWT/Dev Auth)
	if rc.OperationRepo != nil {
		opHandler := NewOperationHandler(rc.OperationRepo, rc.Logger)
		opGroup := r.Group("/operations")
		opGroup.Use(authMiddleware)
		{
			opGroup.GET("", opHandler.List)
			opGroup.GET("/:id", opHandler.GetByID)
		}
	}

	// Admin endpoints (Guarded by RequireRole("admin"))
	adminGroup := r.Group("/admin")
	adminGroup.Use(authMiddleware)
	adminGroup.Use(RequireRole("admin"))
	{
		// Notification Rules
		if rc.RuleRepo != nil {
			var allowedRoles []string
			if rc.Config != nil {
				allowedRoles = rc.Config.AllowedRoles
			}
			ruleHandler := NewAdminRuleHandler(rc.RuleRepo, rc.EmailTemplateRepo, allowedRoles, rc.Logger)
			adminGroup.GET("/notification-rules", ruleHandler.List)
			adminGroup.POST("/notification-rules", ruleHandler.Create)
			adminGroup.GET("/notification-rules/:id", ruleHandler.GetByID)
			adminGroup.PUT("/notification-rules/:id", ruleHandler.Update)
			adminGroup.DELETE("/notification-rules/:id", ruleHandler.Deactivate)
			adminGroup.POST("/notification-rules/:id/deactivate", ruleHandler.Deactivate)
		}

		// Notification Templates
		if rc.NotificationTemplateRepo != nil {
			tmplHandler := NewAdminTemplateHandler(rc.NotificationTemplateRepo, rc.Logger)
			adminGroup.GET("/notification-templates", tmplHandler.List)
			adminGroup.POST("/notification-templates", tmplHandler.Create)
			adminGroup.PUT("/notification-templates/:key", tmplHandler.PutVersion)
		}

		// Test Send
		if rc.RecipientRepo != nil && rc.NotificationTemplateRepo != nil && rc.NotificationRepo != nil && rc.DeliveryRepo != nil {
			var pub notification.NotificationPublisher
			if rc.SSEHub != nil {
				pub = rc.SSEHub
			}
			testSendHandler := NewAdminTestSendHandler(rc.RecipientRepo, rc.NotificationTemplateRepo, rc.NotificationRepo, rc.DeliveryRepo, pub, rc.Logger)
			adminGroup.POST("/notifications/test-send", testSendHandler.TestSend)
		}

		// Deliveries
		if rc.DeliveryRepo != nil {
			delivHandler := NewAdminDeliveryHandler(rc.DeliveryRepo, rc.Logger)
			adminGroup.GET("/notification-deliveries", delivHandler.List)
			adminGroup.POST("/notification-deliveries/:id/retry", delivHandler.Retry)
		}

		// Dead Letters
		if rc.DeadLetterRepo != nil {
			dlHandler := NewAdminDeadLetterHandler(rc.DeadLetterRepo, rc.Ingestor, rc.Logger)
			adminGroup.GET("/notification-dead-letters", dlHandler.List)
			adminGroup.POST("/notification-dead-letters/:id/replay", dlHandler.Replay)
		}

		// Notification Stats
		if rc.StatsRepo != nil {
			statsHandler := NewAdminStatsHandler(rc.StatsRepo, rc.Logger)
			adminGroup.GET("/notification-stats", statsHandler.GetStats)
		}

		// Consumer Status
		groupID := "notification-service"
		if rc.Config != nil && rc.Config.Kafka.GroupID != "" {
			groupID = rc.Config.Kafka.GroupID
		}
		consumerStatusHandler := NewAdminConsumerStatusHandler(rc.ConsumerStatusProvider, groupID, rc.Logger)
		adminGroup.GET("/notification-consumer-status", consumerStatusHandler.GetStatus)

		// Email Settings
		if rc.EmailEngine != nil {
			adminEmailHandler := NewAdminEmailHandler(rc.EmailEngine)
			adminGroup.GET("/email-settings", adminEmailHandler.GetSettings)
			adminGroup.PUT("/email-settings", adminEmailHandler.UpdateSettings)
			adminGroup.POST("/email-settings/test", adminEmailHandler.TestSettings)
		}
	}

	// Email management endpoints
	if rc.EmailEngine != nil {
		emailGroup := r.Group("/email")
		emailGroup.Use(authMiddleware)
		emailGroup.Use(rateLimitMiddleware)

		tmplHandler := NewEmailTemplateHandler(rc.EmailEngine)
		// Template routes: read for admin/manager, write for admin only
		emailGroup.GET("/templates", RequireRole("admin", "manager"), tmplHandler.List)
		emailGroup.GET("/templates/:key", RequireRole("admin", "manager"), tmplHandler.GetByKey)
		emailGroup.PUT("/templates/:key", RequireRole("admin"), tmplHandler.PutVersion)
		emailGroup.POST("/templates/:key/preview", RequireRole("admin", "manager"), tmplHandler.Preview)
		emailGroup.POST("/templates/:key/activate", RequireRole("admin"), tmplHandler.Activate)

		// Message audit & control routes: caller scoped
		msgHandler := NewEmailMessageHandler(rc.EmailEngine)
		emailGroup.GET("/messages", msgHandler.List)
		emailGroup.GET("/messages/:id", msgHandler.GetByID)
		emailGroup.POST("/messages/:id/resend", msgHandler.Resend)
		emailGroup.POST("/messages/:id/cancel", msgHandler.Cancel)

		// Suppression list routes: read for admin/manager, write for admin only
		suppHandler := NewEmailSuppressionHandler(rc.EmailEngine)
		emailGroup.GET("/suppressions", RequireRole("admin", "manager"), suppHandler.List)
		emailGroup.POST("/suppressions", RequireRole("admin"), suppHandler.Add)
		emailGroup.DELETE("/suppressions/:email", RequireRole("admin"), suppHandler.Delete)

		// Webhook endpoint: unauthenticated by JWT, signature checked via X-Signature
		webhookHandler := NewEmailWebhookHandler(rc.EmailEngine, rc.Config.WebhookSecret)
		r.POST("/webhooks/email/:provider", webhookHandler.HandleProviderWebhook)
	}

	// Internal routes
	if rc.Ingestor != nil || rc.EmailEngine != nil {
		internalGroup := r.Group("/internal")
		internalGroup.Use(InternalAuthMiddleware(rc.Config.InternalServiceToken))
		{
			if rc.Ingestor != nil {
				internalEventsHandler := NewInternalEventsHandler(rc.Ingestor, rc.Config.InternalEventsEnabled)
				internalGroup.POST("/events", internalEventsHandler.HandleEvent)
			}
			if rc.EmailEngine != nil {
				emailHandler := NewEmailHandler(rc.EmailEngine)
				internalGroup.POST("/email/send", emailHandler.Send)
			}
		}
	}

	return r
}
