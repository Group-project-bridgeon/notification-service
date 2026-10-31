package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	"notification-service/internal/email"
	appHTTP "notification-service/internal/http"
	"notification-service/internal/ingest"
	"notification-service/internal/kafka"
	appMetrics "notification-service/internal/metrics"
	"notification-service/internal/notification"
	"notification-service/internal/repository"
	"notification-service/internal/sse"
)

func main() {
	// 1. Load configuration and execute startup validations
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[FATAL] Configuration validation failure: %v", err)
	}

	// 2. Initialize JSON structured logger with sensitive key redactor
	logger := initLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	logger.Info("Starting Notification Service",
		slog.String("env", cfg.Env),
		slog.String("port", cfg.Port),
		slog.String("auth_mode", cfg.AuthMode),
		slog.Bool("internal_events_enabled", cfg.InternalEventsEnabled),
		slog.Bool("kafka_enabled", cfg.Kafka.ConsumerEnabled),
	)

	// 3. Initialize PostgreSQL connection pool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := repository.NewDB(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize database pool", slog.String("error", err.Error()))
	}
	defer db.Close()

	// 4. Initialize Repositories and Ingestor
	var pool *pgxpool.Pool
	if db != nil {
		pool = db.Pool
	}
	txManager := repository.NewTxManager(pool)
	inboxRepo := repository.NewInboxRepository(pool)
	recipientRepo := repository.NewRecipientRepository(pool)
	deadLetterRepo := repository.NewDeadLetterRepository(pool)
	ruleRepo := repository.NewNotificationRuleRepository(pool)
	templateRepo := repository.NewNotificationTemplateRepository(pool)
	preferenceRepo := repository.NewPreferenceRepository(pool)
	notificationRepo := repository.NewNotificationRepository(pool)
	deliveryRepo := repository.NewDeliveryRepository(pool)
	operationRepo := repository.NewOperationRepository(pool)

	emailSettingsRepo := repository.NewEmailSettingsRepository(pool)
	emailTemplateRepo := repository.NewEmailTemplateRepository(pool, txManager)
	emailMessageRepo := repository.NewEmailMessageRepository(pool)
	suppressionRepo := repository.NewSuppressionRepository(pool)
	idempotencyRepo := repository.NewIdempotencyKeyRepository(pool)

	encKey, _ := email.ParseEncryptionKey(cfg.EmailEncryptionKey)

	var emailSender email.EmailSender
	if cfg.Env == "production" {
		emailSender = email.NewSMTPSender(email.SMTPOptions{
			Host:    "localhost",
			Port:    1025,
			TLSMode: "starttls",
		}, logger)
	} else {
		emailSender, _ = email.NewLogSender(cfg.Env, logger)
	}

	txWorker := email.NewTransactionalWorker(emailMessageRepo, emailSender, email.NewRealClock(), logger)
	queueWorker := email.NewBusinessQueueWorker(emailSettingsRepo, emailMessageRepo, emailSender, email.NewRealClock(), logger)
	emailEngine := email.NewEngine(
		emailSettingsRepo,
		emailTemplateRepo,
		emailMessageRepo,
		suppressionRepo,
		idempotencyRepo,
		emailSender,
		txWorker,
		queueWorker,
		encKey,
		email.NewRealClock(),
		logger,
	)

	// Execute on-startup sweep to mark in-flight transactional emails failed
	if err := emailEngine.OnStartup(context.Background()); err != nil {
		logger.Warn("Failed running email startup sweep", slog.String("error", err.Error()))
	}

	if cfg.EmailEnabled {
		queueWorker.Start(context.Background())
	}

	sseHub := sse.NewHub(logger)
	ticketStore := sse.NewTicketStore()

	engine := notification.NewEngine(
		ruleRepo,
		templateRepo,
		recipientRepo,
		preferenceRepo,
		notificationRepo,
		deliveryRepo,
		emailEngine,
		sseHub,
		cfg.EmailEnabled,
		logger,
	)

	ingestor := ingest.NewIngestor(txManager, inboxRepo, recipientRepo, engine, logger)
	ingestor.SetOperationRepository(operationRepo)
	ingestor.SetOperationPublisher(sseHub)

	// Configure Prometheus gauge providers
	appMetrics.Default().SetSSEConnectionsProvider(func() int64 {
		if sseHub != nil {
			return int64(sseHub.ClientCount())
		}
		return 0
	})
	appMetrics.Default().SetEmailQueueDepthProvider(func() int64 {
		if emailMessageRepo != nil {
			queuedStatus := domain.EmailMessageStatusQueued
			_, total, err := emailMessageRepo.List(context.Background(), domain.EmailMessageFilter{
				Status:   &queuedStatus,
				PageSize: 1,
			})
			if err == nil {
				return int64(total)
			}
		}
		return 0
	})

	// 5. Initialize Kafka Publisher, Failure Handler, and Consumer
	metrics := kafka.NewMetrics()
	var publisher kafka.Publisher
	if cfg.Kafka.ConsumerEnabled {
		producerClient, err := kafka.NewProducerClient(cfg)
		if err != nil {
			logger.Error("Failed to initialize Kafka producer client", slog.String("error", err.Error()))
		} else {
			defer producerClient.Close()
			publisher = kafka.NewFranzPublisher(producerClient)
		}
	}

	retryCfg := kafka.RetryConfig{
		RetryTopics: cfg.Kafka.RetryTopics,
		RetryDelays: []time.Duration{1 * time.Minute, 10 * time.Minute, 1 * time.Hour},
		DLQTopic:    cfg.Kafka.DLQTopic,
	}
	failureHandler := kafka.NewStandardFailureHandler(publisher, deadLetterRepo, metrics, kafka.NewRealClock(), retryCfg, logger)
	consumer := kafka.NewConsumer(cfg, ingestor, failureHandler, metrics, logger)

	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	if cfg.Kafka.ConsumerEnabled {
		if err := consumer.Start(consumerCtx); err != nil {
			logger.Error("Failed to start Kafka consumer", slog.String("error", err.Error()))
		}
	}
	defer consumer.Stop()

	statsRepo := repository.NewStatsRepository(pool)
	consumerTracker := kafka.NewConsumerLagTracker(consumer)

	// 6. Construct Gin router and attach endpoints
	router := appHTTP.NewRouter(appHTTP.RouterConfig{
		Config:                   cfg,
		Logger:                   logger,
		DB:                       db,
		Kafka:                    consumer,
		Ingestor:                 ingestor,
		NotificationRepo:         notificationRepo,
		PreferenceRepo:           preferenceRepo,
		RuleRepo:                 ruleRepo,
		NotificationTemplateRepo: templateRepo,
		EmailTemplateRepo:        emailTemplateRepo,
		RecipientRepo:            recipientRepo,
		DeliveryRepo:             deliveryRepo,
		DeadLetterRepo:           deadLetterRepo,
		StatsRepo:                statsRepo,
		OperationRepo:            operationRepo,
		ConsumerStatusProvider:   consumerTracker,
		SSEHub:                   sseHub,
		TicketStore:              ticketStore,
		EmailEngine:              emailEngine,
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 7. Start HTTP server in background goroutine
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("HTTP server listening", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failure", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	// 8. Wait for termination signal and execute graceful shutdown
	sig := <-shutdown
	logger.Info("Shutdown signal received", slog.String("signal", sig.String()))

	// Stop Kafka consumer first so in-flight records complete
	consumer.Stop()

	// Terminate live SSE streams and cleanup routines
	sseHub.Close()
	ticketStore.Close()

	// Drain in-flight email workers
	txWorker.Stop()
	queueWorker.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", slog.String("error", err.Error()))
	}

	logger.Info("Notification Service stopped cleanly.")
}

func initLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: email.SlogRedactAttrReplacer,
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	return slog.New(handler)
}
