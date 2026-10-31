package email

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

// BusinessRetrySchedule defines the delay intervals for business email retry attempts.
var BusinessRetrySchedule = []time.Duration{
	1 * time.Minute,  // After 1st failure
	5 * time.Minute,  // After 2nd failure
	15 * time.Minute, // After 3rd failure
	1 * time.Hour,    // After 4th failure
	6 * time.Hour,    // After 5th failure
}

// TransactionalDelays defines the relative wait times before each transactional retry attempt (0s, 5s, 15s).
var TransactionalDelays = []time.Duration{
	0,                // 1st attempt at 0 s
	5 * time.Second,  // 2nd attempt at 5 s
	15 * time.Second, // 3rd attempt at 20 s total (5s + 15s)
}

// TransactionalJob encapsulates in-memory state for immediate high-priority/OTP dispatch.
type TransactionalJob struct {
	MessageID     uuid.UUID
	OutboundEmail *OutboundEmail
	ExpiresAt     *time.Time
}

// TransactionalWorker manages in-memory retries for immediate transactional emails.
type TransactionalWorker struct {
	messageRepo repository.EmailMessageRepository
	sender      EmailSender
	clock       Clock
	logger      *slog.Logger
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewTransactionalWorker constructs a TransactionalWorker.
func NewTransactionalWorker(
	messageRepo repository.EmailMessageRepository,
	sender EmailSender,
	clock Clock,
	logger *slog.Logger,
) *TransactionalWorker {
	if clock == nil {
		clock = NewRealClock()
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &TransactionalWorker{
		messageRepo: messageRepo,
		sender:      sender,
		clock:       clock,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Submit dispatches a transactional job in an in-memory worker goroutine.
func (w *TransactionalWorker) Submit(job *TransactionalJob) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.processJob(job)
	}()
}

func (w *TransactionalWorker) processJob(job *TransactionalJob) {
	for attempt := 1; attempt <= 3; attempt++ {
		delay := TransactionalDelays[attempt-1]
		if delay > 0 {
			select {
			case <-w.ctx.Done():
				return
			case <-w.clock.After(delay):
			}
		}

		now := w.clock.Now()
		if job.ExpiresAt != nil && (now.After(*job.ExpiresAt) || now.Equal(*job.ExpiresAt)) {
			errStr := "expired"
			_ = w.messageRepo.UpdateStatus(w.ctx, job.MessageID, domain.EmailMessageStatusFailed, attempt-1, nil, nil, &errStr, nil)
			w.logger.InfoContext(w.ctx, "transactional email expired before dispatch",
				"message_id", job.MessageID,
				"attempt", attempt,
			)
			return
		}

		// Update to sending status
		_ = w.messageRepo.UpdateStatus(w.ctx, job.MessageID, domain.EmailMessageStatusSending, attempt, nil, nil, nil, nil)

		res, err := w.sender.Send(w.ctx, job.OutboundEmail)
		if err == nil {
			sentAt := w.clock.Now()
			provID := res.ProviderMessageID
			_ = w.messageRepo.UpdateStatus(w.ctx, job.MessageID, domain.EmailMessageStatusSent, attempt, nil, &sentAt, nil, &provID)
			w.logger.InfoContext(w.ctx, "transactional email sent successfully",
				"message_id", job.MessageID,
				"attempt", attempt,
				"provider_message_id", provID,
			)
			return
		}

		w.logger.WarnContext(w.ctx, "transactional email attempt failed",
			"message_id", job.MessageID,
			"attempt", attempt,
			"error", err,
		)

		if attempt == 3 {
			errStr := err.Error()
			_ = w.messageRepo.UpdateStatus(w.ctx, job.MessageID, domain.EmailMessageStatusFailed, attempt, nil, nil, &errStr, nil)
			return
		}

		errStr := err.Error()
		_ = w.messageRepo.UpdateStatus(w.ctx, job.MessageID, domain.EmailMessageStatusQueued, attempt, nil, nil, &errStr, nil)
	}
}

// Stop cleanly drains all in-flight transactional jobs.
func (w *TransactionalWorker) Stop() {
	w.cancel()
	w.wg.Wait()
}

// Wait blocks until all currently active transactional jobs have completed.
func (w *TransactionalWorker) Wait() {
	w.wg.Wait()
}

// BusinessQueueWorker polls for due business email messages and processes them.
type BusinessQueueWorker struct {
	settingsRepo repository.EmailSettingsRepository
	messageRepo  repository.EmailMessageRepository
	sender       EmailSender
	clock        Clock
	logger       *slog.Logger
	stopCh       chan struct{}
	wg           sync.WaitGroup
	pollInterval time.Duration
}

// NewBusinessQueueWorker creates a BusinessQueueWorker.
func NewBusinessQueueWorker(
	settingsRepo repository.EmailSettingsRepository,
	messageRepo repository.EmailMessageRepository,
	sender EmailSender,
	clock Clock,
	logger *slog.Logger,
) *BusinessQueueWorker {
	if clock == nil {
		clock = NewRealClock()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BusinessQueueWorker{
		settingsRepo: settingsRepo,
		messageRepo:  messageRepo,
		sender:       sender,
		clock:        clock,
		logger:       logger,
		stopCh:       make(chan struct{}),
		pollInterval: 1 * time.Second,
	}
}

// Start launches the background polling loop.
func (w *BusinessQueueWorker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.pollLoop(ctx)
	}()
}

func (w *BusinessQueueWorker) pollLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-w.clock.After(w.pollInterval):
			w.ProcessBatch(ctx, 10)
		}
	}
}

// ProcessBatch claims and processes up to limit due email messages.
func (w *BusinessQueueWorker) ProcessBatch(ctx context.Context, limit int) int {
	messages, err := w.messageRepo.ClaimDue(ctx, limit)
	if err != nil {
		w.logger.ErrorContext(ctx, "failed claiming due email messages", "error", err)
		return 0
	}
	if len(messages) == 0 {
		return 0
	}

	settings, err := w.settingsRepo.Get(ctx)
	fromName := "Fashion Wholesale ERP"
	fromAddr := "no-reply@fashionerp.com"
	var replyTo *string
	if err == nil && settings != nil {
		if settings.FromName != "" {
			fromName = settings.FromName
		}
		if settings.FromAddress != "" {
			fromAddr = settings.FromAddress
		}
		replyTo = settings.ReplyTo
	}

	for _, msg := range messages {
		w.processMessage(ctx, msg, fromName, fromAddr, replyTo)
	}

	return len(messages)
}

func (w *BusinessQueueWorker) processMessage(
	ctx context.Context,
	msg *domain.EmailMessage,
	fromName, fromAddr string,
	replyTo *string,
) {
	// Mark as sending
	_ = w.messageRepo.UpdateStatus(ctx, msg.ID, domain.EmailMessageStatusSending, msg.Attempts, nil, nil, nil, nil)

	outbound := &OutboundEmail{
		FromName:    fromName,
		FromAddress: fromAddr,
		To:          msg.ToAddresses,
		Cc:          msg.CcAddresses,
		ReplyTo:     replyTo,
		Subject:     msg.Subject,
		HTMLBody:    msg.BodyHTMLRedacted,
		TextBody:    msg.BodyTextRedacted,
	}

	res, err := w.sender.Send(ctx, outbound)
	if err == nil {
		sentAt := w.clock.Now()
		provID := res.ProviderMessageID
		_ = w.messageRepo.UpdateStatus(ctx, msg.ID, domain.EmailMessageStatusSent, msg.Attempts, nil, &sentAt, nil, &provID)
		w.logger.InfoContext(ctx, "business email sent successfully",
			"message_id", msg.ID,
			"provider_message_id", provID,
		)
		return
	}

	// Failure handling with retry schedule: 1m, 5m, 15m, 1h, 6h -> failed
	newAttempts := msg.Attempts + 1
	w.logger.WarnContext(ctx, "business email send failed",
		"message_id", msg.ID,
		"attempts", newAttempts,
		"error", err,
	)

	if newAttempts > len(BusinessRetrySchedule) {
		errStr := err.Error()
		_ = w.messageRepo.UpdateStatus(ctx, msg.ID, domain.EmailMessageStatusFailed, newAttempts, nil, nil, &errStr, nil)
		return
	}

	delay := BusinessRetrySchedule[newAttempts-1]
	nextAttempt := w.clock.Now().Add(delay)

	if msg.ExpiresAt != nil && nextAttempt.After(*msg.ExpiresAt) {
		errStr := fmt.Sprintf("expired before next retry attempt: %v", err)
		_ = w.messageRepo.UpdateStatus(ctx, msg.ID, domain.EmailMessageStatusFailed, newAttempts, nil, nil, &errStr, nil)
		return
	}

	errStr := err.Error()
	_ = w.messageRepo.UpdateStatus(ctx, msg.ID, domain.EmailMessageStatusQueued, newAttempts, &nextAttempt, nil, &errStr, nil)
}

// Stop cleanly terminates the business queue worker.
func (w *BusinessQueueWorker) Stop() {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}
	w.wg.Wait()
}
