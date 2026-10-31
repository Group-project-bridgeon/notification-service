package repository_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	cleanup := func() {
		pool.Close()
	}

	return pool, cleanup
}

func TestNotificationBatchInsertDeduplication(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	ctx := context.Background()
	recRepo := repository.NewRecipientRepository(pool)
	notifRepo := repository.NewNotificationRepository(pool)

	testUser := &domain.Recipient{
		UserID:           uuid.New(),
		Email:            fmt.Sprintf("dedup-%s@example.com", uuid.New().String()[:8]),
		DisplayName:      "Dedup Test User",
		Roles:            []string{"sales_rep"},
		IsActive:         true,
		SourceOccurredAt: time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	if err := recRepo.UpsertGuarded(ctx, testUser); err != nil {
		t.Fatalf("failed to create test recipient: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM notification_recipients WHERE user_id = $1", testUser.UserID)
	}()

	sharedEventID := uuid.New()
	n1 := &domain.Notification{
		ID:          uuid.New(),
		RecipientID: testUser.UserID,
		EventID:     sharedEventID,
		EventType:   "sales.order.placed",
		Type:        domain.TypeOrderConfirmed,
		Category:    domain.CategorySales,
		Priority:    domain.PriorityNormal,
		Title:       "Order 1001 Placed",
		Body:        "First event body",
		Status:      domain.NotificationStatusUnread,
		CreatedAt:   time.Now().UTC(),
	}

	n2 := &domain.Notification{
		ID:          uuid.New(),
		RecipientID: testUser.UserID,
		EventID:     sharedEventID, // Duplicate recipient_id + event_id
		EventType:   "sales.order.placed",
		Type:        domain.TypeOrderConfirmed,
		Category:    domain.CategorySales,
		Priority:    domain.PriorityNormal,
		Title:       "Order 1001 Duplicate Event",
		Body:        "Duplicate delivery attempt",
		Status:      domain.NotificationStatusUnread,
		CreatedAt:   time.Now().UTC(),
	}

	// Batch insert both simultaneously
	err := notifRepo.BatchInsert(ctx, []*domain.Notification{n1, n2})
	if err != nil {
		t.Fatalf("expected BatchInsert to ignore duplicate without error, got: %v", err)
	}

	// Verify only 1 notification was inserted
	items, total, err := notifRepo.List(ctx, domain.NotificationFilter{
		RecipientID: testUser.UserID,
	})
	if err != nil {
		t.Fatalf("failed listing notifications: %v", err)
	}

	if total != 1 || len(items) != 1 {
		t.Fatalf("expected exactly 1 notification after deduplication, got total=%d len=%d", total, len(items))
	}
	if items[0].Title != "Order 1001 Placed" {
		t.Fatalf("expected original notification preserved, got title: %s", items[0].Title)
	}
}

func TestNotificationListPagination(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	ctx := context.Background()
	recRepo := repository.NewRecipientRepository(pool)
	notifRepo := repository.NewNotificationRepository(pool)

	testUser := &domain.Recipient{
		UserID:           uuid.New(),
		Email:            fmt.Sprintf("page-%s@example.com", uuid.New().String()[:8]),
		DisplayName:      "Pagination Test User",
		Roles:            []string{"inventory_manager"},
		IsActive:         true,
		SourceOccurredAt: time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	if err := recRepo.UpsertGuarded(ctx, testUser); err != nil {
		t.Fatalf("failed to create test recipient: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM notification_recipients WHERE user_id = $1", testUser.UserID)
	}()

	var batch []*domain.Notification
	baseTime := time.Now().UTC()
	for i := 1; i <= 7; i++ {
		batch = append(batch, &domain.Notification{
			ID:          uuid.New(),
			RecipientID: testUser.UserID,
			EventID:     uuid.New(),
			EventType:   "inventory.stock.low",
			Type:        domain.TypeLowStock,
			Category:    domain.CategoryInventory,
			Priority:    domain.PriorityHigh,
			Title:       fmt.Sprintf("Item %d Low Stock", i),
			Body:        "Inventory below reorder point",
			Status:      domain.NotificationStatusUnread,
			CreatedAt:   baseTime.Add(time.Duration(i) * time.Minute),
		})
	}

	if err := notifRepo.BatchInsert(ctx, batch); err != nil {
		t.Fatalf("failed to insert batch notifications: %v", err)
	}

	// Request Page 2 with PageSize 5 (should return remaining 2 items out of 7)
	items, totalCount, err := notifRepo.List(ctx, domain.NotificationFilter{
		RecipientID: testUser.UserID,
		Page:        2,
		PageSize:    5,
		Sort:        "-created_at",
	})
	if err != nil {
		t.Fatalf("failed listing paginated notifications: %v", err)
	}

	if totalCount != 7 {
		t.Errorf("expected totalCount=7, got %d", totalCount)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items on page 2, got %d", len(items))
	}
}

func TestRecipientUpsertGuarded(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	ctx := context.Background()
	recRepo := repository.NewRecipientRepository(pool)

	testUserID := uuid.New()
	tBase := time.Now().UTC().Truncate(time.Microsecond)

	initial := &domain.Recipient{
		UserID:           testUserID,
		Email:            "sync@example.com",
		DisplayName:      "Initial Name",
		Roles:            []string{"sales_rep"},
		IsActive:         true,
		SourceOccurredAt: tBase,
		UpdatedAt:        tBase,
	}

	if err := recRepo.UpsertGuarded(ctx, initial); err != nil {
		t.Fatalf("failed inserting initial recipient: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM notification_recipients WHERE user_id = $1", testUserID)
	}()

	// 1. Attempt out-of-order update with older source_occurred_at
	staleUpdate := &domain.Recipient{
		UserID:           testUserID,
		Email:            "sync@example.com",
		DisplayName:      "Stale Out-of-Order Name",
		Roles:            []string{"sales_rep"},
		IsActive:         true,
		SourceOccurredAt: tBase.Add(-10 * time.Minute), // Older than tBase
		UpdatedAt:        time.Now().UTC(),
	}
	if err := recRepo.UpsertGuarded(ctx, staleUpdate); err != nil {
		t.Fatalf("upsert returned unexpected error: %v", err)
	}

	recAfterStale, err := recRepo.FindByID(ctx, testUserID)
	if err != nil {
		t.Fatalf("failed fetching recipient: %v", err)
	}
	if recAfterStale.DisplayName != "Initial Name" {
		t.Errorf("expected DisplayName to remain 'Initial Name', but was overwritten by stale write: %s", recAfterStale.DisplayName)
	}

	// 2. Attempt valid update with newer source_occurred_at
	freshUpdate := &domain.Recipient{
		UserID:           testUserID,
		Email:            "sync@example.com",
		DisplayName:      "Updated Fresh Name",
		Roles:            []string{"sales_rep", "sales_manager"},
		IsActive:         true,
		SourceOccurredAt: tBase.Add(10 * time.Minute), // Newer than tBase
		UpdatedAt:        time.Now().UTC(),
	}
	if err := recRepo.UpsertGuarded(ctx, freshUpdate); err != nil {
		t.Fatalf("upsert returned unexpected error: %v", err)
	}

	recAfterFresh, err := recRepo.FindByID(ctx, testUserID)
	if err != nil {
		t.Fatalf("failed fetching recipient: %v", err)
	}
	if recAfterFresh.DisplayName != "Updated Fresh Name" {
		t.Errorf("expected DisplayName to update to 'Updated Fresh Name', got: %s", recAfterFresh.DisplayName)
	}
	if len(recAfterFresh.Roles) != 2 {
		t.Errorf("expected 2 roles after fresh update, got: %v", recAfterFresh.Roles)
	}
}

func TestWithTxRollback(t *testing.T) {
	pool, teardown := setupTestDB(t)
	defer teardown()

	ctx := context.Background()
	recRepo := repository.NewRecipientRepository(pool)
	txManager := repository.NewTxManager(pool)

	testUserID := uuid.New()
	tNow := time.Now().UTC()

	recipient := &domain.Recipient{
		UserID:           testUserID,
		Email:            "rollback@example.com",
		DisplayName:      "Rollback Test",
		Roles:            []string{"finance_auditor"},
		IsActive:         true,
		SourceOccurredAt: tNow,
		UpdatedAt:        tNow,
	}

	simulatedErr := errors.New("simulated business error to trigger rollback")

	// Execute inside WithTx and return error
	txErr := txManager.WithTx(ctx, func(txCtx context.Context) error {
		if err := recRepo.UpsertGuarded(txCtx, recipient); err != nil {
			return err
		}
		return simulatedErr
	})

	if !errors.Is(txErr, simulatedErr) {
		t.Fatalf("expected txErr to be simulatedErr, got: %v", txErr)
	}

	// Verify the recipient was NOT committed to the database
	_, err := recRepo.FindByID(ctx, testUserID)
	if err == nil {
		_, _ = pool.Exec(ctx, "DELETE FROM notification_recipients WHERE user_id = $1", testUserID)
		t.Fatalf("expected recipient not to exist after rollback, but it was found")
	}

	if !domain.IsNotFound(err) {
		t.Errorf("expected not_found domain error, got: %v", err)
	}
}
