package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/domain"
)

type statsRepo struct {
	pool *pgxpool.Pool
}

func NewStatsRepository(pool *pgxpool.Pool) StatsRepository {
	return &statsRepo{pool: pool}
}

func (r *statsRepo) GetNotificationStats(ctx context.Context, since time.Time) (*domain.NotificationStats, error) {
	db := getDB(ctx, r.pool)

	stats := &domain.NotificationStats{
		ByCategory: make(map[string]int),
		ByPriority: make(map[string]int),
		Period:     "24h",
	}

	// 1. Category counts
	const catQuery = `
		SELECT category, COUNT(*)
		FROM notifications
		WHERE created_at >= $1
		GROUP BY category;
	`
	catRows, err := db.Query(ctx, catQuery, since)
	if err == nil {
		for catRows.Next() {
			var cat string
			var count int
			if err := catRows.Scan(&cat, &count); err == nil {
				stats.ByCategory[cat] = count
			}
		}
		catRows.Close()
	}

	// 2. Priority counts
	const prioQuery = `
		SELECT priority, COUNT(*)
		FROM notifications
		WHERE created_at >= $1
		GROUP BY priority;
	`
	prioRows, err := db.Query(ctx, prioQuery, since)
	if err == nil {
		for prioRows.Next() {
			var prio string
			var count int
			if err := prioRows.Scan(&prio, &count); err == nil {
				stats.ByPriority[prio] = count
			}
		}
		prioRows.Close()
	}

	// 3. Delivery success rate
	const delivQuery = `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'sent')
		FROM notification_deliveries
		WHERE created_at >= $1;
	`
	_ = db.QueryRow(ctx, delivQuery, since).Scan(&stats.TotalDeliveries, &stats.SuccessfulDeliveries)
	if stats.TotalDeliveries > 0 {
		stats.DeliverySuccessRate = float64(stats.SuccessfulDeliveries) / float64(stats.TotalDeliveries)
	}

	// 4. Dead letters count
	const dlQuery = `
		SELECT COUNT(*)
		FROM notification_dead_letters
		WHERE failed_at >= $1;
	`
	_ = db.QueryRow(ctx, dlQuery, since).Scan(&stats.DeadLettersCount)

	// 5. Email failures count
	const emailFailQuery = `
		SELECT COUNT(*)
		FROM email_messages
		WHERE created_at >= $1 AND status IN ('failed', 'bounced');
	`
	_ = db.QueryRow(ctx, emailFailQuery, since).Scan(&stats.EmailFailuresCount)

	return stats, nil
}
