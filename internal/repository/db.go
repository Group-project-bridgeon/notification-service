package repository

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"notification-service/internal/config"
)

// DB wraps a pgxpool.Pool and exposes database lifecycle methods.
type DB struct {
	Pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewDB initializes a connection pool to PostgreSQL.
func NewDB(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*DB, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.DBMaxOpenConns)
	poolConfig.MinConns = int32(cfg.DBMaxIdleConns)
	poolConfig.MaxConnLifetime = cfg.DBConnMaxLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx connection pool: %w", err)
	}

	// Initial ping verification
	if err := pool.Ping(ctx); err != nil {
		logger.Warn("initial database ping failed (will retry on readiness check)", slog.String("error", err.Error()))
	} else {
		logger.Info("database connection pool established successfully")
	}

	return &DB{
		Pool:   pool,
		logger: logger,
	}, nil
}

// Ping performs a liveness probe on the connection pool.
func (db *DB) Ping(ctx context.Context) error {
	if db == nil || db.Pool == nil {
		return fmt.Errorf("database connection pool is uninitialized")
	}
	return db.Pool.Ping(ctx)
}

// Close shuts down all connections in the pool.
func (db *DB) Close() {
	if db != nil && db.Pool != nil {
		db.Pool.Close()
		db.logger.Info("database connection pool closed")
	}
}
