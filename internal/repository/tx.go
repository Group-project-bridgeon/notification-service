package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txContextKey struct{}
type postCommitKey struct{}

var (
	activeTxKey        = txContextKey{}
	postCommitHooksKey = postCommitKey{}
)

// PostCommitHook is a function executed strictly after transaction commit.
type PostCommitHook func(ctx context.Context)

type postCommitHooks struct {
	hooks []PostCommitHook
}

// HasPostCommitHooks returns true if the context contains an active post-commit hook collector.
func HasPostCommitHooks(ctx context.Context) bool {
	val, ok := ctx.Value(postCommitHooksKey).(*postCommitHooks)
	return ok && val != nil
}

// AddPostCommitHook registers a callback to run after the active transaction successfully commits.
// If no transaction context exists or the transaction is rolled back, the hook is not executed.
func AddPostCommitHook(ctx context.Context, hook PostCommitHook) {
	if val, ok := ctx.Value(postCommitHooksKey).(*postCommitHooks); ok && val != nil {
		val.hooks = append(val.hooks, hook)
	}
}

// WithNewPostCommitHooks creates a context containing a new post-commit hook collector.
func WithNewPostCommitHooks(ctx context.Context) context.Context {
	hooks := &postCommitHooks{hooks: make([]PostCommitHook, 0)}
	return context.WithValue(ctx, postCommitHooksKey, hooks)
}

// ExecutePostCommitHooks triggers all registered post-commit hooks in the collector.
func ExecutePostCommitHooks(ctx context.Context, parentCtx context.Context) {
	if val, ok := ctx.Value(postCommitHooksKey).(*postCommitHooks); ok && val != nil {
		for _, hook := range val.hooks {
			hook(parentCtx)
		}
	}
}

// DBTX is the common interface implemented by both *pgxpool.Pool and pgx.Tx.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// TxRunner abstracts running operations within a transaction context.
type TxRunner interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// TxManager coordinates local PostgreSQL ACID transactions across repository calls.
type TxManager struct {
	pool *pgxpool.Pool
}

// NewTxManager constructs a TxManager.
func NewTxManager(pool *pgxpool.Pool) *TxManager {
	return &TxManager{pool: pool}
}

// WithTx executes the supplied function within a database transaction.
// If an active transaction already exists in the context, it reuses it.
func (tm *TxManager) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, hasTx := ctx.Value(activeTxKey).(pgx.Tx); hasTx {
		return fn(ctx)
	}

	tx, err := tm.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	hooks := &postCommitHooks{hooks: make([]PostCommitHook, 0)}
	txCtx := context.WithValue(ctx, activeTxKey, tx)
	txCtx = context.WithValue(txCtx, postCommitHooksKey, hooks)

	if err := fn(txCtx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	for _, hook := range hooks.hooks {
		hook(ctx)
	}

	return nil
}

// getDB extracts an active transaction from context if available, otherwise returns the pool.
func getDB(ctx context.Context, pool *pgxpool.Pool) DBTX {
	if tx, ok := ctx.Value(activeTxKey).(pgx.Tx); ok {
		return tx
	}
	return pool
}
