package ingest

import (
	"context"
	"errors"

	"notification-service/internal/domain"
)

// ErrNoRuleMatched is returned by a Router when no routing rule applies to the event.
// It is NOT a failure: the inbox row is marked "ignored" and the offset is committed cleanly.
var ErrNoRuleMatched = errors.New("no notification rule matched")

// Router routes a validated event envelope to notification creation logic.
// Implementations run inside the active database transaction propagated through ctx.
type Router interface {
	Route(ctx context.Context, env *domain.EventEnvelope) error
}

// NoopRouter is the default Router until notification fanout is implemented in Prompt 5.
// Every event is cleanly marked "ignored" in the inbox.
type NoopRouter struct{}

func (NoopRouter) Route(_ context.Context, _ *domain.EventEnvelope) error {
	return ErrNoRuleMatched
}
