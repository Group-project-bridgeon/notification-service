package sse

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ticketEntry struct {
	userID    uuid.UUID
	expiresAt time.Time
}

// TicketStore manages short-lived, single-use tickets for authenticating SSE connections.
type TicketStore struct {
	mu      sync.Mutex
	tickets map[string]ticketEntry
	stopCh  chan struct{}
}

// NewTicketStore initializes a TicketStore and starts its background cleanup routine.
func NewTicketStore() *TicketStore {
	ts := &TicketStore{
		tickets: make(map[string]ticketEntry),
		stopCh:  make(chan struct{}),
	}
	go ts.cleanupLoop()
	return ts
}

// Generate creates a cryptographically random, single-use ticket valid for ttl.
func (ts *TicketStore) Generate(userID uuid.UUID, ttl time.Duration) (string, int) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		u := uuid.New()
		copy(b, u[:])
	}
	ticket := "st_" + hex.EncodeToString(b)
	expiresInSec := int(ttl.Seconds())

	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.tickets[ticket] = ticketEntry{
		userID:    userID,
		expiresAt: time.Now().Add(ttl),
	}

	return ticket, expiresInSec
}

// Redeem validates and consumes a ticket. It is strictly single-use and deletes the ticket immediately.
func (ts *TicketStore) Redeem(ticket string) (uuid.UUID, bool) {
	if ticket == "" {
		return uuid.Nil, false
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	entry, ok := ts.tickets[ticket]
	if !ok {
		return uuid.Nil, false
	}

	// Single use: delete unconditionally on redemption attempt
	delete(ts.tickets, ticket)

	if time.Now().After(entry.expiresAt) {
		return uuid.Nil, false
	}

	return entry.userID, true
}

func (ts *TicketStore) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ts.stopCh:
			return
		case now := <-ticker.C:
			ts.mu.Lock()
			for k, v := range ts.tickets {
				if now.After(v.expiresAt) {
					delete(ts.tickets, k)
				}
			}
			ts.mu.Unlock()
		}
	}
}

// Close terminates the background cleanup routine and clears all tickets.
func (ts *TicketStore) Close() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	select {
	case <-ts.stopCh:
		// already closed
	default:
		close(ts.stopCh)
	}
	ts.tickets = make(map[string]ticketEntry)
}
