package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/notification"
)

// DefaultClientBufferSize is the bounded queue size per SSE connection.
const DefaultClientBufferSize = 64

// Event represents a single Server-Sent Event to be pushed downstream to clients.
type Event struct {
	Event string `json:"event"`
	ID    string `json:"id,omitempty"`
	Data  []byte `json:"data"`
}

// Client represents an active HTTP SSE connection.
type Client struct {
	id        uuid.UUID
	userID    uuid.UUID
	send      chan *Event
	closeOnce sync.Once
	closed    bool
}

// ID returns the unique connection ID.
func (c *Client) ID() uuid.UUID {
	return c.id
}

// UserID returns the recipient user ID.
func (c *Client) UserID() uuid.UUID {
	return c.userID
}

// Send returns the read-only event channel.
func (c *Client) Send() <-chan *Event {
	return c.send
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		c.closed = true
		close(c.send)
	})
}

// Hub manages live client connections and dispatches events by user ID.
// It is fully safe for concurrent use, bounds client queues, and drops slow consumers.
type Hub struct {
	mu      sync.RWMutex
	clients map[uuid.UUID]map[*Client]struct{}
	closed  bool
	closeCh chan struct{}
	logger  *slog.Logger
}

// NewHub constructs an in-memory concurrent SSE hub.
func NewHub(logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		clients: make(map[uuid.UUID]map[*Client]struct{}),
		closeCh: make(chan struct{}),
		logger:  logger,
	}
}

// Register connects a new client for a given user ID.
func (h *Hub) Register(userID uuid.UUID) *Client {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}

	c := &Client{
		id:     uuid.New(),
		userID: userID,
		send:   make(chan *Event, DefaultClientBufferSize),
	}

	if _, ok := h.clients[userID]; !ok {
		h.clients[userID] = make(map[*Client]struct{})
	}
	h.clients[userID][c] = struct{}{}

	h.logger.Debug("SSE connection registered",
		"user_id", userID,
		"client_id", c.id,
		"active_connections_for_user", len(h.clients[userID]),
	)

	return c
}

// Unregister disconnects and cleans up a client connection.
func (h *Hub) Unregister(c *Client) {
	if c == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	userClients, ok := h.clients[c.userID]
	if ok {
		delete(userClients, c)
		if len(userClients) == 0 {
			delete(h.clients, c.userID)
		}
	}
	c.close()

	h.logger.Debug("SSE connection unregistered",
		"user_id", c.userID,
		"client_id", c.id,
	)
}

// CloseCh returns a channel that is closed when the hub is shutting down.
func (h *Hub) CloseCh() <-chan struct{} {
	return h.closeCh
}

// ClientCount returns the total number of connected clients across all users.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, userClients := range h.clients {
		total += len(userClients)
	}
	return total
}

// UserClientCount returns the number of active connections for a specific user.
func (h *Hub) UserClientCount(userID uuid.UUID) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.clients[userID])
}

// sendToUser sends an event to all active connections for a user.
// Slow clients whose buffers are full are dropped immediately without blocking others.
func (h *Hub) sendToUser(userID uuid.UUID, evt *Event) {
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return
	}

	userClients, ok := h.clients[userID]
	if !ok || len(userClients) == 0 {
		h.mu.RUnlock()
		return
	}

	targets := make([]*Client, 0, len(userClients))
	for c := range userClients {
		targets = append(targets, c)
	}
	h.mu.RUnlock()

	var slow []*Client
	for _, c := range targets {
		select {
		case c.send <- evt:
		default:
			h.logger.Warn("dropping slow SSE client with full buffer",
				"user_id", c.userID,
				"client_id", c.id,
			)
			slow = append(slow, c)
		}
	}

	for _, c := range slow {
		h.Unregister(c)
	}
}

// PublishNotification implements notification.NotificationPublisher.
// It pushes both the full notification object and updated unread counts to the recipient.
func (h *Hub) PublishNotification(ctx context.Context, notif *domain.Notification, counts notification.UnreadCounts) error {
	if notif == nil {
		return nil
	}

	notifData, err := json.Marshal(notif)
	if err != nil {
		return fmt.Errorf("failed marshaling notification for SSE: %w", err)
	}

	notifEvt := &Event{
		Event: "notification",
		ID:    notif.ID.String(),
		Data:  notifData,
	}
	h.sendToUser(notif.RecipientID, notifEvt)

	// Also send updated unread count
	unreadData, err := json.Marshal(counts)
	if err != nil {
		return fmt.Errorf("failed marshaling unread counts for SSE: %w", err)
	}

	unreadEvt := &Event{
		Event: "unread_count",
		Data:  unreadData,
	}
	h.sendToUser(notif.RecipientID, unreadEvt)

	return nil
}

// PublishUnreadCount sends an updated unread count to a user.
func (h *Hub) PublishUnreadCount(ctx context.Context, userID uuid.UUID, counts notification.UnreadCounts) error {
	unreadData, err := json.Marshal(counts)
	if err != nil {
		return fmt.Errorf("failed marshaling unread counts for SSE: %w", err)
	}

	unreadEvt := &Event{
		Event: "unread_count",
		Data:  unreadData,
	}
	h.sendToUser(userID, unreadEvt)
	return nil
}

// PublishOperation implements ingest.OperationPublisher.
// It delivers operation result events directly to the target actor user.
func (h *Hub) PublishOperation(ctx context.Context, userID uuid.UUID, op any) error {
	var opData []byte
	var err error

	switch v := op.(type) {
	case []byte:
		opData = v
	case json.RawMessage:
		opData = v
	case string:
		opData = []byte(v)
	default:
		opData, err = json.Marshal(op)
		if err != nil {
			return fmt.Errorf("failed marshaling operation for SSE: %w", err)
		}
	}

	evt := &Event{
		Event: "operation",
		Data:  opData,
	}
	h.sendToUser(userID, evt)
	return nil
}

// Close gracefully closes all active client connections and shuts down the hub.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}
	h.closed = true
	close(h.closeCh)

	for _, userClients := range h.clients {
		for c := range userClients {
			c.close()
		}
	}
	h.clients = make(map[uuid.UUID]map[*Client]struct{})
	h.logger.Info("SSE Hub shut down cleanly; all client connections terminated")
}
