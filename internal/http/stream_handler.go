package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/notification"
	"notification-service/internal/repository"
	"notification-service/internal/sse"
)

// StreamHandler manages real-time SSE streaming connections and authentication ticket issuance.
type StreamHandler struct {
	hub         *sse.Hub
	ticketStore *sse.TicketStore
	notifRepo   repository.NotificationRepository
	logger      *slog.Logger
}

// NewStreamHandler creates a StreamHandler.
func NewStreamHandler(
	hub *sse.Hub,
	ticketStore *sse.TicketStore,
	notifRepo repository.NotificationRepository,
	logger *slog.Logger,
) *StreamHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &StreamHandler{
		hub:         hub,
		ticketStore: ticketStore,
		notifRepo:   notifRepo,
		logger:      logger,
	}
}

// IssueTicket handles POST /notifications/stream-ticket.
// Returns a single-use random ticket valid for 30 seconds.
func (h *StreamHandler) IssueTicket(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Authentication required", nil)
		return
	}

	ticket, expiresIn := h.ticketStore.Generate(userID, 30*time.Second)

	RespondSuccess(c, http.StatusOK, gin.H{
		"ticket":     ticket,
		"expires_in": expiresIn,
	})
}

// Stream handles GET /notifications/stream?ticket=...
// Establishes a persistent Server-Sent Events (SSE) connection.
func (h *StreamHandler) Stream(c *gin.Context) {
	ticket := c.Query("ticket")
	if ticket == "" {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Stream ticket query parameter is required", nil)
		return
	}

	userID, ok := h.ticketStore.Redeem(ticket)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Invalid or expired stream ticket", nil)
		return
	}

	w := c.Writer
	flusher, ok := w.(http.Flusher)
	if !ok {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Streaming unsupported by response writer", nil)
		return
	}

	// Disable caching and buffering for SSE
	c.Header("Content-Type", "text/event-stream; charset=utf-8")
	c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	// Flush headers immediately
	flusher.Flush()

	// Reconnect: honour Last-Event-ID by replaying up to 50 newer notifications for that user, then send an unread_count event.
	lastEventID := c.GetHeader("Last-Event-ID")
	if lastEventID != "" {
		h.replaySince(c.Request.Context(), w, flusher, userID, lastEventID)
	}

	// Register client with in-memory SSE Hub
	client := h.hub.Register(userID)
	if client == nil {
		h.logger.WarnContext(c.Request.Context(), "failed registering SSE client: hub is closed", "user_id", userID)
		return
	}
	defer h.hub.Unregister(client)

	// Heartbeat: send comment heartbeat : ping every 25 seconds
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	reqCtx := c.Request.Context()
	for {
		select {
		case <-reqCtx.Done():
			h.logger.InfoContext(reqCtx, "SSE client connection closed",
				"user_id", userID,
				"client_id", client.ID(),
			)
			return

		case <-h.hub.CloseCh():
			h.logger.InfoContext(reqCtx, "SSE hub shutdown, closing connection",
				"user_id", userID,
				"client_id", client.ID(),
			)
			return

		case <-ticker.C:
			if _, err := fmt.Fprintf(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()

		case evt, ok := <-client.Send():
			if !ok {
				// Client buffer overflow (slow client dropped) or closed by hub
				return
			}

			if evt.Event != "" {
				fmt.Fprintf(w, "event: %s\n", evt.Event)
			}
			if evt.ID != "" {
				fmt.Fprintf(w, "id: %s\n", evt.ID)
			}
			fmt.Fprintf(w, "data: %s\n\n", string(evt.Data))
			flusher.Flush()
		}
	}
}

// replaySince replays up to 50 newer notifications on reconnect and sends an unread_count event.
func (h *StreamHandler) replaySince(
	ctx context.Context,
	w io.Writer,
	flusher http.Flusher,
	userID uuid.UUID,
	lastEventIDStr string,
) {
	if h.notifRepo == nil {
		return
	}

	lastID, err := uuid.Parse(lastEventIDStr)
	if err != nil {
		return
	}

	lastNotif, err := h.notifRepo.GetByID(ctx, userID, lastID)
	if err != nil || lastNotif == nil {
		return
	}

	filter := domain.NotificationFilter{
		RecipientID: userID,
		From:        &lastNotif.CreatedAt,
		Sort:        "created_at",
		Page:        1,
		PageSize:    51,
	}

	list, _, err := h.notifRepo.List(ctx, filter)
	if err == nil {
		count := 0
		for _, n := range list {
			if n.ID == lastID {
				continue
			}
			if count >= 50 {
				break
			}
			resp := ToNotificationResponse(n)
			data, err := json.Marshal(resp)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: notification\nid: %s\ndata: %s\n\n", n.ID.String(), string(data))
			count++
		}
		flusher.Flush()
	}

	// Always send unread_count after replay on reconnection
	total, byCat, err := h.notifRepo.GetUnreadCounts(ctx, userID)
	if err == nil {
		counts := notification.UnreadCounts{
			Total:      total,
			ByCategory: byCat,
		}
		data, err := json.Marshal(counts)
		if err == nil {
			fmt.Fprintf(w, "event: unread_count\ndata: %s\n\n", string(data))
			flusher.Flush()
		}
	}
}
