package sse_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
	"notification-service/internal/notification"
	"notification-service/internal/sse"
)

func TestTicketStore_GenerateAndRedeem(t *testing.T) {
	ts := sse.NewTicketStore()
	defer ts.Close()

	userID := uuid.New()
	ticket, expiresIn := ts.Generate(userID, 30*time.Second)

	if ticket == "" {
		t.Fatalf("expected non-empty ticket")
	}
	if expiresIn != 30 {
		t.Errorf("expected expires_in=30, got %d", expiresIn)
	}

	// 1. First redemption should succeed
	redeemedUID, ok := ts.Redeem(ticket)
	if !ok {
		t.Fatalf("expected first redeem to succeed")
	}
	if redeemedUID != userID {
		t.Errorf("expected user ID %s, got %s", userID, redeemedUID)
	}

	// 2. Second redemption should fail (strictly single-use)
	_, ok2 := ts.Redeem(ticket)
	if ok2 {
		t.Fatalf("expected second redeem to fail for single-use ticket")
	}
}

func TestTicketStore_Expiry(t *testing.T) {
	ts := sse.NewTicketStore()
	defer ts.Close()

	userID := uuid.New()
	// Short TTL of 50ms
	ticket, _ := ts.Generate(userID, 50*time.Millisecond)

	time.Sleep(70 * time.Millisecond)

	_, ok := ts.Redeem(ticket)
	if ok {
		t.Fatalf("expected expired ticket redemption to fail")
	}
}

func TestTicketStore_InvalidTicket(t *testing.T) {
	ts := sse.NewTicketStore()
	defer ts.Close()

	_, ok := ts.Redeem("invalid_ticket_key")
	if ok {
		t.Fatalf("expected invalid ticket redemption to fail")
	}
	_, okEmpty := ts.Redeem("")
	if okEmpty {
		t.Fatalf("expected empty ticket redemption to fail")
	}
}

func TestHub_PublishNotificationAndUnreadCount(t *testing.T) {
	hub := sse.NewHub(nil)
	defer hub.Close()

	userA := uuid.New()
	userB := uuid.New()

	clientA := hub.Register(userA)
	defer hub.Unregister(clientA)

	clientB := hub.Register(userB)
	defer hub.Unregister(clientB)

	if hub.ClientCount() != 2 {
		t.Fatalf("expected 2 clients, got %d", hub.ClientCount())
	}
	if hub.UserClientCount(userA) != 1 {
		t.Fatalf("expected 1 client for userA, got %d", hub.UserClientCount(userA))
	}

	notifID := uuid.New()
	notif := &domain.Notification{
		ID:          notifID,
		RecipientID: userA,
		Title:       "Test Notification",
		Body:        "Hello user A",
		Status:      domain.NotificationStatusUnread,
		CreatedAt:   time.Now().UTC(),
	}
	counts := notification.UnreadCounts{
		Total:      3,
		ByCategory: map[string]int{"approval": 3},
	}

	ctx := context.Background()
	if err := hub.PublishNotification(ctx, notif, counts); err != nil {
		t.Fatalf("unexpected error publishing: %v", err)
	}

	// Client A should receive both notification and unread_count events
	select {
	case evt := <-clientA.Send():
		if evt.Event != "notification" {
			t.Errorf("expected event 'notification', got %s", evt.Event)
		}
		if evt.ID != notifID.String() {
			t.Errorf("expected id %s, got %s", notifID.String(), evt.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for client A notification event")
	}

	select {
	case evt := <-clientA.Send():
		if evt.Event != "unread_count" {
			t.Errorf("expected event 'unread_count', got %s", evt.Event)
		}
		var receivedCounts notification.UnreadCounts
		if err := json.Unmarshal(evt.Data, &receivedCounts); err != nil {
			t.Fatalf("failed unmarshaling counts: %v", err)
		}
		if receivedCounts.Total != 3 {
			t.Errorf("expected total 3, got %d", receivedCounts.Total)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for client A unread_count event")
	}

	// Client B should receive nothing (user isolation)
	select {
	case evt := <-clientB.Send():
		t.Fatalf("unexpected event for client B: %+v", evt)
	case <-time.After(100 * time.Millisecond):
		// Expected: no event for User B
	}
}

func TestHub_MultipleConnectionsPerUser(t *testing.T) {
	hub := sse.NewHub(nil)
	defer hub.Close()

	userID := uuid.New()
	client1 := hub.Register(userID)
	defer hub.Unregister(client1)
	client2 := hub.Register(userID)
	defer hub.Unregister(client2)

	if hub.UserClientCount(userID) != 2 {
		t.Fatalf("expected 2 connections for user, got %d", hub.UserClientCount(userID))
	}

	notif := &domain.Notification{
		ID:          uuid.New(),
		RecipientID: userID,
		Title:       "Multi-client test",
		Status:      domain.NotificationStatusUnread,
	}
	counts := notification.UnreadCounts{Total: 1}

	if err := hub.PublishNotification(context.Background(), notif, counts); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Both clients should receive the event
	for i, c := range []*sse.Client{client1, client2} {
		select {
		case evt := <-c.Send():
			if evt.Event != "notification" {
				t.Errorf("client %d: expected notification event, got %s", i+1, evt.Event)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("client %d timed out waiting for event", i+1)
		}
	}
}

func TestHub_PublishOperation(t *testing.T) {
	hub := sse.NewHub(nil)
	defer hub.Close()

	actorUID := uuid.New()
	client := hub.Register(actorUID)
	defer hub.Unregister(client)

	opPayload := map[string]any{
		"operation_id":   "op-12345",
		"operation_type": "sales_order.create",
		"state":          "failed",
		"error":          "credit limit exceeded",
	}

	if err := hub.PublishOperation(context.Background(), actorUID, opPayload); err != nil {
		t.Fatalf("failed publishing operation: %v", err)
	}

	select {
	case evt := <-client.Send():
		if evt.Event != "operation" {
			t.Fatalf("expected event 'operation', got %s", evt.Event)
		}
		var parsed map[string]any
		if err := json.Unmarshal(evt.Data, &parsed); err != nil {
			t.Fatalf("failed unmarshaling operation data: %v", err)
		}
		if parsed["operation_id"] != "op-12345" {
			t.Errorf("expected operation_id op-12345, got %v", parsed["operation_id"])
		}
		if parsed["state"] != "failed" {
			t.Errorf("expected state failed, got %v", parsed["state"])
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for operation event")
	}
}

func TestHub_SlowClientDropped(t *testing.T) {
	hub := sse.NewHub(nil)
	defer hub.Close()

	userID := uuid.New()
	slowClient := hub.Register(userID)

	// Fill the buffer to capacity (DefaultClientBufferSize = 64)
	for i := 0; i < sse.DefaultClientBufferSize; i++ {
		_ = hub.PublishOperation(context.Background(), userID, map[string]int{"idx": i})
	}

	// Sending one more event should cause slowClient buffer to overflow and get dropped
	_ = hub.PublishOperation(context.Background(), userID, map[string]string{"msg": "overflow"})

	// Give the unregister goroutine a brief moment
	time.Sleep(50 * time.Millisecond)

	// Hub should now show 0 active connections for user
	if hub.UserClientCount(userID) != 0 {
		t.Fatalf("expected slow client to be dropped, remaining: %d", hub.UserClientCount(userID))
	}

	// Drain events until channel is closed
	for range slowClient.Send() {
	}
}

func TestHub_CleanShutdown(t *testing.T) {
	hub := sse.NewHub(nil)

	userID := uuid.New()
	client := hub.Register(userID)

	hub.Close()

	// client channel should be closed
	select {
	case _, ok := <-client.Send():
		if ok {
			t.Fatalf("expected client send channel to be closed on hub shutdown")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for client channel to close")
	}

	// Hub should be closed and accept no more clients
	if hub.Register(userID) != nil {
		t.Fatalf("expected Register to return nil on closed hub")
	}
}

func TestHub_ConcurrentRaceSafety(t *testing.T) {
	hub := sse.NewHub(nil)
	defer hub.Close()

	var wg sync.WaitGroup
	const numUsers = 10
	const opsPerWorker = 50

	users := make([]uuid.UUID, numUsers)
	for i := 0; i < numUsers; i++ {
		users[i] = uuid.New()
	}

	// Worker goroutines concurrently registering, publishing, and unregistering
	for i := 0; i < numUsers; i++ {
		u := users[i]
		wg.Add(1)
		go func(uid uuid.UUID) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				c := hub.Register(uid)
				if c != nil {
					_ = hub.PublishOperation(context.Background(), uid, fmt.Sprintf("msg-%d", j))
					// Read or drop
					select {
					case <-c.Send():
					default:
					}
					hub.Unregister(c)
				}
			}
		}(u)
	}

	wg.Wait()
}
