package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"notification-service/internal/domain"
)

type Config struct {
	Brokers       string
	EventType     string
	Count         int
	UserID        string
	Scenario      string
	Via           string
	HTTPURL       string
	InternalToken string
}

func main() {
	defaultBrokers := os.Getenv("KAFKA_BROKERS")
	if defaultBrokers == "" {
		defaultBrokers = "localhost:9092"
	}
	defaultToken := os.Getenv("INTERNAL_SERVICE_TOKEN")
	if defaultToken == "" {
		defaultToken = "internal-secret"
	}

	cfg := Config{}
	flag.StringVar(&cfg.Brokers, "brokers", defaultBrokers, "Comma-separated Kafka broker addresses")
	flag.StringVar(&cfg.EventType, "event", "", "Specific event_type to produce")
	flag.IntVar(&cfg.Count, "count", 1, "Number of events to emit")
	flag.StringVar(&cfg.UserID, "user", "", "User UUID to target or associate as actor")
	flag.StringVar(&cfg.Scenario, "scenario", "", "Scenario to run: sales-order-happy, sales-order-failure, duplicate, out-of-order, poison, bulk, otp-email")
	flag.StringVar(&cfg.Via, "via", "kafka", "Transport: kafka or http (default: kafka)")
	flag.StringVar(&cfg.HTTPURL, "http-url", "http://localhost:8080", "Notification service HTTP base URL")
	flag.StringVar(&cfg.InternalToken, "internal-token", defaultToken, "Internal service token for HTTP ingestion")
	flag.Parse()

	// Handle otp-email directly over HTTP (never uses Kafka)
	if cfg.Scenario == "otp-email" {
		runOTPEmailScenario(cfg)
		return
	}

	// Create dispatcher according to --via
	var dispatcher Dispatcher
	if strings.ToLower(cfg.Via) == "http" {
		dispatcher = &HTTPDispatcher{
			BaseURL:       cfg.HTTPURL,
			InternalToken: cfg.InternalToken,
			Client:        &http.Client{Timeout: 10 * time.Second},
		}
	} else {
		brokers := strings.Split(cfg.Brokers, ",")
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ProducerBatchMaxBytes(10000000), // support large payloads for poison test
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed creating Kafka client: %v\n", err)
			os.Exit(1)
		}
		defer cl.Close()
		dispatcher = &KafkaDispatcher{Client: cl}
	}

	ctx := context.Background()

	// Execute scenario or standalone event
	switch cfg.Scenario {
	case "sales-order-happy":
		runSalesOrderHappy(ctx, dispatcher, cfg)
	case "sales-order-failure":
		runSalesOrderFailure(ctx, dispatcher, cfg)
	case "duplicate":
		runDuplicateScenario(ctx, dispatcher, cfg)
	case "out-of-order":
		runOutOfOrderScenario(ctx, dispatcher, cfg)
	case "poison":
		runPoisonScenario(ctx, dispatcher, cfg)
	case "bulk":
		runBulkScenario(ctx, dispatcher, cfg)
	default:
		if cfg.EventType != "" {
			runSingleEvent(ctx, dispatcher, cfg)
		} else {
			fmt.Println("No scenario or event specified. Use --scenario or --event. See --help.")
			flag.Usage()
		}
	}
}

// Dispatcher abstracts event output (Kafka or HTTP).
type Dispatcher interface {
	Dispatch(ctx context.Context, topic string, key string, envelope *domain.EventEnvelope) error
	DispatchRaw(ctx context.Context, topic string, key string, raw []byte) error
}

type KafkaDispatcher struct {
	Client *kgo.Client
}

func (kd *KafkaDispatcher) Dispatch(ctx context.Context, topic string, key string, env *domain.EventEnvelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return kd.DispatchRaw(ctx, topic, key, b)
}

func (kd *KafkaDispatcher) DispatchRaw(ctx context.Context, topic string, key string, raw []byte) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: raw,
	}
	res := kd.Client.ProduceSync(ctx, rec)
	return res.FirstErr()
}

type HTTPDispatcher struct {
	BaseURL       string
	InternalToken string
	Client        *http.Client
}

func (hd *HTTPDispatcher) Dispatch(ctx context.Context, _ string, _ string, env *domain.EventEnvelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return hd.DispatchRaw(ctx, "", "", b)
}

func (hd *HTTPDispatcher) DispatchRaw(ctx context.Context, _ string, _ string, raw []byte) error {
	url := strings.TrimRight(hd.BaseURL, "/") + "/internal/events"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", hd.InternalToken)

	resp, err := hd.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// TopicForEventType routes each event type to its canonical topic per KAFKA_CONTRACT.md.
func TopicForEventType(eventType string) string {
	parts := strings.Split(eventType, ".")
	prefix := parts[0]
	switch prefix {
	case "user", "role", "identity":
		return "erp.identity.events"
	case "purchase_request", "purchase_order", "goods_receipt", "purchase_return", "purchasing":
		return "erp.purchasing.events"
	case "stock", "reservation", "stock_adjustment", "pick_list", "transfer", "inventory":
		return "erp.inventory.events"
	case "sales_order", "delivery", "quotation", "sales":
		return "erp.sales.events"
	case "customer_return", "supplier_return", "returns":
		return "erp.returns.events"
	case "invoice", "payment", "credit_note", "credit_limit", "expense", "finance":
		return "erp.finance.events"
	case "operation", "saga", "approval", "comment", "platform":
		return "erp.platform.events"
	default:
		return "erp.platform.events"
	}
}

// 1. Scenario: sales-order-happy
func runSalesOrderHappy(ctx context.Context, d Dispatcher, cfg Config) {
	corrID := uuid.New()
	orderID := uuid.New()
	orderNo := fmt.Sprintf("SO-%d", 1000+rand.Intn(9000))
	var actorID *uuid.UUID
	if cfg.UserID != "" {
		if u, err := uuid.Parse(cfg.UserID); err == nil {
			actorID = &u
		}
	}

	events := []struct {
		eventType     string
		aggregateType string
		aggregateID   string
		payload       map[string]any
	}{
		{
			eventType:     "sales_order.confirmed",
			aggregateType: "sales_order",
			aggregateID:   orderID.String(),
			payload: map[string]any{
				"order_id":      orderID.String(),
				"order_no":      orderNo,
				"customer_name": "Acme Industrial Supplies",
				"total":         "14250.00",
				"actor_user_id": cfg.UserID,
			},
		},
		{
			eventType:     "stock.reserved",
			aggregateType: "sales_order",
			aggregateID:   orderID.String(),
			payload: map[string]any{
				"order_id": orderID.String(),
				"order_no": orderNo,
				"items":    2,
			},
		},
		{
			eventType:     "invoice.issued",
			aggregateType: "invoice",
			aggregateID:   uuid.New().String(),
			payload: map[string]any{
				"invoice_no": fmt.Sprintf("INV-%d", 2000+rand.Intn(8000)),
				"order_no":   orderNo,
				"total":      "14250.00",
			},
		},
		{
			eventType:     "delivery.dispatched",
			aggregateType: "delivery",
			aggregateID:   uuid.New().String(),
			payload: map[string]any{
				"delivery_id": uuid.New().String(),
				"order_no":    orderNo,
				"carrier":     "FastFreight",
				"tracking_no": fmt.Sprintf("TRK-%d", 50000+rand.Intn(50000)),
			},
		},
		{
			eventType:     "delivery.delivered",
			aggregateType: "delivery",
			aggregateID:   uuid.New().String(),
			payload: map[string]any{
				"order_no": orderNo,
				"signed_by": "Warehouse Mgr",
			},
		},
		{
			eventType:     "payment.received",
			aggregateType: "payment",
			aggregateID:   uuid.New().String(),
			payload: map[string]any{
				"order_no": orderNo,
				"amount":   "14250.00",
				"method":   "wire_transfer",
			},
		},
	}

	fmt.Printf("[SIMULATE] Running scenario: sales-order-happy (correlation_id: %s, order_no: %s)\n", corrID, orderNo)
	for i, ev := range events {
		pBytes, _ := json.Marshal(ev.payload)
		env := &domain.EventEnvelope{
			EventID:       uuid.New(),
			EventType:     ev.eventType,
			EventVersion:  "1.0",
			OccurredAt:    time.Now().UTC(),
			Producer:      "order-service",
			AggregateType: ev.aggregateType,
			AggregateID:   ev.aggregateID,
			CorrelationID: &corrID,
			ActorUserID:   actorID,
			Payload:       pBytes,
		}
		topic := TopicForEventType(ev.eventType)
		if err := d.Dispatch(ctx, topic, ev.aggregateID, env); err != nil {
			fmt.Printf("  [%d/%d] FAIL %s: %v\n", i+1, len(events), ev.eventType, err)
			return
		}
		fmt.Printf("  [%d/%d] Emitted %-25s -> %s (key=%s)\n", i+1, len(events), ev.eventType, topic, ev.aggregateID)
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println("[SIMULATE] Scenario sales-order-happy completed successfully.")
}

// 2. Scenario: sales-order-failure
func runSalesOrderFailure(ctx context.Context, d Dispatcher, cfg Config) {
	corrID := uuid.New()
	orderID := uuid.New()
	opID := uuid.New()
	orderNo := fmt.Sprintf("SO-FAIL-%d", 1000+rand.Intn(9000))
	var actorID *uuid.UUID
	if cfg.UserID != "" {
		if u, err := uuid.Parse(cfg.UserID); err == nil {
			actorID = &u
		}
	}

	events := []struct {
		eventType     string
		aggregateType string
		aggregateID   string
		payload       map[string]any
	}{
		{
			eventType:     "sales_order.confirmed",
			aggregateType: "sales_order",
			aggregateID:   orderID.String(),
			payload: map[string]any{
				"order_id": orderID.String(),
				"order_no": orderNo,
				"total":    "8500.00",
			},
		},
		{
			eventType:     "stock.reservation_failed",
			aggregateType: "inventory",
			aggregateID:   orderID.String(),
			payload: map[string]any{
				"order_no": orderNo,
				"reason":   "insufficient_stock",
				"item_sku": "SKU-491",
			},
		},
		{
			eventType:     "sales_order.saga_failed",
			aggregateType: "sales_order",
			aggregateID:   orderID.String(),
			payload: map[string]any{
				"order_no": orderNo,
				"reason":   "Stock reservation failed",
			},
		},
		{
			eventType:     "operation.failed",
			aggregateType: "operation",
			aggregateID:   opID.String(),
			payload: map[string]any{
				"operation_id":   opID.String(),
				"operation_type": "sales_order.confirm",
				"entity": map[string]any{
					"type": "sales_order",
					"id":   orderID.String(),
				},
				"state": "failed",
				"error": map[string]any{
					"code":    "insufficient_inventory",
					"message": "Stock reservation failed for item SKU-491",
					"details": []map[string]any{
						{"field": "line_items[0].quantity", "issue": "insufficient_stock"},
					},
				},
				"compensation":  "Payment authorization released",
				"actor_user_id": cfg.UserID,
			},
		},
		{
			eventType:     "saga.compensation_started",
			aggregateType: "saga",
			aggregateID:   orderID.String(),
			payload: map[string]any{
				"saga_type": "order_checkout",
				"order_no":  orderNo,
			},
		},
	}

	fmt.Printf("[SIMULATE] Running scenario: sales-order-failure (correlation_id: %s, order_no: %s)\n", corrID, orderNo)
	for i, ev := range events {
		pBytes, _ := json.Marshal(ev.payload)
		env := &domain.EventEnvelope{
			EventID:       uuid.New(),
			EventType:     ev.eventType,
			EventVersion:  "1.0",
			OccurredAt:    time.Now().UTC(),
			Producer:      "order-service",
			AggregateType: ev.aggregateType,
			AggregateID:   ev.aggregateID,
			CorrelationID: &corrID,
			ActorUserID:   actorID,
			Payload:       pBytes,
		}
		topic := TopicForEventType(ev.eventType)
		if err := d.Dispatch(ctx, topic, ev.aggregateID, env); err != nil {
			fmt.Printf("  [%d/%d] FAIL %s: %v\n", i+1, len(events), ev.eventType, err)
			return
		}
		fmt.Printf("  [%d/%d] Emitted %-25s -> %s (key=%s)\n", i+1, len(events), ev.eventType, topic, ev.aggregateID)
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println("[SIMULATE] Scenario sales-order-failure completed.")
}

// 3. Scenario: duplicate
func runDuplicateScenario(ctx context.Context, d Dispatcher, _ Config) {
	fixedEventID := uuid.New()
	aggID := uuid.New().String()
	fmt.Printf("[SIMULATE] Running scenario: duplicate (event_id: %s, sending 3 identical copies)\n", fixedEventID)

	pBytes, _ := json.Marshal(map[string]any{
		"order_no": "SO-DUP-9999",
		"total":    "250.00",
	})

	env := &domain.EventEnvelope{
		EventID:       fixedEventID,
		EventType:     "sales_order.confirmed",
		EventVersion:  "1.0",
		OccurredAt:    time.Now().UTC(),
		Producer:      "order-service",
		AggregateType: "sales_order",
		AggregateID:   aggID,
		Payload:       pBytes,
	}

	topic := TopicForEventType(env.EventType)
	for i := 1; i <= 3; i++ {
		if err := d.Dispatch(ctx, topic, aggID, env); err != nil {
			fmt.Printf("  Attempt %d failed: %v\n", i, err)
			return
		}
		fmt.Printf("  Attempt %d: Emitted event_id=%s\n", i, fixedEventID)
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Println("[SIMULATE] Scenario duplicate completed (Inbox deduplication should process attempt 1, skip 2 & 3).")
}

// 4. Scenario: out-of-order
func runOutOfOrderScenario(ctx context.Context, d Dispatcher, _ Config) {
	userID := uuid.New()
	tNewer := time.Now().UTC()
	tOlder := tNewer.Add(-1 * time.Hour)

	fmt.Printf("[SIMULATE] Running scenario: out-of-order identity events for user %s\n", userID)

	// Step 1: Send newer event (T2)
	pNewer, _ := json.Marshal(map[string]any{
		"user_id":      userID.String(),
		"display_name": "Alice - Newer Name",
		"email":        "alice.new@example.com",
		"roles":        []string{"manager"},
	})
	envNewer := &domain.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     "user.updated",
		EventVersion:  "1.0",
		OccurredAt:    tNewer,
		Producer:      "auth-service",
		AggregateType: "user",
		AggregateID:   userID.String(),
		Payload:       pNewer,
	}
	if err := d.Dispatch(ctx, "erp.identity.events", userID.String(), envNewer); err != nil {
		fmt.Printf("Failed sending newer event: %v\n", err)
		return
	}
	fmt.Printf("  1. Emitted NEWER event (occurred_at: %s, display_name: %q)\n", tNewer.Format(time.RFC3339), "Alice - Newer Name")

	time.Sleep(100 * time.Millisecond)

	// Step 2: Send older event (T1) arriving later
	pOlder, _ := json.Marshal(map[string]any{
		"user_id":      userID.String(),
		"display_name": "Alice - Stale Name",
		"email":        "alice.stale@example.com",
		"roles":        []string{"employee"},
	})
	envOlder := &domain.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     "user.updated",
		EventVersion:  "1.0",
		OccurredAt:    tOlder,
		Producer:      "auth-service",
		AggregateType: "user",
		AggregateID:   userID.String(),
		Payload:       pOlder,
	}
	if err := d.Dispatch(ctx, "erp.identity.events", userID.String(), envOlder); err != nil {
		fmt.Printf("Failed sending older event: %v\n", err)
		return
	}
	fmt.Printf("  2. Emitted OLDER event (occurred_at: %s, display_name: %q)\n", tOlder.Format(time.RFC3339), "Alice - Stale Name")

	fmt.Println("[SIMULATE] Scenario out-of-order completed (Recipient projection should keep Newer Name).")
}

// 5. Scenario: poison
func runPoisonScenario(ctx context.Context, d Dispatcher, _ Config) {
	topic := "erp.platform.events"
	fmt.Printf("[SIMULATE] Running scenario: poison (producing 4 corrupted messages to %s)\n", topic)

	// A. Garbage bytes
	garbage := []byte("INVALID_GARBAGE_BYTES_$%^&*!#NOT_JSON")
	_ = d.DispatchRaw(ctx, topic, "poison-1", garbage)
	fmt.Println("  1. Emitted garbage non-JSON bytes")

	// B. Valid JSON that is not an envelope
	nonEnvelope := []byte(`{"hello":"world","missing":"envelope_fields"}`)
	_ = d.DispatchRaw(ctx, topic, "poison-2", nonEnvelope)
	fmt.Println("  2. Emitted valid JSON missing required envelope fields")

	// C. Oversized payload (>1MB)
	largeData := strings.Repeat("A", 1024*1024+100) // ~1.05MB
	oversized := []byte(fmt.Sprintf(`{"event_id":"%s","event_type":"operation.started","payload":"%s"}`, uuid.New().String(), largeData))
	_ = d.DispatchRaw(ctx, topic, "poison-3", oversized)
	fmt.Println("  3. Emitted oversized payload (> 1MB)")

	// D. Tombstone (nil value)
	_ = d.DispatchRaw(ctx, topic, "tombstone-key", nil)
	fmt.Println("  4. Emitted nil-value tombstone record")

	fmt.Println("[SIMULATE] Scenario poison completed (DLQ and tombstone skip verification).")
}

// 6. Scenario: bulk
func runBulkScenario(ctx context.Context, d Dispatcher, cfg Config) {
	totalEvents := 5000
	if cfg.Count > 1 {
		totalEvents = cfg.Count
	}
	numAggregates := 500
	fmt.Printf("[SIMULATE] Running scenario: bulk (%d events across %d aggregates)\n", totalEvents, numAggregates)

	start := time.Now()
	for i := 0; i < totalEvents; i++ {
		aggID := fmt.Sprintf("agg-%03d", i%numAggregates)
		pBytes, _ := json.Marshal(map[string]any{
			"order_no": fmt.Sprintf("BULK-%d", i),
			"index":    i,
			"total":    "99.00",
		})
		env := &domain.EventEnvelope{
			EventID:       uuid.New(),
			EventType:     "sales_order.confirmed",
			EventVersion:  "1.0",
			OccurredAt:    time.Now().UTC(),
			Producer:      "bulk-simulator",
			AggregateType: "sales_order",
			AggregateID:   aggID,
			Payload:       pBytes,
		}
		if err := d.Dispatch(ctx, "erp.sales.events", aggID, env); err != nil {
			fmt.Printf("Failed bulk emit at %d: %v\n", i, err)
			return
		}
		if (i+1)%1000 == 0 {
			fmt.Printf("  Dispatched %d / %d events...\n", i+1, totalEvents)
		}
	}
	dur := time.Since(start)
	rate := float64(totalEvents) / dur.Seconds()
	fmt.Printf("[SIMULATE] Bulk completed: %d events in %v (%.1f events/sec)\n", totalEvents, dur, rate)
}

// 7. Scenario: otp-email (Direct HTTP POST to /internal/email/send)
func runOTPEmailScenario(cfg Config) {
	fmt.Println("[SIMULATE] Running scenario: otp-email (HTTP POST /internal/email/send)")

	url := strings.TrimRight(cfg.HTTPURL, "/") + "/internal/email/send"
	bodyMap := map[string]any{
		"template_key": "auth.verify_email_otp",
		"to":           "alex.developer@example.com",
		"variables": map[string]any{
			"name":     "Alex Developer",
			"otp_code": "842910",
		},
	}
	b, _ := json.Marshal(bodyMap)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		fmt.Printf("Failed constructing request: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", cfg.InternalToken)
	req.Header.Set("Idempotency-Key", uuid.New().String())

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("HTTP request failed: %v\n", err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	fmt.Printf("[SIMULATE] Status: %d\nResponse: %s\n", resp.StatusCode, string(respBody))
	fmt.Println("[SIMULATE] Scenario otp-email completed (Rendered email output logged by LogSender).")
}

func runSingleEvent(ctx context.Context, d Dispatcher, cfg Config) {
	topic := TopicForEventType(cfg.EventType)
	aggID := uuid.New().String()
	pBytes, _ := json.Marshal(map[string]any{
		"sample_data": "simulator_run",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	})

	for i := 0; i < cfg.Count; i++ {
		env := &domain.EventEnvelope{
			EventID:       uuid.New(),
			EventType:     cfg.EventType,
			EventVersion:  "1.0",
			OccurredAt:    time.Now().UTC(),
			Producer:      "cmd-simulator",
			AggregateType: "entity",
			AggregateID:   aggID,
			Payload:       pBytes,
		}
		if err := d.Dispatch(ctx, topic, aggID, env); err != nil {
			fmt.Printf("Failed emitting event: %v\n", err)
			return
		}
		fmt.Printf("Emitted %s -> %s (event_id=%s)\n", cfg.EventType, topic, env.EventID)
	}
}
