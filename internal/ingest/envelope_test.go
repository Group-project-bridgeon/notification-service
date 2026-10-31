package ingest

import (
	"strings"
	"testing"
	"time"
)

// validEnvelopeJSON returns a minimal valid event JSON for use in tests.
func validEnvelopeJSON() []byte {
	return []byte(`{
		"event_id":       "7b8e1460-63bf-4787-8e68-07df95a5f782",
		"event_type":     "sales_order.created",
		"event_version":  "1",
		"occurred_at":    "2026-10-12T09:30:00Z",
		"producer":       "sales-service",
		"aggregate_type": "sales_order",
		"aggregate_id":   "9d1c08be-2415-46aa-bd08-8e6ca6f0e495",
		"correlation_id": "8fa8db3b-8c65-4f7b-9e45-d8aa497f1981",
		"actor_user_id":  "e81d8590-bfae-4d8b-9d41-e125032b4943",
		"payload":        {"order_number":"SO-001","total":"14250.00"}
	}`)
}

func TestParseAndValidate_Valid(t *testing.T) {
	env, err := ParseAndValidate(validEnvelopeJSON())
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if env.EventType != "sales_order.created" {
		t.Errorf("unexpected event_type: %s", env.EventType)
	}
	if !env.OccurredAt.UTC().Equal(env.OccurredAt) {
		t.Error("occurred_at should be normalised to UTC")
	}
	loc, _ := time.LoadLocation("UTC")
	if env.OccurredAt.Location() != loc {
		t.Error("occurred_at timezone should be UTC")
	}
	if env.CorrelationID == nil {
		t.Error("expected correlation_id to be parsed")
	}
	if env.ActorUserID == nil {
		t.Error("expected actor_user_id to be parsed")
	}
}

func TestParseAndValidate_OversizedMessage(t *testing.T) {
	big := make([]byte, MaxMessageBytes+1)
	big[0] = '{'
	big[MaxMessageBytes] = '}'
	_, err := ParseAndValidate(big)
	if !IsPermanent(err) {
		t.Fatalf("expected PermanentError, got: %v", err)
	}
}

func TestParseAndValidate_InvalidJSON(t *testing.T) {
	_, err := ParseAndValidate([]byte(`not-json`))
	if !IsPermanent(err) {
		t.Fatalf("expected PermanentError for invalid JSON, got: %v", err)
	}
}

func TestParseAndValidate_MissingRequiredFields(t *testing.T) {
	cases := []struct {
		name  string
		patch string // field to remove from JSON
	}{
		{"missing event_id", `"event_id"`},
		{"missing event_type", `"event_type"`},
		{"missing occurred_at", `"occurred_at"`},
		{"missing producer", `"producer"`},
		{"missing aggregate_type", `"aggregate_type"`},
		{"missing aggregate_id", `"aggregate_id"`},
	}

	base := string(validEnvelopeJSON())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Remove field by replacing its key with a dummy key
			broken := strings.ReplaceAll(base, tc.patch, `"__removed__"`)
			_, err := ParseAndValidate([]byte(broken))
			if !IsPermanent(err) {
				t.Fatalf("expected PermanentError, got: %v", err)
			}
		})
	}
}

func TestParseAndValidate_InvalidEventIDUUID(t *testing.T) {
	bad := strings.ReplaceAll(string(validEnvelopeJSON()),
		`"7b8e1460-63bf-4787-8e68-07df95a5f782"`, `"not-a-uuid"`)
	_, err := ParseAndValidate([]byte(bad))
	if !IsPermanent(err) {
		t.Fatalf("expected PermanentError for bad event_id, got: %v", err)
	}
}

func TestParseAndValidate_BadEventTypePattern(t *testing.T) {
	cases := []string{
		"NoUppercase.event",    // uppercase
		"nodot",                // no dot
		".leading_dot",         // leading dot
		"trailing.dot.",        // trailing dot
		"bad type",             // space
		"type/slash",           // slash
	}
	base := string(validEnvelopeJSON())
	for _, et := range cases {
		t.Run(et, func(t *testing.T) {
			broken := strings.ReplaceAll(base, `"sales_order.created"`, `"`+et+`"`)
			_, err := ParseAndValidate([]byte(broken))
			if !IsPermanent(err) {
				t.Fatalf("expected PermanentError for event_type %q, got: %v", et, err)
			}
		})
	}
}

func TestParseAndValidate_ValidEventTypePatterns(t *testing.T) {
	valid := []string{
		"user.created",
		"purchase_order.approved",
		"stock.low",
		"erp.platform.operation.failed", // extra dots OK
		"order123.created",              // digits allowed
	}
	base := string(validEnvelopeJSON())
	for _, et := range valid {
		t.Run(et, func(t *testing.T) {
			j := strings.ReplaceAll(base, `"sales_order.created"`, `"`+et+`"`)
			env, err := ParseAndValidate([]byte(j))
			if err != nil {
				t.Fatalf("expected valid event_type %q to parse, got: %v", et, err)
			}
			if env.EventType != et {
				t.Errorf("wrong event_type: got %q, want %q", env.EventType, et)
			}
		})
	}
}

func TestParseAndValidate_OccurredAtInvalidFormat(t *testing.T) {
	bad := strings.ReplaceAll(string(validEnvelopeJSON()),
		`"2026-10-12T09:30:00Z"`, `"2026-13-99"`)
	_, err := ParseAndValidate([]byte(bad))
	if !IsPermanent(err) {
		t.Fatalf("expected PermanentError for bad occurred_at, got: %v", err)
	}
}

func TestParseAndValidate_PayloadMustBeObject(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{"array payload", `[1,2,3]`},
		{"string payload", `"hello"`},
		{"number payload", `42`},
		{"null payload", `null`},
		{"empty", ``},
	}
	base := string(validEnvelopeJSON())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Replace the payload value
			broken := strings.Replace(base,
				`{"order_number":"SO-001","total":"14250.00"}`,
				tc.payload, 1)
			_, err := ParseAndValidate([]byte(broken))
			if !IsPermanent(err) {
				t.Fatalf("expected PermanentError for payload %q, got: %v", tc.payload, err)
			}
		})
	}
}

func TestParseAndValidate_InvalidCorrelationIDUUID(t *testing.T) {
	bad := strings.ReplaceAll(string(validEnvelopeJSON()),
		`"8fa8db3b-8c65-4f7b-9e45-d8aa497f1981"`, `"bad-uuid"`)
	_, err := ParseAndValidate([]byte(bad))
	if !IsPermanent(err) {
		t.Fatalf("expected PermanentError for bad correlation_id, got: %v", err)
	}
}

func TestParseAndValidate_OccurredAtNormalisedToUTC(t *testing.T) {
	// Provide occurred_at with non-UTC timezone offset
	withTZ := strings.ReplaceAll(string(validEnvelopeJSON()),
		`"2026-10-12T09:30:00Z"`, `"2026-10-12T15:00:00+05:30"`)
	env, err := ParseAndValidate([]byte(withTZ))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// 15:00:00+05:30 = 09:30:00 UTC
	if env.OccurredAt.Hour() != 9 || env.OccurredAt.Minute() != 30 {
		t.Errorf("expected 09:30 UTC, got %v", env.OccurredAt)
	}
}
