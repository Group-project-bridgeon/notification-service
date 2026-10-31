package notification

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/domain"
)

func TestRenderTemplate_SubstitutionsAndEscaping(t *testing.T) {
	aggregateID := uuid.New()
	actorID := uuid.New()
	corrID := uuid.New()
	eventID := uuid.New()

	payloadData := map[string]any{
		"order_no": "SO-12345",
		"customer": map[string]any{
			"name": "Acme <Corp> & Co.",
		},
		"total":    "149.99",
		"currency": "USD",
		"xss_test": "<script>alert('xss')</script>",
	}
	payloadBytes, err := json.Marshal(payloadData)
	if err != nil {
		t.Fatalf("failed marshaling payload: %v", err)
	}

	env := &domain.EventEnvelope{
		EventID:       eventID,
		EventType:     "sales_order.held_for_credit_approval",
		EventVersion:  "1.0",
		OccurredAt:    time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
		Producer:      "sales-service",
		AggregateType: "sales_order",
		AggregateID:   aggregateID.String(),
		CorrelationID: &corrID,
		ActorUserID:   &actorID,
		Payload:       payloadBytes,
	}

	tests := []struct {
		name     string
		template string
		expected string
	}{
		{
			name:     "Catalog example body",
			template: "Order {{payload.order_no}} for {{payload.customer.name}} ({{payload.total}} {{payload.currency}}) is on hold.",
			expected: "Order SO-12345 for Acme &lt;Corp&gt; &amp; Co. (149.99 USD) is on hold.",
		},
		{
			name:     "Catalog example link",
			template: "/sales-orders/{{aggregate_id}}",
			expected: "/sales-orders/" + aggregateID.String(),
		},
		{
			name:     "Envelope fields",
			template: "Event {{event_type}} by {{actor_user_id}} with corr {{correlation_id}}",
			expected: "Event sales_order.held_for_credit_approval by " + actorID.String() + " with corr " + corrID.String(),
		},
		{
			name:     "HTML escaping protection",
			template: "Malicious payload: {{payload.xss_test}}",
			expected: "Malicious payload: &lt;script&gt;alert(&#39;xss&#39;)&lt;/script&gt;",
		},
		{
			name:     "Missing placeholder field renders as empty string without panic",
			template: "Missing: {{payload.non_existent_key}}",
			expected: "Missing: ",
		},
		{
			name:     "Missing nested field renders as empty string",
			template: "Missing nested: {{payload.customer.non_existent_child}}",
			expected: "Missing nested: ",
		},
		{
			name:     "Unknown top-level token renders as empty string",
			template: "Unknown: {{unknown_field_name}}",
			expected: "Unknown: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RenderTemplate(tt.template, env, nil)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestRenderTemplate_NilNullableFields(t *testing.T) {
	env := &domain.EventEnvelope{
		EventID:       uuid.New(),
		EventType:     "system.ping",
		AggregateID:   uuid.New().String(),
		CorrelationID: nil,
		ActorUserID:   nil,
		Payload:       nil,
	}

	tmpl := "Actor: {{actor_user_id}}, Corr: {{correlation_id}}, Payload: {{payload.missing}}"
	result := RenderTemplate(tmpl, env, nil)
	expected := "Actor: , Corr: , Payload: "
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}
