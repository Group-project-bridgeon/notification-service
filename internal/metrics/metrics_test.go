package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestMetrics_PrometheusExposition(t *testing.T) {
	r := NewRegistry()

	r.RecordConsumed("erp.orders.events", "success", 12*time.Millisecond)
	r.RecordConsumed("erp.orders.events", "retry", 45*time.Millisecond)
	r.SetConsumerLag("erp.orders.events", 0, 5)
	r.IncRetryPublished("erp.orders.events.retry.1m")
	r.IncDLQPublished("erp.orders.events.dlq")
	r.IncInboxDuplicates()
	r.SetEmailQueueDepthProvider(func() int64 { return 3 })
	r.SetSSEConnectionsProvider(func() int64 { return 10 })

	output := string(r.RenderPrometheus())

	expectedSubstrings := []string{
		`messages_consumed_total{topic="erp.orders.events",result="success"} 1`,
		`messages_consumed_total{topic="erp.orders.events",result="retry"} 1`,
		`handler_duration_seconds_bucket{le="0.025"} 1`,
		`consumer_lag{topic="erp.orders.events",partition="0"} 5`,
		`retry_published_total{topic="erp.orders.events.retry.1m"} 1`,
		`dlq_published_total{topic="erp.orders.events.dlq"} 1`,
		`inbox_duplicates_total 1`,
		`email_queue_depth 3`,
		`sse_connections 10`,
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(output, sub) {
			t.Errorf("expected metrics output to contain %q, but got:\n%s", sub, output)
		}
	}
}
