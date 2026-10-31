package notification

import (
	"encoding/json"
	"fmt"
	"html"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"notification-service/internal/domain"
)

var placeholderPattern = regexp.MustCompile(`\{\{([a-zA-Z0-9_.]+)\}\}`)

// RenderTemplate substitutes {{placeholder}} variables with values from the canonical EventEnvelope.
// Injected values are HTML-escaped. Missing fields render as an empty string and log a warning.
func RenderTemplate(tmpl string, env *domain.EventEnvelope, logger *slog.Logger) string {
	if tmpl == "" || env == nil {
		return tmpl
	}
	if logger == nil {
		logger = slog.Default()
	}

	var payloadMap map[string]any
	if len(env.Payload) > 0 {
		_ = json.Unmarshal(env.Payload, &payloadMap)
	}

	return placeholderPattern.ReplaceAllStringFunc(tmpl, func(match string) string {
		token := match[2 : len(match)-2] // strip {{ and }}
		token = strings.TrimSpace(token)

		switch token {
		case "event_type":
			return html.EscapeString(env.EventType)
		case "event_id":
			return html.EscapeString(env.EventID.String())
		case "event_version":
			return html.EscapeString(env.EventVersion)
		case "producer":
			return html.EscapeString(env.Producer)
		case "aggregate_type":
			return html.EscapeString(env.AggregateType)
		case "aggregate_id":
			return html.EscapeString(env.AggregateID)
		case "occurred_at":
			return html.EscapeString(env.OccurredAt.UTC().Format(time.RFC3339))
		case "correlation_id":
			if env.CorrelationID != nil {
				return html.EscapeString(env.CorrelationID.String())
			}
			return ""
		case "actor_user_id":
			if env.ActorUserID != nil {
				return html.EscapeString(env.ActorUserID.String())
			}
			return ""
		}

		if strings.HasPrefix(token, "payload.") {
			path := token[len("payload."):]
			val, found := extractPayloadPath(payloadMap, path)
			if !found {
				logger.Warn("template placeholder field not found in event payload",
					"field", token,
					"event_type", env.EventType,
					"event_id", env.EventID,
				)
				return ""
			}
			return html.EscapeString(formatValue(val))
		}

		logger.Warn("unrecognized template placeholder token",
			"token", token,
			"event_type", env.EventType,
			"event_id", env.EventID,
		)
		return ""
	})
}

func extractPayloadPath(m map[string]any, path string) (any, bool) {
	if m == nil || path == "" {
		return nil, false
	}

	parts := strings.Split(path, ".")
	var current any = m

	for _, part := range parts {
		asMap, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		val, exists := asMap[part]
		if !exists || val == nil {
			return nil, false
		}
		current = val
	}

	return current, true
}

func formatValue(val any) string {
	switch v := val.(type) {
	case string:
		return v
	case float64:
		// Check if it's an integer
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
