package email

import (
	"log/slog"
	"strings"
)

var sensitiveKeys = map[string]bool{
	"password":      true,
	"otp":           true,
	"code":          true,
	"token":         true,
	"secret":        true,
	"authorization": true,
}

// IsSensitiveKey checks whether a key name matches any known sensitive identifiers.
func IsSensitiveKey(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	for s := range sensitiveKeys {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}

// RedactMap recursively creates a sanitized copy of a map, masking sensitive fields.
func RedactMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	output := make(map[string]any, len(input))
	for k, v := range input {
		if IsSensitiveKey(k) {
			output[k] = "[REDACTED]"
			continue
		}

		switch val := v.(type) {
		case map[string]any:
			output[k] = RedactMap(val)
		case []any:
			output[k] = redactSlice(val)
		default:
			output[k] = val
		}
	}
	return output
}

func redactSlice(input []any) []any {
	output := make([]any, len(input))
	for i, v := range input {
		switch val := v.(type) {
		case map[string]any:
			output[i] = RedactMap(val)
		case []any:
			output[i] = redactSlice(val)
		default:
			output[i] = val
		}
	}
	return output
}

// SlogRedactAttrReplacer is an slog replace function that redacts sensitive attributes.
func SlogRedactAttrReplacer(groups []string, a slog.Attr) slog.Attr {
	if IsSensitiveKey(a.Key) {
		return slog.String(a.Key, "[REDACTED]")
	}
	return a
}
