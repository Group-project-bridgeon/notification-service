package repository

import (
	"testing"
)

func TestBuildOrderBy(t *testing.T) {
	whitelist := map[string]string{
		"created_at": "created_at",
		"priority":   "priority",
		"status":     "status",
		"category":   "category",
	}
	defaultClause := "ORDER BY created_at DESC"

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty input returns default",
			input:    "",
			expected: "ORDER BY created_at DESC",
		},
		{
			name:     "single ascending column",
			input:    "priority",
			expected: "ORDER BY priority ASC",
		},
		{
			name:     "single descending column with hyphen",
			input:    "-created_at",
			expected: "ORDER BY created_at DESC",
		},
		{
			name:     "explicit plus ascending prefix",
			input:    "+category",
			expected: "ORDER BY category ASC",
		},
		{
			name:     "multiple columns hyphen descending and ascending",
			input:    "-created_at,priority",
			expected: "ORDER BY created_at DESC, priority ASC",
		},
		{
			name:     "whitespace handling",
			input:    "  -created_at ,  status  ",
			expected: "ORDER BY created_at DESC, status ASC",
		},
		{
			name:     "unwhitelisted column ignored fallback",
			input:    "malicious_col; DROP TABLE notifications;",
			expected: "ORDER BY created_at DESC",
		},
		{
			name:     "mix of whitelisted and unwhitelisted",
			input:    "unknown_col,-priority,evil_injection",
			expected: "ORDER BY priority DESC",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildOrderBy(tc.input, whitelist, defaultClause)
			if got != tc.expected {
				t.Errorf("BuildOrderBy(%q) = %q, expected %q", tc.input, got, tc.expected)
			}
		})
	}
}
