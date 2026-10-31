package repository

import (
	"fmt"
	"strings"
)

// BuildOrderBy parses a sort query parameter string in the style of "-created_at,priority",
// validates each field against a strict whitelist map, and returns a sanitized "ORDER BY col1 DESC, col2 ASC" clause.
// User input is NEVER directly interpolated into SQL. Only whitelisted target column names and explicit ASC/DESC are used.
func BuildOrderBy(sortStr string, whitelist map[string]string, defaultClause string) string {
	trimmed := strings.TrimSpace(sortStr)
	if trimmed == "" {
		return defaultClause
	}

	parts := strings.Split(trimmed, ",")
	clauses := make([]string, 0, len(parts))

	for _, part := range parts {
		token := strings.TrimSpace(part)
		if token == "" {
			continue
		}

		direction := "ASC"
		field := token
		if strings.HasPrefix(token, "-") {
			direction = "DESC"
			field = strings.TrimPrefix(token, "-")
		} else if strings.HasPrefix(token, "+") {
			direction = "ASC"
			field = strings.TrimPrefix(token, "+")
		}

		// Validate against whitelist
		if sqlCol, ok := whitelist[strings.ToLower(field)]; ok {
			clauses = append(clauses, fmt.Sprintf("%s %s", sqlCol, direction))
		}
	}

	if len(clauses) == 0 {
		return defaultClause
	}

	return "ORDER BY " + strings.Join(clauses, ", ")
}
