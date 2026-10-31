package email

import (
	"fmt"
	"html"
	"regexp"
	"strings"

	"notification-service/internal/domain"
)

var placeholderRegex = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}`)

// RenderResult contains both unredacted bodies (for transport send) and redacted bodies (for database storage).
type RenderResult struct {
	Subject          string
	HTMLBody         string
	TextBody         string
	HTMLBodyRedacted string
	TextBodyRedacted string
}

// RenderEmailTemplate validates declared variables, renders placeholders, escapes HTML,
// and produces both unredacted and redacted versions of the email content.
func RenderEmailTemplate(
	tmpl *domain.EmailTemplate,
	variables map[string]any,
	extraSensitiveVars []string,
) (*RenderResult, error) {
	if tmpl == nil {
		return nil, domain.NewAppError(domain.CodeEmailTemplateInvalid, "email template is nil")
	}

	// 1. Validate variables: only variables the template declares are allowed
	declaredMap := make(map[string]bool, len(tmpl.DeclaredVariables))
	for _, v := range tmpl.DeclaredVariables {
		declaredMap[v] = true
	}

	for k := range variables {
		if !declaredMap[k] {
			return nil, domain.NewAppError(domain.CodeEmailTemplateInvalid, fmt.Sprintf("unknown variable '%s' not declared in template '%s'", k, tmpl.TemplateKey))
		}
	}

	// 2. Render unredacted Subject, Text, and HTML
	subject := renderString(tmpl.SubjectTemplate, variables, false)
	textBody := renderString(tmpl.TextTemplate, variables, false)
	htmlBody := renderString(tmpl.HTMLTemplate, variables, true)

	// 3. Compile list of all sensitive variables
	sensitiveSet := make(map[string]bool)
	for _, v := range tmpl.SensitiveVariables {
		sensitiveSet[v] = true
	}
	for _, v := range extraSensitiveVars {
		sensitiveSet[v] = true
	}

	// 4. Produce redacted versions where every occurrence of sensitive values is replaced by ******
	htmlRedacted := htmlBody
	textRedacted := textBody

	for sVar := range sensitiveSet {
		val, exists := variables[sVar]
		if !exists || val == nil {
			continue
		}
		rawValStr := fmt.Sprintf("%v", val)
		if rawValStr == "" {
			continue
		}

		// In text body, replace raw value
		textRedacted = strings.ReplaceAll(textRedacted, rawValStr, "******")

		// In HTML body, replace both escaped and raw value
		escapedValStr := html.EscapeString(rawValStr)
		htmlRedacted = strings.ReplaceAll(htmlRedacted, escapedValStr, "******")
		htmlRedacted = strings.ReplaceAll(htmlRedacted, rawValStr, "******")
	}

	return &RenderResult{
		Subject:          subject,
		HTMLBody:         htmlBody,
		TextBody:         textBody,
		HTMLBodyRedacted: htmlRedacted,
		TextBodyRedacted: textRedacted,
	}, nil
}

func renderString(tmplStr string, variables map[string]any, escapeHTML bool) string {
	return placeholderRegex.ReplaceAllStringFunc(tmplStr, func(match string) string {
		sub := placeholderRegex.FindStringSubmatch(match)
		if len(sub) < 2 {
			return ""
		}
		varName := sub[1]
		val, exists := variables[varName]
		if !exists || val == nil {
			return ""
		}

		strVal := fmt.Sprintf("%v", val)
		if escapeHTML {
			return html.EscapeString(strVal)
		}
		return strVal
	})
}

// ExtractPlaceholders extracts unique variable names inside {{ ... }} from template strings.
func ExtractPlaceholders(texts ...string) []string {
	seen := make(map[string]bool)
	var vars []string
	for _, text := range texts {
		matches := placeholderRegex.FindAllStringSubmatch(text, -1)
		for _, m := range matches {
			if len(m) > 1 && !seen[m[1]] {
				seen[m[1]] = true
				vars = append(vars, m[1])
			}
		}
	}
	return vars
}

// ValidateTemplateVariables verifies that all placeholders used in texts are present in declaredVars.
func ValidateTemplateVariables(declaredVars []string, texts ...string) error {
	declared := make(map[string]bool, len(declaredVars))
	for _, v := range declaredVars {
		declared[v] = true
	}

	seenMissing := make(map[string]bool)
	var missing []string
	for _, text := range texts {
		matches := placeholderRegex.FindAllStringSubmatch(text, -1)
		for _, m := range matches {
			if len(m) > 1 {
				varName := m[1]
				if !declared[varName] && !seenMissing[varName] {
					seenMissing[varName] = true
					missing = append(missing, varName)
				}
			}
		}
	}

	if len(missing) > 0 {
		return domain.NewValidationError(
			fmt.Sprintf("template uses undeclared variable(s): %s", strings.Join(missing, ", ")),
			domain.ErrorDetail{Field: "declared_variables", Issue: "undeclared_variable"},
		)
	}

	return nil
}
