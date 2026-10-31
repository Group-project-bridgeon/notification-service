package email

import (
	"log/slog"
	"testing"
)

func TestRedactMap(t *testing.T) {
	raw := map[string]any{
		"username":      "user@example.com",
		"password":      "plaintext-password",
		"login_otp":     "123456",
		"secret_token":  "super-secret",
		"authorization": "Bearer token",
		"metadata": map[string]any{
			"nested_code": "9999",
			"safe_note":   "hello",
		},
	}

	sanitized := RedactMap(raw)

	if sanitized["username"] != "user@example.com" {
		t.Errorf("expected username to be preserved, got %v", sanitized["username"])
	}
	if sanitized["password"] != "[REDACTED]" {
		t.Errorf("expected password to be redacted, got %v", sanitized["password"])
	}
	if sanitized["login_otp"] != "[REDACTED]" {
		t.Errorf("expected login_otp to be redacted, got %v", sanitized["login_otp"])
	}
	if sanitized["secret_token"] != "[REDACTED]" {
		t.Errorf("expected secret_token to be redacted, got %v", sanitized["secret_token"])
	}
	if sanitized["authorization"] != "[REDACTED]" {
		t.Errorf("expected authorization to be redacted, got %v", sanitized["authorization"])
	}

	nested, ok := sanitized["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map metadata")
	}
	if nested["nested_code"] != "[REDACTED]" {
		t.Errorf("expected nested_code to be redacted, got %v", nested["nested_code"])
	}
	if nested["safe_note"] != "hello" {
		t.Errorf("expected safe_note to be preserved, got %v", nested["safe_note"])
	}
}

func TestSlogRedactAttrReplacer(t *testing.T) {
	attrSafe := slog.String("user_id", "123")
	replSafe := SlogRedactAttrReplacer(nil, attrSafe)
	if replSafe.Value.String() != "123" {
		t.Errorf("expected user_id value preserved, got %s", replSafe.Value.String())
	}

	attrSecret := slog.String("user_password", "secret123")
	replSecret := SlogRedactAttrReplacer(nil, attrSecret)
	if replSecret.Value.String() != "[REDACTED]" {
		t.Errorf("expected user_password redacted, got %s", replSecret.Value.String())
	}
}
