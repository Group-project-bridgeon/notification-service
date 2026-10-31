package config

import (
	"os"
	"reflect"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	clearEnv()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading defaults: %v", err)
	}

	if cfg.Port != "8085" {
		t.Errorf("expected default port 8085, got %s", cfg.Port)
	}
	if cfg.Env != "development" {
		t.Errorf("expected default env development, got %s", cfg.Env)
	}
	if !cfg.InternalEventsEnabled {
		t.Errorf("expected internal events enabled by default in development")
	}
	if cfg.AuthMode != "jwt" {
		t.Errorf("expected default auth mode jwt, got %s", cfg.AuthMode)
	}
	if !reflect.DeepEqual(cfg.Kafka.Topics, DefaultConsumedTopics) {
		t.Errorf("expected default consumed topics %v, got %v", DefaultConsumedTopics, cfg.Kafka.Topics)
	}
	if !reflect.DeepEqual(cfg.Kafka.RetryTopics, DefaultRetryTopics) {
		t.Errorf("expected default retry topics %v, got %v", DefaultRetryTopics, cfg.Kafka.RetryTopics)
	}
}

func TestProductionRefusal_AuthModeDev(t *testing.T) {
	clearEnv()
	os.Setenv("ENV", "production")
	os.Setenv("AUTH_MODE", "dev")
	os.Setenv("INTERNAL_EVENTS_ENABLED", "false")
	defer clearEnv()

	_, err := Load()
	if err == nil {
		t.Fatalf("expected error when ENV=production and AUTH_MODE=dev, got nil")
	}
	expectedMsg := "production safety violation: AUTH_MODE cannot be 'dev' when ENV=production"
	if err.Error() != expectedMsg {
		t.Errorf("expected '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestProductionRefusal_InternalEventsEnabled(t *testing.T) {
	clearEnv()
	os.Setenv("ENV", "production")
	os.Setenv("AUTH_MODE", "jwt")
	os.Setenv("INTERNAL_EVENTS_ENABLED", "true")
	defer clearEnv()

	_, err := Load()
	if err == nil {
		t.Fatalf("expected error when ENV=production and INTERNAL_EVENTS_ENABLED=true, got nil")
	}
	expectedMsg := "production safety violation: INTERNAL_EVENTS_ENABLED cannot be true when ENV=production"
	if err.Error() != expectedMsg {
		t.Errorf("expected '%s', got '%s'", expectedMsg, err.Error())
	}
}

func clearEnv() {
	os.Unsetenv("ENV")
	os.Unsetenv("PORT")
	os.Unsetenv("AUTH_MODE")
	os.Unsetenv("INTERNAL_EVENTS_ENABLED")
	os.Unsetenv("KAFKA_BROKERS")
	os.Unsetenv("KAFKA_START_OFFSET")
	os.Unsetenv("KAFKA_TOPICS")
}
