package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config aggregates all runtime configuration settings for notification-service.
type Config struct {
	Env                   string
	Port                  string
	DatabaseURL           string
	DBMaxOpenConns        int
	DBMaxIdleConns        int
	DBConnMaxLifetime     time.Duration
	AuthMode              string // jwt | dev
	JWTSecret             string
	JWTPublicKey          string
	InternalServiceToken  string
	InternalEventsEnabled bool
	EmailEnabled          bool
	EmailEncryptionKey    string // 32-byte base64 string
	WebhookSecret         string
	CORSAllowedOrigins    []string
	LogLevel              string // debug | info | warn | error
	AllowedRoles          []string

	Kafka KafkaConfig
}

type KafkaConfig struct {
	ConsumerEnabled  bool
	Brokers          []string
	ClientID         string
	GroupID          string
	Topics           []string
	RetryTopics      []string
	DLQTopic         string
	StartOffset      string // earliest | latest
	SecurityProtocol string // PLAINTEXT | SASL_SSL
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
	TLSCAFile        string
	MaxPollRecords   int
	SessionTimeoutMS int
}

// Default topic collections conforming to KAFKA_CONTRACT.md
var (
	DefaultConsumedTopics = []string{
		"erp.auth.events",
		"erp.purchasing.events",
		"erp.inventory.events",
		"erp.sales.events",
		"erp.returns.events",
		"erp.finance.events",
		"erp.platform.events",
	}

	DefaultRetryTopics = []string{
		"erp.notification.retry.1m",
		"erp.notification.retry.10m",
		"erp.notification.retry.1h",
	}
)

// Load reads configuration from environment variables with defaults and validates constraints.
func Load() (*Config, error) {
	env := getEnv("ENV", "development")

	// Default for INTERNAL_EVENTS_ENABLED: true in dev, false in prod
	defaultInternalEvents := env != "production"

	cfg := &Config{
		Env:                   env,
		Port:                  getEnv("PORT", "8085"),
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://notification_user:notification_pass@localhost:5432/notification_db?sslmode=disable"),
		DBMaxOpenConns:        getEnvInt("DB_MAX_OPEN_CONNS", 25),
		DBMaxIdleConns:        getEnvInt("DB_MAX_IDLE_CONNS", 10),
		DBConnMaxLifetime:     getEnvDuration("DB_CONN_MAX_LIFETIME", 5*time.Minute),
		AuthMode:              getEnv("AUTH_MODE", "jwt"),
		JWTSecret:             getEnv("JWT_SECRET", "super-secret-jwt-signing-key"),
		JWTPublicKey:          getEnv("JWT_PUBLIC_KEY", ""),
		InternalServiceToken:  getEnv("INTERNAL_SERVICE_TOKEN", "internal-dev-token"),
		InternalEventsEnabled: getEnvBool("INTERNAL_EVENTS_ENABLED", defaultInternalEvents),
		EmailEnabled:          getEnvBool("EMAIL_ENABLED", false),
		EmailEncryptionKey:    getEnv("EMAIL_ENCRYPTION_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="), // sample 32-byte b64 for dev
		WebhookSecret:         getEnv("WEBHOOK_SECRET", "dev-webhook-secret"),
		CORSAllowedOrigins:    getEnvSlice("CORS_ALLOWED_ORIGINS", []string{"http://localhost:3000", "http://localhost:5173"}),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
		AllowedRoles:          getEnvSlice("ALLOWED_ROLES", []string{"admin", "manager", "purchasing", "warehouse", "sales", "finance"}),

		Kafka: KafkaConfig{
			ConsumerEnabled:  getEnvBool("KAFKA_CONSUMER_ENABLED", true),
			Brokers:          getEnvSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
			ClientID:         getEnv("KAFKA_CLIENT_ID", "notification-service"),
			GroupID:          getEnv("KAFKA_GROUP_ID", "notification-service"),
			Topics:           getEnvSlice("KAFKA_TOPICS", DefaultConsumedTopics),
			RetryTopics:      getEnvSlice("KAFKA_RETRY_TOPICS", DefaultRetryTopics),
			DLQTopic:         getEnv("KAFKA_DLQ_TOPIC", "erp.notification.dlq"),
			StartOffset:      getEnv("KAFKA_START_OFFSET", "earliest"),
			SecurityProtocol: getEnv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
			SASLMechanism:    getEnv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
			SASLUsername:     getEnv("KAFKA_SASL_USERNAME", ""),
			SASLPassword:     getEnv("KAFKA_SASL_PASSWORD", ""),
			TLSCAFile:        getEnv("KAFKA_TLS_CA_FILE", ""),
			MaxPollRecords:   getEnvInt("KAFKA_MAX_POLL_RECORDS", 500),
			SessionTimeoutMS: getEnvInt("KAFKA_SESSION_TIMEOUT_MS", 10000),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate verifies mandatory startup constraints, especially production safety rules.
func (c *Config) Validate() error {
	if c.Env == "production" {
		if c.AuthMode == "dev" {
			return errors.New("production safety violation: AUTH_MODE cannot be 'dev' when ENV=production")
		}
		if c.InternalEventsEnabled {
			return errors.New("production safety violation: INTERNAL_EVENTS_ENABLED cannot be true when ENV=production")
		}
		if c.JWTSecret == "" && c.JWTPublicKey == "" {
			return errors.New("production configuration error: either JWT_SECRET or JWT_PUBLIC_KEY must be provided")
		}
		if c.InternalServiceToken == "" {
			return errors.New("production configuration error: INTERNAL_SERVICE_TOKEN must be non-empty")
		}
	}

	if c.AuthMode != "jwt" && c.AuthMode != "dev" {
		return fmt.Errorf("invalid AUTH_MODE '%s', expected 'jwt' or 'dev'", c.AuthMode)
	}

	if c.Kafka.StartOffset != "earliest" && c.Kafka.StartOffset != "latest" {
		return fmt.Errorf("invalid KAFKA_START_OFFSET '%s', expected 'earliest' or 'latest'", c.Kafka.StartOffset)
	}

	return nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}

func getEnvBool(key string, defaultVal bool) bool {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.ParseBool(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := time.ParseDuration(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}

func getEnvSlice(key string, defaultVal []string) []string {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	parts := strings.Split(valStr, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return defaultVal
	}
	return result
}
