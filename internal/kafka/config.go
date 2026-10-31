package kafka

import (
	"notification-service/internal/config"
)

// Options holds parameters required for initializing the Franz-Go Kafka client.
type Options struct {
	Enabled          bool
	Brokers          []string
	ClientID         string
	GroupID          string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
	TLSCAFile        string
}

// FromConfig converts application config to Kafka client options.
func FromConfig(cfg *config.Config) Options {
	return Options{
		Enabled:          cfg.Kafka.ConsumerEnabled,
		Brokers:          cfg.Kafka.Brokers,
		ClientID:         cfg.Kafka.ClientID,
		GroupID:          cfg.Kafka.GroupID,
		SecurityProtocol: cfg.Kafka.SecurityProtocol,
		SASLMechanism:    cfg.Kafka.SASLMechanism,
		SASLUsername:     cfg.Kafka.SASLUsername,
		SASLPassword:     cfg.Kafka.SASLPassword,
		TLSCAFile:        cfg.Kafka.TLSCAFile,
	}
}
