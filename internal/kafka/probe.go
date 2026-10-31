package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"notification-service/internal/config"
)

// Probe manages broker reachability checks for health readiness.
type Probe struct {
	client  *kgo.Client
	enabled bool
	logger  *slog.Logger
}

// NewProbe initializes a Kafka connectivity probe using Franz-Go.
func NewProbe(cfg *config.Config, logger *slog.Logger) (*Probe, error) {
	if !cfg.Kafka.ConsumerEnabled {
		logger.Info("Kafka consumer is disabled by configuration")
		return &Probe{enabled: false, logger: logger}, nil
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ClientID(cfg.Kafka.ClientID),
		kgo.RequestTimeoutOverhead(5 * time.Second),
	}

	// Security configuration
	if strings.EqualFold(cfg.Kafka.SecurityProtocol, "SASL_SSL") || strings.EqualFold(cfg.Kafka.SecurityProtocol, "SASL_PLAINTEXT") {
		switch strings.ToUpper(cfg.Kafka.SASLMechanism) {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: cfg.Kafka.SASLUsername,
				Pass: cfg.Kafka.SASLPassword,
			}.AsMechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: cfg.Kafka.SASLUsername,
				Pass: cfg.Kafka.SASLPassword,
			}.AsSha512Mechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: cfg.Kafka.SASLUsername,
				Pass: cfg.Kafka.SASLPassword,
			}.AsSha256Mechanism()))
		}
	}

	if strings.EqualFold(cfg.Kafka.SecurityProtocol, "SASL_SSL") || strings.EqualFold(cfg.Kafka.SecurityProtocol, "SSL") {
		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if cfg.Kafka.TLSCAFile != "" {
			caCert, err := os.ReadFile(cfg.Kafka.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read Kafka TLS CA file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsCfg.RootCAs = caCertPool
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize franz-go client: %w", err)
	}

	return &Probe{
		client:  client,
		enabled: true,
		logger:  logger,
	}, nil
}

// Check queries Kafka broker connectivity, returning "connected", "disconnected", or "disabled".
func (p *Probe) Check(ctx context.Context) string {
	if p == nil || !p.enabled {
		return "disabled"
	}
	if p.client == nil {
		return "disconnected"
	}

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := p.client.Ping(pingCtx); err != nil {
		p.logger.Debug("Kafka ping probe failed", slog.String("error", err.Error()))
		return "disconnected"
	}

	return "connected"
}

// Close gracefully terminates the Franz-Go client.
func (p *Probe) Close() {
	if p != nil && p.client != nil {
		p.client.Close()
		p.logger.Info("Kafka probe client closed")
	}
}
