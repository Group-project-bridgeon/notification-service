package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"notification-service/internal/config"
)

// Publisher defines an interface for producing Kafka records and awaiting broker acknowledgement.
type Publisher interface {
	Publish(ctx context.Context, record *kgo.Record) error
}

// FranzPublisher implements Publisher using a Franz-Go client.
type FranzPublisher struct {
	client *kgo.Client
}

// NewFranzPublisher wraps a Franz-Go client for synchronous idempotent record production.
func NewFranzPublisher(client *kgo.Client) *FranzPublisher {
	return &FranzPublisher{client: client}
}

// NewProducerClient initializes an idempotent Franz-Go producer client with acks=all.
func NewProducerClient(cfg *config.Config) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ClientID(cfg.Kafka.ClientID + "-publisher"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

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
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
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

	return kgo.NewClient(opts...)
}

// Publish writes the record to Kafka and waits synchronously for the broker acknowledgement (acks=all).
func (p *FranzPublisher) Publish(ctx context.Context, record *kgo.Record) error {
	if p.client == nil {
		return fmt.Errorf("franz-go client is uninitialized")
	}
	res := p.client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("failed publishing record to topic %s: %w", record.Topic, err)
	}
	return nil
}

// MockPublisher captures published records in memory for unit testing without a live broker.
type MockPublisher struct {
	mu        sync.Mutex
	Published []*kgo.Record
	Err       error
}

// NewMockPublisher constructs a MockPublisher.
func NewMockPublisher() *MockPublisher {
	return &MockPublisher{
		Published: make([]*kgo.Record, 0),
	}
}

// Publish stores the record or returns the configured injected error.
func (m *MockPublisher) Publish(_ context.Context, record *kgo.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.Published = append(m.Published, record)
	return nil
}

// GetPublished returns a copy of all published records.
func (m *MockPublisher) GetPublished() []*kgo.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*kgo.Record, len(m.Published))
	copy(cp, m.Published)
	return cp
}
