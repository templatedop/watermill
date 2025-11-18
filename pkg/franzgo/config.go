package franzgo

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config holds the Kafka client configuration
type Config struct {
	// Broker configuration
	Brokers       []string
	ConsumerGroup string
	ClientID      string

	// Producer configuration
	Producer ProducerConfig

	// Consumer configuration
	Consumer ConsumerConfig

	// DLQ configuration
	DLQ DLQConfig

	// Security configuration
	Security *SecurityConfig

	// Request timeout
	RequestTimeout time.Duration

	// Metadata configuration
	MetadataMaxAge time.Duration
}

// ProducerConfig holds producer-specific configuration
type ProducerConfig struct {
	// Maximum number of in-flight requests
	MaxInFlight int

	// Batch size in bytes
	BatchSize int

	// Linger time before sending batch
	Linger time.Duration

	// Compression codec
	Compression CompressionCodec

	// Enable idempotent writes
	Idempotent bool

	// Transaction timeout (for transactional producers)
	TransactionTimeout time.Duration

	// Max buffered records
	MaxBufferedRecords int

	// Acknowledgements required (-1=all, 0=none, 1=leader)
	RequiredAcks int16
}

// ConsumerConfig holds consumer-specific configuration
type ConsumerConfig struct {
	// Topics to consume from
	Topics []string

	// Session timeout
	SessionTimeout time.Duration

	// Rebalance timeout
	RebalanceTimeout time.Duration

	// Heartbeat interval
	HeartbeatInterval time.Duration

	// Max poll records
	MaxPollRecords int

	// Fetch min bytes
	FetchMinBytes int32

	// Fetch max wait
	FetchMaxWait time.Duration

	// Auto commit interval
	AutoCommitInterval time.Duration

	// Starting offset (earliest or latest)
	StartOffset Offset

	// Enable auto commit
	AutoCommit bool
}

// DLQConfig holds Dead Letter Queue configuration
type DLQConfig struct {
	// Enable DLQ
	Enabled bool

	// DLQ topic name
	Topic string

	// Max retries before sending to DLQ
	MaxRetries int

	// Retry delay
	RetryDelay time.Duration

	// Exponential backoff
	ExponentialBackoff bool

	// Max retry delay (for exponential backoff)
	MaxRetryDelay time.Duration
}

// CompressionCodec defines the compression algorithm
type CompressionCodec string

const (
	CompressionNone   CompressionCodec = "none"
	CompressionGzip   CompressionCodec = "gzip"
	CompressionSnappy CompressionCodec = "snappy"
	CompressionLZ4    CompressionCodec = "lz4"
	CompressionZstd   CompressionCodec = "zstd"
)

// Offset defines the starting offset for consumers
type Offset int64

const (
	OffsetOldest Offset = -2
	OffsetNewest Offset = -1
)

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Brokers:       []string{"localhost:9092"},
		ConsumerGroup: "default-group",
		ClientID:      "franz-go-client",
		Producer: ProducerConfig{
			MaxInFlight:        5,
			BatchSize:          16384, // 16KB
			Linger:             10 * time.Millisecond,
			Compression:        CompressionSnappy,
			Idempotent:         true,
			TransactionTimeout: 60 * time.Second,
			MaxBufferedRecords: 10000,
			RequiredAcks:       -1, // all replicas
		},
		Consumer: ConsumerConfig{
			SessionTimeout:     45 * time.Second,
			RebalanceTimeout:   60 * time.Second,
			HeartbeatInterval:  3 * time.Second,
			MaxPollRecords:     500,
			FetchMinBytes:      1,
			FetchMaxWait:       500 * time.Millisecond,
			AutoCommitInterval: 5 * time.Second,
			StartOffset:        OffsetNewest,
			AutoCommit:         true,
		},
		DLQ: DLQConfig{
			Enabled:            false,
			Topic:              "dlq",
			MaxRetries:         3,
			RetryDelay:         5 * time.Second,
			ExponentialBackoff: true,
			MaxRetryDelay:      5 * time.Minute,
		},
		RequestTimeout: 30 * time.Second,
		MetadataMaxAge: 5 * time.Minute,
	}
}

// EcommerceConfig returns a configuration optimized for ecommerce use cases
func EcommerceConfig(brokers []string, consumerGroup string) *Config {
	config := DefaultConfig()
	config.Brokers = brokers
	config.ConsumerGroup = consumerGroup
	config.ClientID = "ecommerce-client"

	// Optimize for reliability
	config.Producer.RequiredAcks = -1 // All replicas
	config.Producer.Idempotent = true
	config.Producer.Compression = CompressionSnappy
	config.Producer.Linger = 5 * time.Millisecond // Low latency

	// Enable DLQ
	config.DLQ.Enabled = true
	config.DLQ.Topic = "ecommerce-dlq"
	config.DLQ.MaxRetries = 3

	// Consumer optimizations
	config.Consumer.MaxPollRecords = 100
	config.Consumer.FetchMaxWait = 100 * time.Millisecond

	return config
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker is required")
	}

	if c.ConsumerGroup == "" {
		return fmt.Errorf("consumer group is required")
	}

	if c.Consumer.HeartbeatInterval >= c.Consumer.SessionTimeout {
		return fmt.Errorf("heartbeat interval must be less than session timeout")
	}

	if c.Consumer.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval must be positive")
	}

	if c.Consumer.SessionTimeout <= 0 {
		return fmt.Errorf("session timeout must be positive")
	}

	if c.Producer.BatchSize <= 0 {
		return fmt.Errorf("producer batch size must be positive")
	}

	if c.DLQ.Enabled && c.DLQ.Topic == "" {
		return fmt.Errorf("DLQ topic is required when DLQ is enabled")
	}

	return nil
}

// ToKgoOpts converts Config to franz-go kgo options
func (c *Config) ToKgoOpts() ([]kgo.Opt, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.RequestTimeoutOverhead(c.RequestTimeout),
		kgo.MetadataMaxAge(c.MetadataMaxAge),
	}

	// Consumer options
	if c.ConsumerGroup != "" {
		opts = append(opts,
			kgo.ConsumerGroup(c.ConsumerGroup),
			kgo.SessionTimeout(c.Consumer.SessionTimeout),
			kgo.RebalanceTimeout(c.Consumer.RebalanceTimeout),
			kgo.HeartbeatInterval(c.Consumer.HeartbeatInterval),
			kgo.FetchMinBytes(c.Consumer.FetchMinBytes),
			kgo.FetchMaxWait(c.Consumer.FetchMaxWait),
		)

		if c.Consumer.AutoCommit {
			opts = append(opts, kgo.AutoCommitInterval(c.Consumer.AutoCommitInterval))
		} else {
			opts = append(opts, kgo.DisableAutoCommit())
		}

		// Set starting offset
		switch c.Consumer.StartOffset {
		case OffsetOldest:
			opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		case OffsetNewest:
			opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
		}

		// Add topics
		if len(c.Consumer.Topics) > 0 {
			opts = append(opts, kgo.ConsumeTopics(c.Consumer.Topics...))
		}
	}

	// Producer options
	opts = append(opts,
		kgo.ProducerBatchMaxBytes(int32(c.Producer.BatchSize)),
		kgo.ProducerLinger(c.Producer.Linger),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.MaxBufferedRecords(c.Producer.MaxBufferedRecords),
		kgo.RequiredAcks(kgo.RequiredAcks(c.Producer.RequiredAcks)),
	)

	// Compression
	switch c.Producer.Compression {
	case CompressionGzip:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case CompressionSnappy:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case CompressionLZ4:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case CompressionZstd:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	}

	// Idempotent producer
	if c.Producer.Idempotent {
		opts = append(opts, kgo.RecordDeliveryTimeout(c.Producer.TransactionTimeout))
	}

	// Security configuration
	if c.Security != nil {
		secOpts, err := c.Security.ToKgoOpts()
		if err != nil {
			return nil, fmt.Errorf("failed to apply security config: %w", err)
		}
		opts = append(opts, secOpts...)
	}

	return opts, nil
}

// SecurityConfig holds security configuration
type SecurityConfig struct {
	// SASL configuration
	SASL *SASLConfig

	// TLS configuration
	TLS *TLSConfig
}

// SASLConfig holds SASL authentication configuration
type SASLConfig struct {
	// Mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	Mechanism SASLMechanism

	// Username and password
	Username string
	Password string
}

// SASLMechanism defines the SASL authentication mechanism
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "PLAIN"
	SASLScramSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLScramSHA512 SASLMechanism = "SCRAM-SHA-512"
)

// TLSConfig holds TLS configuration
type TLSConfig struct {
	// TLS config
	Config *tls.Config
}

// ToKgoOpts converts SecurityConfig to franz-go options
func (sc *SecurityConfig) ToKgoOpts() ([]kgo.Opt, error) {
	var opts []kgo.Opt

	// SASL configuration
	if sc.SASL != nil {
		var mechanism sasl.Mechanism
		var err error

		switch sc.SASL.Mechanism {
		case SASLPlain:
			mechanism = plain.Auth{
				User: sc.SASL.Username,
				Pass: sc.SASL.Password,
			}.AsMechanism()

		case SASLScramSHA256:
			mechanism = scram.Auth{
				User: sc.SASL.Username,
				Pass: sc.SASL.Password,
			}.AsSha256Mechanism()

		case SASLScramSHA512:
			mechanism = scram.Auth{
				User: sc.SASL.Username,
				Pass: sc.SASL.Password,
			}.AsSha512Mechanism()

		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", sc.SASL.Mechanism)
		}

		if err != nil {
			return nil, err
		}

		opts = append(opts, kgo.SASL(mechanism))
	}

	// TLS configuration
	if sc.TLS != nil && sc.TLS.Config != nil {
		opts = append(opts, kgo.DialTLSConfig(sc.TLS.Config))
	}

	return opts, nil
}

// NewSASLPlainConfig creates SASL PLAIN configuration
func NewSASLPlainConfig(username, password string) *SASLConfig {
	return &SASLConfig{
		Mechanism: SASLPlain,
		Username:  username,
		Password:  password,
	}
}

// NewSASLScramConfig creates SASL SCRAM configuration
func NewSASLScramConfig(mechanism SASLMechanism, username, password string) *SASLConfig {
	if mechanism != SASLScramSHA256 && mechanism != SASLScramSHA512 {
		mechanism = SASLScramSHA256
	}

	return &SASLConfig{
		Mechanism: mechanism,
		Username:  username,
		Password:  password,
	}
}

// ConfigBuilder provides a fluent API for building configurations
type ConfigBuilder struct {
	config *Config
}

// NewConfigBuilder creates a new config builder
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: DefaultConfig(),
	}
}

// WithBrokers sets the Kafka brokers
func (cb *ConfigBuilder) WithBrokers(brokers []string) *ConfigBuilder {
	cb.config.Brokers = brokers
	return cb
}

// WithConsumerGroup sets the consumer group
func (cb *ConfigBuilder) WithConsumerGroup(group string) *ConfigBuilder {
	cb.config.ConsumerGroup = group
	return cb
}

// WithClientID sets the client ID
func (cb *ConfigBuilder) WithClientID(clientID string) *ConfigBuilder {
	cb.config.ClientID = clientID
	return cb
}

// WithSASL sets SASL authentication
func (cb *ConfigBuilder) WithSASL(sasl *SASLConfig) *ConfigBuilder {
	if cb.config.Security == nil {
		cb.config.Security = &SecurityConfig{}
	}
	cb.config.Security.SASL = sasl
	return cb
}

// WithTLS sets TLS configuration
func (cb *ConfigBuilder) WithTLS(tlsConfig *tls.Config) *ConfigBuilder {
	if cb.config.Security == nil {
		cb.config.Security = &SecurityConfig{}
	}
	cb.config.Security.TLS = &TLSConfig{Config: tlsConfig}
	return cb
}

// Build builds the configuration
func (cb *ConfigBuilder) Build() (*Config, error) {
	if err := cb.config.Validate(); err != nil {
		return nil, err
	}
	return cb.config, nil
}
