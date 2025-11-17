package kafka

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
)

// Config holds the configuration for Kafka client
type Config struct {
	// Brokers is the list of Kafka broker addresses
	Brokers []string

	// ConsumerGroup is the consumer group ID
	ConsumerGroup string

	// ClientID identifies the client
	ClientID string

	// Kafka protocol version
	Version string

	// Producer Configuration
	Producer ProducerConfig

	// Consumer Configuration
	Consumer ConsumerConfig

	// DLQ Configuration
	DLQ DLQConfig

	// Logger for watermill
	Logger watermill.LoggerAdapter
}

// ProducerConfig holds producer-specific configuration
type ProducerConfig struct {
	// MaxMessageBytes is the maximum permitted size of a message
	MaxMessageBytes int

	// RequiredAcks determines the level of acknowledgement reliability
	// 0 = NoResponse, 1 = WaitForLocal, -1 = WaitForAll
	RequiredAcks sarama.RequiredAcks

	// Compression codec (none, gzip, snappy, lz4, zstd)
	Compression sarama.CompressionCodec

	// MaxRetries for sending messages
	MaxRetries int

	// RetryBackoff duration between retries
	RetryBackoff time.Duration

	// Idempotent enables idempotent producer
	Idempotent bool

	// Timeout for produce requests
	Timeout time.Duration

	// Return successes from broker
	ReturnSuccesses bool

	// Return errors from broker
	ReturnErrors bool
}

// ConsumerConfig holds consumer-specific configuration
type ConsumerConfig struct {
	// InitialOffset determines where to start consuming (oldest or newest)
	InitialOffset int64

	// SessionTimeout is the timeout for consumer session
	SessionTimeout time.Duration

	// HeartbeatInterval is the interval between heartbeats
	HeartbeatInterval time.Duration

	// RebalanceTimeout is the maximum allowed time for rebalance
	RebalanceTimeout time.Duration

	// MaxProcessingTime is the maximum time a message can be processed
	MaxProcessingTime time.Duration

	// FetchMin is the minimum bytes to fetch in a request
	FetchMin int32

	// FetchDefault is the default bytes to fetch in a request
	FetchDefault int32

	// FetchMax is the maximum bytes to fetch in a request
	FetchMax int32

	// MaxWaitTime is the maximum time to wait for fetch response
	MaxWaitTime time.Duration

	// ReturnErrors specifies whether to return errors to channel
	ReturnErrors bool

	// OffsetCommitInterval is how often to commit offsets
	OffsetCommitInterval time.Duration

	// AutoCommit enables automatic offset committing
	AutoCommit bool
}

// DLQConfig holds Dead Letter Queue configuration
type DLQConfig struct {
	// Enabled determines if DLQ is enabled
	Enabled bool

	// Topic is the DLQ topic name
	Topic string

	// MaxRetries before sending to DLQ
	MaxRetries int

	// RetryDelay is the delay between retries
	RetryDelay time.Duration

	// ExponentialBackoff enables exponential backoff for retries
	ExponentialBackoff bool
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Brokers:       []string{"localhost:9092"},
		ConsumerGroup: "default-consumer-group",
		ClientID:      "watermill-client",
		Version:       "3.6.0",
		Producer: ProducerConfig{
			MaxMessageBytes: 1000000, // 1MB
			RequiredAcks:    sarama.WaitForAll,
			Compression:     sarama.CompressionSnappy,
			MaxRetries:      5,
			RetryBackoff:    100 * time.Millisecond,
			Idempotent:      true,
			Timeout:         10 * time.Second,
			ReturnSuccesses: true,
			ReturnErrors:    true,
		},
		Consumer: ConsumerConfig{
			InitialOffset:        sarama.OffsetNewest,
			SessionTimeout:       20 * time.Second,
			HeartbeatInterval:    3 * time.Second,
			RebalanceTimeout:     60 * time.Second,
			MaxProcessingTime:    30 * time.Second,
			FetchMin:             1,
			FetchDefault:         1024 * 1024, // 1MB
			FetchMax:             10 * 1024 * 1024, // 10MB
			MaxWaitTime:          500 * time.Millisecond,
			ReturnErrors:         true,
			OffsetCommitInterval: 1 * time.Second,
			AutoCommit:           true,
		},
		DLQ: DLQConfig{
			Enabled:            true,
			Topic:              "dlq",
			MaxRetries:         3,
			RetryDelay:         5 * time.Second,
			ExponentialBackoff: true,
		},
		Logger: watermill.NewStdLogger(false, false),
	}
}

// EcommerceConfig returns configuration optimized for ecommerce microservices
func EcommerceConfig(brokers []string, consumerGroup string) *Config {
	cfg := DefaultConfig()
	cfg.Brokers = brokers
	cfg.ConsumerGroup = consumerGroup
	cfg.ClientID = "ecommerce-service"

	// Ecommerce optimizations
	cfg.Producer.RequiredAcks = sarama.WaitForAll // Ensure all replicas acknowledge
	cfg.Producer.Idempotent = true                 // Prevent duplicate orders
	cfg.Producer.Compression = sarama.CompressionLZ4
	cfg.Producer.Timeout = 15 * time.Second

	// Consumer optimizations for order processing
	cfg.Consumer.SessionTimeout = 30 * time.Second
	cfg.Consumer.HeartbeatInterval = 5 * time.Second
	cfg.Consumer.MaxProcessingTime = 60 * time.Second // Allow time for payment processing
	cfg.Consumer.AutoCommit = false                    // Manual commit for exactly-once processing

	// DLQ for failed orders
	cfg.DLQ.Enabled = true
	cfg.DLQ.Topic = "ecommerce-dlq"
	cfg.DLQ.MaxRetries = 5
	cfg.DLQ.RetryDelay = 10 * time.Second
	cfg.DLQ.ExponentialBackoff = true

	return cfg
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrInvalidConfig{Field: "Brokers", Reason: "at least one broker required"}
	}
	if c.ConsumerGroup == "" {
		return ErrInvalidConfig{Field: "ConsumerGroup", Reason: "consumer group is required"}
	}
	if c.Consumer.HeartbeatInterval >= c.Consumer.SessionTimeout {
		return ErrInvalidConfig{
			Field:  "Consumer.HeartbeatInterval",
			Reason: "heartbeat interval must be less than session timeout",
		}
	}
	if c.DLQ.Enabled && c.DLQ.Topic == "" {
		return ErrInvalidConfig{Field: "DLQ.Topic", Reason: "DLQ topic required when DLQ is enabled"}
	}
	return nil
}

// GetSaramaConfig converts our config to Sarama config
func (c *Config) GetSaramaConfig() (*sarama.Config, error) {
	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return nil, err
	}

	config := sarama.NewConfig()
	config.Version = version
	config.ClientID = c.ClientID

	// Producer configuration
	config.Producer.MaxMessageBytes = c.Producer.MaxMessageBytes
	config.Producer.RequiredAcks = c.Producer.RequiredAcks
	config.Producer.Compression = c.Producer.Compression
	config.Producer.Retry.Max = c.Producer.MaxRetries
	config.Producer.Retry.Backoff = c.Producer.RetryBackoff
	config.Producer.Idempotent = c.Producer.Idempotent
	config.Producer.Timeout = c.Producer.Timeout
	config.Producer.Return.Successes = c.Producer.ReturnSuccesses
	config.Producer.Return.Errors = c.Producer.ReturnErrors

	// Consumer configuration
	config.Consumer.Offsets.Initial = c.Consumer.InitialOffset
	config.Consumer.Group.Session.Timeout = c.Consumer.SessionTimeout
	config.Consumer.Group.Heartbeat.Interval = c.Consumer.HeartbeatInterval
	config.Consumer.Group.Rebalance.Timeout = c.Consumer.RebalanceTimeout
	config.Consumer.MaxProcessingTime = c.Consumer.MaxProcessingTime
	config.Consumer.Fetch.Min = c.Consumer.FetchMin
	config.Consumer.Fetch.Default = c.Consumer.FetchDefault
	config.Consumer.Fetch.Max = c.Consumer.FetchMax
	config.Consumer.MaxWaitTime = c.Consumer.MaxWaitTime
	config.Consumer.Return.Errors = c.Consumer.ReturnErrors

	// Offset management
	if c.Consumer.AutoCommit {
		config.Consumer.Offsets.AutoCommit.Enable = true
		config.Consumer.Offsets.AutoCommit.Interval = c.Consumer.OffsetCommitInterval
	} else {
		config.Consumer.Offsets.AutoCommit.Enable = false
	}

	// Metadata settings
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = 250 * time.Millisecond
	config.Metadata.RefreshFrequency = 10 * time.Minute

	// Network settings
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	return config, nil
}
