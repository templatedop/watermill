package franzgo

import (
	"crypto/tls"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	if len(config.Brokers) == 0 {
		t.Error("Default brokers should not be empty")
	}

	if config.ClientID == "" {
		t.Error("Default ClientID should not be empty")
	}

	if config.Producer.BatchSize <= 0 {
		t.Error("Default batch size should be positive")
	}

	if config.Consumer.SessionTimeout <= 0 {
		t.Error("Default session timeout should be positive")
	}
}

func TestConfigBuilder(t *testing.T) {
	brokers := []string{"localhost:9092", "localhost:9093"}
	group := "test-group"
	clientID := "test-client"

	config := NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup(group).
		WithClientID(clientID).
		Build()

	if len(config.Brokers) != len(brokers) {
		t.Errorf("Expected %d brokers, got %d", len(brokers), len(config.Brokers))
	}

	for i, broker := range brokers {
		if config.Brokers[i] != broker {
			t.Errorf("Expected broker %s, got %s", broker, config.Brokers[i])
		}
	}

	if config.ConsumerGroup != group {
		t.Errorf("Expected consumer group %s, got %s", group, config.ConsumerGroup)
	}

	if config.ClientID != clientID {
		t.Errorf("Expected client ID %s, got %s", clientID, config.ClientID)
	}
}

func TestConfigBuilderWithCompression(t *testing.T) {
	tests := []struct {
		name        string
		compression CompressionType
	}{
		{"None", CompressionNone},
		{"Gzip", CompressionGzip},
		{"Snappy", CompressionSnappy},
		{"Lz4", CompressionLz4},
		{"Zstd", CompressionZstd},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewConfigBuilder().
				WithCompression(tt.compression).
				Build()

			if config.Producer.Compression != tt.compression {
				t.Errorf("Expected compression %v, got %v", tt.compression, config.Producer.Compression)
			}
		})
	}
}

func TestConfigBuilderWithProducerSettings(t *testing.T) {
	batchSize := 2048
	linger := 20 * time.Millisecond
	maxRetries := 5

	config := NewConfigBuilder().
		WithProducerBatchSize(batchSize).
		WithProducerLinger(linger).
		WithProducerMaxRetries(maxRetries).
		Build()

	if config.Producer.BatchSize != batchSize {
		t.Errorf("Expected batch size %d, got %d", batchSize, config.Producer.BatchSize)
	}

	if config.Producer.Linger != linger {
		t.Errorf("Expected linger %v, got %v", linger, config.Producer.Linger)
	}

	if config.Producer.MaxRetries != maxRetries {
		t.Errorf("Expected max retries %d, got %d", maxRetries, config.Producer.MaxRetries)
	}
}

func TestConfigBuilderWithConsumerSettings(t *testing.T) {
	sessionTimeout := 60 * time.Second
	heartbeatInterval := 5 * time.Second
	maxPollRecords := 1000

	config := NewConfigBuilder().
		WithConsumerSessionTimeout(sessionTimeout).
		WithConsumerHeartbeatInterval(heartbeatInterval).
		WithConsumerMaxPollRecords(maxPollRecords).
		Build()

	if config.Consumer.SessionTimeout != sessionTimeout {
		t.Errorf("Expected session timeout %v, got %v", sessionTimeout, config.Consumer.SessionTimeout)
	}

	if config.Consumer.HeartbeatInterval != heartbeatInterval {
		t.Errorf("Expected heartbeat interval %v, got %v", heartbeatInterval, config.Consumer.HeartbeatInterval)
	}

	if config.Consumer.MaxPollRecords != maxPollRecords {
		t.Errorf("Expected max poll records %d, got %d", maxPollRecords, config.Consumer.MaxPollRecords)
	}
}

func TestConfigBuilderWithDLQ(t *testing.T) {
	dlqTopic := "test-dlq"
	maxRetries := 3
	retryDelay := 5 * time.Second

	config := NewConfigBuilder().
		WithDLQ(dlqTopic, maxRetries, retryDelay).
		Build()

	if !config.DLQ.Enabled {
		t.Error("DLQ should be enabled")
	}

	if config.DLQ.Topic != dlqTopic {
		t.Errorf("Expected DLQ topic %s, got %s", dlqTopic, config.DLQ.Topic)
	}

	if config.DLQ.MaxRetries != maxRetries {
		t.Errorf("Expected max retries %d, got %d", maxRetries, config.DLQ.MaxRetries)
	}

	if config.DLQ.RetryDelay != retryDelay {
		t.Errorf("Expected retry delay %v, got %v", retryDelay, config.DLQ.RetryDelay)
	}
}

func TestConfigBuilderWithSASL(t *testing.T) {
	tests := []struct {
		name       string
		mechanism  SASLMechanism
		username   string
		password   string
	}{
		{"PLAIN", SASLPlain, "user1", "pass1"},
		{"SCRAM-SHA-256", SASLScramSHA256, "user2", "pass2"},
		{"SCRAM-SHA-512", SASLScramSHA512, "user3", "pass3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saslConfig := NewSASLScramConfig(tt.mechanism, tt.username, tt.password)
			config := NewConfigBuilder().
				WithSASL(saslConfig).
				Build()

			if config.Security == nil {
				t.Fatal("Security config should not be nil")
			}

			if config.Security.SASL == nil {
				t.Fatal("SASL config should not be nil")
			}

			if config.Security.SASL.Mechanism != tt.mechanism {
				t.Errorf("Expected mechanism %v, got %v", tt.mechanism, config.Security.SASL.Mechanism)
			}

			if config.Security.SASL.Username != tt.username {
				t.Errorf("Expected username %s, got %s", tt.username, config.Security.SASL.Username)
			}

			if config.Security.SASL.Password != tt.password {
				t.Errorf("Expected password %s, got %s", tt.password, config.Security.SASL.Password)
			}
		})
	}
}

func TestConfigBuilderWithTLS(t *testing.T) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	config := NewConfigBuilder().
		WithTLS(tlsConfig).
		Build()

	if config.Security == nil {
		t.Fatal("Security config should not be nil")
	}

	if config.Security.TLS == nil {
		t.Fatal("TLS config should not be nil")
	}

	if config.Security.TLS.MinVersion != tls.VersionTLS12 {
		t.Errorf("Expected TLS version %d, got %d", tls.VersionTLS12, config.Security.TLS.MinVersion)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "Valid config",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "Empty brokers",
			config: &Config{
				Brokers: []string{},
			},
			wantErr: true,
		},
		{
			name: "Negative batch size",
			config: func() *Config {
				c := DefaultConfig()
				c.Producer.BatchSize = -1
				return c
			}(),
			wantErr: true,
		},
		{
			name: "Zero session timeout",
			config: func() *Config {
				c := DefaultConfig()
				c.Consumer.SessionTimeout = 0
				return c
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewSASLScramConfig(t *testing.T) {
	mechanism := SASLScramSHA256
	username := "test-user"
	password := "test-pass"

	config := NewSASLScramConfig(mechanism, username, password)

	if config.Mechanism != mechanism {
		t.Errorf("Expected mechanism %v, got %v", mechanism, config.Mechanism)
	}

	if config.Username != username {
		t.Errorf("Expected username %s, got %s", username, config.Username)
	}

	if config.Password != password {
		t.Errorf("Expected password %s, got %s", password, config.Password)
	}
}

func TestConfigClone(t *testing.T) {
	original := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("test-group").
		WithClientID("test-client").
		Build()

	// Modify clone
	cloned := DefaultConfig()
	cloned.Brokers = make([]string, len(original.Brokers))
	copy(cloned.Brokers, original.Brokers)
	cloned.ConsumerGroup = original.ConsumerGroup
	cloned.ClientID = original.ClientID

	// Modify original
	original.Brokers[0] = "different:9092"
	original.ConsumerGroup = "different-group"

	// Verify clone wasn't affected
	if cloned.Brokers[0] == original.Brokers[0] {
		t.Error("Clone's brokers were affected by original modification")
	}

	if cloned.ConsumerGroup == original.ConsumerGroup {
		t.Error("Clone's consumer group was affected by original modification")
	}
}

func TestCompressionTypeString(t *testing.T) {
	tests := []struct {
		compression CompressionType
		expected    string
	}{
		{CompressionNone, "none"},
		{CompressionGzip, "gzip"},
		{CompressionSnappy, "snappy"},
		{CompressionLz4, "lz4"},
		{CompressionZstd, "zstd"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.compression.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestSASLMechanismString(t *testing.T) {
	tests := []struct {
		mechanism SASLMechanism
		expected  string
	}{
		{SASLPlain, "PLAIN"},
		{SASLScramSHA256, "SCRAM-SHA-256"},
		{SASLScramSHA512, "SCRAM-SHA-512"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.mechanism.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestConfigBuilderChaining(t *testing.T) {
	// Test that builder methods can be chained
	config := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("test").
		WithClientID("test").
		WithCompression(CompressionZstd).
		WithProducerBatchSize(2048).
		WithProducerLinger(10 * time.Millisecond).
		WithConsumerSessionTimeout(30 * time.Second).
		WithDLQ("test-dlq", 3, 5*time.Second).
		Build()

	if config == nil {
		t.Fatal("Builder returned nil config")
	}

	// Verify all settings were applied
	if len(config.Brokers) != 1 || config.Brokers[0] != "localhost:9092" {
		t.Error("Brokers not set correctly")
	}

	if config.ConsumerGroup != "test" {
		t.Error("Consumer group not set correctly")
	}

	if config.Producer.Compression != CompressionZstd {
		t.Error("Compression not set correctly")
	}

	if !config.DLQ.Enabled {
		t.Error("DLQ not enabled")
	}
}

func BenchmarkConfigBuilder(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = NewConfigBuilder().
			WithBrokers([]string{"localhost:9092"}).
			WithConsumerGroup("test-group").
			WithClientID("test-client").
			Build()
	}
}

func BenchmarkDefaultConfig(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = DefaultConfig()
	}
}
