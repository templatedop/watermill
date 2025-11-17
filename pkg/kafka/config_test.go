package kafka

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config == nil {
		t.Fatal("DefaultConfig returned nil")
	}

	if len(config.Brokers) == 0 {
		t.Error("Expected default brokers to be set")
	}

	if config.ConsumerGroup == "" {
		t.Error("Expected default consumer group to be set")
	}

	if config.ClientID == "" {
		t.Error("Expected default client ID to be set")
	}

	// Validate producer defaults
	if config.Producer.RequiredAcks != sarama.WaitForAll {
		t.Error("Expected RequiredAcks to be WaitForAll")
	}

	if !config.Producer.Idempotent {
		t.Error("Expected idempotent producer")
	}

	// Validate consumer defaults
	if config.Consumer.SessionTimeout <= 0 {
		t.Error("Expected positive session timeout")
	}

	if config.Consumer.HeartbeatInterval <= 0 {
		t.Error("Expected positive heartbeat interval")
	}

	// Validate heartbeat is less than session timeout
	if config.Consumer.HeartbeatInterval >= config.Consumer.SessionTimeout {
		t.Error("HeartbeatInterval should be less than SessionTimeout")
	}
}

func TestEcommerceConfig(t *testing.T) {
	brokers := []string{"localhost:9092", "localhost:9093"}
	group := "test-group"

	config := EcommerceConfig(brokers, group)

	if config == nil {
		t.Fatal("EcommerceConfig returned nil")
	}

	if len(config.Brokers) != len(brokers) {
		t.Errorf("Expected %d brokers, got %d", len(brokers), len(config.Brokers))
	}

	if config.ConsumerGroup != group {
		t.Errorf("Expected consumer group %s, got %s", group, config.ConsumerGroup)
	}

	// Validate DLQ is enabled
	if !config.DLQ.Enabled {
		t.Error("Expected DLQ to be enabled in ecommerce config")
	}

	// Validate compression
	if config.Producer.Compression != sarama.CompressionSnappy {
		t.Error("Expected Snappy compression")
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Brokers:       []string{"localhost:9092"},
				ConsumerGroup: "test-group",
				ClientID:      "test-client",
				Version:       "3.6.0",
				Consumer: ConsumerConfig{
					SessionTimeout:    20 * time.Second,
					HeartbeatInterval: 3 * time.Second,
				},
			},
			expectErr: false,
		},
		{
			name: "no brokers",
			config: &Config{
				ConsumerGroup: "test-group",
			},
			expectErr: true,
		},
		{
			name: "no consumer group",
			config: &Config{
				Brokers: []string{"localhost:9092"},
			},
			expectErr: true,
		},
		{
			name: "heartbeat >= session timeout",
			config: &Config{
				Brokers:       []string{"localhost:9092"},
				ConsumerGroup: "test-group",
				Consumer: ConsumerConfig{
					SessionTimeout:    10 * time.Second,
					HeartbeatInterval: 15 * time.Second,
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestConfigValidateExtended(t *testing.T) {
	config := DefaultConfig()
	config.Brokers = []string{"localhost:9092"}
	config.ConsumerGroup = "test-group"

	result := config.ValidateExtended(ValidationLevelWarning)
	if result == nil {
		t.Fatal("Expected validation result")
	}

	if !result.Valid && len(result.Errors) == 0 {
		t.Error("If invalid, should have errors")
	}
}

func TestConfigValidateForEnvironment(t *testing.T) {
	config := DefaultConfig()
	config.Brokers = []string{"localhost:9092"}
	config.ConsumerGroup = "test-group"

	tests := []struct {
		env           string
		expectWarning bool
	}{
		{"development", false},
		{"production", true}, // Should warn about RequiredAcks if not WaitForAll
		{"staging", false},
	}

	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			result := config.ValidateForEnvironment(tt.env)
			if result == nil {
				t.Fatal("Expected validation result")
			}

			if tt.expectWarning && len(result.Warnings) == 0 {
				t.Error("Expected warnings for production environment")
			}
		})
	}
}

func TestConfigClone(t *testing.T) {
	original := DefaultConfig()
	original.Brokers = []string{"localhost:9092"}

	clone := original
	clone.Brokers[0] = "localhost:9093"

	// This is a shallow copy test - in real implementation,
	// we might want deep copy functionality
	if original.Brokers[0] == clone.Brokers[0] {
		t.Log("Note: Config uses shallow copy semantics")
	}
}

func BenchmarkDefaultConfig(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = DefaultConfig()
	}
}

func BenchmarkConfigValidate(b *testing.B) {
	config := DefaultConfig()
	config.Brokers = []string{"localhost:9092"}
	config.ConsumerGroup = "test-group"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = config.Validate()
	}
}
