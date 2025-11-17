package kafka

import (
	"fmt"
	"strings"
	"time"
)

// ValidationLevel defines the strictness of validation
type ValidationLevel int

const (
	// ValidationLevelError only returns errors for critical issues
	ValidationLevelError ValidationLevel = iota
	// ValidationLevelWarning returns warnings for potential issues
	ValidationLevelWarning
	// ValidationLevelStrict returns errors for any non-optimal configuration
	ValidationLevelStrict
)

// ValidationResult contains validation results
type ValidationResult struct {
	Valid    bool
	Errors   []string
	Warnings []string
}

// Error implements the error interface
func (v *ValidationResult) Error() string {
	if len(v.Errors) == 0 {
		return ""
	}
	return fmt.Sprintf("validation errors: %s", strings.Join(v.Errors, "; "))
}

// HasErrors returns true if there are validation errors
func (v *ValidationResult) HasErrors() bool {
	return len(v.Errors) > 0
}

// HasWarnings returns true if there are validation warnings
func (v *ValidationResult) HasWarnings() bool {
	return len(v.Warnings) > 0
}

// String returns a formatted string of all issues
func (v *ValidationResult) String() string {
	var parts []string

	if len(v.Errors) > 0 {
		parts = append(parts, fmt.Sprintf("Errors: %s", strings.Join(v.Errors, "; ")))
	}

	if len(v.Warnings) > 0 {
		parts = append(parts, fmt.Sprintf("Warnings: %s", strings.Join(v.Warnings, "; ")))
	}

	if len(parts) == 0 {
		return "Configuration is valid"
	}

	return strings.Join(parts, ". ")
}

// ValidateExtended performs comprehensive configuration validation
func (c *Config) ValidateExtended(level ValidationLevel) *ValidationResult {
	result := &ValidationResult{
		Valid:    true,
		Errors:   []string{},
		Warnings: []string{},
	}

	// Basic validation first
	if err := c.Validate(); err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, err.Error())
		return result
	}

	// Validate brokers
	c.validateBrokers(result, level)

	// Validate consumer configuration
	c.validateConsumer(result, level)

	// Validate producer configuration
	c.validateProducer(result, level)

	// Validate DLQ configuration
	c.validateDLQ(result, level)

	// Performance warnings
	if level >= ValidationLevelWarning {
		c.validatePerformance(result)
	}

	// Production readiness checks
	if level >= ValidationLevelStrict {
		c.validateProductionReadiness(result)
	}

	result.Valid = !result.HasErrors()
	return result
}

// validateBrokers validates broker configuration
func (c *Config) validateBrokers(result *ValidationResult, level ValidationLevel) {
	if len(c.Brokers) == 0 {
		result.Errors = append(result.Errors, "no brokers configured")
		return
	}

	// Check for localhost in production (warning)
	if level >= ValidationLevelWarning {
		for _, broker := range c.Brokers {
			if strings.Contains(broker, "localhost") || strings.Contains(broker, "127.0.0.1") {
				result.Warnings = append(result.Warnings,
					"localhost broker detected - may not be production configuration")
				break
			}
		}
	}

	// Recommend multiple brokers for HA
	if level >= ValidationLevelWarning && len(c.Brokers) == 1 {
		result.Warnings = append(result.Warnings,
			"single broker configured - consider using multiple brokers for high availability")
	}
}

// validateConsumer validates consumer configuration
func (c *Config) validateConsumer(result *ValidationResult, level ValidationLevel) {
	// Heartbeat interval must be less than session timeout
	if c.Consumer.HeartbeatInterval >= c.Consumer.SessionTimeout {
		result.Errors = append(result.Errors,
			fmt.Sprintf("HeartbeatInterval (%v) must be less than SessionTimeout (%v)",
				c.Consumer.HeartbeatInterval, c.Consumer.SessionTimeout))
	}

	// Recommended: heartbeat should be 1/3 of session timeout
	if level >= ValidationLevelWarning {
		recommendedHeartbeat := c.Consumer.SessionTimeout / 3
		if c.Consumer.HeartbeatInterval > recommendedHeartbeat {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("HeartbeatInterval (%v) should be < SessionTimeout/3 (%v) for optimal rebalancing",
					c.Consumer.HeartbeatInterval, recommendedHeartbeat))
		}
	}

	// Session timeout should be reasonable
	if level >= ValidationLevelWarning {
		if c.Consumer.SessionTimeout < 6*time.Second {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("SessionTimeout (%v) is very low - may cause frequent rebalances",
					c.Consumer.SessionTimeout))
		}
		if c.Consumer.SessionTimeout > 5*time.Minute {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("SessionTimeout (%v) is very high - may delay failure detection",
					c.Consumer.SessionTimeout))
		}
	}

	// MaxProcessingTime should be reasonable
	if c.Consumer.MaxProcessingTime < c.Consumer.HeartbeatInterval {
		result.Warnings = append(result.Warnings,
			"MaxProcessingTime should be >= HeartbeatInterval")
	}

	// Fetch configuration
	if c.Consumer.FetchMax < c.Consumer.FetchDefault {
		result.Errors = append(result.Errors,
			fmt.Sprintf("FetchMax (%d) must be >= FetchDefault (%d)",
				c.Consumer.FetchMax, c.Consumer.FetchDefault))
	}

	if c.Consumer.FetchDefault < c.Consumer.FetchMin {
		result.Errors = append(result.Errors,
			fmt.Sprintf("FetchDefault (%d) must be >= FetchMin (%d)",
				c.Consumer.FetchDefault, c.Consumer.FetchMin))
	}
}

// validateProducer validates producer configuration
func (c *Config) validateProducer(result *ValidationResult, level ValidationLevel) {
	// Check for data loss scenarios
	if level >= ValidationLevelWarning {
		if c.Producer.RequiredAcks == 0 {
			result.Warnings = append(result.Warnings,
				"RequiredAcks=0 (NoResponse) - messages may be lost if broker fails")
		} else if c.Producer.RequiredAcks == 1 {
			result.Warnings = append(result.Warnings,
				"RequiredAcks=1 (WaitForLocal) - messages may be lost if leader fails before replication")
		}
	}

	// Production readiness for acks
	if level >= ValidationLevelStrict && c.Producer.RequiredAcks != -1 {
		result.Errors = append(result.Errors,
			"RequiredAcks should be -1 (WaitForAll) for production to prevent data loss")
	}

	// Idempotency check
	if level >= ValidationLevelWarning && !c.Producer.Idempotent {
		result.Warnings = append(result.Warnings,
			"Idempotent producer disabled - duplicate messages possible")
	}

	// Max message size
	if level >= ValidationLevelWarning && c.Producer.MaxMessageBytes > 10*1024*1024 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("MaxMessageBytes (%d) is very large - may impact performance",
				c.Producer.MaxMessageBytes))
	}

	// Timeout configuration
	if c.Producer.Timeout < time.Second {
		result.Warnings = append(result.Warnings,
			"Producer timeout is very low - may cause premature timeouts")
	}

	// Retry configuration
	if level >= ValidationLevelWarning && c.Producer.MaxRetries == 0 {
		result.Warnings = append(result.Warnings,
			"MaxRetries=0 - transient failures will not be retried")
	}
}

// validateDLQ validates DLQ configuration
func (c *Config) validateDLQ(result *ValidationResult, level ValidationLevel) {
	if !c.DLQ.Enabled {
		if level >= ValidationLevelWarning {
			result.Warnings = append(result.Warnings,
				"DLQ is disabled - failed messages will be lost after max retries")
		}
		return
	}

	// DLQ topic must be set
	if c.DLQ.Topic == "" {
		result.Errors = append(result.Errors, "DLQ enabled but topic not specified")
	}

	// DLQ topic shouldn't be same as input topics (would create a loop)
	if level >= ValidationLevelWarning {
		// This is a basic check - full check would require knowing all input topics
		if strings.Contains(c.DLQ.Topic, "dlq") || strings.Contains(c.DLQ.Topic, "DLQ") {
			// Good naming convention
		} else {
			result.Warnings = append(result.Warnings,
				"DLQ topic name should contain 'dlq' or 'DLQ' for clarity")
		}
	}

	// Retry configuration
	if c.DLQ.MaxRetries == 0 {
		result.Warnings = append(result.Warnings,
			"DLQ MaxRetries=0 - messages will go to DLQ immediately on first failure")
	}
}

// validatePerformance validates performance-related settings
func (c *Config) validatePerformance(result *ValidationResult) {
	// Compression
	if c.Producer.Compression == 0 {
		result.Warnings = append(result.Warnings,
			"No compression enabled - consider using Snappy or LZ4 for better throughput")
	}

	// Batch fetching
	if c.Consumer.FetchDefault < 100*1024 {
		result.Warnings = append(result.Warnings,
			"FetchDefault is low - may impact throughput")
	}

	// Auto-commit interval
	if c.Consumer.AutoCommit && c.Consumer.OffsetCommitInterval < 100*time.Millisecond {
		result.Warnings = append(result.Warnings,
			"Very frequent offset commits - may impact performance")
	}
}

// validateProductionReadiness validates production readiness
func (c *Config) validateProductionReadiness(result *ValidationResult) {
	// Must have proper acks for production
	if c.Producer.RequiredAcks != -1 {
		result.Errors = append(result.Errors,
			"Production requires RequiredAcks=-1 (WaitForAll)")
	}

	// Should have idempotency enabled
	if !c.Producer.Idempotent {
		result.Errors = append(result.Errors,
			"Production requires idempotent producer")
	}

	// Should have DLQ enabled
	if !c.DLQ.Enabled {
		result.Errors = append(result.Errors,
			"Production requires DLQ enabled")
	}

	// Should have reasonable timeouts
	if c.Consumer.SessionTimeout < 10*time.Second {
		result.Errors = append(result.Errors,
			"Production requires SessionTimeout >= 10s")
	}

	// Should have compression
	if c.Producer.Compression == 0 {
		result.Warnings = append(result.Warnings,
			"Production should use compression (Snappy or LZ4)")
	}
}

// ValidateForEnvironment validates configuration for a specific environment
func (c *Config) ValidateForEnvironment(env string) *ValidationResult {
	switch env {
	case "development", "dev", "local":
		return c.ValidateExtended(ValidationLevelWarning)
	case "staging", "stage":
		return c.ValidateExtended(ValidationLevelStrict)
	case "production", "prod":
		return c.ValidateExtended(ValidationLevelStrict)
	default:
		return c.ValidateExtended(ValidationLevelWarning)
	}
}

// QuickValidate performs a quick validation (errors only)
func (c *Config) QuickValidate() error {
	result := c.ValidateExtended(ValidationLevelError)
	if !result.Valid {
		return result
	}
	return nil
}

// ValidateAndWarn validates and logs warnings
func (c *Config) ValidateAndWarn() error {
	result := c.ValidateExtended(ValidationLevelWarning)

	if result.HasWarnings() && c.Logger != nil {
		for _, warning := range result.Warnings {
			c.Logger.Info("Configuration warning", map[string]interface{}{
				"warning": warning,
			})
		}
	}

	if !result.Valid {
		return result
	}

	return nil
}
