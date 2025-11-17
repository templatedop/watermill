package kafka

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/ThreeDotsLabs/watermill"
)

// StructuredLogger wraps slog for structured logging
type StructuredLogger struct {
	logger *slog.Logger
	level  slog.Level
}

// NewStructuredLogger creates a new structured logger with JSON output
func NewStructuredLogger(level slog.Level) *StructuredLogger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Customize attribute names for consistency
			if a.Key == slog.TimeKey {
				return slog.Attr{Key: "timestamp", Value: a.Value}
			}
			if a.Key == slog.LevelKey {
				return slog.Attr{Key: "level", Value: a.Value}
			}
			if a.Key == slog.MessageKey {
				return slog.Attr{Key: "message", Value: a.Value}
			}
			return a
		},
	})

	return &StructuredLogger{
		logger: slog.New(handler),
		level:  level,
	}
}

// NewStructuredLoggerWithWriter creates a structured logger with custom writer
func NewStructuredLoggerWithWriter(writer io.Writer, level slog.Level) *StructuredLogger {
	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{
		Level: level,
	})

	return &StructuredLogger{
		logger: slog.New(handler),
		level:  level,
	}
}

// NewTextLogger creates a human-readable text logger
func NewTextLogger(level slog.Level) *StructuredLogger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})

	return &StructuredLogger{
		logger: slog.New(handler),
		level:  level,
	}
}

// Error logs an error message with fields
func (l *StructuredLogger) Error(msg string, err error, fields watermill.LogFields) {
	attrs := l.fieldsToAttrs(fields)
	if err != nil {
		attrs = append(attrs, slog.Any("error", err.Error()))
	}
	l.logger.LogAttrs(context.Background(), slog.LevelError, msg, attrs...)
}

// Info logs an info message with fields
func (l *StructuredLogger) Info(msg string, fields watermill.LogFields) {
	attrs := l.fieldsToAttrs(fields)
	l.logger.LogAttrs(context.Background(), slog.LevelInfo, msg, attrs...)
}

// Debug logs a debug message with fields
func (l *StructuredLogger) Debug(msg string, fields watermill.LogFields) {
	attrs := l.fieldsToAttrs(fields)
	l.logger.LogAttrs(context.Background(), slog.LevelDebug, msg, attrs...)
}

// Trace logs a trace message with fields
func (l *StructuredLogger) Trace(msg string, fields watermill.LogFields) {
	// slog doesn't have trace, use debug with trace marker
	attrs := l.fieldsToAttrs(fields)
	attrs = append(attrs, slog.Bool("trace", true))
	l.logger.LogAttrs(context.Background(), slog.LevelDebug, msg, attrs...)
}

// With returns a new logger with additional fields
func (l *StructuredLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	attrs := l.fieldsToAttrs(fields)
	return &StructuredLogger{
		logger: l.logger.With(attrs...),
		level:  l.level,
	}
}

// fieldsToAttrs converts watermill fields to slog attributes
func (l *StructuredLogger) fieldsToAttrs(fields watermill.LogFields) []slog.Attr {
	if fields == nil {
		return []slog.Attr{}
	}

	attrs := make([]slog.Attr, 0, len(fields))
	for k, v := range fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	return attrs
}

// LogContext adds contextual information to log entries
type LogContext struct {
	ServiceName   string
	ServiceID     string
	Environment   string
	Version       string
	CorrelationID string
	TraceID       string
	SpanID        string
}

// ContextualLogger wraps a structured logger with context
type ContextualLogger struct {
	*StructuredLogger
	context LogContext
}

// NewContextualLogger creates a logger with context
func NewContextualLogger(level slog.Level, ctx LogContext) *ContextualLogger {
	baseLogger := NewStructuredLogger(level)

	// Add context fields to all log entries
	contextAttrs := []any{
		slog.String("service", ctx.ServiceName),
		slog.String("service_id", ctx.ServiceID),
		slog.String("environment", ctx.Environment),
		slog.String("version", ctx.Version),
	}

	return &ContextualLogger{
		StructuredLogger: &StructuredLogger{
			logger: baseLogger.logger.With(contextAttrs...),
			level:  level,
		},
		context: ctx,
	}
}

// WithCorrelation adds correlation ID to log entries
func (l *ContextualLogger) WithCorrelation(correlationID string) *ContextualLogger {
	return &ContextualLogger{
		StructuredLogger: &StructuredLogger{
			logger: l.logger.With(slog.String("correlation_id", correlationID)),
			level:  l.level,
		},
		context: l.context,
	}
}

// WithTrace adds trace context to log entries
func (l *ContextualLogger) WithTrace(traceID, spanID string) *ContextualLogger {
	return &ContextualLogger{
		StructuredLogger: &StructuredLogger{
			logger: l.logger.With(
				slog.String("trace_id", traceID),
				slog.String("span_id", spanID),
			),
			level: l.level,
		},
		context: l.context,
	}
}

// KafkaLogger provides Kafka-specific logging helpers
type KafkaLogger struct {
	*StructuredLogger
}

// NewKafkaLogger creates a Kafka-specific logger
func NewKafkaLogger(level slog.Level) *KafkaLogger {
	return &KafkaLogger{
		StructuredLogger: NewStructuredLogger(level),
	}
}

// LogProduced logs a message production event
func (l *KafkaLogger) LogProduced(topic, messageID string, partition int32, offset int64, latencyMs int64) {
	l.Info("Message produced", watermill.LogFields{
		"event_type": "message_produced",
		"topic":      topic,
		"message_id": messageID,
		"partition":  partition,
		"offset":     offset,
		"latency_ms": latencyMs,
	})
}

// LogConsumed logs a message consumption event
func (l *KafkaLogger) LogConsumed(topic, messageID string, partition int32, offset int64) {
	l.Info("Message consumed", watermill.LogFields{
		"event_type": "message_consumed",
		"topic":      topic,
		"message_id": messageID,
		"partition":  partition,
		"offset":     offset,
	})
}

// LogProcessed logs a message processing completion
func (l *KafkaLogger) LogProcessed(topic, messageID string, success bool, latencyMs int64, err error) {
	fields := watermill.LogFields{
		"event_type": "message_processed",
		"topic":      topic,
		"message_id": messageID,
		"success":    success,
		"latency_ms": latencyMs,
	}

	if success {
		l.Info("Message processed successfully", fields)
	} else {
		l.Error("Message processing failed", err, fields)
	}
}

// LogBatchProcessed logs a batch processing event
func (l *KafkaLogger) LogBatchProcessed(topic string, batchSize int, totalBytes int64, latencyMs int64, successCount int, failedCount int) {
	l.Info("Batch processed", watermill.LogFields{
		"event_type":    "batch_processed",
		"topic":         topic,
		"batch_size":    batchSize,
		"total_bytes":   totalBytes,
		"latency_ms":    latencyMs,
		"success_count": successCount,
		"failed_count":  failedCount,
	})
}

// LogRebalance logs a consumer rebalance event
func (l *KafkaLogger) LogRebalance(consumerGroup string, assignedPartitions []int32, revokedPartitions []int32) {
	l.Info("Consumer rebalance", watermill.LogFields{
		"event_type":          "consumer_rebalance",
		"consumer_group":      consumerGroup,
		"assigned_partitions": assignedPartitions,
		"revoked_partitions":  revokedPartitions,
	})
}

// LogDLQ logs a message sent to DLQ
func (l *KafkaLogger) LogDLQ(topic, messageID string, retryCount int, reason string, err error) {
	l.Error("Message sent to DLQ", err, watermill.LogFields{
		"event_type":  "dlq_message",
		"topic":       topic,
		"message_id":  messageID,
		"retry_count": retryCount,
		"reason":      reason,
	})
}

// LogStateChange logs a processor state change
func (l *KafkaLogger) LogStateChange(processorName, key string, oldValue, newValue interface{}) {
	l.Info("Processor state changed", watermill.LogFields{
		"event_type":     "state_change",
		"processor_name": processorName,
		"key":            key,
		"old_value":      oldValue,
		"new_value":      newValue,
	})
}
