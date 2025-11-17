package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// BatchErrorStrategy determines how to handle errors in batch processing
type BatchErrorStrategy int

const (
	// BatchErrorStrategyFailFast stops on first error and nacks all messages
	BatchErrorStrategyFailFast BatchErrorStrategy = iota

	// BatchErrorStrategySkipErrors continues processing and acks successful messages
	BatchErrorStrategySkipErrors

	// BatchErrorStrategyAllOrNothing processes all, but only acks if all succeed
	BatchErrorStrategyAllOrNothing

	// BatchErrorStrategyRetryFailed retries failed messages individually
	BatchErrorStrategyRetryFailed
)

// BatchConfig configures advanced batch processing
type BatchConfig struct {
	// MaxBatchSize is the maximum number of messages per batch
	MaxBatchSize int

	// MaxBatchBytes is the maximum total bytes per batch
	MaxBatchBytes int64

	// BatchTimeout triggers batch processing after this duration
	BatchTimeout time.Duration

	// ErrorStrategy determines how to handle errors
	ErrorStrategy BatchErrorStrategy

	// PartitionByKey groups messages by key for ordered processing
	PartitionByKey bool

	// MaxRetries for individual messages (used with BatchErrorStrategyRetryFailed)
	MaxRetries int

	// RetryDelay between retries
	RetryDelay time.Duration

	// EnableMetrics enables batch metrics collection
	EnableMetrics bool
}

// DefaultBatchConfig returns sensible defaults
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		MaxBatchSize:  100,
		MaxBatchBytes: 10 * 1024 * 1024, // 10MB
		BatchTimeout:  5 * time.Second,
		ErrorStrategy: BatchErrorStrategyAllOrNothing,
		MaxRetries:    3,
		RetryDelay:    1 * time.Second,
		EnableMetrics: true,
	}
}

// AdvancedBatchConsumer provides advanced batch processing
type AdvancedBatchConsumer struct {
	client  *Client
	config  *BatchConfig
	logger  watermill.LoggerAdapter
	metrics *BatchMetrics
}

// NewAdvancedBatchConsumer creates a new advanced batch consumer
func NewAdvancedBatchConsumer(client *Client, config *BatchConfig) *AdvancedBatchConsumer {
	if config == nil {
		config = DefaultBatchConfig()
	}

	var metrics *BatchMetrics
	if config.EnableMetrics {
		metrics = NewBatchMetrics()
	}

	return &AdvancedBatchConsumer{
		client:  client,
		config:  config,
		logger:  client.logger,
		metrics: metrics,
	}
}

// AdvancedBatchHandler processes a batch with more control
type AdvancedBatchHandler func(ctx context.Context, batch *MessageBatch) error

// MessageBatch represents a batch of messages with metadata
type MessageBatch struct {
	Messages   []*message.Message
	TotalBytes int64
	Keys       map[string][]*message.Message // For partitioned batching
}

// SubscribeBatch subscribes with advanced batch processing
func (c *AdvancedBatchConsumer) SubscribeBatch(ctx context.Context, topic string, handler AdvancedBatchHandler) error {
	messages, err := c.client.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return ErrSubscribeFailed{Topic: topic, Err: err}
	}

	if c.config.PartitionByKey {
		go c.processPartitionedBatch(ctx, messages, handler)
	} else {
		go c.processSimpleBatch(ctx, messages, handler)
	}

	return nil
}

// processSimpleBatch processes messages in simple time/size batches
func (c *AdvancedBatchConsumer) processSimpleBatch(ctx context.Context, messages <-chan *message.Message, handler AdvancedBatchHandler) {
	batch := &MessageBatch{
		Messages: make([]*message.Message, 0, c.config.MaxBatchSize),
	}
	ticker := time.NewTicker(c.config.BatchTimeout)
	defer ticker.Stop()

	processBatch := func() {
		if len(batch.Messages) == 0 {
			return
		}

		start := time.Now()
		c.executeBatch(ctx, batch, handler)

		if c.metrics != nil {
			c.metrics.RecordBatch(len(batch.Messages), batch.TotalBytes, time.Since(start))
		}

		// Reset batch
		batch.Messages = batch.Messages[:0]
		batch.TotalBytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			processBatch()
			return
		case <-ticker.C:
			processBatch()
		case msg, ok := <-messages:
			if !ok {
				processBatch()
				return
			}

			batch.Messages = append(batch.Messages, msg)
			batch.TotalBytes += int64(len(msg.Payload))

			// Process if batch size or bytes limit reached
			if len(batch.Messages) >= c.config.MaxBatchSize || batch.TotalBytes >= c.config.MaxBatchBytes {
				processBatch()
			}
		}
	}
}

// processPartitionedBatch processes messages grouped by partition key
func (c *AdvancedBatchConsumer) processPartitionedBatch(ctx context.Context, messages <-chan *message.Message, handler AdvancedBatchHandler) {
	batches := make(map[string]*MessageBatch)
	ticker := time.NewTicker(c.config.BatchTimeout)
	defer ticker.Stop()

	processAllBatches := func() {
		for key, batch := range batches {
			if len(batch.Messages) == 0 {
				continue
			}

			start := time.Now()
			c.executeBatch(ctx, batch, handler)

			if c.metrics != nil {
				c.metrics.RecordBatch(len(batch.Messages), batch.TotalBytes, time.Since(start))
			}

			delete(batches, key)
		}
	}

	for {
		select {
		case <-ctx.Done():
			processAllBatches()
			return
		case <-ticker.C:
			processAllBatches()
		case msg, ok := <-messages:
			if !ok {
				processAllBatches()
				return
			}

			key := msg.Metadata.Get("partition_key")
			if key == "" {
				key = "default"
			}

			batch, exists := batches[key]
			if !exists {
				batch = &MessageBatch{
					Messages: make([]*message.Message, 0, c.config.MaxBatchSize),
					Keys:     make(map[string][]*message.Message),
				}
				batches[key] = batch
			}

			batch.Messages = append(batch.Messages, msg)
			batch.TotalBytes += int64(len(msg.Payload))
			batch.Keys[key] = append(batch.Keys[key], msg)

			// Process if batch size limit reached
			if len(batch.Messages) >= c.config.MaxBatchSize || batch.TotalBytes >= c.config.MaxBatchBytes {
				start := time.Now()
				c.executeBatch(ctx, batch, handler)

				if c.metrics != nil {
					c.metrics.RecordBatch(len(batch.Messages), batch.TotalBytes, time.Since(start))
				}

				delete(batches, key)
			}
		}
	}
}

// executeBatch executes the batch based on error strategy
func (c *AdvancedBatchConsumer) executeBatch(ctx context.Context, batch *MessageBatch, handler AdvancedBatchHandler) {
	switch c.config.ErrorStrategy {
	case BatchErrorStrategyFailFast:
		c.executeFailFast(ctx, batch, handler)
	case BatchErrorStrategySkipErrors:
		c.executeSkipErrors(ctx, batch, handler)
	case BatchErrorStrategyAllOrNothing:
		c.executeAllOrNothing(ctx, batch, handler)
	case BatchErrorStrategyRetryFailed:
		c.executeRetryFailed(ctx, batch, handler)
	}
}

// executeFailFast stops on first error
func (c *AdvancedBatchConsumer) executeFailFast(ctx context.Context, batch *MessageBatch, handler AdvancedBatchHandler) {
	if err := handler(ctx, batch); err != nil {
		c.logger.Error("Batch processing failed", err, watermill.LogFields{
			"batch_size": len(batch.Messages),
		})
		for _, msg := range batch.Messages {
			msg.Nack()
		}
		if c.metrics != nil {
			c.metrics.RecordError()
		}
	} else {
		for _, msg := range batch.Messages {
			msg.Ack()
		}
		if c.metrics != nil {
			c.metrics.RecordSuccess()
		}
	}
}

// executeSkipErrors processes each message individually on error
func (c *AdvancedBatchConsumer) executeSkipErrors(ctx context.Context, batch *MessageBatch, handler AdvancedBatchHandler) {
	if err := handler(ctx, batch); err != nil {
		c.logger.Error("Batch processing failed, retrying individually", err, nil)

		// Process each message individually
		for _, msg := range batch.Messages {
			singleBatch := &MessageBatch{
				Messages:   []*message.Message{msg},
				TotalBytes: int64(len(msg.Payload)),
			}

			if err := handler(ctx, singleBatch); err != nil {
				c.logger.Error("Individual message failed", err, watermill.LogFields{
					"message_uuid": msg.UUID,
				})
				msg.Nack()
				if c.metrics != nil {
					c.metrics.RecordError()
				}
			} else {
				msg.Ack()
			}
		}
	} else {
		for _, msg := range batch.Messages {
			msg.Ack()
		}
		if c.metrics != nil {
			c.metrics.RecordSuccess()
		}
	}
}

// executeAllOrNothing acks all or nacks all
func (c *AdvancedBatchConsumer) executeAllOrNothing(ctx context.Context, batch *MessageBatch, handler AdvancedBatchHandler) {
	if err := handler(ctx, batch); err != nil {
		c.logger.Error("Batch processing failed, nacking all", err, watermill.LogFields{
			"batch_size": len(batch.Messages),
		})
		for _, msg := range batch.Messages {
			msg.Nack()
		}
		if c.metrics != nil {
			c.metrics.RecordError()
		}
	} else {
		for _, msg := range batch.Messages {
			msg.Ack()
		}
		if c.metrics != nil {
			c.metrics.RecordSuccess()
		}
	}
}

// executeRetryFailed retries failed messages with backoff
func (c *AdvancedBatchConsumer) executeRetryFailed(ctx context.Context, batch *MessageBatch, handler AdvancedBatchHandler) {
	if err := handler(ctx, batch); err != nil {
		c.logger.Error("Batch processing failed, retrying with backoff", err, nil)

		// Retry logic
		for _, msg := range batch.Messages {
			success := false
			for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
				if attempt > 0 {
					time.Sleep(c.config.RetryDelay * time.Duration(attempt))
				}

				singleBatch := &MessageBatch{
					Messages:   []*message.Message{msg},
					TotalBytes: int64(len(msg.Payload)),
				}

				if err := handler(ctx, singleBatch); err == nil {
					success = true
					break
				}
			}

			if success {
				msg.Ack()
			} else {
				msg.Nack()
				if c.metrics != nil {
					c.metrics.RecordError()
				}
			}
		}
	} else {
		for _, msg := range batch.Messages {
			msg.Ack()
		}
		if c.metrics != nil {
			c.metrics.RecordSuccess()
		}
	}
}

// GetMetrics returns batch metrics
func (c *AdvancedBatchConsumer) GetMetrics() *BatchMetrics {
	return c.metrics
}

// BatchMetrics tracks batch processing metrics
type BatchMetrics struct {
	mu                    sync.RWMutex
	totalBatches          int64
	totalMessages         int64
	totalBytes            int64
	totalProcessingTimeMs int64
	successfulBatches     int64
	failedBatches         int64
}

// NewBatchMetrics creates new batch metrics
func NewBatchMetrics() *BatchMetrics {
	return &BatchMetrics{}
}

// RecordBatch records a processed batch
func (m *BatchMetrics) RecordBatch(messageCount int, bytes int64, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalBatches++
	m.totalMessages += int64(messageCount)
	m.totalBytes += bytes
	m.totalProcessingTimeMs += duration.Milliseconds()
}

// RecordSuccess records a successful batch
func (m *BatchMetrics) RecordSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.successfulBatches++
}

// RecordError records a failed batch
func (m *BatchMetrics) RecordError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedBatches++
}

// GetStats returns current metrics
func (m *BatchMetrics) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	avgBatchSize := float64(0)
	if m.totalBatches > 0 {
		avgBatchSize = float64(m.totalMessages) / float64(m.totalBatches)
	}

	avgProcessingTime := float64(0)
	if m.totalBatches > 0 {
		avgProcessingTime = float64(m.totalProcessingTimeMs) / float64(m.totalBatches)
	}

	successRate := float64(0)
	if m.totalBatches > 0 {
		successRate = float64(m.successfulBatches) / float64(m.totalBatches) * 100
	}

	return map[string]interface{}{
		"total_batches":           m.totalBatches,
		"total_messages":          m.totalMessages,
		"total_bytes":             m.totalBytes,
		"avg_batch_size":          avgBatchSize,
		"avg_processing_time_ms":  avgProcessingTime,
		"successful_batches":      m.successfulBatches,
		"failed_batches":          m.failedBatches,
		"success_rate_percent":    successRate,
		"throughput_messages_sec": m.getThroughput(),
	}
}

// getThroughput calculates messages per second
func (m *BatchMetrics) getThroughput() float64 {
	if m.totalProcessingTimeMs == 0 {
		return 0
	}
	return float64(m.totalMessages) / (float64(m.totalProcessingTimeMs) / 1000.0)
}

// Reset resets all metrics
func (m *BatchMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalBatches = 0
	m.totalMessages = 0
	m.totalBytes = 0
	m.totalProcessingTimeMs = 0
	m.successfulBatches = 0
	m.failedBatches = 0
}
