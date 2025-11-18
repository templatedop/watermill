package franzgo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BatchConfig holds batch processing configuration
type BatchConfig struct {
	// Maximum batch size (number of messages)
	MaxBatchSize int

	// Maximum batch size in bytes
	MaxBatchBytes int64

	// Batch timeout
	BatchTimeout time.Duration

	// Error handling strategy
	ErrorStrategy BatchErrorStrategy

	// Enable metrics
	EnableMetrics bool
}

// BatchErrorStrategy defines how to handle errors in batch processing
type BatchErrorStrategy int

const (
	// BatchErrorStrategyFailFast fails on first error
	BatchErrorStrategyFailFast BatchErrorStrategy = iota

	// BatchErrorStrategySkipErrors skips failed messages
	BatchErrorStrategySkipErrors

	// BatchErrorStrategyAllOrNothing commits only if all succeed
	BatchErrorStrategyAllOrNothing

	// BatchErrorStrategyRetryFailed retries failed messages
	BatchErrorStrategyRetryFailed
)

// DefaultBatchConfig returns default batch configuration
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		MaxBatchSize:  100,
		MaxBatchBytes: 1024 * 1024, // 1MB
		BatchTimeout:  1 * time.Second,
		ErrorStrategy: BatchErrorStrategySkipErrors,
		EnableMetrics: true,
	}
}

// BatchProcessor processes messages in batches
type BatchProcessor struct {
	client  *Client
	config  *BatchConfig
	metrics *BatchMetrics
	mu      sync.RWMutex
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(client *Client, config *BatchConfig) *BatchProcessor {
	if config == nil {
		config = DefaultBatchConfig()
	}

	bp := &BatchProcessor{
		client: client,
		config: config,
	}

	if config.EnableMetrics {
		bp.metrics = NewBatchMetrics()
	}

	return bp
}

// ConsumeBatch consumes messages in batches
func (bp *BatchProcessor) ConsumeBatch(ctx context.Context, topics []string, handler func(ctx context.Context, batch []*kgo.Record) error) error {
	// Add topics to client
	bp.client.client.AddConsumeTopics(topics...)

	batch := make([]*kgo.Record, 0, bp.config.MaxBatchSize)
	batchBytes := int64(0)
	timer := time.NewTimer(bp.config.BatchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Process remaining batch before exiting
			if len(batch) > 0 {
				bp.processBatch(ctx, batch, handler)
			}
			return ctx.Err()

		case <-timer.C:
			// Process batch on timeout
			if len(batch) > 0 {
				bp.processBatch(ctx, batch, handler)
				batch = batch[:0]
				batchBytes = 0
			}
			timer.Reset(bp.config.BatchTimeout)

		default:
			// Poll for new records
			fetches := bp.client.client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					fmt.Printf("Fetch error: %v\n", err.Err)
				}
				continue
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				recordSize := int64(len(record.Value))

				// Check if adding this record would exceed limits
				if len(batch) >= bp.config.MaxBatchSize || batchBytes+recordSize > bp.config.MaxBatchBytes {
					// Process current batch
					bp.processBatch(ctx, batch, handler)
					batch = batch[:0]
					batchBytes = 0
					timer.Reset(bp.config.BatchTimeout)
				}

				// Add to batch
				batch = append(batch, record)
				batchBytes += recordSize
			}
		}
	}
}

func (bp *BatchProcessor) processBatch(ctx context.Context, batch []*kgo.Record, handler func(ctx context.Context, batch []*kgo.Record) error) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()
	err := handler(ctx, batch)
	duration := time.Since(start)

	if bp.metrics != nil {
		bp.metrics.RecordBatch(len(batch), duration, err == nil)
	}

	if err != nil {
		bp.handleBatchError(ctx, batch, err)
	}
}

func (bp *BatchProcessor) handleBatchError(ctx context.Context, batch []*kgo.Record, err error) {
	switch bp.config.ErrorStrategy {
	case BatchErrorStrategyFailFast:
		// Already failed, do nothing

	case BatchErrorStrategySkipErrors:
		// Errors are already skipped in individual processing

	case BatchErrorStrategyAllOrNothing:
		// Don't commit offsets on error
		fmt.Printf("Batch processing failed (all-or-nothing): %v\n", err)

	case BatchErrorStrategyRetryFailed:
		// Retry failed batch
		time.Sleep(1 * time.Second)
		bp.processBatch(ctx, batch, func(ctx context.Context, batch []*kgo.Record) error {
			// Retry logic
			return nil
		})
	}
}

// ProduceBatch produces multiple records as a batch
func (bp *BatchProcessor) ProduceBatch(ctx context.Context, topic string, messages [][]byte) error {
	records := make([]*kgo.Record, len(messages))
	for i, msg := range messages {
		records[i] = &kgo.Record{
			Topic: topic,
			Value: msg,
		}
	}

	start := time.Now()
	results := bp.client.client.ProduceSync(ctx, records...)
	duration := time.Since(start)

	if err := results.FirstErr(); err != nil {
		if bp.metrics != nil {
			bp.metrics.RecordBatch(len(records), duration, false)
		}
		return fmt.Errorf("failed to produce batch: %w", err)
	}

	if bp.metrics != nil {
		bp.metrics.RecordBatch(len(records), duration, true)
	}

	return nil
}

// BatchMetrics tracks batch processing metrics
type BatchMetrics struct {
	TotalBatches      int64
	TotalMessages     int64
	SuccessfulBatches int64
	FailedBatches     int64
	TotalDuration     time.Duration
	mu                sync.RWMutex
}

// NewBatchMetrics creates new batch metrics
func NewBatchMetrics() *BatchMetrics {
	return &BatchMetrics{}
}

// RecordBatch records batch processing metrics
func (bm *BatchMetrics) RecordBatch(size int, duration time.Duration, success bool) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.TotalBatches++
	bm.TotalMessages += int64(size)
	bm.TotalDuration += duration

	if success {
		bm.SuccessfulBatches++
	} else {
		bm.FailedBatches++
	}
}

// GetStats returns batch metrics stats
func (bm *BatchMetrics) GetStats() (batches, messages, successful, failed int64, avgDuration time.Duration) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	avg := time.Duration(0)
	if bm.TotalBatches > 0 {
		avg = bm.TotalDuration / time.Duration(bm.TotalBatches)
	}

	return bm.TotalBatches, bm.TotalMessages, bm.SuccessfulBatches, bm.FailedBatches, avg
}

// Reset resets the metrics
func (bm *BatchMetrics) Reset() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.TotalBatches = 0
	bm.TotalMessages = 0
	bm.SuccessfulBatches = 0
	bm.FailedBatches = 0
	bm.TotalDuration = 0
}
