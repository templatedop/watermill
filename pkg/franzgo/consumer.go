package franzgo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HandlerFunc is the message handler function
type HandlerFunc func(ctx context.Context, record *kgo.Record) error

// Consumer provides high-level consumer functionality
type Consumer struct {
	client    *Client
	dlq       *DLQHandler
	mu        sync.RWMutex
	running   bool
	stopChan  chan struct{}
}

// NewConsumer creates a new consumer
func NewConsumer(client *Client) *Consumer {
	consumer := &Consumer{
		client:   client,
		stopChan: make(chan struct{}),
	}

	// Initialize DLQ if enabled
	if client.config.DLQ.Enabled {
		consumer.dlq = NewDLQHandler(client, &client.config.DLQ)
	}

	return consumer
}

// Consume starts consuming messages from topics
func (c *Consumer) Consume(ctx context.Context, topics []string, handler HandlerFunc) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("consumer is already running")
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	// Add topics to client
	c.client.client.AddConsumeTopics(topics...)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopChan:
			return nil
		default:
			fetches := c.client.client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					fmt.Printf("Fetch error: %v\n", err.Err)
				}
				continue
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				if err := c.handleRecord(ctx, record, handler); err != nil {
					fmt.Printf("Handler error: %v\n", err)
				}
			}
		}
	}
}

func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record, handler HandlerFunc) error {
	// Try to process the message
	err := handler(ctx, record)
	if err != nil {
		// If DLQ is enabled, handle retry logic
		if c.dlq != nil {
			return c.dlq.HandleFailedMessage(ctx, record, err)
		}
		return err
	}

	return nil
}

// ConsumeWithRetry consumes messages with automatic retry
func (c *Consumer) ConsumeWithRetry(ctx context.Context, topics []string, handler HandlerFunc, maxRetries int) error {
	wrappedHandler := func(ctx context.Context, record *kgo.Record) error {
		var lastErr error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			err := handler(ctx, record)
			if err == nil {
				return nil
			}

			lastErr = err
			if attempt < maxRetries {
				// Exponential backoff
				backoff := time.Duration(1<<uint(attempt)) * time.Second
				time.Sleep(backoff)
			}
		}
		return lastErr
	}

	return c.Consume(ctx, topics, wrappedHandler)
}

// Stop stops the consumer
func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		close(c.stopChan)
	}
}

// CommitOffsets commits the current offsets
func (c *Consumer) CommitOffsets(ctx context.Context) error {
	return c.client.client.CommitUncommittedOffsets(ctx)
}

// DLQHandler handles Dead Letter Queue logic
type DLQHandler struct {
	client     *Client
	config     *DLQConfig
	retryInfo  map[string]*retryInfo
	mu         sync.RWMutex
}

type retryInfo struct {
	attempts  int
	firstSeen time.Time
	lastRetry time.Time
}

// NewDLQHandler creates a new DLQ handler
func NewDLQHandler(client *Client, config *DLQConfig) *DLQHandler {
	return &DLQHandler{
		client:    client,
		config:    config,
		retryInfo: make(map[string]*retryInfo),
	}
}

// HandleFailedMessage handles a failed message
func (dlq *DLQHandler) HandleFailedMessage(ctx context.Context, record *kgo.Record, err error) error {
	key := string(record.Key)

	dlq.mu.Lock()
	info, exists := dlq.retryInfo[key]
	if !exists {
		info = &retryInfo{
			attempts:  0,
			firstSeen: time.Now(),
		}
		dlq.retryInfo[key] = info
	}
	info.attempts++
	info.lastRetry = time.Now()
	dlq.mu.Unlock()

	// Check if we should send to DLQ
	if info.attempts > dlq.config.MaxRetries {
		return dlq.sendToDLQ(ctx, record, err)
	}

	// Calculate retry delay
	delay := dlq.calculateRetryDelay(info.attempts)
	time.Sleep(delay)

	return err
}

func (dlq *DLQHandler) calculateRetryDelay(attempt int) time.Duration {
	if !dlq.config.ExponentialBackoff {
		return dlq.config.RetryDelay
	}

	delay := dlq.config.RetryDelay * time.Duration(1<<uint(attempt-1))
	if delay > dlq.config.MaxRetryDelay {
		delay = dlq.config.MaxRetryDelay
	}

	return delay
}

func (dlq *DLQHandler) sendToDLQ(ctx context.Context, record *kgo.Record, err error) error {
	dlqRecord := &kgo.Record{
		Topic: dlq.config.Topic,
		Key:   record.Key,
		Value: record.Value,
		Headers: append(record.Headers,
			kgo.RecordHeader{Key: "dlq_reason", Value: []byte(err.Error())},
			kgo.RecordHeader{Key: "dlq_timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
			kgo.RecordHeader{Key: "original_topic", Value: []byte(record.Topic)},
		),
	}

	results := dlq.client.client.ProduceSync(ctx, dlqRecord)
	if prodErr := results.FirstErr(); prodErr != nil {
		return fmt.Errorf("failed to send to DLQ: %w", prodErr)
	}

	// Clean up retry info
	dlq.mu.Lock()
	delete(dlq.retryInfo, string(record.Key))
	dlq.mu.Unlock()

	return nil
}

// ConsumeDLQ consumes messages from the DLQ
func (dlq *DLQHandler) ConsumeDLQ(ctx context.Context, handler HandlerFunc) error {
	consumer := NewConsumer(dlq.client)
	return consumer.Consume(ctx, []string{dlq.config.Topic}, handler)
}

// EcommerceConsumer provides ecommerce-specific consumer methods
type EcommerceConsumer struct {
	*Consumer
}

// NewEcommerceConsumer creates a new ecommerce consumer
func NewEcommerceConsumer(client *Client) *EcommerceConsumer {
	return &EcommerceConsumer{
		Consumer: NewConsumer(client),
	}
}

// ConsumeOrders consumes order events
func (ec *EcommerceConsumer) ConsumeOrders(ctx context.Context, handler func(ctx context.Context, order *OrderEvent) error) error {
	return ec.Consume(ctx, []string{"orders"}, func(ctx context.Context, record *kgo.Record) error {
		var order OrderEvent
		codec := &JSONCodec{}
		if err := codec.Decode(record.Value, &order); err != nil {
			return fmt.Errorf("failed to decode order: %w", err)
		}
		return handler(ctx, &order)
	})
}

// ConsumePayments consumes payment events
func (ec *EcommerceConsumer) ConsumePayments(ctx context.Context, handler func(ctx context.Context, payment *PaymentEvent) error) error {
	return ec.Consume(ctx, []string{"payments"}, func(ctx context.Context, record *kgo.Record) error {
		var payment PaymentEvent
		codec := &JSONCodec{}
		if err := codec.Decode(record.Value, &payment); err != nil {
			return fmt.Errorf("failed to decode payment: %w", err)
		}
		return handler(ctx, &payment)
	})
}

// ConsumeInventory consumes inventory events
func (ec *EcommerceConsumer) ConsumeInventory(ctx context.Context, handler func(ctx context.Context, inventory *InventoryEvent) error) error {
	return ec.Consume(ctx, []string{"inventory"}, func(ctx context.Context, record *kgo.Record) error {
		var inventory InventoryEvent
		codec := &JSONCodec{}
		if err := codec.Decode(record.Value, &inventory); err != nil {
			return fmt.Errorf("failed to decode inventory: %w", err)
		}
		return handler(ctx, &inventory)
	})
}

// ConsumerMetrics tracks consumer metrics
type ConsumerMetrics struct {
	MessagesConsumed int64
	BytesConsumed    int64
	Errors           int64
	Retries          int64
	DLQMessages      int64
	mu               sync.RWMutex
}

// NewConsumerMetrics creates new consumer metrics
func NewConsumerMetrics() *ConsumerMetrics {
	return &ConsumerMetrics{}
}

// RecordConsumed records a consumed message
func (cm *ConsumerMetrics) RecordConsumed(size int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.MessagesConsumed++
	cm.BytesConsumed += int64(size)
}

// RecordError records an error
func (cm *ConsumerMetrics) RecordError() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.Errors++
}

// RecordRetry records a retry
func (cm *ConsumerMetrics) RecordRetry() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.Retries++
}

// RecordDLQ records a DLQ message
func (cm *ConsumerMetrics) RecordDLQ() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.DLQMessages++
}

// GetStats returns current stats
func (cm *ConsumerMetrics) GetStats() (messages, bytes, errors, retries, dlq int64) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.MessagesConsumed, cm.BytesConsumed, cm.Errors, cm.Retries, cm.DLQMessages
}
