package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

// DLQMessage represents a message in the dead letter queue
type DLQMessage struct {
	OriginalTopic   string            `json:"original_topic"`
	OriginalMessage json.RawMessage   `json:"original_message"`
	Error           string            `json:"error"`
	RetryCount      int               `json:"retry_count"`
	FirstFailedAt   time.Time         `json:"first_failed_at"`
	LastFailedAt    time.Time         `json:"last_failed_at"`
	Metadata        map[string]string `json:"metadata"`
}

// DLQHandler handles dead letter queue operations
type DLQHandler struct {
	publisher message.Publisher
	config    *DLQConfig
	logger    watermill.LoggerAdapter
	retries   map[string]*retryInfo
	mu        sync.RWMutex
}

type retryInfo struct {
	count         int
	firstFailedAt time.Time
	lastFailedAt  time.Time
}

// NewDLQHandler creates a new DLQ handler
func NewDLQHandler(publisher message.Publisher, config *DLQConfig, logger watermill.LoggerAdapter) *DLQHandler {
	return &DLQHandler{
		publisher: publisher,
		config:    config,
		logger:    logger,
		retries:   make(map[string]*retryInfo),
	}
}

// Send sends a failed message to the DLQ
func (d *DLQHandler) Send(ctx context.Context, msg *message.Message, err error) error {
	if !d.config.Enabled {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Track retry count
	info, exists := d.retries[msg.UUID]
	if !exists {
		info = &retryInfo{
			firstFailedAt: time.Now(),
		}
		d.retries[msg.UUID] = info
	}
	info.count++
	info.lastFailedAt = time.Now()

	// Check if we should retry or send to DLQ
	if info.count <= d.config.MaxRetries {
		d.logger.Info("Message will be retried", watermill.LogFields{
			"message_uuid": msg.UUID,
			"retry_count":  info.count,
			"max_retries":  d.config.MaxRetries,
		})
		return nil
	}

	// Send to DLQ
	dlqMsg := DLQMessage{
		OriginalTopic:   msg.Metadata.Get("topic"),
		OriginalMessage: json.RawMessage(msg.Payload),
		Error:           err.Error(),
		RetryCount:      info.count,
		FirstFailedAt:   info.firstFailedAt,
		LastFailedAt:    info.lastFailedAt,
		Metadata:        msg.Metadata,
	}

	dlqPayload, err := json.Marshal(dlqMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ message: %w", err)
	}

	dlqMessage := message.NewMessage(uuid.New().String(), dlqPayload)
	dlqMessage.Metadata.Set("dlq_reason", "max_retries_exceeded")
	dlqMessage.Metadata.Set("original_uuid", msg.UUID)
	dlqMessage.Metadata.Set("timestamp", time.Now().Format(time.RFC3339))

	if err := d.publisher.Publish(d.config.Topic, dlqMessage); err != nil {
		return fmt.Errorf("failed to publish to DLQ: %w", err)
	}

	d.logger.Info("Message sent to DLQ", watermill.LogFields{
		"message_uuid":   msg.UUID,
		"original_topic": dlqMsg.OriginalTopic,
		"retry_count":    info.count,
		"error":          err.Error(),
	})

	// Clean up retry info
	delete(d.retries, msg.UUID)

	return nil
}

// ShouldRetry checks if a message should be retried
func (d *DLQHandler) ShouldRetry(msg *message.Message) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	info, exists := d.retries[msg.UUID]
	if !exists {
		return true
	}

	return info.count < d.config.MaxRetries
}

// GetRetryCount returns the retry count for a message
func (d *DLQHandler) GetRetryCount(msg *message.Message) int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	info, exists := d.retries[msg.UUID]
	if !exists {
		return 0
	}

	return info.count
}

// GetRetryDelay calculates the retry delay based on retry count
func (d *DLQHandler) GetRetryDelay(retryCount int) time.Duration {
	if !d.config.ExponentialBackoff {
		return d.config.RetryDelay
	}

	// Exponential backoff: delay * 2^retryCount
	delay := d.config.RetryDelay
	for i := 0; i < retryCount; i++ {
		delay *= 2
	}

	// Cap at 5 minutes
	maxDelay := 5 * time.Minute
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// CleanupOldRetries removes retry info older than a certain duration
func (d *DLQHandler) CleanupOldRetries(maxAge time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	for uuid, info := range d.retries {
		if now.Sub(info.lastFailedAt) > maxAge {
			delete(d.retries, uuid)
		}
	}
}

// DLQConsumer provides methods to consume and process DLQ messages
type DLQConsumer struct {
	client *Client
	logger watermill.LoggerAdapter
}

// NewDLQConsumer creates a new DLQ consumer
func NewDLQConsumer(client *Client) *DLQConsumer {
	return &DLQConsumer{
		client: client,
		logger: client.logger,
	}
}

// DLQMessageHandler is a function that processes DLQ messages
type DLQMessageHandler func(ctx context.Context, dlqMsg DLQMessage) error

// Subscribe subscribes to the DLQ topic
func (c *DLQConsumer) Subscribe(ctx context.Context, handler DLQMessageHandler) error {
	messages, err := c.client.subscriber.Subscribe(ctx, c.client.config.DLQ.Topic)
	if err != nil {
		return ErrSubscribeFailed{Topic: c.client.config.DLQ.Topic, Err: err}
	}

	go c.processMessages(ctx, messages, handler)
	return nil
}

func (c *DLQConsumer) processMessages(ctx context.Context, messages <-chan *message.Message, handler DLQMessageHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			var dlqMsg DLQMessage
			if err := json.Unmarshal(msg.Payload, &dlqMsg); err != nil {
				c.logger.Error("Failed to unmarshal DLQ message", err, watermill.LogFields{
					"message_uuid": msg.UUID,
				})
				msg.Nack()
				continue
			}

			if err := handler(ctx, dlqMsg); err != nil {
				c.logger.Error("Failed to process DLQ message", err, watermill.LogFields{
					"message_uuid":   msg.UUID,
					"original_topic": dlqMsg.OriginalTopic,
				})
				msg.Nack()
				continue
			}

			msg.Ack()
		}
	}
}

// Retry retries a DLQ message by publishing it back to the original topic
func (c *DLQConsumer) Retry(ctx context.Context, dlqMsg DLQMessage) error {
	msg := message.NewMessage(uuid.New().String(), dlqMsg.OriginalMessage)
	msg.Metadata = dlqMsg.Metadata
	msg.Metadata.Set("retried_from_dlq", "true")
	msg.Metadata.Set("retry_timestamp", time.Now().Format(time.RFC3339))

	return c.client.publisher.Publish(dlqMsg.OriginalTopic, msg)
}

// Archive archives a DLQ message to a separate topic for later analysis
func (c *DLQConsumer) Archive(ctx context.Context, dlqMsg DLQMessage, archiveTopic string) error {
	payload, err := json.Marshal(dlqMsg)
	if err != nil {
		return err
	}

	msg := message.NewMessage(uuid.New().String(), payload)
	msg.Metadata.Set("archived_at", time.Now().Format(time.RFC3339))

	return c.client.publisher.Publish(archiveTopic, msg)
}

// RetryMiddleware is a middleware that implements retry logic
func RetryMiddleware(maxRetries int, delay time.Duration, exponentialBackoff bool) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			var lastErr error

			for attempt := 0; attempt <= maxRetries; attempt++ {
				if attempt > 0 {
					retryDelay := delay
					if exponentialBackoff {
						retryDelay = delay * time.Duration(1<<uint(attempt-1))
					}
					time.Sleep(retryDelay)
				}

				messages, err := h(msg)
				if err == nil {
					return messages, nil
				}

				lastErr = err
			}

			return nil, ErrMaxRetriesExceeded{Attempts: maxRetries + 1, LastErr: lastErr}
		}
	}
}
