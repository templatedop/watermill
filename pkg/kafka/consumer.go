package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// MessageHandler is a function that processes a message
type MessageHandler func(ctx context.Context, msg *message.Message) error

// Consumer provides high-level methods for consuming messages
type Consumer struct {
	client *Client
	logger watermill.LoggerAdapter
}

// NewConsumer creates a new Consumer
func NewConsumer(client *Client) *Consumer {
	return &Consumer{
		client: client,
		logger: client.logger,
	}
}

// GetSubscriber returns the underlying subscriber
func (c *Consumer) GetSubscriber() message.Subscriber {
	return c.client.subscriber
}

// Subscribe subscribes to a topic with a handler
func (c *Consumer) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		err := handler(ctx, msg)
		return nil, err
	}

	return c.client.Subscribe(ctx, topic, handlerFunc)
}

// SubscribeWithRetry subscribes to a topic with retry logic
func (c *Consumer) SubscribeWithRetry(ctx context.Context, topic string, handler MessageHandler, maxRetries int) error {
	handlerFunc := func(msg *message.Message) ([]*message.Message, error) {
		var lastErr error

		for attempt := 0; attempt <= maxRetries; attempt++ {
			if attempt > 0 {
				c.logger.Info("Retrying message processing", watermill.LogFields{
					"message_uuid": msg.UUID,
					"attempt":      attempt,
					"max_retries":  maxRetries,
				})

				// Exponential backoff
				backoff := time.Duration(attempt) * c.client.config.DLQ.RetryDelay
				time.Sleep(backoff)
			}

			err := handler(ctx, msg)
			if err == nil {
				return nil, nil
			}

			lastErr = err
			c.logger.Error("Message processing failed", err, watermill.LogFields{
				"message_uuid": msg.UUID,
				"attempt":      attempt + 1,
			})
		}

		return nil, ErrMaxRetriesExceeded{Attempts: maxRetries + 1, LastErr: lastErr}
	}

	return c.client.Subscribe(ctx, topic, handlerFunc)
}

// AddHandler adds a handler to the router for a specific topic
func (c *Consumer) AddHandler(router *message.Router, topic string, handler MessageHandler) {
	handlerFunc := func(msg *message.Message) error {
		return handler(context.Background(), msg)
	}

	router.AddNoPublisherHandler(
		"handler_"+topic,
		topic,
		c.client.subscriber,
		handlerFunc,
	)
}

// AddHandlerWithPublisher adds a handler that can also publish messages
func (c *Consumer) AddHandlerWithPublisher(router *message.Router, subscribeTopic string, publishTopic string, handler message.HandlerFunc) {
	router.AddHandler(
		"handler_"+subscribeTopic+"_to_"+publishTopic,
		subscribeTopic,
		c.client.subscriber,
		publishTopic,
		c.client.publisher,
		handler,
	)
}

// UnmarshalMessage unmarshals a message payload into a struct
func (c *Consumer) UnmarshalMessage(msg *message.Message, v interface{}) error {
	return json.Unmarshal(msg.Payload, v)
}

// GetMessageMetadata retrieves metadata from a message
func (c *Consumer) GetMessageMetadata(msg *message.Message, key string) string {
	return msg.Metadata.Get(key)
}

// GetMessageTimestamp retrieves the timestamp from a message
func (c *Consumer) GetMessageTimestamp(msg *message.Message) (time.Time, error) {
	timestampStr := msg.Metadata.Get("timestamp")
	if timestampStr == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, timestampStr)
}

// EcommerceConsumer provides ecommerce-specific consuming methods
type EcommerceConsumer struct {
	*Consumer
}

// NewEcommerceConsumer creates a new EcommerceConsumer
func NewEcommerceConsumer(client *Client) *EcommerceConsumer {
	return &EcommerceConsumer{
		Consumer: NewConsumer(client),
	}
}

// OrderCreatedHandler is the signature for order created event handlers
type OrderCreatedHandler func(ctx context.Context, order OrderEvent) error

// SubscribeOrderCreated subscribes to order created events
func (c *EcommerceConsumer) SubscribeOrderCreated(ctx context.Context, handler OrderCreatedHandler) error {
	return c.Subscribe(ctx, "orders.created", func(ctx context.Context, msg *message.Message) error {
		var order OrderEvent
		if err := c.UnmarshalMessage(msg, &order); err != nil {
			return err
		}
		return handler(ctx, order)
	})
}

// OrderUpdatedHandler is the signature for order updated event handlers
type OrderUpdatedHandler func(ctx context.Context, order OrderEvent) error

// SubscribeOrderUpdated subscribes to order updated events
func (c *EcommerceConsumer) SubscribeOrderUpdated(ctx context.Context, handler OrderUpdatedHandler) error {
	return c.Subscribe(ctx, "orders.updated", func(ctx context.Context, msg *message.Message) error {
		var order OrderEvent
		if err := c.UnmarshalMessage(msg, &order); err != nil {
			return err
		}
		return handler(ctx, order)
	})
}

// PaymentProcessedHandler is the signature for payment processed event handlers
type PaymentProcessedHandler func(ctx context.Context, payment PaymentEvent) error

// SubscribePaymentProcessed subscribes to payment processed events
func (c *EcommerceConsumer) SubscribePaymentProcessed(ctx context.Context, handler PaymentProcessedHandler) error {
	return c.Subscribe(ctx, "payments.processed", func(ctx context.Context, msg *message.Message) error {
		var payment PaymentEvent
		if err := c.UnmarshalMessage(msg, &payment); err != nil {
			return err
		}
		return handler(ctx, payment)
	})
}

// InventoryReservedHandler is the signature for inventory reserved event handlers
type InventoryReservedHandler func(ctx context.Context, inventory InventoryEvent) error

// SubscribeInventoryReserved subscribes to inventory reserved events
func (c *EcommerceConsumer) SubscribeInventoryReserved(ctx context.Context, handler InventoryReservedHandler) error {
	return c.Subscribe(ctx, "inventory.reserved", func(ctx context.Context, msg *message.Message) error {
		var inventory InventoryEvent
		if err := c.UnmarshalMessage(msg, &inventory); err != nil {
			return err
		}
		return handler(ctx, inventory)
	})
}

// ShipmentCreatedHandler is the signature for shipment created event handlers
type ShipmentCreatedHandler func(ctx context.Context, shipment ShipmentEvent) error

// SubscribeShipmentCreated subscribes to shipment created events
func (c *EcommerceConsumer) SubscribeShipmentCreated(ctx context.Context, handler ShipmentCreatedHandler) error {
	return c.Subscribe(ctx, "shipments.created", func(ctx context.Context, msg *message.Message) error {
		var shipment ShipmentEvent
		if err := c.UnmarshalMessage(msg, &shipment); err != nil {
			return err
		}
		return handler(ctx, shipment)
	})
}

// BatchConsumer provides batch processing capabilities
type BatchConsumer struct {
	*Consumer
	batchSize int
	batchTimeout time.Duration
}

// NewBatchConsumer creates a new BatchConsumer
func NewBatchConsumer(client *Client, batchSize int, batchTimeout time.Duration) *BatchConsumer {
	return &BatchConsumer{
		Consumer:     NewConsumer(client),
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
	}
}

// BatchHandler is a function that processes a batch of messages
type BatchHandler func(ctx context.Context, messages []*message.Message) error

// SubscribeBatch subscribes to a topic and processes messages in batches
func (c *BatchConsumer) SubscribeBatch(ctx context.Context, topic string, handler BatchHandler) error {
	messages, err := c.client.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return ErrSubscribeFailed{Topic: topic, Err: err}
	}

	go c.processBatch(ctx, messages, handler)
	return nil
}

func (c *BatchConsumer) processBatch(ctx context.Context, messages <-chan *message.Message, handler BatchHandler) {
	batch := make([]*message.Message, 0, c.batchSize)
	ticker := time.NewTicker(c.batchTimeout)
	defer ticker.Stop()

	processBatch := func() {
		if len(batch) == 0 {
			return
		}

		if err := handler(ctx, batch); err != nil {
			c.logger.Error("Batch processing failed", err, watermill.LogFields{
				"batch_size": len(batch),
			})
			// Nack all messages in batch
			for _, msg := range batch {
				msg.Nack()
			}
		} else {
			// Ack all messages in batch
			for _, msg := range batch {
				msg.Ack()
			}
		}

		batch = batch[:0] // Clear batch
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
			batch = append(batch, msg)
			if len(batch) >= c.batchSize {
				processBatch()
			}
		}
	}
}
