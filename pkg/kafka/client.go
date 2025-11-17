package kafka

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Client is the main Kafka client that manages producers and consumers
type Client struct {
	config    *Config
	publisher *kafka.Publisher
	subscriber *kafka.Subscriber
	router    *message.Router
	dlq       *DLQHandler
	logger    watermill.LoggerAdapter
	mu        sync.RWMutex
	closed    bool
}

// NewClient creates a new Kafka client with the given configuration
func NewClient(config *Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	saramaConfig, err := config.GetSaramaConfig()
	if err != nil {
		return nil, err
	}

	// Create publisher
	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               config.Brokers,
			Marshaler:             kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaConfig,
		},
		config.Logger,
	)
	if err != nil {
		return nil, err
	}

	// Create subscriber
	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               config.Brokers,
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaConfig,
			ConsumerGroup:         config.ConsumerGroup,
		},
		config.Logger,
	)
	if err != nil {
		publisher.Close()
		return nil, err
	}

	client := &Client{
		config:     config,
		publisher:  publisher,
		subscriber: subscriber,
		logger:     config.Logger,
	}

	// Initialize DLQ if enabled
	if config.DLQ.Enabled {
		client.dlq = NewDLQHandler(publisher, &config.DLQ, config.Logger)
	}

	return client, nil
}

// GetPublisher returns the underlying Watermill publisher
func (c *Client) GetPublisher() message.Publisher {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.publisher
}

// GetSubscriber returns the underlying Watermill subscriber
func (c *Client) GetSubscriber() message.Subscriber {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.subscriber
}

// GetDLQ returns the DLQ handler
func (c *Client) GetDLQ() *DLQHandler {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dlq
}

// Publish publishes a message to a topic
func (c *Client) Publish(ctx context.Context, topic string, messages ...*message.Message) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return ErrClientClosed{}
	}

	for _, msg := range messages {
		if err := c.publisher.Publish(topic, msg); err != nil {
			return ErrPublishFailed{Topic: topic, Err: err}
		}
	}

	return nil
}

// Subscribe subscribes to a topic and processes messages with the given handler
func (c *Client) Subscribe(ctx context.Context, topic string, handler message.HandlerFunc) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return ErrClientClosed{}
	}

	messages, err := c.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return ErrSubscribeFailed{Topic: topic, Err: err}
	}

	go c.processMessages(ctx, messages, handler)

	return nil
}

// processMessages processes messages from the subscription
func (c *Client) processMessages(ctx context.Context, messages <-chan *message.Message, handler message.HandlerFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			// Process message with handler
			producedMessages, err := handler(msg)
			if err != nil {
				c.logger.Error("Failed to process message", err, watermill.LogFields{
					"message_uuid": msg.UUID,
					"topic":        msg.Metadata.Get("topic"),
				})

				// Send to DLQ if enabled
				if c.dlq != nil {
					if dlqErr := c.dlq.Send(ctx, msg, err); dlqErr != nil {
						c.logger.Error("Failed to send message to DLQ", dlqErr, nil)
					}
				}

				msg.Nack()
				continue
			}

			// Publish produced messages
			for _, producedMsg := range producedMessages {
				topic := producedMsg.Metadata.Get("topic")
				if topic == "" {
					c.logger.Error("Produced message has no topic", nil, watermill.LogFields{
						"message_uuid": producedMsg.UUID,
					})
					continue
				}

				if err := c.publisher.Publish(topic, producedMsg); err != nil {
					c.logger.Error("Failed to publish produced message", err, watermill.LogFields{
						"message_uuid": producedMsg.UUID,
						"topic":        topic,
					})
				}
			}

			msg.Ack()
		}
	}
}

// CreateRouter creates a new message router with middleware
func (c *Client) CreateRouter() (*message.Router, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, ErrClientClosed{}
	}

	router, err := message.NewRouter(message.RouterConfig{}, c.logger)
	if err != nil {
		return nil, err
	}

	// Add default middleware
	router.AddMiddleware(
		// Recoverer middleware handles panics
		message.Recoverer,
		// Retry middleware
		NewRetryMiddleware(c.config.DLQ.MaxRetries, c.config.DLQ.RetryDelay, c.config.DLQ.ExponentialBackoff),
		// Logging middleware
		NewLoggingMiddleware(c.logger),
		// Metrics middleware (if you want to add custom metrics)
		NewMetricsMiddleware(),
	)

	c.router = router
	return router, nil
}

// RunRouter runs the message router (blocking)
func (c *Client) RunRouter(ctx context.Context) error {
	c.mu.RLock()
	router := c.router
	c.mu.RUnlock()

	if router == nil {
		return ErrInvalidConfig{Field: "router", Reason: "router not initialized, call CreateRouter first"}
	}

	return router.Run(ctx)
}

// Close closes the client and all its resources
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	var errs []error

	if c.router != nil {
		if err := c.router.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if c.publisher != nil {
		if err := c.publisher.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if c.subscriber != nil {
		if err := c.subscriber.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs[0] // Return first error
	}

	return nil
}

// IsClosed returns whether the client is closed
func (c *Client) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}
