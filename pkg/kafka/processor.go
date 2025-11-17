package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

// ProcessorFunc is a callback function for processing messages with state
// Similar to Goka's processor callback
type ProcessorFunc func(ctx *ProcessorContext) error

// ProcessorConfig configures a stateful processor
type ProcessorConfig struct {
	// Topic to consume from
	Topic string

	// GroupTable topic for persisting state (compacted topic)
	GroupTable string

	// Codec for state serialization
	Codec Codec

	// Storage for local state cache
	Storage Storage

	// Joins to other group tables
	Joins map[string]string

	// Loopback topic (optional, for self-referencing flows)
	LoopbackTopic string

	// Output topics
	OutputTopics []string
}

// Processor implements stateful stream processing with Kafka-backed state
// Similar to Goka's Processor with group tables
type Processor struct {
	config     *ProcessorConfig
	client     *Client
	logger     watermill.LoggerAdapter
	callback   ProcessorFunc
	storage    Storage
	codec      Codec
	views      map[string]*View
	mu         sync.RWMutex
	running    bool
	cancelFunc context.CancelFunc
}

// NewProcessor creates a new stateful processor
func NewProcessor(client *Client, config *ProcessorConfig, callback ProcessorFunc) (*Processor, error) {
	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	if config.GroupTable == "" {
		return nil, fmt.Errorf("group table is required")
	}

	if config.Codec == nil {
		config.Codec = NewJSONCodec()
	}

	if config.Storage == nil {
		config.Storage = NewMemoryStorage()
	}

	processor := &Processor{
		config:   config,
		client:   client,
		logger:   client.logger,
		callback: callback,
		storage:  config.Storage,
		codec:    config.Codec,
		views:    make(map[string]*View),
	}

	// Initialize join views
	for name, topic := range config.Joins {
		view, err := NewView(client, topic, config.Codec, config.Storage)
		if err != nil {
			return nil, fmt.Errorf("failed to create join view %s: %w", name, err)
		}
		processor.views[name] = view
	}

	return processor, nil
}

// Start starts the processor
func (p *Processor) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("processor already running")
	}
	p.running = true
	p.mu.Unlock()

	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)
	p.cancelFunc = cancel

	// Start join views
	for _, view := range p.views {
		if err := view.Start(ctx); err != nil {
			return fmt.Errorf("failed to start view: %w", err)
		}
	}

	// Subscribe to input topic
	messages, err := p.client.subscriber.Subscribe(ctx, p.config.Topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	// Start processing messages
	go p.processMessages(ctx, messages)

	// Start state table recovery/synchronization
	go p.syncStateTable(ctx)

	p.logger.Info("Processor started", watermill.LogFields{
		"topic":       p.config.Topic,
		"group_table": p.config.GroupTable,
	})

	return nil
}

// Stop stops the processor
func (p *Processor) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	if p.cancelFunc != nil {
		p.cancelFunc()
	}

	// Stop views
	for _, view := range p.views {
		if err := view.Stop(); err != nil {
			p.logger.Error("Failed to stop view", err, nil)
		}
	}

	p.running = false
	return nil
}

// processMessages processes incoming messages
func (p *Processor) processMessages(ctx context.Context, messages <-chan *message.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			if err := p.processMessage(ctx, msg); err != nil {
				p.logger.Error("Failed to process message", err, watermill.LogFields{
					"message_uuid": msg.UUID,
				})
				msg.Nack()
			} else {
				msg.Ack()
			}
		}
	}
}

// processMessage processes a single message
func (p *Processor) processMessage(ctx context.Context, msg *message.Message) error {
	// Extract key from message metadata
	key := msg.Metadata.Get("partition_key")
	if key == "" {
		key = msg.UUID // Fallback to message ID
	}

	// Create processor context
	procCtx := NewProcessorContext(ctx, msg, p.storage, p.codec, key)

	// Set up emitter
	procCtx.SetEmitter(func(topic string, key string, value interface{}) error {
		return p.emit(topic, key, value)
	})

	// Set up loopback
	if p.config.LoopbackTopic != "" {
		procCtx.SetLoopback(func(key string, value interface{}) error {
			return p.emit(p.config.LoopbackTopic, key, value)
		})
	}

	// Add joins
	for name, view := range p.views {
		procCtx.AddJoin(name, view)
	}

	// Call user callback
	if err := p.callback(procCtx); err != nil {
		return fmt.Errorf("processor callback failed: %w", err)
	}

	// Persist state to group table
	if err := p.persistState(key); err != nil {
		return fmt.Errorf("failed to persist state: %w", err)
	}

	return nil
}

// persistState persists the current state to the group table
func (p *Processor) persistState(key string) error {
	// Get current state
	data, err := p.storage.Get(key)
	if err != nil {
		// Key might have been deleted
		return nil
	}

	// Publish to group table (compacted topic)
	msg := message.NewMessage(uuid.New().String(), data)
	msg.Metadata.Set("partition_key", key)

	return p.client.publisher.Publish(p.config.GroupTable, msg)
}

// syncStateTable synchronizes state from the group table
func (p *Processor) syncStateTable(ctx context.Context) {
	// Subscribe to group table to rebuild state
	messages, err := p.client.subscriber.Subscribe(ctx, p.config.GroupTable)
	if err != nil {
		p.logger.Error("Failed to subscribe to group table", err, nil)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			key := msg.Metadata.Get("partition_key")
			if key == "" {
				msg.Ack()
				continue
			}

			// Store in local cache
			if err := p.storage.Set(key, msg.Payload); err != nil {
				p.logger.Error("Failed to sync state", err, watermill.LogFields{
					"key": key,
				})
			}

			msg.Ack()
		}
	}
}

// emit sends a message to a topic
func (p *Processor) emit(topic string, key string, value interface{}) error {
	data, err := p.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("failed to encode value: %w", err)
	}

	msg := message.NewMessage(uuid.New().String(), data)
	msg.Metadata.Set("partition_key", key)

	return p.client.publisher.Publish(topic, msg)
}

// GetStorage returns the processor's storage
func (p *Processor) GetStorage() Storage {
	return p.storage
}

// TypedProcessor is a generic version of Processor
type TypedProcessor[T any] struct {
	*Processor
}

// TypedProcessorFunc is a typed processor callback
type TypedProcessorFunc[T any] func(ctx *TypedProcessorContext[T]) error

// NewTypedProcessor creates a typed processor
func NewTypedProcessor[T any](
	client *Client,
	config *ProcessorConfig,
	callback TypedProcessorFunc[T],
) (*TypedProcessor[T], error) {
	// Wrap callback
	wrappedCallback := func(ctx *ProcessorContext) error {
		typedCtx := NewTypedProcessorContext[T](ctx)
		return callback(typedCtx)
	}

	processor, err := NewProcessor(client, config, wrappedCallback)
	if err != nil {
		return nil, err
	}

	return &TypedProcessor[T]{
		Processor: processor,
	}, nil
}
