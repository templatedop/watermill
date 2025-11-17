package kafka

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
)

// ProcessorContext provides access to state and message within a processor
// Similar to Goka's Context with Value()/SetValue() methods
type ProcessorContext struct {
	ctx     context.Context
	msg     *message.Message
	storage Storage
	codec   Codec
	key     string

	// Emitter for sending messages
	emitter func(topic string, key string, value interface{}) error

	// Loopback for sending messages back to processor
	loopback func(key string, value interface{}) error

	// Join table lookups
	joins map[string]*View
}

// NewProcessorContext creates a new processor context
func NewProcessorContext(
	ctx context.Context,
	msg *message.Message,
	storage Storage,
	codec Codec,
	key string,
) *ProcessorContext {
	return &ProcessorContext{
		ctx:     ctx,
		msg:     msg,
		storage: storage,
		codec:   codec,
		key:     key,
		joins:   make(map[string]*View),
	}
}

// Context returns the underlying context
func (c *ProcessorContext) Context() context.Context {
	return c.ctx
}

// Message returns the current message being processed
func (c *ProcessorContext) Message() *message.Message {
	return c.msg
}

// Key returns the message key
func (c *ProcessorContext) Key() string {
	return c.key
}

// Value retrieves the current state value
func (c *ProcessorContext) Value() (interface{}, error) {
	data, err := c.storage.Get(c.key)
	if err != nil {
		// Key doesn't exist, return nil
		return nil, nil
	}

	var value interface{}
	if err := c.codec.Decode(data, &value); err != nil {
		return nil, fmt.Errorf("failed to decode state: %w", err)
	}

	return value, nil
}

// SetValue updates the state value
func (c *ProcessorContext) SetValue(value interface{}) error {
	data, err := c.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}

	if err := c.storage.Set(c.key, data); err != nil {
		return fmt.Errorf("failed to set state: %w", err)
	}

	return nil
}

// Delete removes the state value
func (c *ProcessorContext) Delete() error {
	return c.storage.Delete(c.key)
}

// Emit sends a message to a topic
func (c *ProcessorContext) Emit(topic string, key string, value interface{}) error {
	if c.emitter == nil {
		return fmt.Errorf("emitter not configured")
	}
	return c.emitter(topic, key, value)
}

// Loopback sends a message back to the processor's input
func (c *ProcessorContext) Loopback(key string, value interface{}) error {
	if c.loopback == nil {
		return fmt.Errorf("loopback not configured")
	}
	return c.loopback(key, value)
}

// Join looks up a value from a joined table
func (c *ProcessorContext) Join(table string) (interface{}, error) {
	view, exists := c.joins[table]
	if !exists {
		return nil, fmt.Errorf("join table not found: %s", table)
	}

	return view.Get(c.key)
}

// SetEmitter configures the emit function
func (c *ProcessorContext) SetEmitter(emitter func(topic string, key string, value interface{}) error) {
	c.emitter = emitter
}

// SetLoopback configures the loopback function
func (c *ProcessorContext) SetLoopback(loopback func(key string, value interface{}) error) {
	c.loopback = loopback
}

// AddJoin adds a join table view
func (c *ProcessorContext) AddJoin(name string, view *View) {
	c.joins[name] = view
}

// TypedProcessorContext is a generic version of ProcessorContext
type TypedProcessorContext[T any] struct {
	*ProcessorContext
}

// NewTypedProcessorContext creates a typed processor context
func NewTypedProcessorContext[T any](base *ProcessorContext) *TypedProcessorContext[T] {
	return &TypedProcessorContext[T]{
		ProcessorContext: base,
	}
}

// Value retrieves the typed state value
func (c *TypedProcessorContext[T]) Value() (*T, error) {
	data, err := c.storage.Get(c.key)
	if err != nil {
		// Key doesn't exist, return nil
		return nil, nil
	}

	var value T
	if err := c.codec.Decode(data, &value); err != nil {
		return nil, fmt.Errorf("failed to decode state: %w", err)
	}

	return &value, nil
}

// SetValue updates the typed state value
func (c *TypedProcessorContext[T]) SetValue(value T) error {
	data, err := c.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}

	if err := c.storage.Set(c.key, data); err != nil {
		return fmt.Errorf("failed to set state: %w", err)
	}

	return nil
}

// Emit sends a typed message to a topic
func (c *TypedProcessorContext[T]) Emit(topic string, key string, value T) error {
	if c.emitter == nil {
		return fmt.Errorf("emitter not configured")
	}
	return c.emitter(topic, key, value)
}

// Loopback sends a typed message back to the processor
func (c *TypedProcessorContext[T]) Loopback(key string, value T) error {
	if c.loopback == nil {
		return fmt.Errorf("loopback not configured")
	}
	return c.loopback(key, value)
}

// Join looks up a typed value from a joined table
func (c *TypedProcessorContext[T]) Join(table string) (*T, error) {
	view, exists := c.joins[table]
	if !exists {
		return nil, fmt.Errorf("join table not found: %s", table)
	}

	value, err := view.Get(c.key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, nil
	}

	typedValue, ok := value.(*T)
	if !ok {
		return nil, fmt.Errorf("join value type mismatch")
	}

	return typedValue, nil
}
