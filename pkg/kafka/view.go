package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// View provides read-only access to a group table
// Similar to Goka's View for querying state from external services
type View struct {
	client     *Client
	topic      string
	codec      Codec
	storage    Storage
	logger     watermill.LoggerAdapter
	mu         sync.RWMutex
	running    bool
	cancelFunc context.CancelFunc
	ready      chan struct{}
}

// NewView creates a new view for a group table
func NewView(client *Client, topic string, codec Codec, storage Storage) (*View, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	if codec == nil {
		codec = NewJSONCodec()
	}

	if storage == nil {
		storage = NewMemoryStorage()
	}

	return &View{
		client:  client,
		topic:   topic,
		codec:   codec,
		storage: storage,
		logger:  client.logger,
		ready:   make(chan struct{}),
	}, nil
}

// Start starts the view and begins syncing state
func (v *View) Start(ctx context.Context) error {
	v.mu.Lock()
	if v.running {
		v.mu.Unlock()
		return fmt.Errorf("view already running")
	}
	v.running = true
	v.mu.Unlock()

	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)
	v.cancelFunc = cancel

	// Subscribe to the group table
	messages, err := v.client.subscriber.Subscribe(ctx, v.topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	// Start syncing in background
	go v.syncState(ctx, messages)

	v.logger.Info("View started", watermill.LogFields{
		"topic": v.topic,
	})

	return nil
}

// Stop stops the view
func (v *View) Stop() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.running {
		return nil
	}

	if v.cancelFunc != nil {
		v.cancelFunc()
	}

	v.running = false
	return nil
}

// syncState continuously syncs state from the group table
func (v *View) syncState(ctx context.Context, messages <-chan *message.Message) {
	// Signal ready after initial sync
	// In production, you might want to wait for all partitions to be caught up
	defer close(v.ready)

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

			// Update local cache
			if err := v.storage.Set(key, msg.Payload); err != nil {
				v.logger.Error("Failed to update view", err, watermill.LogFields{
					"key": key,
				})
			}

			msg.Ack()
		}
	}
}

// Get retrieves a value by key
func (v *View) Get(key string) (interface{}, error) {
	data, err := v.storage.Get(key)
	if err != nil {
		return nil, nil // Key doesn't exist
	}

	var value interface{}
	if err := v.codec.Decode(data, &value); err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}

	return value, nil
}

// Has checks if a key exists
func (v *View) Has(key string) (bool, error) {
	return v.storage.Has(key)
}

// Iterator returns an iterator over all keys
func (v *View) Iterator() (ViewIterator, error) {
	return v.storage.Iterator()
}

// WaitReady waits for the view to be ready (initial sync complete)
func (v *View) WaitReady() {
	<-v.ready
}

// ViewIterator iterates over view entries
type ViewIterator interface {
	StorageIterator
}

// TypedView is a generic version of View
type TypedView[T any] struct {
	*View
}

// NewTypedView creates a typed view
func NewTypedView[T any](client *Client, topic string, codec Codec, storage Storage) (*TypedView[T], error) {
	view, err := NewView(client, topic, codec, storage)
	if err != nil {
		return nil, err
	}

	return &TypedView[T]{
		View: view,
	}, nil
}

// Get retrieves a typed value by key
func (v *TypedView[T]) Get(key string) (*T, error) {
	data, err := v.storage.Get(key)
	if err != nil {
		return nil, nil // Key doesn't exist
	}

	var value T
	if err := v.codec.Decode(data, &value); err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}

	return &value, nil
}

// ViewManager manages multiple views
type ViewManager struct {
	views  map[string]*View
	mu     sync.RWMutex
	client *Client
}

// NewViewManager creates a new view manager
func NewViewManager(client *Client) *ViewManager {
	return &ViewManager{
		views:  make(map[string]*View),
		client: client,
	}
}

// AddView adds a view to the manager
func (m *ViewManager) AddView(name string, view *View) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.views[name] = view
}

// GetView retrieves a view by name
func (m *ViewManager) GetView(name string) (*View, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	view, exists := m.views[name]
	if !exists {
		return nil, fmt.Errorf("view not found: %s", name)
	}

	return view, nil
}

// StartAll starts all views
func (m *ViewManager) StartAll(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, view := range m.views {
		if err := view.Start(ctx); err != nil {
			return fmt.Errorf("failed to start view %s: %w", name, err)
		}
	}

	return nil
}

// StopAll stops all views
func (m *ViewManager) StopAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var firstErr error
	for _, view := range m.views {
		if err := view.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// WaitAllReady waits for all views to be ready
func (m *ViewManager) WaitAllReady() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, view := range m.views {
		view.WaitReady()
	}
}
