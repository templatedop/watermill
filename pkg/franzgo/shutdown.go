package franzgo

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ShutdownConfig configures graceful shutdown behavior
type ShutdownConfig struct {
	// Timeout for the entire shutdown process
	Timeout time.Duration

	// Time to wait for in-flight messages to complete
	DrainTimeout time.Duration

	// Time to wait for producer flush
	FlushTimeout time.Duration

	// Time to wait for offset commits
	CommitTimeout time.Duration

	// Whether to commit offsets during shutdown
	CommitOffsets bool

	// Whether to flush producer buffers during shutdown
	FlushProducer bool

	// Signals to listen for (default: SIGINT, SIGTERM)
	Signals []os.Signal

	// Callback when shutdown starts
	OnShutdownStart func()

	// Callback when shutdown completes
	OnShutdownComplete func(error)

	// Callback for shutdown progress updates
	OnProgress func(stage string, duration time.Duration)
}

// DefaultShutdownConfig returns default shutdown configuration
func DefaultShutdownConfig() *ShutdownConfig {
	return &ShutdownConfig{
		Timeout:        30 * time.Second,
		DrainTimeout:   10 * time.Second,
		FlushTimeout:   5 * time.Second,
		CommitTimeout:  5 * time.Second,
		CommitOffsets:  true,
		FlushProducer:  true,
		Signals:        []os.Signal{syscall.SIGINT, syscall.SIGTERM},
	}
}

// ShutdownManager manages graceful shutdown
type ShutdownManager struct {
	config          *ShutdownConfig
	client          *Client
	producer        *Producer
	consumer        *Consumer
	shutdownChan    chan struct{}
	shutdownOnce    sync.Once
	isShuttingDown  bool
	mu              sync.RWMutex
	waitGroup       sync.WaitGroup
	signalChan      chan os.Signal
}

// NewShutdownManager creates a new shutdown manager
func NewShutdownManager(client *Client, config *ShutdownConfig) *ShutdownManager {
	if config == nil {
		config = DefaultShutdownConfig()
	}

	sm := &ShutdownManager{
		config:       config,
		client:       client,
		shutdownChan: make(chan struct{}),
		signalChan:   make(chan os.Signal, 1),
	}

	// Setup signal handling if signals are configured
	if len(config.Signals) > 0 {
		signal.Notify(sm.signalChan, config.Signals...)
	}

	return sm
}

// SetProducer sets the producer to be gracefully shut down
func (sm *ShutdownManager) SetProducer(producer *Producer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.producer = producer
}

// SetConsumer sets the consumer to be gracefully shut down
func (sm *ShutdownManager) SetConsumer(consumer *Consumer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.consumer = consumer
}

// IsShuttingDown returns true if shutdown has been initiated
func (sm *ShutdownManager) IsShuttingDown() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.isShuttingDown
}

// WaitForSignal blocks until a shutdown signal is received
func (sm *ShutdownManager) WaitForSignal() os.Signal {
	return <-sm.signalChan
}

// Shutdown initiates graceful shutdown
func (sm *ShutdownManager) Shutdown(ctx context.Context) error {
	var shutdownErr error

	sm.shutdownOnce.Do(func() {
		sm.mu.Lock()
		sm.isShuttingDown = true
		sm.mu.Unlock()

		// Close shutdown channel to signal all goroutines
		close(sm.shutdownChan)

		// Call shutdown start callback
		if sm.config.OnShutdownStart != nil {
			sm.config.OnShutdownStart()
		}

		// Create context with timeout
		shutdownCtx, cancel := context.WithTimeout(ctx, sm.config.Timeout)
		defer cancel()

		start := time.Now()
		shutdownErr = sm.performShutdown(shutdownCtx)

		// Call shutdown complete callback
		if sm.config.OnShutdownComplete != nil {
			sm.config.OnShutdownComplete(shutdownErr)
		}

		if sm.config.OnProgress != nil {
			sm.config.OnProgress("shutdown_complete", time.Since(start))
		}
	})

	return shutdownErr
}

// performShutdown executes the shutdown sequence
func (sm *ShutdownManager) performShutdown(ctx context.Context) error {
	var errs []error

	// Stage 1: Stop accepting new messages
	stageStart := time.Now()
	if sm.config.OnProgress != nil {
		sm.config.OnProgress("stop_accepting", 0)
	}
	sm.stopAcceptingMessages()
	if sm.config.OnProgress != nil {
		sm.config.OnProgress("stop_accepting_complete", time.Since(stageStart))
	}

	// Stage 2: Drain in-flight messages
	stageStart = time.Now()
	if sm.config.OnProgress != nil {
		sm.config.OnProgress("drain_messages", 0)
	}
	if err := sm.drainInFlightMessages(ctx); err != nil {
		errs = append(errs, fmt.Errorf("drain failed: %w", err))
	}
	if sm.config.OnProgress != nil {
		sm.config.OnProgress("drain_messages_complete", time.Since(stageStart))
	}

	// Stage 3: Flush producer buffers
	if sm.config.FlushProducer && sm.producer != nil {
		stageStart = time.Now()
		if sm.config.OnProgress != nil {
			sm.config.OnProgress("flush_producer", 0)
		}
		if err := sm.flushProducer(ctx); err != nil {
			errs = append(errs, fmt.Errorf("flush failed: %w", err))
		}
		if sm.config.OnProgress != nil {
			sm.config.OnProgress("flush_producer_complete", time.Since(stageStart))
		}
	}

	// Stage 4: Commit offsets
	if sm.config.CommitOffsets && sm.consumer != nil {
		stageStart = time.Now()
		if sm.config.OnProgress != nil {
			sm.config.OnProgress("commit_offsets", 0)
		}
		if err := sm.commitOffsets(ctx); err != nil {
			errs = append(errs, fmt.Errorf("commit failed: %w", err))
		}
		if sm.config.OnProgress != nil {
			sm.config.OnProgress("commit_offsets_complete", time.Since(stageStart))
		}
	}

	// Stage 5: Close client connection
	stageStart = time.Now()
	if sm.config.OnProgress != nil {
		sm.config.OnProgress("close_client", 0)
	}
	if err := sm.client.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close failed: %w", err))
	}
	if sm.config.OnProgress != nil {
		sm.config.OnProgress("close_client_complete", time.Since(stageStart))
	}

	if len(errs) > 0 {
		return fmt.Errorf("shutdown completed with errors: %v", errs)
	}

	return nil
}

// stopAcceptingMessages marks that we should stop accepting new work
func (sm *ShutdownManager) stopAcceptingMessages() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.consumer != nil {
		sm.consumer.mu.Lock()
		sm.consumer.running = false
		sm.consumer.mu.Unlock()
	}
}

// drainInFlightMessages waits for in-flight messages to complete
func (sm *ShutdownManager) drainInFlightMessages(ctx context.Context) error {
	drainCtx, cancel := context.WithTimeout(ctx, sm.config.DrainTimeout)
	defer cancel()

	// Wait for all tracked work to complete with timeout
	done := make(chan struct{})
	go func() {
		sm.waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-drainCtx.Done():
		return fmt.Errorf("drain timeout: some messages may not have completed")
	}
}

// flushProducer flushes pending producer messages
func (sm *ShutdownManager) flushProducer(ctx context.Context) error {
	if sm.producer == nil {
		return nil
	}

	flushCtx, cancel := context.WithTimeout(ctx, sm.config.FlushTimeout)
	defer cancel()

	kgoClient := sm.client.GetKgoClient()

	// Flush all pending records
	done := make(chan error, 1)
	go func() {
		err := kgoClient.Flush(flushCtx)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("producer flush failed: %w", err)
		}
		return nil
	case <-flushCtx.Done():
		return fmt.Errorf("producer flush timeout")
	}
}

// commitOffsets commits consumer offsets
func (sm *ShutdownManager) commitOffsets(ctx context.Context) error {
	if sm.consumer == nil {
		return nil
	}

	commitCtx, cancel := context.WithTimeout(ctx, sm.config.CommitTimeout)
	defer cancel()

	kgoClient := sm.client.GetKgoClient()

	// Commit unmarked offsets
	done := make(chan error, 1)
	go func() {
		offsets := kgoClient.UncommittedOffsets()
		if len(offsets) == 0 {
			done <- nil
			return
		}

		err := kgoClient.CommitUncommittedOffsets(commitCtx)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("offset commit failed: %w", err)
		}
		return nil
	case <-commitCtx.Done():
		return fmt.Errorf("offset commit timeout")
	}
}

// ShutdownChan returns a channel that is closed when shutdown is initiated
func (sm *ShutdownManager) ShutdownChan() <-chan struct{} {
	return sm.shutdownChan
}

// TrackWork increments the wait group for tracking in-flight work
func (sm *ShutdownManager) TrackWork() {
	sm.waitGroup.Add(1)
}

// DoneWork decrements the wait group when work completes
func (sm *ShutdownManager) DoneWork() {
	sm.waitGroup.Done()
}

// ShutdownAwareConsumer wraps a consumer with shutdown awareness
type ShutdownAwareConsumer struct {
	*Consumer
	shutdownManager *ShutdownManager
}

// NewShutdownAwareConsumer creates a consumer that respects shutdown signals
func NewShutdownAwareConsumer(consumer *Consumer, shutdownManager *ShutdownManager) *ShutdownAwareConsumer {
	shutdownManager.SetConsumer(consumer)
	return &ShutdownAwareConsumer{
		Consumer:        consumer,
		shutdownManager: shutdownManager,
	}
}

// Consume wraps the consumer's Consume method with shutdown awareness
func (sac *ShutdownAwareConsumer) Consume(ctx context.Context, topics []string, handler HandlerFunc) error {
	// Wrap handler to track work and check shutdown
	wrappedHandler := func(ctx context.Context, record *kgo.Record) error {
		// Check if shutting down before starting new work
		if sac.shutdownManager.IsShuttingDown() {
			return fmt.Errorf("shutdown in progress, not processing new messages")
		}

		// Track this work
		sac.shutdownManager.TrackWork()
		defer sac.shutdownManager.DoneWork()

		return handler(ctx, record)
	}

	return sac.Consumer.Consume(ctx, topics, wrappedHandler)
}

// ShutdownAwareProducer wraps a producer with shutdown awareness
type ShutdownAwareProducer struct {
	*Producer
	shutdownManager *ShutdownManager
}

// NewShutdownAwareProducer creates a producer that respects shutdown signals
func NewShutdownAwareProducer(producer *Producer, shutdownManager *ShutdownManager) *ShutdownAwareProducer {
	shutdownManager.SetProducer(producer)
	return &ShutdownAwareProducer{
		Producer:        producer,
		shutdownManager: shutdownManager,
	}
}

// Produce wraps the producer's Produce method with shutdown awareness
func (sap *ShutdownAwareProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	if sap.shutdownManager.IsShuttingDown() {
		return fmt.Errorf("shutdown in progress, not accepting new messages")
	}

	return sap.Producer.Produce(ctx, topic, key, value)
}

// ProduceWithHeaders wraps the producer's ProduceWithHeaders method
func (sap *ShutdownAwareProducer) ProduceWithHeaders(ctx context.Context, topic string, key, value []byte, headers map[string]string) error {
	if sap.shutdownManager.IsShuttingDown() {
		return fmt.Errorf("shutdown in progress, not accepting new messages")
	}

	return sap.Producer.ProduceWithHeaders(ctx, topic, key, value, headers)
}

// ShutdownMiddleware creates a middleware that checks for shutdown
func ShutdownMiddleware(manager *ShutdownManager) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			// Check if shutting down
			if manager.IsShuttingDown() {
				return fmt.Errorf("shutdown in progress, not processing message")
			}

			// Track work
			manager.TrackWork()
			defer manager.DoneWork()

			// Check context with shutdown signal
			select {
			case <-manager.ShutdownChan():
				return fmt.Errorf("shutdown signal received")
			case <-ctx.Done():
				return ctx.Err()
			default:
				return next(ctx, record)
			}
		}
	}
}

// GracefulApplication provides a complete application lifecycle manager
type GracefulApplication struct {
	client          *Client
	shutdownManager *ShutdownManager
	healthChecker   *HealthChecker
	startCallbacks  []func(context.Context) error
	stopCallbacks   []func(context.Context) error
	mu              sync.RWMutex
}

// NewGracefulApplication creates a new graceful application manager
func NewGracefulApplication(client *Client, shutdownConfig *ShutdownConfig, healthConfig *HealthConfig) *GracefulApplication {
	return &GracefulApplication{
		client:          client,
		shutdownManager: NewShutdownManager(client, shutdownConfig),
		healthChecker:   NewHealthChecker(client, healthConfig),
		startCallbacks:  make([]func(context.Context) error, 0),
		stopCallbacks:   make([]func(context.Context) error, 0),
	}
}

// OnStart registers a callback to run on application start
func (ga *GracefulApplication) OnStart(callback func(context.Context) error) {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	ga.startCallbacks = append(ga.startCallbacks, callback)
}

// OnStop registers a callback to run on application stop
func (ga *GracefulApplication) OnStop(callback func(context.Context) error) {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	ga.stopCallbacks = append(ga.stopCallbacks, callback)
}

// Run runs the application with graceful shutdown
func (ga *GracefulApplication) Run(ctx context.Context) error {
	// Execute start callbacks
	ga.mu.RLock()
	startCallbacks := ga.startCallbacks
	ga.mu.RUnlock()

	for _, callback := range startCallbacks {
		if err := callback(ctx); err != nil {
			return fmt.Errorf("start callback failed: %w", err)
		}
	}

	// Wait for shutdown signal
	sig := ga.shutdownManager.WaitForSignal()
	fmt.Printf("Received shutdown signal: %v\n", sig)

	// Execute stop callbacks
	ga.mu.RLock()
	stopCallbacks := ga.stopCallbacks
	ga.mu.RUnlock()

	for _, callback := range stopCallbacks {
		if err := callback(ctx); err != nil {
			fmt.Printf("Stop callback error: %v\n", err)
		}
	}

	// Perform graceful shutdown
	return ga.shutdownManager.Shutdown(ctx)
}

// GetShutdownManager returns the shutdown manager
func (ga *GracefulApplication) GetShutdownManager() *ShutdownManager {
	return ga.shutdownManager
}

// GetHealthChecker returns the health checker
func (ga *GracefulApplication) GetHealthChecker() *HealthChecker {
	return ga.healthChecker
}

// GetClient returns the Kafka client
func (ga *GracefulApplication) GetClient() *Client {
	return ga.client
}

// ShutdownWithContext initiates shutdown with context
func ShutdownWithContext(ctx context.Context, client *Client, producer *Producer, consumer *Consumer) error {
	config := DefaultShutdownConfig()
	manager := NewShutdownManager(client, config)

	if producer != nil {
		manager.SetProducer(producer)
	}

	if consumer != nil {
		manager.SetConsumer(consumer)
	}

	return manager.Shutdown(ctx)
}

// MustShutdown performs shutdown and panics on error (for testing)
func MustShutdown(ctx context.Context, client *Client) {
	config := DefaultShutdownConfig()
	config.Timeout = 5 * time.Second

	manager := NewShutdownManager(client, config)
	if err := manager.Shutdown(ctx); err != nil {
		panic(fmt.Sprintf("shutdown failed: %v", err))
	}
}
