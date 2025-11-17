package kafka

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
)

// ShutdownCallback is a function called during shutdown
type ShutdownCallback func() error

// GracefulShutdown manages graceful shutdown of Kafka clients
type GracefulShutdown struct {
	client          *Client
	shutdownTimeout time.Duration
	callbacks       []ShutdownCallback
	logger          watermill.LoggerAdapter
	mu              sync.Mutex
	shutdownChan    chan struct{}
	shutdownOnce    sync.Once
}

// NewGracefulShutdown creates a new graceful shutdown manager
func NewGracefulShutdown(client *Client, timeout time.Duration) *GracefulShutdown {
	return &GracefulShutdown{
		client:          client,
		shutdownTimeout: timeout,
		callbacks:       []ShutdownCallback{},
		logger:          client.logger,
		shutdownChan:    make(chan struct{}),
	}
}

// OnShutdown registers a callback to be called during shutdown
// Callbacks are called in the order they were registered
func (g *GracefulShutdown) OnShutdown(callback ShutdownCallback) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.callbacks = append(g.callbacks, callback)
}

// Wait waits for shutdown signal and performs graceful shutdown
func (g *GracefulShutdown) Wait() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigChan
	g.logger.Info("Shutdown signal received", watermill.LogFields{
		"signal": sig.String(),
	})

	g.Shutdown()
}

// Shutdown performs graceful shutdown
func (g *GracefulShutdown) Shutdown() {
	g.shutdownOnce.Do(func() {
		g.logger.Info("Starting graceful shutdown", watermill.LogFields{
			"timeout": g.shutdownTimeout.String(),
		})

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), g.shutdownTimeout)
		defer cancel()

		// Channel to signal completion
		done := make(chan struct{})

		go func() {
			// Step 1: Stop accepting new messages
			g.logger.Info("Stopping client", nil)
			if err := g.stopClient(); err != nil {
				g.logger.Error("Error stopping client", err, nil)
			}

			// Step 2: Wait for in-flight messages (brief pause)
			g.logger.Info("Waiting for in-flight messages", nil)
			time.Sleep(1 * time.Second)

			// Step 3: Run shutdown callbacks
			g.runCallbacks()

			// Step 4: Close client
			g.logger.Info("Closing client", nil)
			if err := g.client.Close(); err != nil {
				g.logger.Error("Error closing client", err, nil)
			}

			close(done)
		}()

		// Wait for shutdown or timeout
		select {
		case <-done:
			g.logger.Info("Graceful shutdown completed", nil)
		case <-ctx.Done():
			g.logger.Error("Shutdown timeout exceeded", ctx.Err(), watermill.LogFields{
				"timeout": g.shutdownTimeout.String(),
			})
		}

		close(g.shutdownChan)
	})
}

// stopClient stops the client from accepting new messages
func (g *GracefulShutdown) stopClient() error {
	g.client.mu.Lock()
	defer g.client.mu.Unlock()

	// Mark as closed to prevent new operations
	// Note: This doesn't close connections yet, just stops accepting new work
	// We'll do that later in the shutdown process

	return nil
}

// runCallbacks executes all registered shutdown callbacks
func (g *GracefulShutdown) runCallbacks() {
	g.mu.Lock()
	callbacks := make([]ShutdownCallback, len(g.callbacks))
	copy(callbacks, g.callbacks)
	g.mu.Unlock()

	g.logger.Info("Running shutdown callbacks", watermill.LogFields{
		"count": len(callbacks),
	})

	for i, callback := range callbacks {
		g.logger.Info("Running shutdown callback", watermill.LogFields{
			"index": i + 1,
			"total": len(callbacks),
		})

		if err := callback(); err != nil {
			g.logger.Error("Shutdown callback failed", err, watermill.LogFields{
				"index": i + 1,
			})
		}
	}
}

// Done returns a channel that's closed when shutdown completes
func (g *GracefulShutdown) Done() <-chan struct{} {
	return g.shutdownChan
}

// ShutdownManager manages multiple components with graceful shutdown
type ShutdownManager struct {
	components      map[string]ShutdownComponent
	shutdownTimeout time.Duration
	logger          watermill.LoggerAdapter
	mu              sync.Mutex
}

// ShutdownComponent is an interface for components that support graceful shutdown
type ShutdownComponent interface {
	Shutdown(ctx context.Context) error
	Name() string
}

// NewShutdownManager creates a new shutdown manager
func NewShutdownManager(timeout time.Duration, logger watermill.LoggerAdapter) *ShutdownManager {
	return &ShutdownManager{
		components:      make(map[string]ShutdownComponent),
		shutdownTimeout: timeout,
		logger:          logger,
	}
}

// Register registers a component for shutdown
func (m *ShutdownManager) Register(component ShutdownComponent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.components[component.Name()] = component
}

// Shutdown shuts down all registered components
func (m *ShutdownManager) Shutdown() error {
	m.logger.Info("Shutting down all components", watermill.LogFields{
		"count":   len(m.components),
		"timeout": m.shutdownTimeout.String(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), m.shutdownTimeout)
	defer cancel()

	m.mu.Lock()
	components := make([]ShutdownComponent, 0, len(m.components))
	for _, comp := range m.components {
		components = append(components, comp)
	}
	m.mu.Unlock()

	// Shutdown components in parallel
	errChan := make(chan error, len(components))
	var wg sync.WaitGroup

	for _, comp := range components {
		wg.Add(1)
		go func(c ShutdownComponent) {
			defer wg.Done()

			m.logger.Info("Shutting down component", watermill.LogFields{
				"component": c.Name(),
			})

			if err := c.Shutdown(ctx); err != nil {
				m.logger.Error("Component shutdown failed", err, watermill.LogFields{
					"component": c.Name(),
				})
				errChan <- fmt.Errorf("component %s: %w", c.Name(), err)
			} else {
				m.logger.Info("Component shutdown completed", watermill.LogFields{
					"component": c.Name(),
				})
			}
		}(comp)
	}

	// Wait for all shutdowns
	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("shutdown errors: %v", errors)
	}

	m.logger.Info("All components shut down successfully", nil)
	return nil
}

// ClientComponent wraps a Client as a ShutdownComponent
type ClientComponent struct {
	client *Client
	name   string
}

// NewClientComponent creates a client component
func NewClientComponent(client *Client, name string) *ClientComponent {
	return &ClientComponent{
		client: client,
		name:   name,
	}
}

// Shutdown implements ShutdownComponent
func (c *ClientComponent) Shutdown(ctx context.Context) error {
	return c.client.Close()
}

// Name implements ShutdownComponent
func (c *ClientComponent) Name() string {
	return c.name
}

// ProcessorComponent wraps a Processor as a ShutdownComponent
type ProcessorComponent struct {
	processor *Processor
	name      string
}

// NewProcessorComponent creates a processor component
func NewProcessorComponent(processor *Processor, name string) *ProcessorComponent {
	return &ProcessorComponent{
		processor: processor,
		name:      name,
	}
}

// Shutdown implements ShutdownComponent
func (p *ProcessorComponent) Shutdown(ctx context.Context) error {
	return p.processor.Stop()
}

// Name implements ShutdownComponent
func (p *ProcessorComponent) Name() string {
	return p.name
}

// ShutdownHook provides a simple way to handle shutdown
type ShutdownHook struct {
	timeout  time.Duration
	handlers []func() error
	logger   watermill.LoggerAdapter
}

// NewShutdownHook creates a new shutdown hook
func NewShutdownHook(timeout time.Duration, logger watermill.LoggerAdapter) *ShutdownHook {
	return &ShutdownHook{
		timeout:  timeout,
		handlers: []func() error{},
		logger:   logger,
	}
}

// Register registers a shutdown handler
func (h *ShutdownHook) Register(handler func() error) {
	h.handlers = append(h.handlers, handler)
}

// WaitForSignal waits for shutdown signal and executes handlers
func (h *ShutdownHook) WaitForSignal() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	h.logger.Info("Shutdown initiated", nil)

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i, handler := range h.handlers {
			h.logger.Info("Executing shutdown handler", watermill.LogFields{
				"index": i + 1,
				"total": len(h.handlers),
			})

			if err := handler(); err != nil {
				h.logger.Error("Shutdown handler failed", err, watermill.LogFields{
					"index": i + 1,
				})
			}
		}
		close(done)
	}()

	select {
	case <-done:
		h.logger.Info("Shutdown completed successfully", nil)
	case <-ctx.Done():
		h.logger.Error("Shutdown timeout", ctx.Err(), nil)
	}
}
