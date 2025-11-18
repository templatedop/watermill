package franzgo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Middleware defines the middleware interface
type Middleware func(HandlerFunc) HandlerFunc

// LoggingMiddleware logs message processing
func LoggingMiddleware() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			start := time.Now()
			err := next(ctx, record)
			duration := time.Since(start)

			if err != nil {
				fmt.Printf("[ERROR] Topic: %s, Partition: %d, Offset: %d, Duration: %v, Error: %v\n",
					record.Topic, record.Partition, record.Offset, duration, err)
			} else {
				fmt.Printf("[INFO] Topic: %s, Partition: %d, Offset: %d, Duration: %v\n",
					record.Topic, record.Partition, record.Offset, duration)
			}

			return err
		}
	}
}

// RetryMiddleware adds retry logic to message processing
func RetryMiddleware(maxRetries int, delay time.Duration) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			var lastErr error

			for attempt := 0; attempt <= maxRetries; attempt++ {
				err := next(ctx, record)
				if err == nil {
					return nil
				}

				lastErr = err
				if attempt < maxRetries {
					// Exponential backoff
					backoff := delay * time.Duration(1<<uint(attempt))
					time.Sleep(backoff)
				}
			}

			return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
		}
	}
}

// TimeoutMiddleware adds timeout to message processing
func TimeoutMiddleware(timeout time.Duration) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			errChan := make(chan error, 1)
			go func() {
				errChan <- next(ctx, record)
			}()

			select {
			case err := <-errChan:
				return err
			case <-ctx.Done():
				return fmt.Errorf("handler timeout after %v", timeout)
			}
		}
	}
}

// MetricsMiddleware tracks message processing metrics
func MetricsMiddleware(metrics *MessageMetrics) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			start := time.Now()
			err := next(ctx, record)
			duration := time.Since(start)

			metrics.RecordProcessed(record.Topic, duration, err == nil)

			return err
		}
	}
}

// ThrottleMiddleware limits message processing rate
func ThrottleMiddleware(maxPerSecond int) Middleware {
	limiter := make(chan struct{}, maxPerSecond)
	ticker := time.NewTicker(time.Second)

	go func() {
		for range ticker.C {
			// Drain the limiter
			for len(limiter) > 0 {
				<-limiter
			}
		}
	}()

	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			select {
			case limiter <- struct{}{}:
				return next(ctx, record)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// CircuitBreakerMiddleware implements circuit breaker pattern
func CircuitBreakerMiddleware(failureThreshold int, timeout time.Duration) Middleware {
	var (
		failures    int32
		lastFailure time.Time
		mu          sync.RWMutex
	)

	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			mu.RLock()
			currentFailures := atomic.LoadInt32(&failures)
			timeSinceLastFailure := time.Since(lastFailure)
			mu.RUnlock()

			// Circuit is open
			if currentFailures >= int32(failureThreshold) && timeSinceLastFailure < timeout {
				return fmt.Errorf("circuit breaker is open")
			}

			// Try to process
			err := next(ctx, record)

			if err != nil {
				mu.Lock()
				atomic.AddInt32(&failures, 1)
				lastFailure = time.Now()
				mu.Unlock()
				return err
			}

			// Reset on success
			if currentFailures > 0 {
				atomic.StoreInt32(&failures, 0)
			}

			return nil
		}
	}
}

// DeduplicationMiddleware prevents processing duplicate messages
func DeduplicationMiddleware(window time.Duration) Middleware {
	type entry struct {
		timestamp time.Time
	}

	cache := make(map[string]entry)
	mu := sync.RWMutex{}

	// Cleanup goroutine
	go func() {
		ticker := time.NewTicker(window / 2)
		defer ticker.Stop()

		for range ticker.C {
			mu.Lock()
			now := time.Now()
			for key, e := range cache {
				if now.Sub(e.timestamp) > window {
					delete(cache, key)
				}
			}
			mu.Unlock()
		}
	}()

	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			// Use key+offset as unique identifier
			id := fmt.Sprintf("%s:%d:%d", record.Topic, record.Partition, record.Offset)

			mu.RLock()
			_, exists := cache[id]
			mu.RUnlock()

			if exists {
				// Already processed
				return nil
			}

			// Process message
			err := next(ctx, record)

			if err == nil {
				// Mark as processed
				mu.Lock()
				cache[id] = entry{timestamp: time.Now()}
				mu.Unlock()
			}

			return err
		}
	}
}

// MessageMetrics tracks message processing metrics
type MessageMetrics struct {
	processed map[string]int64
	errors    map[string]int64
	durations map[string]time.Duration
	mu        sync.RWMutex
}

// NewMessageMetrics creates new message metrics
func NewMessageMetrics() *MessageMetrics {
	return &MessageMetrics{
		processed: make(map[string]int64),
		errors:    make(map[string]int64),
		durations: make(map[string]time.Duration),
	}
}

// RecordProcessed records a processed message
func (mm *MessageMetrics) RecordProcessed(topic string, duration time.Duration, success bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.processed[topic]++
	mm.durations[topic] += duration

	if !success {
		mm.errors[topic]++
	}
}

// GetStats returns metrics for a topic
func (mm *MessageMetrics) GetStats(topic string) (processed, errors int64, avgDuration time.Duration) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	p := mm.processed[topic]
	e := mm.errors[topic]
	d := mm.durations[topic]

	if p > 0 {
		avgDuration = d / time.Duration(p)
	}

	return p, e, avgDuration
}

// Chain chains multiple middlewares together
func Chain(middlewares ...Middleware) Middleware {
	return func(final HandlerFunc) HandlerFunc {
		// Apply middlewares in reverse order
		handler := final
		for i := len(middlewares) - 1; i >= 0; i-- {
			handler = middlewares[i](handler)
		}
		return handler
	}
}

// RecoveryMiddleware recovers from panics
func RecoveryMiddleware() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic recovered: %v", r)
				}
			}()

			return next(ctx, record)
		}
	}
}

// HeadersMiddleware adds custom headers to records
func HeadersMiddleware(headers map[string]string) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			// Add headers to context for producer to use
			for k, v := range headers {
				record.Headers = append(record.Headers, kgo.RecordHeader{
					Key:   k,
					Value: []byte(v),
				})
			}

			return next(ctx, record)
		}
	}
}
