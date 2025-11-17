package kafka

import (
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// NewRetryMiddleware creates a retry middleware
func NewRetryMiddleware(maxRetries int, delay time.Duration, exponentialBackoff bool) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			var lastErr error

			for attempt := 0; attempt <= maxRetries; attempt++ {
				if attempt > 0 {
					retryDelay := delay
					if exponentialBackoff {
						// Exponential backoff: delay * 2^(attempt-1)
						retryDelay = delay * time.Duration(1<<uint(attempt-1))
						// Cap at 1 minute
						if retryDelay > time.Minute {
							retryDelay = time.Minute
						}
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

// NewLoggingMiddleware creates a logging middleware
func NewLoggingMiddleware(logger watermill.LoggerAdapter) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			start := time.Now()

			logger.Info("Processing message", watermill.LogFields{
				"message_uuid": msg.UUID,
				"topic":        msg.Metadata.Get("topic"),
			})

			messages, err := h(msg)

			duration := time.Since(start)

			if err != nil {
				logger.Error("Message processing failed", err, watermill.LogFields{
					"message_uuid": msg.UUID,
					"duration_ms":  duration.Milliseconds(),
				})
			} else {
				logger.Info("Message processed successfully", watermill.LogFields{
					"message_uuid":     msg.UUID,
					"duration_ms":      duration.Milliseconds(),
					"produced_messages": len(messages),
				})
			}

			return messages, err
		}
	}
}

// MetricsCollector collects metrics for message processing
type MetricsCollector struct {
	mu                    sync.RWMutex
	messagesProcessed     int64
	messagesFailed        int64
	totalProcessingTimeMs int64
	messagesInFlight      int64
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

// GetMetrics returns current metrics
func (m *MetricsCollector) GetMetrics() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]int64{
		"messages_processed":       m.messagesProcessed,
		"messages_failed":          m.messagesFailed,
		"total_processing_time_ms": m.totalProcessingTimeMs,
		"messages_in_flight":       m.messagesInFlight,
	}
}

// GetAverageProcessingTime returns average processing time in milliseconds
func (m *MetricsCollector) GetAverageProcessingTime() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.messagesProcessed == 0 {
		return 0
	}

	return float64(m.totalProcessingTimeMs) / float64(m.messagesProcessed)
}

// Reset resets all metrics
func (m *MetricsCollector) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messagesProcessed = 0
	m.messagesFailed = 0
	m.totalProcessingTimeMs = 0
	m.messagesInFlight = 0
}

// NewMetricsMiddleware creates a metrics collection middleware
func NewMetricsMiddleware() message.HandlerMiddleware {
	collector := NewMetricsCollector()

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			collector.mu.Lock()
			collector.messagesInFlight++
			collector.mu.Unlock()

			start := time.Now()
			messages, err := h(msg)
			duration := time.Since(start)

			collector.mu.Lock()
			collector.messagesInFlight--
			collector.totalProcessingTimeMs += duration.Milliseconds()

			if err != nil {
				collector.messagesFailed++
			} else {
				collector.messagesProcessed++
			}
			collector.mu.Unlock()

			return messages, err
		}
	}
}

// NewTimeoutMiddleware creates a middleware that enforces a timeout
func NewTimeoutMiddleware(timeout time.Duration) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			done := make(chan struct{})
			var messages []*message.Message
			var err error

			go func() {
				messages, err = h(msg)
				close(done)
			}()

			select {
			case <-done:
				return messages, err
			case <-time.After(timeout):
				return nil, ErrInvalidConfig{Field: "timeout", Reason: "message processing timeout exceeded"}
			}
		}
	}
}

// NewDuplicateDetectionMiddleware creates a middleware that detects duplicate messages
func NewDuplicateDetectionMiddleware(ttl time.Duration) message.HandlerMiddleware {
	cache := &duplicateCache{
		cache: make(map[string]time.Time),
		ttl:   ttl,
	}

	// Start cleanup routine
	go cache.cleanup()

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			if cache.isDuplicate(msg.UUID) {
				// Message is a duplicate, skip processing
				return nil, nil
			}

			messages, err := h(msg)

			// Mark as processed
			cache.add(msg.UUID)

			return messages, err
		}
	}
}

type duplicateCache struct {
	mu    sync.RWMutex
	cache map[string]time.Time
	ttl   time.Duration
}

func (c *duplicateCache) isDuplicate(uuid string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.cache[uuid]
	return exists
}

func (c *duplicateCache) add(uuid string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[uuid] = time.Now()
}

func (c *duplicateCache) cleanup() {
	ticker := time.NewTicker(c.ttl / 2)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for uuid, timestamp := range c.cache {
			if now.Sub(timestamp) > c.ttl {
				delete(c.cache, uuid)
			}
		}
		c.mu.Unlock()
	}
}

// NewThrottleMiddleware creates a middleware that throttles message processing
func NewThrottleMiddleware(maxPerSecond int) message.HandlerMiddleware {
	throttle := &throttler{
		maxPerSecond: maxPerSecond,
		tokens:       maxPerSecond,
		lastRefill:   time.Now(),
	}

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			throttle.wait()
			return h(msg)
		}
	}
}

type throttler struct {
	mu           sync.Mutex
	maxPerSecond int
	tokens       int
	lastRefill   time.Time
}

func (t *throttler) wait() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Refill tokens based on time elapsed
	now := time.Now()
	elapsed := now.Sub(t.lastRefill)
	tokensToAdd := int(elapsed.Seconds() * float64(t.maxPerSecond))

	if tokensToAdd > 0 {
		t.tokens += tokensToAdd
		if t.tokens > t.maxPerSecond {
			t.tokens = t.maxPerSecond
		}
		t.lastRefill = now
	}

	// Wait until we have a token
	for t.tokens == 0 {
		t.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		t.mu.Lock()

		// Refill again
		now = time.Now()
		elapsed = now.Sub(t.lastRefill)
		tokensToAdd = int(elapsed.Seconds() * float64(t.maxPerSecond))

		if tokensToAdd > 0 {
			t.tokens += tokensToAdd
			if t.tokens > t.maxPerSecond {
				t.tokens = t.maxPerSecond
			}
			t.lastRefill = now
		}
	}

	t.tokens--
}

// NewCircuitBreakerMiddleware creates a circuit breaker middleware
func NewCircuitBreakerMiddleware(failureThreshold int, timeout time.Duration) message.HandlerMiddleware {
	cb := &circuitBreaker{
		failureThreshold: failureThreshold,
		timeout:          timeout,
		state:            "closed",
	}

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			if !cb.allow() {
				return nil, ErrInvalidConfig{Field: "circuit_breaker", Reason: "circuit breaker is open"}
			}

			messages, err := h(msg)

			if err != nil {
				cb.recordFailure()
			} else {
				cb.recordSuccess()
			}

			return messages, err
		}
	}
}

type circuitBreaker struct {
	mu               sync.RWMutex
	failureThreshold int
	timeout          time.Duration
	failures         int
	lastFailureTime  time.Time
	state            string // "closed", "open", "half-open"
}

func (cb *circuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case "closed":
		return true
	case "open":
		if time.Since(cb.lastFailureTime) > cb.timeout {
			cb.state = "half-open"
			return true
		}
		return false
	case "half-open":
		return true
	default:
		return false
	}
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailureTime = time.Now()

	if cb.failures >= cb.failureThreshold {
		cb.state = "open"
	}
}

func (cb *circuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == "half-open" {
		cb.state = "closed"
		cb.failures = 0
	}
}
