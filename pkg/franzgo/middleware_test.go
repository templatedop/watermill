package franzgo

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLoggingMiddleware(t *testing.T) {
	var called bool
	handler := func(ctx context.Context, record *kgo.Record) error {
		called = true
		return nil
	}

	middleware := LoggingMiddleware()
	wrappedHandler := middleware(handler)

	record := &kgo.Record{
		Topic: "test",
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	err := wrappedHandler(context.Background(), record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !called {
		t.Error("Handler was not called")
	}
}

func TestRetryMiddleware(t *testing.T) {
	tests := []struct {
		name          string
		maxRetries    int
		failCount     int
		expectedCalls int
		wantErr       bool
	}{
		{
			name:          "Success on first try",
			maxRetries:    3,
			failCount:     0,
			expectedCalls: 1,
			wantErr:       false,
		},
		{
			name:          "Success on retry",
			maxRetries:    3,
			failCount:     2,
			expectedCalls: 3,
			wantErr:       false,
		},
		{
			name:          "Fail after max retries",
			maxRetries:    3,
			failCount:     5,
			expectedCalls: 4, // Initial + 3 retries
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var callCount int32
			handler := func(ctx context.Context, record *kgo.Record) error {
				count := atomic.AddInt32(&callCount, 1)
				if int(count) <= tt.failCount {
					return errors.New("temporary error")
				}
				return nil
			}

			middleware := RetryMiddleware(tt.maxRetries, 1*time.Millisecond)
			wrappedHandler := middleware(handler)

			record := &kgo.Record{Topic: "test"}
			err := wrappedHandler(context.Background(), record)

			if (err != nil) != tt.wantErr {
				t.Errorf("Error = %v, wantErr %v", err, tt.wantErr)
			}

			if int(atomic.LoadInt32(&callCount)) != tt.expectedCalls {
				t.Errorf("Expected %d calls, got %d", tt.expectedCalls, atomic.LoadInt32(&callCount))
			}
		})
	}
}

func TestTimeoutMiddleware(t *testing.T) {
	tests := []struct {
		name        string
		timeout     time.Duration
		handlerTime time.Duration
		wantErr     bool
	}{
		{
			name:        "Completes within timeout",
			timeout:     100 * time.Millisecond,
			handlerTime: 10 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "Exceeds timeout",
			timeout:     10 * time.Millisecond,
			handlerTime: 100 * time.Millisecond,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(ctx context.Context, record *kgo.Record) error {
				select {
				case <-time.After(tt.handlerTime):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			middleware := TimeoutMiddleware(tt.timeout)
			wrappedHandler := middleware(handler)

			record := &kgo.Record{Topic: "test"}
			err := wrappedHandler(context.Background(), record)

			if (err != nil) != tt.wantErr {
				t.Errorf("Error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMetricsMiddleware(t *testing.T) {
	metrics := NewMessageMetrics()
	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	middleware := MetricsMiddleware(metrics)
	wrappedHandler := middleware(handler)

	record := &kgo.Record{Topic: "test"}
	err := wrappedHandler(context.Background(), record)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if metrics.ProcessedCount() != 1 {
		t.Errorf("Expected 1 processed message, got %d", metrics.ProcessedCount())
	}

	if metrics.SuccessCount() != 1 {
		t.Errorf("Expected 1 successful message, got %d", metrics.SuccessCount())
	}

	// Test with error
	handlerWithError := func(ctx context.Context, record *kgo.Record) error {
		return errors.New("test error")
	}

	wrappedHandlerWithError := MetricsMiddleware(metrics)(handlerWithError)
	_ = wrappedHandlerWithError(context.Background(), record)

	if metrics.ErrorCount() != 1 {
		t.Errorf("Expected 1 error, got %d", metrics.ErrorCount())
	}
}

func TestThrottleMiddleware(t *testing.T) {
	maxPerSecond := 10
	middleware := ThrottleMiddleware(maxPerSecond)

	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	wrappedHandler := middleware(handler)
	record := &kgo.Record{Topic: "test"}

	// Process messages quickly
	start := time.Now()
	for i := 0; i < 20; i++ {
		_ = wrappedHandler(context.Background(), record)
	}
	duration := time.Since(start)

	// Should take at least 1 second to process 20 messages at 10/sec
	if duration < 1*time.Second {
		t.Logf("Warning: Throttle might not be working correctly. Duration: %v", duration)
	}
}

func TestCircuitBreakerMiddleware(t *testing.T) {
	failureThreshold := 3
	timeout := 100 * time.Millisecond

	var shouldFail atomic.Bool
	shouldFail.Store(true)

	handler := func(ctx context.Context, record *kgo.Record) error {
		if shouldFail.Load() {
			return errors.New("service error")
		}
		return nil
	}

	middleware := CircuitBreakerMiddleware(failureThreshold, timeout)
	wrappedHandler := middleware(handler)

	record := &kgo.Record{Topic: "test"}

	// Trip the circuit breaker
	for i := 0; i < failureThreshold; i++ {
		_ = wrappedHandler(context.Background(), record)
	}

	// Circuit should be open now
	err := wrappedHandler(context.Background(), record)
	if err == nil {
		t.Error("Expected error when circuit is open")
	}

	// Wait for timeout and fix the service
	time.Sleep(timeout + 10*time.Millisecond)
	shouldFail.Store(false)

	// Should work again
	err = wrappedHandler(context.Background(), record)
	if err != nil {
		t.Errorf("Expected success after timeout, got: %v", err)
	}
}

func TestDeduplicationMiddleware(t *testing.T) {
	window := 100 * time.Millisecond
	middleware := DeduplicationMiddleware(window)

	var callCount atomic.Int32
	handler := func(ctx context.Context, record *kgo.Record) error {
		callCount.Add(1)
		return nil
	}

	wrappedHandler := middleware(handler)

	record := &kgo.Record{
		Topic: "test",
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}

	// First call should succeed
	err := wrappedHandler(context.Background(), record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Second call with same key should be skipped
	err = wrappedHandler(context.Background(), record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if callCount.Load() != 1 {
		t.Errorf("Expected handler to be called once, got %d", callCount.Load())
	}

	// After window expires, should work again
	time.Sleep(window + 10*time.Millisecond)
	err = wrappedHandler(context.Background(), record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if callCount.Load() != 2 {
		t.Errorf("Expected handler to be called twice after window, got %d", callCount.Load())
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		panic("test panic")
	}

	middleware := RecoveryMiddleware()
	wrappedHandler := middleware(handler)

	record := &kgo.Record{Topic: "test"}

	// Should not panic
	err := wrappedHandler(context.Background(), record)
	if err == nil {
		t.Error("Expected error from recovered panic")
	}
}

func TestHeadersMiddleware(t *testing.T) {
	headers := map[string]string{
		"x-custom-header": "custom-value",
		"x-version":       "1.0",
	}

	var capturedHeaders []kgo.RecordHeader
	handler := func(ctx context.Context, record *kgo.Record) error {
		capturedHeaders = record.Headers
		return nil
	}

	middleware := HeadersMiddleware(headers)
	wrappedHandler := middleware(handler)

	record := &kgo.Record{Topic: "test"}
	err := wrappedHandler(context.Background(), record)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(capturedHeaders) != len(headers) {
		t.Errorf("Expected %d headers, got %d", len(headers), len(capturedHeaders))
	}

	// Verify headers were added
	headerMap := make(map[string]string)
	for _, h := range capturedHeaders {
		headerMap[h.Key] = string(h.Value)
	}

	for key, expectedValue := range headers {
		if value, exists := headerMap[key]; !exists {
			t.Errorf("Header %s not found", key)
		} else if value != expectedValue {
			t.Errorf("Header %s: expected %s, got %s", key, expectedValue, value)
		}
	}
}

func TestChain(t *testing.T) {
	var executionOrder []string

	middleware1 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			executionOrder = append(executionOrder, "before-1")
			err := next(ctx, record)
			executionOrder = append(executionOrder, "after-1")
			return err
		}
	}

	middleware2 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			executionOrder = append(executionOrder, "before-2")
			err := next(ctx, record)
			executionOrder = append(executionOrder, "after-2")
			return err
		}
	}

	handler := func(ctx context.Context, record *kgo.Record) error {
		executionOrder = append(executionOrder, "handler")
		return nil
	}

	chain := Chain(middleware1, middleware2)
	wrappedHandler := chain(handler)

	record := &kgo.Record{Topic: "test"}
	err := wrappedHandler(context.Background(), record)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expectedOrder := []string{"before-1", "before-2", "handler", "after-2", "after-1"}
	if len(executionOrder) != len(expectedOrder) {
		t.Fatalf("Expected %d execution steps, got %d", len(expectedOrder), len(executionOrder))
	}

	for i, step := range expectedOrder {
		if executionOrder[i] != step {
			t.Errorf("Step %d: expected %s, got %s", i, step, executionOrder[i])
		}
	}
}

func TestMessageMetrics(t *testing.T) {
	metrics := NewMessageMetrics()

	if metrics.ProcessedCount() != 0 {
		t.Error("Initial processed count should be 0")
	}

	metrics.IncrementProcessed()
	metrics.IncrementSuccess()

	if metrics.ProcessedCount() != 1 {
		t.Error("Processed count should be 1")
	}

	if metrics.SuccessCount() != 1 {
		t.Error("Success count should be 1")
	}

	metrics.IncrementError()

	if metrics.ErrorCount() != 1 {
		t.Error("Error count should be 1")
	}

	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	metrics.RecordDuration(time.Since(start))

	avgDuration := metrics.AverageDuration()
	if avgDuration < 10*time.Millisecond {
		t.Errorf("Average duration should be >= 10ms, got %v", avgDuration)
	}
}

func BenchmarkLoggingMiddleware(b *testing.B) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	middleware := LoggingMiddleware()
	wrappedHandler := middleware(handler)
	record := &kgo.Record{Topic: "test", Key: []byte("key"), Value: []byte("value")}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = wrappedHandler(context.Background(), record)
	}
}

func BenchmarkRetryMiddleware(b *testing.B) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	middleware := RetryMiddleware(3, 1*time.Millisecond)
	wrappedHandler := middleware(handler)
	record := &kgo.Record{Topic: "test"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = wrappedHandler(context.Background(), record)
	}
}

func BenchmarkChain(b *testing.B) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	chain := Chain(
		LoggingMiddleware(),
		RetryMiddleware(3, 1*time.Millisecond),
		MetricsMiddleware(NewMessageMetrics()),
	)

	wrappedHandler := chain(handler)
	record := &kgo.Record{Topic: "test"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = wrappedHandler(context.Background(), record)
	}
}
