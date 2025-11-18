// Payment Service - Production-Ready Payment Processing with Exactly-Once Semantics
// Demonstrates: Transactional Processing, Idempotency, Retry Logic, Circuit Breaker

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/templatedop/watermill/pkg/franzgo"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PaymentRequest represents a payment request
type PaymentRequest struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
	Timestamp  time.Time `json:"timestamp"`
}

// PaymentResult represents the result of a payment
type PaymentResult struct {
	OrderID       string    `json:"order_id"`
	TransactionID string    `json:"transaction_id"`
	Status        string    `json:"status"` // success, failed, pending
	Amount        float64   `json:"amount"`
	ProcessedAt   time.Time `json:"processed_at"`
	ErrorMessage  string    `json:"error_message,omitempty"`
}

// PaymentService handles payment processing with exactly-once semantics
type PaymentService struct {
	client         *franzgo.Client
	txnProducer    *franzgo.TransactionalProducer
	consumer       *franzgo.Consumer
	idempotent     *franzgo.IdempotentProducer
	app            *franzgo.GracefulApplication
	metrics        *PaymentMetrics
	circuitBreaker *CircuitBreaker
}

// PaymentMetrics tracks payment-specific metrics
type PaymentMetrics struct {
	paymentsProcessed prometheus.Counter
	paymentsSucceeded prometheus.Counter
	paymentsFailed    prometheus.Counter
	paymentsDuplicate prometheus.Counter
	paymentAmount     prometheus.Histogram
	processingTime    prometheus.Histogram
}

// CircuitBreaker implements circuit breaker pattern for payment gateway
type CircuitBreaker struct {
	failureThreshold int
	resetTimeout     time.Duration
	failureCount     int
	lastFailureTime  time.Time
	state            string // closed, open, half-open
}

func newCircuitBreaker(failureThreshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		resetTimeout:     resetTimeout,
		state:            "closed",
	}
}

func (cb *CircuitBreaker) call(fn func() error) error {
	// Check if circuit should reset
	if cb.state == "open" && time.Since(cb.lastFailureTime) > cb.resetTimeout {
		cb.state = "half-open"
		cb.failureCount = 0
	}

	// If circuit is open, fail fast
	if cb.state == "open" {
		return fmt.Errorf("circuit breaker is open")
	}

	// Try the call
	err := fn()
	if err != nil {
		cb.failureCount++
		cb.lastFailureTime = time.Now()

		if cb.failureCount >= cb.failureThreshold {
			cb.state = "open"
			log.Printf("⚡ Circuit breaker opened after %d failures", cb.failureCount)
		}
		return err
	}

	// Success - reset circuit breaker
	if cb.state == "half-open" {
		cb.state = "closed"
		cb.failureCount = 0
		log.Println("✅ Circuit breaker closed")
	}

	return nil
}

// NewPaymentService creates a production-ready payment service
func NewPaymentService(brokers []string) (*PaymentService, error) {
	// Configuration
	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("payment-service").
		WithClientID("payment-service-1").
		WithProducerBatchSize(16384).
		WithCompression(franzgo.CompressionZstd).
		Build()

	// Create client
	client, err := franzgo.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	// Create transactional producer for exactly-once semantics
	txnConfig := &franzgo.TransactionalConfig{
		TransactionalID:    "payment-service-txn-1",
		TransactionTimeout: 60 * time.Second,
	}

	txnProducer, err := franzgo.NewTransactionalProducer(config, txnConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional producer: %w", err)
	}

	// Create idempotent producer for deduplication
	producer := franzgo.NewProducer(client)
	idempotent := franzgo.NewIdempotentProducer(producer)

	// Create consumer
	consumer := franzgo.NewConsumer(client, nil)

	// Create graceful application
	shutdownConfig := &franzgo.ShutdownConfig{
		Timeout:       30 * time.Second,
		DrainTimeout:  10 * time.Second,
		FlushTimeout:  5 * time.Second,
		CommitTimeout: 5 * time.Second,
		CommitOffsets: true,
		FlushProducer: true,
		Signals:       []os.Signal{os.Interrupt, os.Kill},
		OnShutdownStart: func() {
			log.Println("🛑 Payment Service: Initiating graceful shutdown...")
		},
	}

	app := franzgo.NewGracefulApplication(client, shutdownConfig, nil)

	// Initialize metrics
	metrics := &PaymentMetrics{
		paymentsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "payments_processed_total",
			Help: "Total number of payment requests processed",
		}),
		paymentsSucceeded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "payments_succeeded_total",
			Help: "Total number of successful payments",
		}),
		paymentsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "payments_failed_total",
			Help: "Total number of failed payments",
		}),
		paymentsDuplicate: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "payments_duplicate_total",
			Help: "Total number of duplicate payment attempts",
		}),
		paymentAmount: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "payment_amount",
			Help:    "Payment amounts",
			Buckets: prometheus.LinearBuckets(0, 50, 20), // 0-1000 in 50 increments
		}),
		processingTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "payment_processing_seconds",
			Help:    "Payment processing time in seconds",
			Buckets: prometheus.DefBuckets,
		}),
	}

	// Register metrics
	prometheus.MustRegister(
		metrics.paymentsProcessed,
		metrics.paymentsSucceeded,
		metrics.paymentsFailed,
		metrics.paymentsDuplicate,
		metrics.paymentAmount,
		metrics.processingTime,
	)

	service := &PaymentService{
		client:         client,
		txnProducer:    txnProducer,
		consumer:       consumer,
		idempotent:     idempotent,
		app:            app,
		metrics:        metrics,
		circuitBreaker: newCircuitBreaker(5, 30*time.Second),
	}

	return service, nil
}

// processPaymentRequest processes a payment request with exactly-once semantics
func (ps *PaymentService) processPaymentRequest(ctx context.Context, record *kgo.Record) error {
	start := time.Now()
	defer func() {
		ps.metrics.processingTime.Observe(time.Since(start).Seconds())
	}()

	// Parse payment request
	var request PaymentRequest
	if err := json.Unmarshal(record.Value, &request); err != nil {
		return fmt.Errorf("failed to unmarshal payment request: %w", err)
	}

	log.Printf("💳 Processing payment for order %s (amount: $%.2f)", request.OrderID, request.Amount)
	ps.metrics.paymentsProcessed.Inc()

	// Use transactional producer for exactly-once semantics
	err := ps.txnProducer.ExecuteTransaction(ctx, func(ctx context.Context) error {
		// Simulate payment gateway call with circuit breaker
		result, err := ps.processWithCircuitBreaker(request)
		if err != nil {
			log.Printf("❌ Payment failed for order %s: %v", request.OrderID, err)
			ps.metrics.paymentsFailed.Inc()

			// Send to failure topic
			failureData, _ := json.Marshal(map[string]interface{}{
				"order_id": request.OrderID,
				"error":    err.Error(),
				"timestamp": time.Now(),
			})

			return ps.txnProducer.Produce(ctx, "payment-failures", []byte(request.OrderID), failureData)
		}

		// Payment succeeded
		log.Printf("✅ Payment succeeded for order %s (txn: %s)", request.OrderID, result.TransactionID)
		ps.metrics.paymentsSucceeded.Inc()
		ps.metrics.paymentAmount.Observe(request.Amount)

		// Produce payment success event (exactly-once within transaction)
		successData, _ := json.Marshal(result)
		if err := ps.txnProducer.Produce(ctx, "payment-results", []byte(request.OrderID), successData); err != nil {
			return err
		}

		// Produce order update event (exactly-once within transaction)
		orderUpdateData, _ := json.Marshal(map[string]interface{}{
			"event_type": "paid",
			"order_id":   request.OrderID,
			"order": map[string]interface{}{
				"status": "paid",
			},
			"timestamp": time.Now(),
		})

		return ps.txnProducer.Produce(ctx, "order-events", []byte(request.OrderID), orderUpdateData)
	})

	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	return nil
}

// processWithCircuitBreaker processes payment with circuit breaker protection
func (ps *PaymentService) processWithCircuitBreaker(request PaymentRequest) (*PaymentResult, error) {
	var result *PaymentResult
	var err error

	cbErr := ps.circuitBreaker.call(func() error {
		result, err = ps.callPaymentGateway(request)
		return err
	})

	if cbErr != nil {
		return nil, cbErr
	}

	return result, nil
}

// callPaymentGateway simulates calling a payment gateway API
func (ps *PaymentService) callPaymentGateway(request PaymentRequest) (*PaymentResult, error) {
	// Simulate network latency
	time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)

	// Simulate payment gateway failures (10% failure rate)
	if rand.Float64() < 0.1 {
		return nil, fmt.Errorf("payment gateway error: connection timeout")
	}

	// Simulate successful payment
	result := &PaymentResult{
		OrderID:       request.OrderID,
		TransactionID: fmt.Sprintf("TXN-%d", time.Now().UnixNano()),
		Status:        "success",
		Amount:        request.Amount,
		ProcessedAt:   time.Now(),
	}

	return result, nil
}

// Start starts the payment service
func (ps *PaymentService) Start() error {
	log.Println("🚀 Starting Payment Service...")

	// Setup HTTP endpoints
	ps.setupHTTPEndpoints()

	// Setup application lifecycle
	ps.app.OnStart(func(ctx context.Context) error {
		log.Println("📊 Payment Service initializing...")

		// Create middleware chain
		middleware := franzgo.Chain(
			franzgo.LoggingMiddleware(),
			franzgo.MetricsMiddleware(franzgo.NewMessageMetrics()),
			franzgo.RetryMiddleware(3, 2*time.Second),
			franzgo.TimeoutMiddleware(30*time.Second),
			franzgo.RecoveryMiddleware(),
		)

		// Wrap handler with middleware
		handler := middleware(ps.processPaymentRequest)

		// Start consumer
		go func() {
			if err := ps.consumer.Consume(ctx, []string{"payment-requests"}, handler); err != nil {
				log.Printf("❌ Consumer error: %v", err)
			}
		}()

		log.Println("✅ Payment Service started successfully")
		log.Println("📊 Metrics available at http://localhost:8081/metrics")
		log.Println("❤️  Health checks at http://localhost:8081/health")
		log.Println("💳 Circuit breaker status at http://localhost:8081/circuit-breaker")

		return nil
	})

	ps.app.OnStop(func(ctx context.Context) error {
		log.Println("🛑 Payment Service stopping...")

		// Close transactional producer
		if err := ps.txnProducer.Close(); err != nil {
			log.Printf("⚠️  Error closing transactional producer: %v", err)
		}

		log.Println("✅ Payment Service stopped")
		return nil
	})

	// Run application
	ctx := context.Background()
	return ps.app.Run(ctx)
}

// setupHTTPEndpoints sets up HTTP endpoints
func (ps *PaymentService) setupHTTPEndpoints() {
	// Health endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		health := map[string]interface{}{
			"status":          "healthy",
			"circuit_breaker": ps.circuitBreaker.state,
			"timestamp":       time.Now(),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(health)
	})

	// Circuit breaker status
	http.HandleFunc("/circuit-breaker", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]interface{}{
			"state":          ps.circuitBreaker.state,
			"failure_count":  ps.circuitBreaker.failureCount,
			"last_failure":   ps.circuitBreaker.lastFailureTime,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	// Metrics endpoint (Prometheus)
	http.Handle("/metrics", promhttp.Handler())

	// Start HTTP server
	go func() {
		log.Println("🌐 HTTP server starting on :8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Printf("❌ HTTP server error: %v", err)
		}
	}()
}

func main() {
	// Get Kafka brokers from environment or use default
	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	// Create payment service
	service, err := NewPaymentService(brokers)
	if err != nil {
		log.Fatalf("❌ Failed to create payment service: %v", err)
	}

	// Start service
	if err := service.Start(); err != nil {
		log.Fatalf("❌ Payment service error: %v", err)
	}
}
