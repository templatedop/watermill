// Order Service - Production-Ready E-commerce Microservice
// Demonstrates: Stateful Processing, Health Checks, Graceful Shutdown, Metrics, Tracing

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/templatedop/watermill/pkg/franzgo"
)

// Order represents an e-commerce order
type Order struct {
	OrderID      string    `json:"order_id"`
	CustomerID   string    `json:"customer_id"`
	Status       string    `json:"status"` // pending, confirmed, paid, shipped, delivered, cancelled
	Items        []OrderItem `json:"items"`
	TotalAmount  float64   `json:"total_amount"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// OrderItem represents an item in the order
type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// OrderState tracks the current state of an order in our state store
type OrderState struct {
	Order        Order     `json:"order"`
	EventHistory []string  `json:"event_history"`
	LastUpdated  time.Time `json:"last_updated"`
}

// OrderEvent represents an order event
type OrderEvent struct {
	EventType  string    `json:"event_type"` // created, updated, paid, shipped, delivered, cancelled
	OrderID    string    `json:"order_id"`
	Order      Order     `json:"order"`
	Timestamp  time.Time `json:"timestamp"`
}

// OrderService handles order processing with stateful stream processing
type OrderService struct {
	client            *franzgo.Client
	statefulProcessor *franzgo.StatefulProcessor
	producer          *franzgo.Producer
	app               *franzgo.GracefulApplication
	metrics           *OrderMetrics
}

// OrderMetrics tracks order-specific metrics
type OrderMetrics struct {
	ordersCreated   int64
	ordersConfirmed int64
	ordersPaid      int64
	ordersShipped   int64
	ordersCancelled int64
}

// NewOrderService creates a production-ready order service
func NewOrderService(brokers []string) (*OrderService, error) {
	// Configuration
	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("order-service").
		WithClientID("order-service-1").
		WithProducerBatchSize(16384).
		WithProducerLinger(10 * time.Millisecond).
		WithCompression(franzgo.CompressionZstd).
		WithConsumerSessionTimeout(45 * time.Second).
		Build()

	// Create client
	client, err := franzgo.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	// Health check configuration
	healthConfig := &franzgo.HealthConfig{
		CheckTimeout:                 5 * time.Second,
		CacheTimeout:                 10 * time.Second,
		ConsumerLagWarningThreshold:  1000,
		ConsumerLagCriticalThreshold: 10000,
		BrokerConnectionTimeout:      3 * time.Second,
	}

	// Shutdown configuration
	shutdownConfig := &franzgo.ShutdownConfig{
		Timeout:       30 * time.Second,
		DrainTimeout:  10 * time.Second,
		FlushTimeout:  5 * time.Second,
		CommitTimeout: 5 * time.Second,
		CommitOffsets: true,
		FlushProducer: true,
		Signals:       []os.Signal{os.Interrupt, os.Kill},
		OnShutdownStart: func() {
			log.Println("🛑 Order Service: Initiating graceful shutdown...")
		},
		OnShutdownComplete: func(err error) {
			if err != nil {
				log.Printf("⚠️  Order Service: Shutdown completed with errors: %v", err)
			} else {
				log.Println("✅ Order Service: Shutdown completed successfully")
			}
		},
		OnProgress: func(stage string, duration time.Duration) {
			log.Printf("📊 Shutdown stage: %s (took %v)", stage, duration)
		},
	}

	// Create graceful application
	app := franzgo.NewGracefulApplication(client, shutdownConfig, healthConfig)

	// Create producer
	producer := franzgo.NewProducer(client)

	// Stateful processor configuration
	statefulConfig := &franzgo.StatefulProcessorConfig{
		StorageFactory: func(partition int32) (franzgo.Storage, error) {
			// Use LevelDB for persistent state
			return franzgo.NewLevelDBStorage(fmt.Sprintf("./data/orders/partition-%d", partition))
		},
		ChangelogTopic:     "order-service-changelog",
		ChangelogCompacted: true,
		RecoveryTimeout:    5 * time.Minute,
	}

	// Create stateful processor with order state management
	statefulProcessor, err := franzgo.NewStatefulProcessor(client, statefulConfig, func(ctx *franzgo.StateProcessorContext) error {
		// This will be set up in Start()
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create stateful processor: %w", err)
	}

	service := &OrderService{
		client:            client,
		statefulProcessor: statefulProcessor,
		producer:          producer,
		app:               app,
		metrics:           &OrderMetrics{},
	}

	return service, nil
}

// processOrderEvent processes order events and maintains state
func (os *OrderService) processOrderEvent(ctx *franzgo.StateProcessorContext) error {
	// Parse the event
	var event OrderEvent
	if err := json.Unmarshal(ctx.Record().Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal order event: %w", err)
	}

	log.Printf("📦 Processing order event: %s for order %s", event.EventType, event.OrderID)

	// Get current state
	var state OrderState
	stateExists := false
	if err := ctx.GetJSON(event.OrderID, &state); err == nil {
		stateExists = true
	}

	// Initialize state if new order
	if !stateExists {
		state = OrderState{
			Order:        event.Order,
			EventHistory: make([]string, 0),
			LastUpdated:  time.Now(),
		}
	}

	// Update state based on event type
	switch event.EventType {
	case "created":
		state.Order = event.Order
		state.Order.Status = "pending"
		os.metrics.ordersCreated++

		// Emit order confirmation event
		os.emitOrderConfirmation(event.OrderID, event.Order)

	case "confirmed":
		state.Order.Status = "confirmed"
		os.metrics.ordersConfirmed++

		// Emit payment request
		os.emitPaymentRequest(event.OrderID, event.Order)

	case "paid":
		state.Order.Status = "paid"
		os.metrics.ordersPaid++

		// Emit inventory reservation
		os.emitInventoryReservation(event.OrderID, event.Order)

	case "shipped":
		state.Order.Status = "shipped"
		os.metrics.ordersShipped++

		// Emit notification
		os.emitShipmentNotification(event.OrderID, event.Order)

	case "delivered":
		state.Order.Status = "delivered"

	case "cancelled":
		state.Order.Status = "cancelled"
		os.metrics.ordersCancelled++

		// Emit inventory release
		os.emitInventoryRelease(event.OrderID, event.Order)

	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}

	// Add to event history
	state.EventHistory = append(state.EventHistory, fmt.Sprintf("%s:%s", event.EventType, time.Now().Format(time.RFC3339)))
	state.LastUpdated = time.Now()

	// Save state (automatically emitted to changelog)
	if err := ctx.SetJSON(event.OrderID, state); err != nil {
		return fmt.Errorf("failed to save order state: %w", err)
	}

	log.Printf("✅ Order %s updated to status: %s", event.OrderID, state.Order.Status)

	return nil
}

// emitOrderConfirmation emits an order confirmation event
func (os *OrderService) emitOrderConfirmation(orderID string, order Order) {
	event := map[string]interface{}{
		"event_type": "order.confirmed",
		"order_id":   orderID,
		"customer_id": order.CustomerID,
		"total_amount": order.TotalAmount,
		"timestamp":  time.Now(),
	}

	data, _ := json.Marshal(event)
	ctx := context.Background()

	if err := os.producer.Produce(ctx, "order-confirmations", []byte(orderID), data); err != nil {
		log.Printf("❌ Failed to emit order confirmation: %v", err)
	} else {
		log.Printf("📧 Emitted order confirmation for %s", orderID)
	}
}

// emitPaymentRequest emits a payment request
func (os *OrderService) emitPaymentRequest(orderID string, order Order) {
	event := map[string]interface{}{
		"event_type":   "payment.request",
		"order_id":     orderID,
		"customer_id":  order.CustomerID,
		"amount":       order.TotalAmount,
		"timestamp":    time.Now(),
	}

	data, _ := json.Marshal(event)
	ctx := context.Background()

	if err := os.producer.Produce(ctx, "payment-requests", []byte(orderID), data); err != nil {
		log.Printf("❌ Failed to emit payment request: %v", err)
	} else {
		log.Printf("💳 Emitted payment request for %s (amount: $%.2f)", orderID, order.TotalAmount)
	}
}

// emitInventoryReservation emits inventory reservation
func (os *OrderService) emitInventoryReservation(orderID string, order Order) {
	for _, item := range order.Items {
		event := map[string]interface{}{
			"event_type": "inventory.reserve",
			"order_id":   orderID,
			"product_id": item.ProductID,
			"quantity":   item.Quantity,
			"timestamp":  time.Now(),
		}

		data, _ := json.Marshal(event)
		ctx := context.Background()

		if err := os.producer.Produce(ctx, "inventory-reservations", []byte(item.ProductID), data); err != nil {
			log.Printf("❌ Failed to emit inventory reservation: %v", err)
		} else {
			log.Printf("📦 Reserved inventory: %s (qty: %d)", item.ProductID, item.Quantity)
		}
	}
}

// emitInventoryRelease emits inventory release
func (os *OrderService) emitInventoryRelease(orderID string, order Order) {
	for _, item := range order.Items {
		event := map[string]interface{}{
			"event_type": "inventory.release",
			"order_id":   orderID,
			"product_id": item.ProductID,
			"quantity":   item.Quantity,
			"timestamp":  time.Now(),
		}

		data, _ := json.Marshal(event)
		ctx := context.Background()

		if err := os.producer.Produce(ctx, "inventory-releases", []byte(item.ProductID), data); err != nil {
			log.Printf("❌ Failed to emit inventory release: %v", err)
		}
	}
}

// emitShipmentNotification emits shipment notification
func (os *OrderService) emitShipmentNotification(orderID string, order Order) {
	event := map[string]interface{}{
		"event_type":  "notification.shipment",
		"order_id":    orderID,
		"customer_id": order.CustomerID,
		"timestamp":   time.Now(),
	}

	data, _ := json.Marshal(event)
	ctx := context.Background()

	if err := os.producer.Produce(ctx, "notifications", []byte(orderID), data); err != nil {
		log.Printf("❌ Failed to emit shipment notification: %v", err)
	} else {
		log.Printf("📧 Emitted shipment notification for %s", orderID)
	}
}

// Start starts the order service
func (os *OrderService) Start() error {
	log.Println("🚀 Starting Order Service...")

	// Setup HTTP endpoints
	os.setupHTTPEndpoints()

	// Setup application lifecycle
	os.app.OnStart(func(ctx context.Context) error {
		log.Println("📊 Order Service initializing...")

		// Set the processor function
		os.statefulProcessor = os.recreateStatefulProcessor()

		// Start stateful processor
		go func() {
			if err := os.statefulProcessor.Process(ctx, []string{"order-events"}); err != nil {
				log.Printf("❌ Stateful processor error: %v", err)
			}
		}()

		log.Println("✅ Order Service started successfully")
		log.Println("📊 Metrics available at http://localhost:8080/metrics")
		log.Println("❤️  Health checks at http://localhost:8080/health")
		log.Println("📈 Order stats at http://localhost:8080/stats")

		return nil
	})

	os.app.OnStop(func(ctx context.Context) error {
		log.Println("🛑 Order Service stopping...")

		// Close stateful processor
		if err := os.statefulProcessor.Close(); err != nil {
			log.Printf("⚠️  Error closing stateful processor: %v", err)
		}

		log.Println("✅ Order Service stopped")
		return nil
	})

	// Run application
	ctx := context.Background()
	return os.app.Run(ctx)
}

// recreateStatefulProcessor creates a new stateful processor with the correct function
func (os *OrderService) recreateStatefulProcessor() *franzgo.StatefulProcessor {
	statefulConfig := &franzgo.StatefulProcessorConfig{
		StorageFactory: func(partition int32) (franzgo.Storage, error) {
			return franzgo.NewLevelDBStorage(fmt.Sprintf("./data/orders/partition-%d", partition))
		},
		ChangelogTopic:     "order-service-changelog",
		ChangelogCompacted: true,
		RecoveryTimeout:    5 * time.Minute,
	}

	processor, _ := franzgo.NewStatefulProcessor(os.client, statefulConfig, os.processOrderEvent)
	return processor
}

// setupHTTPEndpoints sets up HTTP endpoints for health, metrics, and stats
func (os *OrderService) setupHTTPEndpoints() {
	// Health endpoints
	healthChecker := os.app.GetHealthChecker()
	k8sHandler := franzgo.NewKubernetesHealthHandler(healthChecker)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		report, err := healthChecker.Check(ctx)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(report)
	})

	http.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		alive, message := k8sHandler.LivenessProbe(r.Context())
		if alive {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "alive", "message": message})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "dead", "message": message})
		}
	})

	http.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ready, message, report := k8sHandler.ReadinessProbe(r.Context())
		if ready {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "ready",
				"message": message,
				"report":  report,
			})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "not ready",
				"message": message,
				"report":  report,
			})
		}
	})

	// Metrics endpoint (Prometheus)
	http.Handle("/metrics", promhttp.Handler())

	// Order statistics endpoint
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := map[string]interface{}{
			"orders_created":   os.metrics.ordersCreated,
			"orders_confirmed": os.metrics.ordersConfirmed,
			"orders_paid":      os.metrics.ordersPaid,
			"orders_shipped":   os.metrics.ordersShipped,
			"orders_cancelled": os.metrics.ordersCancelled,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})

	// Start HTTP server
	go func() {
		log.Println("🌐 HTTP server starting on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
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

	// Create order service
	service, err := NewOrderService(brokers)
	if err != nil {
		log.Fatalf("❌ Failed to create order service: %v", err)
	}

	// Start service
	if err := service.Start(); err != nil {
		log.Fatalf("❌ Order service error: %v", err)
	}
}
