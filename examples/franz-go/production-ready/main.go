// Franz-go Production-Ready Example
// Demonstrates production-ready features: health checks, metrics, graceful shutdown, monitoring

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.cept.gov.in/it-2.0-common/watermill/pkg/franzgo"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OrderEvent represents an order
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Total      float64   `json:"total"`
	Timestamp  time.Time `json:"timestamp"`
}

type Application struct {
	client   *franzgo.Client
	consumer *franzgo.Consumer
	producer *franzgo.Producer
	app      *franzgo.GracefulApplication
	metrics  *Metrics
}

type Metrics struct {
	ordersProcessed int64
	ordersFailed    int64
	totalRevenue    float64
}

func main() {
	log.Println("🚀 Starting Production-Ready Franz-go Application...")

	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	// Create production configuration
	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("production-ready-group").
		WithClientID("production-app").
		WithCompression(franzgo.CompressionZstd).
		WithProducerAcks(franzgo.AllISRAcks).
		WithConsumerSessionTimeout(45 * time.Second).
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

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
		Signals:       []os.Signal{os.Interrupt, syscall.SIGTERM},
		OnShutdownStart: func() {
			log.Println("🛑 Initiating graceful shutdown...")
		},
		OnShutdownComplete: func(err error) {
			if err != nil {
				log.Printf("⚠️  Shutdown completed with errors: %v", err)
			} else {
				log.Println("✅ Shutdown completed successfully")
			}
		},
		OnProgress: func(stage string, duration time.Duration) {
			log.Printf("📊 Shutdown stage: %s (took %v)", stage, duration)
		},
	}

	// Create graceful application
	app := franzgo.NewGracefulApplication(client, shutdownConfig, healthConfig)

	application := &Application{
		client:   client,
		consumer: franzgo.NewConsumer(client),
		producer: franzgo.NewProducer(client),
		app:      app,
		metrics:  &Metrics{},
	}

	// Setup HTTP endpoints for observability
	application.setupHTTPEndpoints()

	// Setup application lifecycle
	app.OnStart(func(ctx context.Context) error {
		log.Println("📊 Application initializing...")

		// Start consuming messages
		go application.processOrders(ctx)

		log.Println("✅ Application started successfully")
		log.Println("📊 Metrics: http://localhost:8080/metrics")
		log.Println("❤️  Health: http://localhost:8080/health")
		log.Println("📈 Stats: http://localhost:8080/stats")

		return nil
	})

	app.OnStop(func(ctx context.Context) error {
		log.Println("🛑 Application stopping...")
		log.Printf("📊 Final metrics - Processed: %d, Failed: %d, Revenue: $%.2f",
			application.metrics.ordersProcessed,
			application.metrics.ordersFailed,
			application.metrics.totalRevenue)
		return nil
	})

	// Run application
	ctx := context.Background()
	if err := app.Run(ctx); err != nil {
		log.Fatalf("❌ Application error: %v", err)
	}
}

// processOrders processes orders with error handling and metrics
func (a *Application) processOrders(ctx context.Context) {
	err := a.consumer.Consume(ctx, []string{"orders.production"}, func(record *kgo.Record) error {
		var order OrderEvent
		if err := json.Unmarshal(record.Value, &order); err != nil {
			a.metrics.ordersFailed++
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		log.Printf("📦 Processing order: %s (customer: %s, total: $%.2f)",
			order.OrderID, order.CustomerID, order.Total)

		// Simulate processing
		time.Sleep(100 * time.Millisecond)

		// Update metrics
		a.metrics.ordersProcessed++
		a.metrics.totalRevenue += order.Total

		// Emit processed event
		processedEvent := map[string]interface{}{
			"order_id":    order.OrderID,
			"processed_at": time.Now(),
			"status":      "completed",
		}

		data, _ := json.Marshal(processedEvent)
		if err := a.producer.Produce(ctx, "orders.processed", []byte(order.OrderID), data); err != nil {
			log.Printf("⚠️  Failed to emit processed event: %v", err)
		}

		log.Printf("✅ Order %s processed successfully", order.OrderID)
		return nil
	})

	if err != nil {
		log.Printf("❌ Consumer error: %v", err)
	}
}

// setupHTTPEndpoints sets up health, metrics, and stats endpoints
func (a *Application) setupHTTPEndpoints() {
	healthChecker := a.app.GetHealthChecker()
	k8sHandler := franzgo.NewKubernetesHealthHandler(healthChecker)

	// Health check endpoint
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

	// Kubernetes liveness probe
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

	// Kubernetes readiness probe
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

	// Prometheus metrics endpoint
	http.Handle("/metrics", promhttp.Handler())

	// Application statistics endpoint
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := map[string]interface{}{
			"orders_processed": a.metrics.ordersProcessed,
			"orders_failed":    a.metrics.ordersFailed,
			"total_revenue":    a.metrics.totalRevenue,
			"average_order":    0.0,
		}

		if a.metrics.ordersProcessed > 0 {
			stats["average_order"] = a.metrics.totalRevenue / float64(a.metrics.ordersProcessed)
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
