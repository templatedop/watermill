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

	"github.com/templatedop/watermill/pkg/franzgo"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Example 1: Health Checks
func exampleHealthChecks() {
	fmt.Println("\n=== Example 1: Health Checks ===")

	// Create config and client
	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("health-check-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create health checker with custom configuration
	healthConfig := &franzgo.HealthConfig{
		CheckTimeout:                 5 * time.Second,
		CacheTimeout:                 10 * time.Second,
		ConsumerLagWarningThreshold:  1000,
		ConsumerLagCriticalThreshold: 10000,
		BrokerConnectionTimeout:      3 * time.Second,
	}

	checker := franzgo.NewHealthChecker(client, healthConfig)

	// Add custom health checks
	checker.AddCheck(func(ctx context.Context) franzgo.HealthCheck {
		start := time.Now()
		check := franzgo.HealthCheck{
			Name:      "custom_check",
			Timestamp: start,
			Metadata:  make(map[string]interface{}),
		}

		// Custom business logic check
		// For example, check external dependency
		time.Sleep(10 * time.Millisecond) // Simulate check

		check.Duration = time.Since(start)
		check.Status = franzgo.HealthStatusHealthy
		check.Message = "Custom check passed"
		return check
	})

	// Add metadata check
	checker.AddCheck(checker.MetadataHealthCheck())

	// Perform health check
	ctx := context.Background()
	report, err := checker.Check(ctx)
	if err != nil {
		log.Printf("Health check failed: %v", err)
		return
	}

	// Display results
	fmt.Printf("Overall Status: %s\n", report.Status)
	fmt.Printf("Total Checks: %d\n", report.TotalChecks)
	fmt.Printf("Healthy: %d, Degraded: %d, Unhealthy: %d\n",
		report.HealthyChecks, report.DegradedChecks, report.UnhealthyChecks)

	for _, check := range report.Checks {
		fmt.Printf("  - %s: %s (%v) - %s\n",
			check.Name, check.Status, check.Duration, check.Message)
		if check.Error != "" {
			fmt.Printf("    Error: %s\n", check.Error)
		}
	}

	// Readiness and liveness checks
	fmt.Printf("Is Ready: %v\n", checker.IsReady(ctx))
	fmt.Printf("Is Healthy: %v\n", checker.IsHealthy(ctx))
}

// Example 2: Kubernetes Health Probes
func exampleKubernetesHealthProbes() {
	fmt.Println("\n=== Example 2: Kubernetes Health Probes ===")

	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("k8s-health-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	checker := franzgo.NewHealthChecker(client, nil)
	k8sHandler := franzgo.NewKubernetesHealthHandler(checker)

	// Setup HTTP endpoints for Kubernetes probes
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		alive, message := k8sHandler.LivenessProbe(ctx)

		if alive {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(message))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(message))
		}
	})

	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ready, message, report := k8sHandler.ReadinessProbe(ctx)

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

	fmt.Println("Health endpoints available at:")
	fmt.Println("  Liveness:  http://localhost:8080/healthz")
	fmt.Println("  Readiness: http://localhost:8080/readyz")

	// Start HTTP server in background
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Simulate running
	time.Sleep(5 * time.Second)
}

// Example 3: Health Monitoring
func exampleHealthMonitoring() {
	fmt.Println("\n=== Example 3: Health Monitoring ===")

	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("health-monitor-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	checker := franzgo.NewHealthChecker(client, nil)

	// Create health monitor
	monitor := franzgo.NewHealthMonitor(checker, 5*time.Second)

	// Set up callbacks for status changes
	monitor.OnHealthy(func(report *franzgo.HealthReport) {
		fmt.Printf("✓ Service became HEALTHY at %s\n", report.Timestamp)
	})

	monitor.OnDegraded(func(report *franzgo.HealthReport) {
		fmt.Printf("⚠ Service became DEGRADED at %s\n", report.Timestamp)
		for _, check := range report.Checks {
			if check.Status == franzgo.HealthStatusDegraded {
				fmt.Printf("  - %s: %s\n", check.Name, check.Message)
			}
		}
	})

	monitor.OnUnhealthy(func(report *franzgo.HealthReport) {
		fmt.Printf("✗ Service became UNHEALTHY at %s\n", report.Timestamp)
		for _, check := range report.Checks {
			if check.Status == franzgo.HealthStatusUnhealthy {
				fmt.Printf("  - %s: %s\n", check.Name, check.Error)
			}
		}
	})

	// Start monitoring
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go monitor.Start(ctx)

	// Simulate running
	time.Sleep(30 * time.Second)
	monitor.Stop()

	fmt.Printf("Final status: %s\n", monitor.GetLastStatus())
}

// Example 4: Basic Graceful Shutdown
func exampleBasicGracefulShutdown() {
	fmt.Println("\n=== Example 4: Basic Graceful Shutdown ===")

	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("shutdown-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create shutdown manager
	shutdownConfig := franzgo.DefaultShutdownConfig()
	shutdownConfig.Timeout = 30 * time.Second
	shutdownConfig.OnShutdownStart = func() {
		fmt.Println("🛑 Shutdown initiated...")
	}
	shutdownConfig.OnShutdownComplete = func(err error) {
		if err != nil {
			fmt.Printf("⚠ Shutdown completed with errors: %v\n", err)
		} else {
			fmt.Println("✓ Shutdown completed successfully")
		}
	}
	shutdownConfig.OnProgress = func(stage string, duration time.Duration) {
		fmt.Printf("  - %s (took %v)\n", stage, duration)
	}

	manager := franzgo.NewShutdownManager(client, shutdownConfig)

	// Create producer and consumer
	producer := franzgo.NewProducer(client)
	consumer := franzgo.NewConsumer(client, nil)

	manager.SetProducer(producer)
	manager.SetConsumer(consumer)

	// Simulate some work
	ctx := context.Background()

	// Produce some messages
	for i := 0; i < 10; i++ {
		_ = producer.Produce(ctx, "test-topic", []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Simulate signal after 2 seconds
	time.Sleep(2 * time.Second)
	fmt.Println("Simulating shutdown signal...")

	// Perform graceful shutdown
	if err := manager.Shutdown(ctx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}

// Example 5: Shutdown with Signal Handling
func exampleShutdownWithSignals() {
	fmt.Println("\n=== Example 5: Shutdown with Signal Handling ===")

	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("signal-shutdown-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create shutdown manager
	shutdownConfig := franzgo.DefaultShutdownConfig()
	shutdownConfig.Signals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}

	manager := franzgo.NewShutdownManager(client, shutdownConfig)

	// Setup producer and consumer
	producer := franzgo.NewProducer(client)
	consumer := franzgo.NewConsumer(client, nil)

	manager.SetProducer(producer)
	manager.SetConsumer(consumer)

	// Start consumer in background
	go func() {
		handler := func(ctx context.Context, record *kgo.Record) error {
			fmt.Printf("Processing: %s = %s\n", record.Key, record.Value)
			time.Sleep(100 * time.Millisecond) // Simulate work
			return nil
		}

		ctx := context.Background()
		_ = consumer.Consume(ctx, []string{"test-topic"}, handler)
	}()

	// Wait for shutdown signal
	fmt.Println("Service running. Press Ctrl+C to shutdown...")
	sig := manager.WaitForSignal()
	fmt.Printf("Received signal: %v\n", sig)

	// Graceful shutdown
	ctx := context.Background()
	if err := manager.Shutdown(ctx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}

// Example 6: Shutdown-Aware Consumer
func exampleShutdownAwareConsumer() {
	fmt.Println("\n=== Example 6: Shutdown-Aware Consumer ===")

	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("shutdown-aware-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	manager := franzgo.NewShutdownManager(client, nil)

	// Create shutdown-aware consumer
	consumer := franzgo.NewConsumer(client, nil)
	shutdownAwareConsumer := franzgo.NewShutdownAwareConsumer(consumer, manager)

	// Handler that tracks work
	handler := func(ctx context.Context, record *kgo.Record) error {
		fmt.Printf("Processing: %s\n", record.Key)
		time.Sleep(500 * time.Millisecond) // Simulate work

		// Check if shutdown was initiated during processing
		if manager.IsShuttingDown() {
			fmt.Println("Shutdown detected, finishing current message...")
		}

		return nil
	}

	// Start consumer
	go func() {
		ctx := context.Background()
		if err := shutdownAwareConsumer.Consume(ctx, []string{"test-topic"}, handler); err != nil {
			fmt.Printf("Consumer stopped: %v\n", err)
		}
	}()

	// Simulate running for a bit
	time.Sleep(3 * time.Second)

	// Initiate shutdown
	ctx := context.Background()
	fmt.Println("Initiating shutdown...")
	if err := manager.Shutdown(ctx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}
}

// Example 7: Graceful Application
func exampleGracefulApplication() {
	fmt.Println("\n=== Example 7: Graceful Application ===")

	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("graceful-app-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create graceful application
	app := franzgo.NewGracefulApplication(client, nil, nil)

	// Register startup callbacks
	app.OnStart(func(ctx context.Context) error {
		fmt.Println("🚀 Application starting...")

		// Initialize resources
		producer := franzgo.NewProducer(client)
		consumer := franzgo.NewConsumer(client, nil)

		// Setup shutdown
		app.GetShutdownManager().SetProducer(producer)
		app.GetShutdownManager().SetConsumer(consumer)

		// Start background tasks
		go func() {
			// Example background task
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-app.GetShutdownManager().ShutdownChan():
					fmt.Println("Background task stopping...")
					return
				case <-ticker.C:
					fmt.Println("Background task tick")
				}
			}
		}()

		return nil
	})

	// Register shutdown callbacks
	app.OnStop(func(ctx context.Context) error {
		fmt.Println("🛑 Application stopping...")
		// Cleanup resources
		return nil
	})

	// Run application
	ctx := context.Background()
	if err := app.Run(ctx); err != nil {
		log.Printf("Application error: %v", err)
	}
}

// Example 8: Health Checks with Shutdown
func exampleHealthChecksWithShutdown() {
	fmt.Println("\n=== Example 8: Health Checks with Shutdown ===")

	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("health-shutdown-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create graceful application with both health checks and shutdown
	app := franzgo.NewGracefulApplication(client, nil, nil)

	// Setup health endpoints
	healthChecker := app.GetHealthChecker()
	k8sHandler := franzgo.NewKubernetesHealthHandler(healthChecker)

	http.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		alive, message := k8sHandler.LivenessProbe(r.Context())
		if alive {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(message))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(message))
		}
	})

	http.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ready, message, _ := k8sHandler.ReadinessProbe(r.Context())
		if ready {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(message))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(message))
		}
	})

	// Start HTTP server
	go func() {
		fmt.Println("Health endpoints at http://localhost:8080/health/{live,ready}")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Application startup
	app.OnStart(func(ctx context.Context) error {
		fmt.Println("Application started")
		return nil
	})

	// Application shutdown
	app.OnStop(func(ctx context.Context) error {
		fmt.Println("Application stopped")
		return nil
	})

	// Run application
	ctx := context.Background()
	if err := app.Run(ctx); err != nil {
		log.Printf("Application error: %v", err)
	}
}

// Example 9: Production-Ready Service
func exampleProductionReadyService() {
	fmt.Println("\n=== Example 9: Production-Ready Service ===")

	// Configuration from environment
	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("prod-service").
		WithClientID("prod-service-1").
		WithProducerBatchSize(16384).
		WithProducerLinger(10 * time.Millisecond).
		WithConsumerSessionTimeout(45 * time.Second).
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Shutdown configuration
	shutdownConfig := &franzgo.ShutdownConfig{
		Timeout:       30 * time.Second,
		DrainTimeout:  10 * time.Second,
		FlushTimeout:  5 * time.Second,
		CommitTimeout: 5 * time.Second,
		CommitOffsets: true,
		FlushProducer: true,
		Signals:       []os.Signal{syscall.SIGINT, syscall.SIGTERM},
		OnShutdownStart: func() {
			log.Println("Initiating graceful shutdown...")
		},
		OnShutdownComplete: func(err error) {
			if err != nil {
				log.Printf("Shutdown completed with errors: %v", err)
			} else {
				log.Println("Shutdown completed successfully")
			}
		},
		OnProgress: func(stage string, duration time.Duration) {
			log.Printf("Shutdown stage: %s (took %v)", stage, duration)
		},
	}

	// Health check configuration
	healthConfig := &franzgo.HealthConfig{
		CheckTimeout:                 5 * time.Second,
		CacheTimeout:                 10 * time.Second,
		ConsumerLagWarningThreshold:  1000,
		ConsumerLagCriticalThreshold: 10000,
		BrokerConnectionTimeout:      3 * time.Second,
	}

	// Create graceful application
	app := franzgo.NewGracefulApplication(client, shutdownConfig, healthConfig)

	// Setup health monitoring
	monitor := franzgo.NewHealthMonitor(app.GetHealthChecker(), 30*time.Second)
	monitor.OnDegraded(func(report *franzgo.HealthReport) {
		log.Printf("WARNING: Service degraded - %d checks unhealthy", report.DegradedChecks)
	})
	monitor.OnUnhealthy(func(report *franzgo.HealthReport) {
		log.Printf("CRITICAL: Service unhealthy - %d checks failed", report.UnhealthyChecks)
	})

	// Setup HTTP endpoints
	setupHTTPEndpoints(app)

	// Application startup
	app.OnStart(func(ctx context.Context) error {
		log.Println("Starting production service...")

		// Start health monitoring
		go monitor.Start(ctx)

		// Initialize business logic
		producer := franzgo.NewProducer(client)
		consumer := franzgo.NewConsumer(client, nil)

		app.GetShutdownManager().SetProducer(producer)
		app.GetShutdownManager().SetConsumer(consumer)

		// Start consumer with middleware
		go func() {
			middleware := franzgo.Chain(
				franzgo.LoggingMiddleware(),
				franzgo.MetricsMiddleware(franzgo.NewMessageMetrics()),
				franzgo.TimeoutMiddleware(30*time.Second),
				franzgo.RecoveryMiddleware(),
				franzgo.ShutdownMiddleware(app.GetShutdownManager()),
			)

			handler := middleware(func(ctx context.Context, record *kgo.Record) error {
				// Business logic here
				log.Printf("Processing message: %s", record.Key)
				return nil
			})

			if err := consumer.Consume(ctx, []string{"orders"}, handler); err != nil {
				log.Printf("Consumer error: %v", err)
			}
		}()

		log.Println("Service started successfully")
		return nil
	})

	// Application shutdown
	app.OnStop(func(ctx context.Context) error {
		log.Println("Cleaning up resources...")
		monitor.Stop()
		return nil
	})

	// Run application
	ctx := context.Background()
	if err := app.Run(ctx); err != nil {
		log.Fatalf("Application error: %v", err)
	}
}

// Helper function to setup HTTP endpoints
func setupHTTPEndpoints(app *franzgo.GracefulApplication) {
	healthChecker := app.GetHealthChecker()
	k8sHandler := franzgo.NewKubernetesHealthHandler(healthChecker)

	// Liveness probe
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		alive, message := k8sHandler.LivenessProbe(r.Context())
		if alive {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "alive", "message": message})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "dead", "message": message})
		}
	})

	// Readiness probe
	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
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

	// Full health report
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

	// Startup endpoint
	go func() {
		log.Println("HTTP server listening on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()
}

func main() {
	// Run examples (comment out the ones you don't want to run)

	// Phase 5 Features
	// exampleHealthChecks()
	// exampleKubernetesHealthProbes()
	// exampleHealthMonitoring()
	// exampleBasicGracefulShutdown()
	// exampleShutdownWithSignals()
	// exampleShutdownAwareConsumer()
	// exampleGracefulApplication()
	// exampleHealthChecksWithShutdown()
	exampleProductionReadyService()

	// Keep main running for signal examples
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
