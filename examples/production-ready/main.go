package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"
)

// This example demonstrates all 5 production-ready features:
// 1. Health Check Endpoints
// 2. Structured Logging
// 3. Configuration Validation
// 4. Graceful Shutdown
// 5. Message Metadata Helpers

func main() {
	// ===========================================================================
	// 1. Structured Logging
	// ===========================================================================
	logger := kafka.NewStructuredLogger(slog.LevelInfo)
	logger.Info("Starting production-ready Kafka service", nil)

	// Contextual logger with service information
	contextLogger := kafka.NewContextualLogger(slog.LevelInfo, kafka.LogContext{
		ServiceName: "order-processor",
		ServiceID:   "instance-001",
		Environment: "production",
		Version:     "1.0.0",
	})

	// ===========================================================================
	// 2. Configuration with Validation
	// ===========================================================================
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"production-consumer-group",
	)
	config.Logger = logger

	// Validate configuration for production
	contextLogger.Info("Validating configuration", nil)
	validationResult := config.ValidateForEnvironment("production")

	if validationResult.HasWarnings() {
		for _, warning := range validationResult.Warnings {
			contextLogger.Info("Configuration warning", map[string]interface{}{
				"warning": warning,
			})
		}
	}

	if !validationResult.Valid {
		log.Fatalf("Configuration validation failed: %s", validationResult)
	}

	contextLogger.Info("Configuration validated successfully", nil)

	// Create client
	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// ===========================================================================
	// 3. Health Check Endpoints
	// ===========================================================================
	contextLogger.Info("Starting health check server", map[string]interface{}{
		"port": "8081",
	})

	go func() {
		mux := http.NewServeMux()

		// Health endpoints
		mux.HandleFunc("/health", kafka.HealthHandler(client))
		mux.HandleFunc("/health/ready", kafka.ReadinessHandler(client))
		mux.HandleFunc("/health/live", kafka.LivenessHandler(client))

		// Metrics endpoint (placeholder)
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintf(w, "# HELP kafka_messages_processed Total messages processed\n")
			fmt.Fprintf(w, "kafka_messages_processed 12345\n")
		})

		if err := http.ListenAndServe(":8081", mux); err != nil {
			log.Printf("Health server error: %v", err)
		}
	}()

	// ===========================================================================
	// 4. Graceful Shutdown
	// ===========================================================================
	shutdown := kafka.NewGracefulShutdown(client, 30*time.Second)

	// Register shutdown callbacks
	shutdown.OnShutdown(func() error {
		contextLogger.Info("Flushing metrics", nil)
		// Flush metrics here
		return nil
	})

	shutdown.OnShutdown(func() error {
		contextLogger.Info("Saving processor state", nil)
		// Save state here
		return nil
	})

	// ===========================================================================
	// 5. Message Metadata Helpers
	// ===========================================================================
	metadataHelper := kafka.NewMessageHelper()

	// Start producer
	go runProducer(client, metadataHelper, contextLogger)

	// Start consumer
	go runConsumer(client, metadataHelper, contextLogger)

	// ===========================================================================
	// Wait for shutdown signal
	// ===========================================================================
	contextLogger.Info("Service is ready", map[string]interface{}{
		"health_endpoint": "http://localhost:8081/health",
	})

	shutdown.Wait()
	contextLogger.Info("Service stopped", nil)
}

// runProducer demonstrates message production with metadata
func runProducer(client *kafka.Client, helper *kafka.MessageHelper, logger *kafka.ContextualLogger) {
	time.Sleep(2 * time.Second) // Wait for consumer

	producer := kafka.NewProducer(client)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		// Create order event
		order := kafka.OrderEvent{
			OrderID:    fmt.Sprintf("ORD-%d", i),
			CustomerID: fmt.Sprintf("CUST-%d", i),
			Status:     "pending",
			Total:      float64(100 * i),
		}

		// Create message
		msg, _ := producer.(*kafka.Producer).createMessage(order)

		// Add rich metadata using helper
		correlationID := helper.GenerateCorrelationID()
		helper.SetCorrelationID(msg, correlationID)
		helper.SetTraceContext(msg, "trace-"+correlationID, "span-"+fmt.Sprintf("%d", i))
		helper.SetUserID(msg, fmt.Sprintf("user-%d", i))
		helper.SetSessionID(msg, fmt.Sprintf("session-%d", i))
		helper.SetSource(msg, "order-api")
		helper.SetSourceVersion(msg, "1.0.0")
		helper.SetPriority(msg, i%10)
		helper.SetContentType(msg, "application/json")

		// Add custom headers
		helper.SetHeaders(msg, map[string]string{
			"request_id": fmt.Sprintf("req-%d", i),
			"region":     "us-west-2",
		})

		// Publish
		if err := client.Publish(ctx, "orders.production", msg); err != nil {
			logger.Error("Failed to publish", err, map[string]interface{}{
				"order_id":       order.OrderID,
				"correlation_id": correlationID,
			})
		} else {
			logger.Info("Order published", map[string]interface{}{
				"order_id":       order.OrderID,
				"correlation_id": correlationID,
				"user_id":        helper.GetUserID(msg),
				"priority":       i % 10,
			})
		}

		time.Sleep(1 * time.Second)
	}
}

// runConsumer demonstrates message consumption with metadata
func runConsumer(client *kafka.Client, helper *kafka.MessageHelper, logger *kafka.ContextualLogger) {
	consumer := kafka.NewConsumer(client)
	ctx := context.Background()

	err := consumer.Subscribe(ctx, "orders.production", func(ctx context.Context, msg *message.Message) error {
		// Extract metadata
		correlationID := helper.GetCorrelationID(msg)
		traceID, spanID := helper.GetTraceContext(msg)
		userID := helper.GetUserID(msg)
		source := helper.GetSource(msg)
		priority, _ := helper.GetPriority(msg)
		age, _ := helper.GetAge(msg)
		headers, _ := helper.GetHeaders(msg)

		// Create correlated logger
		correlatedLogger := logger.WithCorrelation(correlationID).WithTrace(traceID, spanID)

		// Log with full context
		correlatedLogger.Info("Processing order", map[string]interface{}{
			"message_id": msg.UUID,
			"user_id":    userID,
			"source":     source,
			"priority":   priority,
			"age_ms":     age.Milliseconds(),
			"headers":    headers,
		})

		// Decode order
		var order kafka.OrderEvent
		if err := consumer.UnmarshalMessage(msg, &order); err != nil {
			helper.SetLastError(msg, err)
			correlatedLogger.Error("Failed to unmarshal", err, nil)
			return err
		}

		// Process order
		processingStart := time.Now()
		time.Sleep(100 * time.Millisecond) // Simulate processing

		// Set processing metadata
		helper.SetProcessedAt(msg, time.Now())

		processingTime := time.Since(processingStart)

		correlatedLogger.Info("Order processed successfully", map[string]interface{}{
			"order_id":        order.OrderID,
			"customer_id":     order.CustomerID,
			"total":           order.Total,
			"processing_ms":   processingTime.Milliseconds(),
		})

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
}
