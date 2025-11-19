package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"gitlab.cept.gov.in/it2.0common/watermill/pkg/kafka"
)

func main() {
	// Create logger
	logger := watermill.NewStdLogger(true, true)

	// Create configuration with DLQ enabled
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"dlq-example-group",
	)
	config.Logger = logger
	config.DLQ.Enabled = true
	config.DLQ.Topic = "ecommerce-dlq"
	config.DLQ.MaxRetries = 3
	config.DLQ.RetryDelay = 2 * time.Second
	config.DLQ.ExponentialBackoff = true

	// Create Kafka client
	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start DLQ consumer in background
	go startDLQConsumer(ctx, client, logger)

	// Start regular consumer with failing handler
	go startFailingConsumer(ctx, client, logger)

	// Simulate publishing messages that will fail
	go simulateFailingMessages(ctx, client)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	cancel()
	time.Sleep(1 * time.Second)
}

// startFailingConsumer starts a consumer that intentionally fails some messages
func startFailingConsumer(ctx context.Context, client *kafka.Client, logger watermill.LoggerAdapter) {
	router, err := client.CreateRouter()
	if err != nil {
		log.Fatalf("Failed to create router: %v", err)
	}

	// Add handler that fails for specific orders
	router.AddNoPublisherHandler(
		"failing_order_handler",
		"orders.test",
		client.GetSubscriber(),
		func(msg *message.Message) error {
			var order kafka.OrderEvent
			if err := json.Unmarshal(msg.Payload, &order); err != nil {
				return err
			}

			log.Printf("Processing order: %s", order.OrderID)

			// Simulate failure for orders with "FAIL" prefix
			if len(order.OrderID) >= 4 && order.OrderID[:4] == "FAIL" {
				return errors.New("simulated processing error")
			}

			// Simulate random transient errors
			if order.Total > 1000 {
				return errors.New("amount too high - validation error")
			}

			log.Printf("Successfully processed order: %s", order.OrderID)
			return nil
		},
	)

	if err := router.Run(ctx); err != nil {
		log.Printf("Router error: %v", err)
	}
}

// startDLQConsumer starts a consumer for the DLQ
func startDLQConsumer(ctx context.Context, client *kafka.Client, logger watermill.LoggerAdapter) {
	time.Sleep(1 * time.Second) // Wait for client to be ready

	dlqConsumer := kafka.NewDLQConsumer(client)

	err := dlqConsumer.Subscribe(ctx, func(ctx context.Context, dlqMsg kafka.DLQMessage) error {
		log.Printf("DLQ Message received:")
		log.Printf("  Original Topic: %s", dlqMsg.OriginalTopic)
		log.Printf("  Error: %s", dlqMsg.Error)
		log.Printf("  Retry Count: %d", dlqMsg.RetryCount)
		log.Printf("  First Failed: %s", dlqMsg.FirstFailedAt.Format(time.RFC3339))
		log.Printf("  Last Failed: %s", dlqMsg.LastFailedAt.Format(time.RFC3339))

		// Decide what to do with failed message
		// Option 1: Log and archive
		if err := dlqConsumer.Archive(ctx, dlqMsg, "failed-orders-archive"); err != nil {
			log.Printf("Failed to archive message: %v", err)
		}

		// Option 2: Retry if it's a transient error
		// if shouldRetry(dlqMsg) {
		//     if err := dlqConsumer.Retry(ctx, dlqMsg); err != nil {
		//         log.Printf("Failed to retry message: %v", err)
		//     }
		// }

		// Option 3: Send alert to operations team
		// alertOps(dlqMsg)

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe to DLQ: %v", err)
	}
}

// simulateFailingMessages publishes messages that will fail
func simulateFailingMessages(ctx context.Context, client *kafka.Client) {
	time.Sleep(2 * time.Second) // Wait for consumers to be ready

	producer := kafka.NewEcommerceProducer(client)

	orders := []kafka.OrderEvent{
		{
			OrderID:    "SUCCESS-001",
			CustomerID: "CUST-1",
			Status:     "pending",
			Total:      99.99,
		},
		{
			OrderID:    "FAIL-001", // This will fail
			CustomerID: "CUST-2",
			Status:     "pending",
			Total:      149.99,
		},
		{
			OrderID:    "SUCCESS-002",
			CustomerID: "CUST-3",
			Status:     "pending",
			Total:      49.99,
		},
		{
			OrderID:    "FAIL-002", // This will fail
			CustomerID: "CUST-4",
			Status:     "pending",
			Total:      199.99,
		},
		{
			OrderID:    "VALIDATION-FAIL",
			CustomerID: "CUST-5",
			Status:     "pending",
			Total:      1500.00, // This will fail validation
		},
	}

	for _, order := range orders {
		if err := producer.PublishEvent(ctx, "orders.test", order); err != nil {
			log.Printf("Failed to publish order: %v", err)
		} else {
			log.Printf("Published order: %s", order.OrderID)
		}
		time.Sleep(1 * time.Second)
	}
}

// Helper function to determine if a message should be retried
func shouldRetry(dlqMsg kafka.DLQMessage) bool {
	// Retry transient errors, but not validation errors
	return dlqMsg.RetryCount < 5 &&
		dlqMsg.Error != "amount too high - validation error"
}
