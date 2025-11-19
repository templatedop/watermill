// Franz-go DLQ (Dead Letter Queue) Example
// Demonstrates error handling with DLQ pattern using franz-go

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.cept.gov.in/it-2.0-common/watermill/pkg/franzgo"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OrderEvent represents an order event
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Status     string    `json:"status"`
	Total      float64   `json:"total"`
	Timestamp  time.Time `json:"timestamp"`
}

// DLQMessage represents a message in the dead letter queue
type DLQMessage struct {
	OriginalTopic  string    `json:"original_topic"`
	OriginalKey    string    `json:"original_key"`
	OriginalValue  []byte    `json:"original_value"`
	Error          string    `json:"error"`
	RetryCount     int       `json:"retry_count"`
	FirstFailedAt  time.Time `json:"first_failed_at"`
	LastFailedAt   time.Time `json:"last_failed_at"`
	FailureReason  string    `json:"failure_reason"`
}

var (
	// Track retry attempts for each message
	retryAttempts = make(map[string]int)
	maxRetries    = 3
)

func main() {
	log.Println("🚀 Starting Franz-go DLQ Example...")

	// Get Kafka brokers from environment or use default
	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	// Create franz-go configuration
	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("dlq-example-group").
		WithClientID("dlq-example").
		WithAutoOffsetReset(franzgo.OffsetEarliest).
		Build()

	// Create client
	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	log.Println("✅ Franz-go client created successfully")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start DLQ consumer in background
	log.Println("📥 Starting DLQ consumer...")
	go startDLQConsumer(ctx, client)

	// Give DLQ consumer time to start
	time.Sleep(1 * time.Second)

	// Start main consumer with failing handler
	log.Println("📥 Starting main consumer with failure simulation...")
	go startFailingConsumer(ctx, client)

	// Give consumers time to start
	time.Sleep(2 * time.Second)

	// Simulate publishing messages that will fail
	log.Println("📤 Publishing test messages...")
	go simulateFailingMessages(ctx, client)

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	log.Println("✅ DLQ example running. Press Ctrl+C to stop")
	<-sigChan

	log.Println("🛑 Shutting down...")
	cancel()
	time.Sleep(2 * time.Second)
	log.Println("✅ Shutdown complete")
}

// startFailingConsumer starts a consumer that intentionally fails some messages
func startFailingConsumer(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)

	err := consumer.Consume(ctx, []string{"orders.test"}, func(record *kgo.Record) error {
		var order OrderEvent
		if err := json.Unmarshal(record.Value, &order); err != nil {
			log.Printf("❌ Failed to unmarshal order: %v", err)
			return sendToDLQ(ctx, client, record, err, "unmarshal_error")
		}

		log.Printf("📦 Processing order: %s (total: $%.2f)", order.OrderID, order.Total)

		// Simulate failure for orders with "FAIL" prefix
		if len(order.OrderID) >= 4 && order.OrderID[:4] == "FAIL" {
			err := errors.New("simulated processing error")
			log.Printf("❌ Order %s failed: %v", order.OrderID, err)
			return sendToDLQ(ctx, client, record, err, "processing_error")
		}

		// Simulate validation error for high-value orders
		if order.Total > 1000 {
			err := errors.New("amount too high - validation error")
			log.Printf("❌ Order %s failed validation: %v", order.OrderID, err)
			return sendToDLQ(ctx, client, record, err, "validation_error")
		}

		// Simulate transient errors (can be retried)
		if order.OrderID == "TRANSIENT-ERROR" {
			messageKey := string(record.Key)
			retryAttempts[messageKey]++

			if retryAttempts[messageKey] <= maxRetries {
				err := fmt.Errorf("transient error (attempt %d/%d)", retryAttempts[messageKey], maxRetries)
				log.Printf("⚠️  Order %s encountered transient error: %v", order.OrderID, err)
				return err // This will cause a redelivery
			}

			// Max retries exceeded, send to DLQ
			err := errors.New("max retries exceeded for transient error")
			log.Printf("❌ Order %s exceeded max retries: %v", order.OrderID, err)
			return sendToDLQ(ctx, client, record, err, "max_retries_exceeded")
		}

		log.Printf("✅ Successfully processed order: %s", order.OrderID)
		return nil
	})

	if err != nil {
		log.Printf("❌ Consumer error: %v", err)
	}
}

// sendToDLQ sends a failed message to the dead letter queue
func sendToDLQ(ctx context.Context, client *franzgo.Client, record *kgo.Record, err error, reason string) error {
	messageKey := string(record.Key)

	// Track retry count
	if _, exists := retryAttempts[messageKey]; !exists {
		retryAttempts[messageKey] = 0
	}
	retryAttempts[messageKey]++

	dlqMsg := DLQMessage{
		OriginalTopic:  record.Topic,
		OriginalKey:    string(record.Key),
		OriginalValue:  record.Value,
		Error:          err.Error(),
		RetryCount:     retryAttempts[messageKey],
		FirstFailedAt:  time.Now(), // In production, you'd track this properly
		LastFailedAt:   time.Now(),
		FailureReason:  reason,
	}

	data, marshalErr := json.Marshal(dlqMsg)
	if marshalErr != nil {
		log.Printf("❌ Failed to marshal DLQ message: %v", marshalErr)
		return marshalErr
	}

	// Send to DLQ topic
	producer := franzgo.NewProducer(client)
	if err := producer.Produce(ctx, "ecommerce-dlq", record.Key, data); err != nil {
		log.Printf("❌ Failed to send message to DLQ: %v", err)
		return err
	}

	log.Printf("📮 Message sent to DLQ: topic=%s, key=%s, reason=%s, retries=%d",
		record.Topic, string(record.Key), reason, retryAttempts[messageKey])

	// Return nil to acknowledge the message (it's been sent to DLQ)
	return nil
}

// startDLQConsumer starts a consumer for the DLQ
func startDLQConsumer(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)

	log.Println("👂 DLQ consumer listening...")

	err := consumer.Consume(ctx, []string{"ecommerce-dlq"}, func(record *kgo.Record) error {
		var dlqMsg DLQMessage
		if err := json.Unmarshal(record.Value, &dlqMsg); err != nil {
			log.Printf("❌ Failed to unmarshal DLQ message: %v", err)
			return err
		}

		log.Println("\n" + repeat("=", 80))
		log.Println("🚨 DLQ Message Received:")
		log.Printf("  Original Topic: %s", dlqMsg.OriginalTopic)
		log.Printf("  Original Key: %s", dlqMsg.OriginalKey)
		log.Printf("  Error: %s", dlqMsg.Error)
		log.Printf("  Failure Reason: %s", dlqMsg.FailureReason)
		log.Printf("  Retry Count: %d", dlqMsg.RetryCount)
		log.Printf("  First Failed: %s", dlqMsg.FirstFailedAt.Format(time.RFC3339))
		log.Printf("  Last Failed: %s", dlqMsg.LastFailedAt.Format(time.RFC3339))

		// Parse original message for logging
		var order OrderEvent
		if err := json.Unmarshal(dlqMsg.OriginalValue, &order); err == nil {
			log.Printf("  Original Order ID: %s", order.OrderID)
			log.Printf("  Original Customer ID: %s", order.CustomerID)
			log.Printf("  Original Total: $%.2f", order.Total)
		}
		log.Println(repeat("=", 80) + "\n")

		// Handle DLQ message based on failure reason
		switch dlqMsg.FailureReason {
		case "validation_error":
			log.Println("  Action: Logging validation error and archiving")
			// In production: Archive to long-term storage, send alert

		case "processing_error":
			log.Println("  Action: Logging processing error for investigation")
			// In production: Create ticket, alert ops team

		case "max_retries_exceeded":
			log.Println("  Action: Max retries exceeded, needs manual intervention")
			// In production: Alert on-call engineer

		case "unmarshal_error":
			log.Println("  Action: Invalid message format, archiving")
			// In production: Log for data quality investigation

		default:
			log.Println("  Action: Unknown error type, logging for investigation")
		}

		// Options for handling DLQ messages:
		// 1. Archive to separate topic for later analysis
		// 2. Send to monitoring/alerting system
		// 3. Store in database for manual review
		// 4. Attempt reprocessing after fixing the issue

		return nil
	})

	if err != nil {
		log.Printf("❌ DLQ consumer error: %v", err)
	}
}

// simulateFailingMessages publishes messages that will succeed or fail
func simulateFailingMessages(ctx context.Context, client *franzgo.Client) {
	producer := franzgo.NewProducer(client)

	orders := []OrderEvent{
		{
			OrderID:    "SUCCESS-001",
			CustomerID: "CUST-1",
			Status:     "pending",
			Total:      99.99,
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "FAIL-001", // This will fail with processing error
			CustomerID: "CUST-2",
			Status:     "pending",
			Total:      149.99,
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "SUCCESS-002",
			CustomerID: "CUST-3",
			Status:     "pending",
			Total:      49.99,
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "FAIL-002", // This will fail with processing error
			CustomerID: "CUST-4",
			Status:     "pending",
			Total:      199.99,
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "VALIDATION-FAIL",
			CustomerID: "CUST-5",
			Status:     "pending",
			Total:      1500.00, // This will fail validation
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "TRANSIENT-ERROR",
			CustomerID: "CUST-6",
			Status:     "pending",
			Total:      299.99,
			Timestamp:  time.Now(),
		},
	}

	for _, order := range orders {
		data, err := json.Marshal(order)
		if err != nil {
			log.Printf("❌ Failed to marshal order: %v", err)
			continue
		}

		if err := producer.Produce(ctx, "orders.test", []byte(order.OrderID), data); err != nil {
			log.Printf("❌ Failed to publish order %s: %v", order.OrderID, err)
		} else {
			log.Printf("📤 Published order: %s", order.OrderID)
		}

		time.Sleep(1 * time.Second)
	}

	log.Println("✅ All test messages published")
}

// Helper function for string repetition (like Python's str * n)
func repeat(s string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += s
	}
	return result
}
