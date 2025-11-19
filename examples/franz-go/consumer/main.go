// Franz-go Consumer Example
// Demonstrates consuming messages from multiple topics with franz-go

package main

import (
	"context"
	"encoding/json"
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
	Total      float64   `json:"total"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
}

// PaymentEvent represents a payment event
type PaymentEvent struct {
	PaymentID string    `json:"payment_id"`
	OrderID   string    `json:"order_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// InventoryEvent represents an inventory event
type InventoryEvent struct {
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Action    string    `json:"action"` // reserved, released
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	log.Println("🚀 Starting Franz-go Consumer Example...")

	// Get Kafka brokers from environment or use default
	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	// Create franz-go configuration
	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("franz-go-consumer-group").
		WithClientID("franz-go-consumer").
		WithAutoOffsetReset(franzgo.OffsetEarliest).
		WithConsumerSessionTimeout(30 * time.Second).
		WithConsumerRebalanceTimeout(60 * time.Second).
		Build()

	// Create franz-go client
	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	log.Println("✅ Franz-go client created successfully")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create consumer
	consumer := franzgo.NewConsumer(client)

	// Define topics to consume
	topics := []string{
		"orders.created",
		"orders.updated",
		"payments.processed",
		"payments.failed",
		"inventory.reserved",
		"inventory.released",
	}

	log.Printf("📥 Subscribing to topics: %v", topics)

	// Start consuming messages
	go func() {
		err := consumer.Consume(ctx, topics, func(record *kgo.Record) error {
			return handleMessage(record)
		})
		if err != nil {
			log.Printf("❌ Consumer error: %v", err)
			cancel()
		}
	}()

	log.Println("✅ Consumer started, waiting for messages...")
	log.Println("Press Ctrl+C to stop")

	// Wait for interrupt signal
	<-sigChan

	log.Println("🛑 Shutting down consumer...")
	cancel()
	time.Sleep(2 * time.Second) // Give time for graceful shutdown
	log.Println("✅ Consumer stopped")
}

// handleMessage routes messages to appropriate handlers based on topic
func handleMessage(record *kgo.Record) error {
	topic := record.Topic

	switch topic {
	case "orders.created":
		return handleOrderCreated(record)
	case "orders.updated":
		return handleOrderUpdated(record)
	case "payments.processed":
		return handlePaymentProcessed(record)
	case "payments.failed":
		return handlePaymentFailed(record)
	case "inventory.reserved":
		return handleInventoryReserved(record)
	case "inventory.released":
		return handleInventoryReleased(record)
	default:
		log.Printf("⚠️  Unknown topic: %s", topic)
		return nil
	}
}

// Order handlers
func handleOrderCreated(record *kgo.Record) error {
	var order OrderEvent
	if err := json.Unmarshal(record.Value, &order); err != nil {
		return fmt.Errorf("failed to unmarshal order: %w", err)
	}

	log.Printf("📦 Processing order: %s for customer %s, total: $%.2f",
		order.OrderID, order.CustomerID, order.Total)

	// Simulate business logic
	time.Sleep(100 * time.Millisecond)

	log.Printf("✅ Order %s processed successfully", order.OrderID)
	return nil
}

func handleOrderUpdated(record *kgo.Record) error {
	var order OrderEvent
	if err := json.Unmarshal(record.Value, &order); err != nil {
		return fmt.Errorf("failed to unmarshal order: %w", err)
	}

	log.Printf("🔄 Order updated: %s, new status: %s", order.OrderID, order.Status)

	// Simulate processing
	time.Sleep(50 * time.Millisecond)

	return nil
}

// Payment handlers
func handlePaymentProcessed(record *kgo.Record) error {
	var payment PaymentEvent
	if err := json.Unmarshal(record.Value, &payment); err != nil {
		return fmt.Errorf("failed to unmarshal payment: %w", err)
	}

	log.Printf("💳 Payment processed: %s for order %s, amount: $%.2f",
		payment.PaymentID, payment.OrderID, payment.Amount)

	// Trigger fulfillment process
	time.Sleep(50 * time.Millisecond)

	log.Printf("✅ Payment %s processed successfully", payment.PaymentID)
	return nil
}

func handlePaymentFailed(record *kgo.Record) error {
	var payment PaymentEvent
	if err := json.Unmarshal(record.Value, &payment); err != nil {
		return fmt.Errorf("failed to unmarshal payment: %w", err)
	}

	log.Printf("❌ Payment failed: %s for order %s", payment.PaymentID, payment.OrderID)

	// Handle failed payment (e.g., notify customer, cancel order)
	// In production, you might want to:
	// 1. Send notification to customer
	// 2. Cancel or hold the order
	// 3. Log to monitoring system

	return nil
}

// Inventory handlers
func handleInventoryReserved(record *kgo.Record) error {
	var inventory InventoryEvent
	if err := json.Unmarshal(record.Value, &inventory); err != nil {
		return fmt.Errorf("failed to unmarshal inventory: %w", err)
	}

	log.Printf("📦 Inventory reserved: %d units of product %s",
		inventory.Quantity, inventory.ProductID)

	// Update inventory database
	time.Sleep(30 * time.Millisecond)

	log.Printf("✅ Inventory reserved for product %s", inventory.ProductID)
	return nil
}

func handleInventoryReleased(record *kgo.Record) error {
	var inventory InventoryEvent
	if err := json.Unmarshal(record.Value, &inventory); err != nil {
		return fmt.Errorf("failed to unmarshal inventory: %w", err)
	}

	log.Printf("📤 Inventory released: %d units of product %s",
		inventory.Quantity, inventory.ProductID)

	// Update inventory database
	time.Sleep(30 * time.Millisecond)

	return nil
}
