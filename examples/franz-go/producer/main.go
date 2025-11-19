// Franz-go Producer Example
// Demonstrates producing messages to Kafka topics with franz-go

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"gitlab.cept.gov.in/it-2.0-common/watermill/pkg/franzgo"
)

// OrderItem represents an item in an order
type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// OrderEvent represents an order event
type OrderEvent struct {
	OrderID    string                 `json:"order_id"`
	CustomerID string                 `json:"customer_id"`
	Status     string                 `json:"status"`
	Items      []OrderItem            `json:"items,omitempty"`
	Total      float64                `json:"total"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Timestamp  time.Time              `json:"timestamp"`
}

// PaymentEvent represents a payment event
type PaymentEvent struct {
	PaymentID string    `json:"payment_id"`
	OrderID   string    `json:"order_id"`
	Amount    float64   `json:"amount"`
	Method    string    `json:"method"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// InventoryEvent represents an inventory event
type InventoryEvent struct {
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	log.Println("🚀 Starting Franz-go Producer Example...")

	// Get Kafka brokers from environment or use default
	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	// Create franz-go configuration with optimized producer settings
	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithClientID("franz-go-producer").
		WithProducerBatchSize(16384).          // 16KB batch size
		WithProducerLinger(10 * time.Millisecond). // Wait up to 10ms to batch messages
		WithCompression(franzgo.CompressionZstd).  // Use Zstandard compression
		WithProducerAcks(franzgo.AllISRAcks).      // Wait for all in-sync replicas
		Build()

	// Create franz-go client
	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	log.Println("✅ Franz-go client created successfully")

	// Create producer
	producer := franzgo.NewProducer(client)
	ctx := context.Background()

	// Example 1: Publish simple order events
	log.Println("\n=== Example 1: Publishing Order Events ===")
	publishOrderEvents(ctx, producer)

	// Example 2: Publish batch events
	log.Println("\n=== Example 2: Publishing Batch Events ===")
	publishBatchEvents(ctx, producer)

	// Example 3: Publish with partition key (for ordering)
	log.Println("\n=== Example 3: Publishing with Partition Keys ===")
	publishWithPartitionKey(ctx, producer)

	// Example 4: Publish different event types
	log.Println("\n=== Example 4: Publishing Different Event Types ===")
	publishDifferentEvents(ctx, producer)

	// Flush any pending messages
	if err := producer.Flush(ctx); err != nil {
		log.Fatalf("❌ Failed to flush producer: %v", err)
	}

	log.Println("\n✅ All events published successfully!")
}

func publishOrderEvents(ctx context.Context, producer *franzgo.Producer) {
	order := OrderEvent{
		OrderID:    "ORD-12345",
		CustomerID: "CUST-67890",
		Status:     "pending",
		Items: []OrderItem{
			{
				ProductID: "PROD-1",
				Quantity:  2,
				Price:     29.99,
			},
			{
				ProductID: "PROD-2",
				Quantity:  1,
				Price:     99.99,
			},
		},
		Total: 159.97,
		Metadata: map[string]interface{}{
			"source":    "web",
			"region":    "US-WEST",
			"campaign":  "summer-sale",
		},
		Timestamp: time.Now(),
	}

	// Marshal to JSON
	data, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("❌ Failed to marshal order: %v", err)
	}

	// Publish to orders.created topic
	if err := producer.Produce(ctx, "orders.created", []byte(order.OrderID), data); err != nil {
		log.Fatalf("❌ Failed to publish order: %v", err)
	}

	log.Printf("✅ Order %s published successfully (total: $%.2f)", order.OrderID, order.Total)
}

func publishBatchEvents(ctx context.Context, producer *franzgo.Producer) {
	orders := []OrderEvent{
		{
			OrderID:    "ORD-100",
			CustomerID: "CUST-1",
			Status:     "pending",
			Total:      49.99,
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "ORD-101",
			CustomerID: "CUST-2",
			Status:     "pending",
			Total:      79.99,
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "ORD-102",
			CustomerID: "CUST-3",
			Status:     "pending",
			Total:      129.99,
			Timestamp:  time.Now(),
		},
	}

	// Publish all orders in a batch
	for _, order := range orders {
		data, err := json.Marshal(order)
		if err != nil {
			log.Printf("❌ Failed to marshal order %s: %v", order.OrderID, err)
			continue
		}

		if err := producer.Produce(ctx, "orders.created", []byte(order.OrderID), data); err != nil {
			log.Printf("❌ Failed to publish order %s: %v", order.OrderID, err)
			continue
		}

		log.Printf("✅ Order %s queued for publishing (total: $%.2f)", order.OrderID, order.Total)
	}

	// Flush to ensure all messages are sent
	if err := producer.Flush(ctx); err != nil {
		log.Fatalf("❌ Failed to flush batch: %v", err)
	}

	log.Printf("✅ Batch of %d orders published successfully", len(orders))
}

func publishWithPartitionKey(ctx context.Context, producer *franzgo.Producer) {
	// All orders for the same customer will go to the same partition
	// This ensures ordering for per-customer processing
	customerID := "CUST-12345"

	log.Printf("📦 Publishing orders for customer %s (will go to same partition)", customerID)

	for i := 1; i <= 5; i++ {
		order := OrderEvent{
			OrderID:    fmt.Sprintf("ORD-%s-%d", customerID, i),
			CustomerID: customerID,
			Status:     "pending",
			Total:      float64(i) * 29.99,
			Timestamp:  time.Now(),
		}

		data, err := json.Marshal(order)
		if err != nil {
			log.Printf("❌ Failed to marshal order: %v", err)
			continue
		}

		// Use customerID as partition key to ensure ordering
		if err := producer.Produce(ctx, "orders.created", []byte(customerID), data); err != nil {
			log.Printf("❌ Failed to publish order: %v", err)
			continue
		}

		log.Printf("✅ Order %s published with partition key %s", order.OrderID, customerID)
		time.Sleep(100 * time.Millisecond) // Simulate processing delay
	}

	// Flush to ensure all messages are sent
	if err := producer.Flush(ctx); err != nil {
		log.Fatalf("❌ Failed to flush: %v", err)
	}
}

func publishDifferentEvents(ctx context.Context, producer *franzgo.Producer) {
	// Publish order event
	order := OrderEvent{
		OrderID:    "ORD-99999",
		CustomerID: "CUST-55555",
		Status:     "confirmed",
		Total:      199.99,
		Timestamp:  time.Now(),
	}

	orderData, _ := json.Marshal(order)
	if err := producer.Produce(ctx, "orders.confirmed", []byte(order.OrderID), orderData); err != nil {
		log.Printf("❌ Failed to publish order: %v", err)
	} else {
		log.Printf("✅ Order %s published to orders.confirmed", order.OrderID)
	}

	// Publish payment event
	payment := PaymentEvent{
		PaymentID: "PAY-11111",
		OrderID:   "ORD-99999",
		Amount:    199.99,
		Method:    "credit_card",
		Status:    "processed",
		Timestamp: time.Now(),
	}

	paymentData, _ := json.Marshal(payment)
	if err := producer.Produce(ctx, "payments.processed", []byte(payment.PaymentID), paymentData); err != nil {
		log.Printf("❌ Failed to publish payment: %v", err)
	} else {
		log.Printf("✅ Payment %s published to payments.processed", payment.PaymentID)
	}

	// Publish inventory event
	inventory := InventoryEvent{
		ProductID: "PROD-123",
		Quantity:  50,
		Action:    "reserved",
		Timestamp: time.Now(),
	}

	inventoryData, _ := json.Marshal(inventory)
	if err := producer.Produce(ctx, "inventory.reserved", []byte(inventory.ProductID), inventoryData); err != nil {
		log.Printf("❌ Failed to publish inventory: %v", err)
	} else {
		log.Printf("✅ Inventory event published for product %s", inventory.ProductID)
	}

	// Flush all pending messages
	if err := producer.Flush(ctx); err != nil {
		log.Fatalf("❌ Failed to flush: %v", err)
	}

	log.Println("✅ All different event types published successfully")
}
