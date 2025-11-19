// Franz-go Batch Consumer Example
// Demonstrates batch processing with franz-go

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

// OrderEvent represents an order
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Total      float64   `json:"total"`
	Timestamp  time.Time `json:"timestamp"`
}

func main() {
	log.Println("🚀 Starting Franz-go Batch Consumer Example...")

	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("batch-consumer-group").
		WithClientID("batch-consumer").
		WithFetchMaxBytes(1048576). // 1MB batches
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start batch consumer
	go runBatchConsumer(ctx, client)

	// Give consumer time to start
	time.Sleep(2 * time.Second)

	// Produce test messages
	go produceTestMessages(ctx, client)

	log.Println("✅ Batch consumer running. Press Ctrl+C to stop")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 Shutting down...")
	cancel()
	time.Sleep(1 * time.Second)
	log.Println("✅ Shutdown complete")
}

// runBatchConsumer demonstrates batch processing
func runBatchConsumer(ctx context.Context, client *franzgo.Client) {
	batchConsumer := franzgo.NewBatchConsumer(client, &franzgo.BatchConfig{
		MaxBatchSize:    50,                   // Process up to 50 messages at once
		BatchTimeout:    3 * time.Second,      // Or wait 3 seconds
		MaxWaitTime:     5 * time.Second,      // Max poll wait time
		ProcessTimeout:  30 * time.Second,     // Timeout for processing a batch
		ErrorStrategy:   franzgo.SkipOnError,  // Skip failed messages
		FlushOnShutdown: true,                 // Process remaining messages on shutdown
	})

	log.Println("📥 Batch consumer listening...")

	err := batchConsumer.ConsumeBatch(ctx, []string{"orders.batch"}, func(records []*kgo.Record) error {
		start := time.Now()
		log.Printf("\n" + repeat("=", 80))
		log.Printf("📦 Processing batch of %d messages", len(records))

		orders := make([]OrderEvent, 0, len(records))
		totalAmount := 0.0

		// Parse all messages in batch
		for _, record := range records {
			var order OrderEvent
			if err := json.Unmarshal(record.Value, &order); err != nil {
				log.Printf("⚠️  Failed to unmarshal order: %v", err)
				continue
			}

			orders = append(orders, order)
			totalAmount += order.Total
		}

		log.Printf("📊 Batch statistics:")
		log.Printf("  - Messages: %d", len(orders))
		log.Printf("  - Total amount: $%.2f", totalAmount)
		log.Printf("  - Average order: $%.2f", totalAmount/float64(len(orders)))

		// Simulate batch processing (e.g., bulk database insert)
		time.Sleep(200 * time.Millisecond)

		// Example: Bulk operations
		if err := processBulkInsert(orders); err != nil {
			return fmt.Errorf("bulk insert failed: %w", err)
		}

		duration := time.Since(start)
		log.Printf("✅ Batch processed successfully in %v", duration)
		log.Printf("  - Throughput: %.2f messages/sec", float64(len(orders))/duration.Seconds())
		log.Println(repeat("=", 80))

		return nil
	})

	if err != nil {
		log.Printf("❌ Batch consumer error: %v", err)
	}
}

// processBulkInsert simulates a bulk database insert
func processBulkInsert(orders []OrderEvent) error {
	// In a real application, this would be a bulk database insert
	// For example: INSERT INTO orders VALUES (...), (...), ...
	log.Printf("💾 Executing bulk insert for %d orders", len(orders))
	return nil
}

// produceTestMessages generates test messages
func produceTestMessages(ctx context.Context, client *franzgo.Client) {
	producer := franzgo.NewProducer(client)

	log.Println("📤 Producing test messages...")

	// Produce 100 messages to demonstrate batching
	for i := 1; i <= 100; i++ {
		order := OrderEvent{
			OrderID:    fmt.Sprintf("ORD-%d", i),
			CustomerID: fmt.Sprintf("CUST-%d", (i%10)+1),
			Total:      float64(50 + (i * 5)),
			Timestamp:  time.Now(),
		}

		data, _ := json.Marshal(order)
		if err := producer.Produce(ctx, "orders.batch", []byte(order.OrderID), data); err != nil {
			log.Printf("❌ Failed to produce: %v", err)
		}

		// Produce messages at different rates to demonstrate batching
		if i%20 == 0 {
			log.Printf("📤 Produced %d messages", i)
			// Flush to force batch processing
			producer.Flush(ctx)
			time.Sleep(4 * time.Second) // Wait for batch timeout
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}

	log.Println("✅ All test messages produced")
}

// repeat creates a string by repeating s n times
func repeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
