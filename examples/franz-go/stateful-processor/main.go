// Franz-go Stateful Processor Example
// Demonstrates stateful stream processing with franz-go

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

// OrderEvent represents an incoming order
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Total      float64   `json:"total"`
	Timestamp  time.Time `json:"timestamp"`
}

// CustomerStats represents aggregated customer statistics
type CustomerStats struct {
	CustomerID   string    `json:"customer_id"`
	TotalOrders  int       `json:"total_orders"`
	TotalSpent   float64   `json:"total_spent"`
	LastOrderAt  time.Time `json:"last_order_at"`
	AverageOrder float64   `json:"average_order"`
}

func main() {
	log.Println("🚀 Starting Franz-go Stateful Processor Example...")

	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	// Create configuration
	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("customer-stats-processor").
		WithClientID("stateful-processor").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	// Create stateful processor configuration
	statefulConfig := &franzgo.StatefulProcessorConfig{
		StorageFactory: func(partition int32) (franzgo.Storage, error) {
			return franzgo.NewMemoryStorage(), nil
		},
		ChangelogTopic:     "customer-stats-changelog",
		ChangelogCompacted: true,
		RecoveryTimeout:    5 * time.Minute,
	}

	// Create stateful processor
	processor, err := franzgo.NewStatefulProcessor(client, statefulConfig, processOrderAndUpdateStats)
	if err != nil {
		log.Fatalf("❌ Failed to create processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processor
	go func() {
		if err := processor.Process(ctx, []string{"orders.created"}); err != nil {
			log.Printf("❌ Processor error: %v", err)
		}
	}()

	// Give processor time to start
	time.Sleep(2 * time.Second)

	// Simulate orders
	go simulateOrders(ctx, client)

	log.Println("✅ Stateful processor running. Press Ctrl+C to stop")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 Shutting down...")
	cancel()
	processor.Close()
	log.Println("✅ Shutdown complete")
}

// processOrderAndUpdateStats processes orders and maintains customer statistics
func processOrderAndUpdateStats(ctx *franzgo.StateProcessorContext) error {
	// Parse incoming order
	var order OrderEvent
	if err := json.Unmarshal(ctx.Record().Value, &order); err != nil {
		return fmt.Errorf("failed to unmarshal order: %w", err)
	}

	log.Printf("📦 Processing order %s for customer %s (amount: $%.2f)",
		order.OrderID, order.CustomerID, order.Total)

	// Get current stats for this customer
	var stats CustomerStats
	err := ctx.GetJSON(order.CustomerID, &stats)
	if err != nil {
		// First order for this customer
		stats = CustomerStats{
			CustomerID: order.CustomerID,
		}
	}

	// Update statistics
	stats.TotalOrders++
	stats.TotalSpent += order.Total
	stats.LastOrderAt = order.Timestamp
	stats.AverageOrder = stats.TotalSpent / float64(stats.TotalOrders)

	// Save updated stats
	if err := ctx.SetJSON(order.CustomerID, stats); err != nil {
		return fmt.Errorf("failed to save stats: %w", err)
	}

	log.Printf("✅ Updated stats for customer %s: %d orders, $%.2f total, $%.2f avg",
		stats.CustomerID, stats.TotalOrders, stats.TotalSpent, stats.AverageOrder)

	return nil
}

// simulateOrders generates sample orders
func simulateOrders(ctx context.Context, client *franzgo.Client) {
	producer := franzgo.NewProducer(client)

	customers := []string{"CUST-1", "CUST-2", "CUST-3"}
	orderID := 1

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
			customerID := customers[orderID%len(customers)]
			order := OrderEvent{
				OrderID:    fmt.Sprintf("ORD-%d", orderID),
				CustomerID: customerID,
				Total:      float64(50 + (orderID * 10)),
				Timestamp:  time.Now(),
			}

			data, _ := json.Marshal(order)
			if err := producer.Produce(ctx, "orders.created", []byte(order.OrderID), data); err != nil {
				log.Printf("❌ Failed to produce order: %v", err)
			} else {
				log.Printf("📤 Published order: %s for %s", order.OrderID, order.CustomerID)
			}

			orderID++
		}
	}
}
