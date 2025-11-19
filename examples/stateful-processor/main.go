package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"gitlab.cept.gov.in/it2.0common/watermill/pkg/kafka"
)

// OrderStats represents aggregated order statistics
type OrderStats struct {
	CustomerID   string  `json:"customer_id"`
	TotalOrders  int     `json:"total_orders"`
	TotalSpent   float64 `json:"total_spent"`
	LastOrderAt  string  `json:"last_order_at"`
	AverageOrder float64 `json:"average_order"`
}

func main() {
	logger := watermill.NewStdLogger(true, true)

	// Create Kafka client
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"order-stats-processor",
	)
	config.Logger = logger

	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create stateful processor
	processor, err := kafka.NewTypedProcessor[OrderStats](
		client,
		&kafka.ProcessorConfig{
			Topic:      "orders.created",
			GroupTable: "order-stats-table", // Compacted topic for state
			Codec:      kafka.NewJSONCodec(),
			Storage:    kafka.NewMemoryStorage(),
		},
		processOrderStats,
	)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processor
	if err := processor.Start(ctx); err != nil {
		log.Fatalf("Failed to start processor: %v", err)
	}

	// Start producer to simulate orders
	go simulateOrders(ctx, client)

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	processor.Stop()
}

// processOrderStats is the stateful processor callback
func processOrderStats(ctx *kafka.TypedProcessorContext[OrderStats]) error {
	// Decode incoming order
	var order kafka.OrderEvent
	if err := kafka.NewJSONCodec().Decode(ctx.Message().Payload, &order); err != nil {
		return fmt.Errorf("failed to decode order: %w", err)
	}

	log.Printf("Processing order %s for customer %s", order.OrderID, order.CustomerID)

	// Get current stats (state)
	stats, err := ctx.Value()
	if err != nil {
		return fmt.Errorf("failed to get stats: %w", err)
	}

	// Initialize if first order
	if stats == nil {
		stats = &OrderStats{
			CustomerID: order.CustomerID,
		}
	}

	// Update stats
	stats.TotalOrders++
	stats.TotalSpent += order.Total
	stats.LastOrderAt = time.Now().Format(time.RFC3339)
	stats.AverageOrder = stats.TotalSpent / float64(stats.TotalOrders)

	// Save updated state
	if err := ctx.SetValue(*stats); err != nil {
		return fmt.Errorf("failed to set stats: %w", err)
	}

	log.Printf("Updated stats for %s: %d orders, $%.2f total, $%.2f average",
		stats.CustomerID, stats.TotalOrders, stats.TotalSpent, stats.AverageOrder)

	// Emit alert if high-value customer
	if stats.TotalSpent > 1000 {
		alert := map[string]interface{}{
			"customer_id": stats.CustomerID,
			"total_spent": stats.TotalSpent,
			"message":     "High-value customer alert!",
		}
		if err := ctx.Emit("customer-alerts", stats.CustomerID, alert); err != nil {
			log.Printf("Failed to emit alert: %v", err)
		}
	}

	return nil
}

// simulateOrders simulates order creation
func simulateOrders(ctx context.Context, client *kafka.Client) {
	time.Sleep(2 * time.Second) // Wait for processor to start

	producer := kafka.NewEcommerceProducer(client)

	customers := []string{"CUST-1", "CUST-2", "CUST-3"}
	orderNum := 1

	for {
		select {
		case <-ctx.Done():
			return
		default:
			for _, customerID := range customers {
				order := kafka.OrderEvent{
					OrderID:    fmt.Sprintf("ORD-%d", orderNum),
					CustomerID: customerID,
					Status:     "pending",
					Items: []kafka.OrderItem{
						{
							ProductID: "PROD-1",
							Quantity:  1,
							Price:     99.99,
						},
					},
					Total: 99.99,
				}

				if err := producer.PublishOrderCreated(ctx, order); err != nil {
					log.Printf("Failed to publish order: %v", err)
				} else {
					log.Printf("Published order %s for %s", order.OrderID, customerID)
				}

				orderNum++
				time.Sleep(2 * time.Second)
			}
		}
	}
}
