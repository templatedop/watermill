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

// This example demonstrates advanced batch processing with different strategies

func main() {
	logger := watermill.NewStdLogger(true, true)

	// Create Kafka client
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"batch-consumer-group",
	)
	config.Logger = logger

	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Example 1: Simple batch processing
	go runSimpleBatch(ctx, client, logger)

	// Example 2: Batch with error handling
	go runBatchWithErrorHandling(ctx, client, logger)

	// Example 3: Partitioned batch processing
	go runPartitionedBatch(ctx, client, logger)

	// Example 4: Batch with metrics
	go runBatchWithMetrics(ctx, client, logger)

	// Simulate producing messages
	go produceTestMessages(ctx, client)

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
}

// runSimpleBatch demonstrates simple batch processing
func runSimpleBatch(ctx context.Context, client *kafka.Client, logger watermill.LoggerAdapter) {
	// Simple batch consumer with defaults
	config := kafka.DefaultBatchConfig()
	config.MaxBatchSize = 50
	config.BatchTimeout = 3 * time.Second

	batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)

	err := batchConsumer.SubscribeBatch(ctx, "orders.simple", func(ctx context.Context, batch *kafka.MessageBatch) error {
		log.Printf("📦 Simple Batch: Processing %d messages (%d bytes)",
			len(batch.Messages), batch.TotalBytes)

		// Process all messages in batch
		for _, msg := range batch.Messages {
			var order kafka.OrderEvent
			if err := kafka.NewJSONCodec().Decode(msg.Payload, &order); err != nil {
				return fmt.Errorf("failed to decode: %w", err)
			}

			log.Printf("  - Order: %s, Amount: $%.2f", order.OrderID, order.Total)
		}

		// Simulate processing time
		time.Sleep(100 * time.Millisecond)

		log.Printf("✅ Batch completed successfully")
		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
}

// runBatchWithErrorHandling demonstrates different error strategies
func runBatchWithErrorHandling(ctx context.Context, client *kafka.Client, logger watermill.LoggerAdapter) {
	time.Sleep(1 * time.Second) // Stagger consumers

	// Batch with skip errors strategy
	config := kafka.DefaultBatchConfig()
	config.MaxBatchSize = 30
	config.BatchTimeout = 5 * time.Second
	config.ErrorStrategy = kafka.BatchErrorStrategySkipErrors

	batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)

	err := batchConsumer.SubscribeBatch(ctx, "orders.error-test", func(ctx context.Context, batch *kafka.MessageBatch) error {
		log.Printf("🔄 Error Handling Batch: Processing %d messages", len(batch.Messages))

		// Simulate some messages failing
		for i, msg := range batch.Messages {
			var order kafka.OrderEvent
			if err := kafka.NewJSONCodec().Decode(msg.Payload, &order); err != nil {
				return err
			}

			// Simulate error on every 3rd message
			if i%3 == 0 && order.Total > 100 {
				log.Printf("  ❌ Simulated error for order %s", order.OrderID)
				return fmt.Errorf("simulated error for testing")
			}

			log.Printf("  ✓ Processed order %s", order.OrderID)
		}

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
}

// runPartitionedBatch demonstrates partitioned batch processing
func runPartitionedBatch(ctx context.Context, client *kafka.Client, logger watermill.LoggerAdapter) {
	time.Sleep(2 * time.Second) // Stagger consumers

	// Batch with partitioning by key
	config := kafka.DefaultBatchConfig()
	config.MaxBatchSize = 20
	config.BatchTimeout = 4 * time.Second
	config.PartitionByKey = true

	batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)

	err := batchConsumer.SubscribeBatch(ctx, "orders.partitioned", func(ctx context.Context, batch *kafka.MessageBatch) error {
		log.Printf("🔑 Partitioned Batch: Processing %d messages across %d keys",
			len(batch.Messages), len(batch.Keys))

		// Process by partition key
		for key, messages := range batch.Keys {
			log.Printf("  Key '%s': %d messages", key, len(messages))

			total := 0.0
			for _, msg := range messages {
				var order kafka.OrderEvent
				if err := kafka.NewJSONCodec().Decode(msg.Payload, &order); err != nil {
					return err
				}
				total += order.Total
			}

			log.Printf("  Total for '%s': $%.2f", key, total)
		}

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
}

// runBatchWithMetrics demonstrates batch processing with metrics
func runBatchWithMetrics(ctx context.Context, client *kafka.Client, logger watermill.LoggerAdapter) {
	time.Sleep(3 * time.Second) // Stagger consumers

	// Batch with metrics enabled
	config := kafka.DefaultBatchConfig()
	config.MaxBatchSize = 100
	config.MaxBatchBytes = 1024 * 1024 // 1MB
	config.BatchTimeout = 5 * time.Second
	config.EnableMetrics = true
	config.ErrorStrategy = kafka.BatchErrorStrategyRetryFailed
	config.MaxRetries = 2
	config.RetryDelay = 500 * time.Millisecond

	batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)

	// Start metrics reporter
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics := batchConsumer.GetMetrics()
				if metrics != nil {
					stats := metrics.GetStats()
					log.Printf("📊 Batch Metrics:")
					log.Printf("   Total Batches: %v", stats["total_batches"])
					log.Printf("   Total Messages: %v", stats["total_messages"])
					log.Printf("   Avg Batch Size: %.2f", stats["avg_batch_size"])
					log.Printf("   Avg Processing Time: %.2f ms", stats["avg_processing_time_ms"])
					log.Printf("   Success Rate: %.2f%%", stats["success_rate_percent"])
					log.Printf("   Throughput: %.2f msg/sec", stats["throughput_messages_sec"])
					log.Printf("   Total Bytes: %v", stats["total_bytes"])
				}
			}
		}
	}()

	err := batchConsumer.SubscribeBatch(ctx, "orders.metrics", func(ctx context.Context, batch *kafka.MessageBatch) error {
		log.Printf("📈 Metrics Batch: %d messages, %d bytes",
			len(batch.Messages), batch.TotalBytes)

		// Bulk process for efficiency
		orderCount := 0
		totalAmount := 0.0

		for _, msg := range batch.Messages {
			var order kafka.OrderEvent
			if err := kafka.NewJSONCodec().Decode(msg.Payload, &order); err != nil {
				return err
			}

			orderCount++
			totalAmount += order.Total
		}

		log.Printf("   Processed %d orders, Total: $%.2f", orderCount, totalAmount)

		// Simulate batch database insert
		time.Sleep(50 * time.Millisecond)

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
}

// produceTestMessages produces test messages for batch processing
func produceTestMessages(ctx context.Context, client *kafka.Client) {
	time.Sleep(5 * time.Second) // Wait for consumers to start

	producer := kafka.NewEcommerceProducer(client)

	topics := []string{"orders.simple", "orders.error-test", "orders.partitioned", "orders.metrics"}
	customers := []string{"CUST-A", "CUST-B", "CUST-C"}

	orderNum := 1
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	log.Println("🚀 Starting to produce test messages...")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Produce to each topic
			for _, topic := range topics {
				for _, customerID := range customers {
					order := kafka.OrderEvent{
						OrderID:    fmt.Sprintf("ORD-%d", orderNum),
						CustomerID: customerID,
						Status:     "pending",
						Items: []kafka.OrderItem{
							{
								ProductID: "PROD-1",
								Quantity:  1,
								Price:     float64(50 + (orderNum % 100)),
							},
						},
						Total: float64(50 + (orderNum % 100)),
					}

					if err := producer.PublishEvent(ctx, topic, order); err != nil {
						log.Printf("Failed to publish: %v", err)
					}

					orderNum++
				}
			}
		}
	}
}
