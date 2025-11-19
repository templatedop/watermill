package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"gitlab.cept.gov.in/it2.0common/watermill/pkg/kafka"
)

func main() {
	// Create logger
	logger := watermill.NewStdLogger(true, true)

	// Create configuration
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"producer-group",
	)
	config.Logger = logger

	// Create Kafka client
	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Create producer
	producer := kafka.NewEcommerceProducer(client)

	ctx := context.Background()

	// Example 1: Publish order events
	publishOrderEvents(ctx, producer)

	// Example 2: Publish batch events
	publishBatchEvents(ctx, producer)

	// Example 3: Publish with partition key
	publishWithPartitionKey(ctx, producer)

	log.Println("All events published successfully!")
}

func publishOrderEvents(ctx context.Context, producer *kafka.EcommerceProducer) {
	log.Println("Publishing order events...")

	order := kafka.OrderEvent{
		OrderID:    "ORD-12345",
		CustomerID: "CUST-67890",
		Status:     "pending",
		Items: []kafka.OrderItem{
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
			"timestamp": time.Now().Unix(),
		},
	}

	if err := producer.PublishOrderCreated(ctx, order); err != nil {
		log.Fatalf("Failed to publish order created: %v", err)
	}

	log.Printf("Order %s published successfully", order.OrderID)
}

func publishBatchEvents(ctx context.Context, producer *kafka.EcommerceProducer) {
	log.Println("Publishing batch events...")

	orders := []interface{}{
		kafka.OrderEvent{
			OrderID:    "ORD-100",
			CustomerID: "CUST-1",
			Status:     "pending",
			Total:      49.99,
		},
		kafka.OrderEvent{
			OrderID:    "ORD-101",
			CustomerID: "CUST-2",
			Status:     "pending",
			Total:      79.99,
		},
		kafka.OrderEvent{
			OrderID:    "ORD-102",
			CustomerID: "CUST-3",
			Status:     "pending",
			Total:      129.99,
		},
	}

	if err := producer.PublishBatch(ctx, "orders.created", orders); err != nil {
		log.Fatalf("Failed to publish batch: %v", err)
	}

	log.Printf("Batch of %d orders published successfully", len(orders))
}

func publishWithPartitionKey(ctx context.Context, producer *kafka.EcommerceProducer) {
	log.Println("Publishing with partition key...")

	// All orders for the same customer will go to the same partition
	customerID := "CUST-12345"

	for i := 1; i <= 3; i++ {
		order := kafka.OrderEvent{
			OrderID:    fmt.Sprintf("ORD-%s-%d", customerID, i),
			CustomerID: customerID,
			Status:     "pending",
			Total:      float64(i) * 29.99,
		}

		if err := producer.PublishWithKey(ctx, "orders.created", customerID, order); err != nil {
			log.Fatalf("Failed to publish order with key: %v", err)
		}

		log.Printf("Order %s published with partition key %s", order.OrderID, customerID)
	}
}
