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
	"gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"
)

// This example demonstrates stream-table joins
// We join order events with customer data to enrich orders

type Customer struct {
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
	Email      string `json:"email"`
	Tier       string `json:"tier"` // "gold", "silver", "bronze"
}

type EnrichedOrder struct {
	OrderID      string  `json:"order_id"`
	CustomerID   string  `json:"customer_id"`
	CustomerName string  `json:"customer_name"`
	CustomerTier string  `json:"customer_tier"`
	Total        float64 `json:"total"`
	Discount     float64 `json:"discount"`
	FinalTotal   float64 `json:"final_total"`
}

func main() {
	logger := watermill.NewStdLogger(true, true)

	// Create Kafka client
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"order-enrichment-joiner",
	)
	config.Logger = logger

	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create stream-table joiner
	// Left side: orders stream
	// Right side: customers table
	joiner, err := kafka.NewStreamJoiner(
		client,
		&kafka.JoinConfig{
			LeftTopic:   "orders.created",
			RightTopic:  "customers-table", // Compacted topic with customer data
			OutputTopic: "orders.enriched",
			JoinType:    kafka.LeftJoin, // Keep all orders even if customer not found
			Codec:       kafka.NewJSONCodec(),
		},
		enrichOrder,
	)
	if err != nil {
		log.Fatalf("Failed to create joiner: %v", err)
	}

	// Start joiner
	if err := joiner.Start(ctx); err != nil {
		log.Fatalf("Failed to start joiner: %v", err)
	}

	// Simulate customer data
	go publishCustomers(ctx, client)

	// Simulate orders
	go publishOrders(ctx, client)

	// Consume enriched orders
	go consumeEnrichedOrders(ctx, client)

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	joiner.Stop()
}

// enrichOrder is the join function
func enrichOrder(ctx context.Context, key string, leftValue interface{}, rightValue interface{}) (interface{}, error) {
	// Decode order (left side)
	orderMap, ok := leftValue.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid order format")
	}

	enriched := EnrichedOrder{
		OrderID:    orderMap["order_id"].(string),
		CustomerID: orderMap["customer_id"].(string),
		Total:      orderMap["total"].(float64),
	}

	// Apply customer data if available (right side)
	if rightValue != nil {
		customerMap, ok := rightValue.(map[string]interface{})
		if ok {
			enriched.CustomerName = customerMap["name"].(string)
			enriched.CustomerTier = customerMap["tier"].(string)

			// Apply tier-based discounts
			switch enriched.CustomerTier {
			case "gold":
				enriched.Discount = enriched.Total * 0.15 // 15% discount
			case "silver":
				enriched.Discount = enriched.Total * 0.10 // 10% discount
			case "bronze":
				enriched.Discount = enriched.Total * 0.05 // 5% discount
			}
		}
	}

	enriched.FinalTotal = enriched.Total - enriched.Discount

	log.Printf("Enriched order %s: %s (%s tier) - $%.2f -> $%.2f (saved $%.2f)",
		enriched.OrderID, enriched.CustomerName, enriched.CustomerTier,
		enriched.Total, enriched.FinalTotal, enriched.Discount)

	return enriched, nil
}

// publishCustomers publishes customer data to the customers table
func publishCustomers(ctx context.Context, client *kafka.Client) {
	time.Sleep(1 * time.Second)

	customers := []Customer{
		{CustomerID: "CUST-1", Name: "Alice Johnson", Email: "alice@example.com", Tier: "gold"},
		{CustomerID: "CUST-2", Name: "Bob Smith", Email: "bob@example.com", Tier: "silver"},
		{CustomerID: "CUST-3", Name: "Charlie Brown", Email: "charlie@example.com", Tier: "bronze"},
	}

	producer := kafka.NewProducer(client)

	for _, customer := range customers {
		if err := producer.PublishWithKey(ctx, "customers-table", customer.CustomerID, customer); err != nil {
			log.Printf("Failed to publish customer: %v", err)
		} else {
			log.Printf("Published customer: %s (%s tier)", customer.Name, customer.Tier)
		}
	}
}

// publishOrders publishes order events
func publishOrders(ctx context.Context, client *kafka.Client) {
	time.Sleep(3 * time.Second) // Wait for customers to be published

	producer := kafka.NewEcommerceProducer(client)

	orders := []kafka.OrderEvent{
		{
			OrderID:    "ORD-1",
			CustomerID: "CUST-1",
			Status:     "pending",
			Total:      200.00,
		},
		{
			OrderID:    "ORD-2",
			CustomerID: "CUST-2",
			Status:     "pending",
			Total:      150.00,
		},
		{
			OrderID:    "ORD-3",
			CustomerID: "CUST-3",
			Status:     "pending",
			Total:      100.00,
		},
		{
			OrderID:    "ORD-4",
			CustomerID: "CUST-UNKNOWN", // Customer not in table
			Status:     "pending",
			Total:      75.00,
		},
	}

	for _, order := range orders {
		if err := producer.PublishOrderCreated(ctx, order); err != nil {
			log.Printf("Failed to publish order: %v", err)
		} else {
			log.Printf("Published order: %s for %s", order.OrderID, order.CustomerID)
		}
		time.Sleep(2 * time.Second)
	}
}

// consumeEnrichedOrders consumes the enriched orders
func consumeEnrichedOrders(ctx context.Context, client *kafka.Client) {
	time.Sleep(2 * time.Second)

	consumer := kafka.NewConsumer(client)

	err := consumer.Subscribe(ctx, "orders.enriched", func(ctx context.Context, msg *kafka.Message) error {
		var enriched EnrichedOrder
		if err := consumer.UnmarshalMessage(msg, &enriched); err != nil {
			return err
		}

		log.Printf("📦 Received enriched order: %s - Customer: %s (%s) - Final: $%.2f",
			enriched.OrderID, enriched.CustomerName, enriched.CustomerTier, enriched.FinalTotal)

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
}
