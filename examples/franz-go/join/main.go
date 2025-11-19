// Franz-go Stream-Table Join Example
// Demonstrates joining a stream (orders) with a table (customers) using franz-go

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

// Order represents an order event (stream)
type Order struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Total      float64   `json:"total"`
	Timestamp  time.Time `json:"timestamp"`
}

// Customer represents customer data (table)
type Customer struct {
	CustomerID string `json:"customer_id"`
	Name       string `json:"name"`
	Email      string `json:"email"`
	Tier       string `json:"tier"` // "gold", "silver", "bronze"
}

// EnrichedOrder represents the joined result
type EnrichedOrder struct {
	OrderID      string    `json:"order_id"`
	CustomerID   string    `json:"customer_id"`
	CustomerName string    `json:"customer_name"`
	CustomerTier string    `json:"customer_tier"`
	Total        float64   `json:"total"`
	Discount     float64   `json:"discount"`
	FinalTotal   float64   `json:"final_total"`
	Timestamp    time.Time `json:"timestamp"`
}

var customerTable = make(map[string]Customer)

func main() {
	log.Println("🚀 Starting Franz-go Stream-Table Join Example...")

	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("order-enrichment").
		WithClientID("join-example").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Consume customer table (compacted topic)
	go consumeCustomerTable(ctx, client)

	// Give time for customer table to populate
	time.Sleep(2 * time.Second)

	// Process orders stream and join with customer table
	go processOrdersWithJoin(ctx, client)

	// Simulate data
	go publishCustomers(ctx, client)
	time.Sleep(1 * time.Second)
	go publishOrders(ctx, client)

	log.Println("✅ Join example running. Press Ctrl+C to stop")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 Shutting down...")
	cancel()
	time.Sleep(1 * time.Second)
	log.Println("✅ Shutdown complete")
}

// consumeCustomerTable populates the local customer table from a compacted topic
func consumeCustomerTable(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)

	log.Println("📥 Loading customer table...")

	err := consumer.Consume(ctx, []string{"customers-table"}, func(record *kgo.Record) error {
		customerID := string(record.Key)

		if len(record.Value) == 0 {
			// Tombstone - delete customer
			delete(customerTable, customerID)
			log.Printf("🗑️  Deleted customer: %s", customerID)
			return nil
		}

		var customer Customer
		if err := json.Unmarshal(record.Value, &customer); err != nil {
			return fmt.Errorf("failed to unmarshal customer: %w", err)
		}

		customerTable[customerID] = customer
		log.Printf("📋 Added customer: %s (%s, tier: %s)", customerID, customer.Name, customer.Tier)

		return nil
	})

	if err != nil {
		log.Printf("❌ Customer table consumer error: %v", err)
	}
}

// processOrdersWithJoin processes orders and enriches them with customer data
func processOrdersWithJoin(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)
	producer := franzgo.NewProducer(client)

	log.Println("📥 Processing orders with customer enrichment...")

	err := consumer.Consume(ctx, []string{"orders.created"}, func(record *kgo.Record) error {
		var order Order
		if err := json.Unmarshal(record.Value, &order); err != nil {
			return fmt.Errorf("failed to unmarshal order: %w", err)
		}

		log.Printf("📦 Processing order: %s for customer %s", order.OrderID, order.CustomerID)

		// Join with customer table
		customer, found := customerTable[order.CustomerID]
		if !found {
			log.Printf("⚠️  Customer %s not found in table", order.CustomerID)
			customer = Customer{
				CustomerID: order.CustomerID,
				Name:       "Unknown",
				Tier:       "bronze",
			}
		}

		// Calculate discount based on tier
		discount := 0.0
		switch customer.Tier {
		case "gold":
			discount = order.Total * 0.15 // 15% discount
		case "silver":
			discount = order.Total * 0.10 // 10% discount
		case "bronze":
			discount = order.Total * 0.05 // 5% discount
		}

		// Create enriched order
		enrichedOrder := EnrichedOrder{
			OrderID:      order.OrderID,
			CustomerID:   order.CustomerID,
			CustomerName: customer.Name,
			CustomerTier: customer.Tier,
			Total:        order.Total,
			Discount:     discount,
			FinalTotal:   order.Total - discount,
			Timestamp:    order.Timestamp,
		}

		// Publish enriched order
		data, _ := json.Marshal(enrichedOrder)
		if err := producer.Produce(ctx, "orders.enriched", []byte(enrichedOrder.OrderID), data); err != nil {
			return fmt.Errorf("failed to produce enriched order: %w", err)
		}

		log.Printf("✅ Enriched order: %s - %s (%s tier), discount: $%.2f, final: $%.2f",
			enrichedOrder.OrderID, enrichedOrder.CustomerName, enrichedOrder.CustomerTier,
			enrichedOrder.Discount, enrichedOrder.FinalTotal)

		return nil
	})

	if err != nil {
		log.Printf("❌ Order processor error: %v", err)
	}
}

// publishCustomers publishes sample customer data
func publishCustomers(ctx context.Context, client *franzgo.Client) {
	producer := franzgo.NewProducer(client)

	customers := []Customer{
		{CustomerID: "CUST-1", Name: "Alice Johnson", Email: "alice@example.com", Tier: "gold"},
		{CustomerID: "CUST-2", Name: "Bob Smith", Email: "bob@example.com", Tier: "silver"},
		{CustomerID: "CUST-3", Name: "Charlie Brown", Email: "charlie@example.com", Tier: "bronze"},
	}

	for _, customer := range customers {
		data, _ := json.Marshal(customer)
		if err := producer.Produce(ctx, "customers-table", []byte(customer.CustomerID), data); err != nil {
			log.Printf("❌ Failed to produce customer: %v", err)
		} else {
			log.Printf("📤 Published customer: %s (%s)", customer.CustomerID, customer.Name)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// publishOrders publishes sample orders
func publishOrders(ctx context.Context, client *franzgo.Client) {
	producer := franzgo.NewProducer(client)

	orders := []Order{
		{OrderID: "ORD-1", CustomerID: "CUST-1", Total: 100.00, Timestamp: time.Now()},
		{OrderID: "ORD-2", CustomerID: "CUST-2", Total: 150.00, Timestamp: time.Now()},
		{OrderID: "ORD-3", CustomerID: "CUST-3", Total: 200.00, Timestamp: time.Now()},
		{OrderID: "ORD-4", CustomerID: "CUST-1", Total: 250.00, Timestamp: time.Now()},
	}

	for _, order := range orders {
		data, _ := json.Marshal(order)
		if err := producer.Produce(ctx, "orders.created", []byte(order.OrderID), data); err != nil {
			log.Printf("❌ Failed to produce order: %v", err)
		} else {
			log.Printf("📤 Published order: %s", order.OrderID)
		}
		time.Sleep(2 * time.Second)
	}
}
