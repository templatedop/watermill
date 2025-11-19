// Franz-go E-commerce Example
// Demonstrates a complete e-commerce order processing pipeline with franz-go

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

// Order represents an e-commerce order
type Order struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	Total      float64     `json:"total"`
	Status     string      `json:"status"` // pending, confirmed, paid, shipped, delivered
	Timestamp  time.Time   `json:"timestamp"`
}

// OrderItem represents an item in an order
type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// PaymentEvent represents a payment event
type PaymentEvent struct {
	PaymentID string    `json:"payment_id"`
	OrderID   string    `json:"order_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"` // processed, failed
	Timestamp time.Time `json:"timestamp"`
}

// ShipmentEvent represents a shipment event
type ShipmentEvent struct {
	ShipmentID   string    `json:"shipment_id"`
	OrderID      string    `json:"order_id"`
	TrackingCode string    `json:"tracking_code"`
	Status       string    `json:"status"` // preparing, shipped, delivered
	Timestamp    time.Time `json:"timestamp"`
}

func main() {
	log.Println("🚀 Starting E-commerce Order Processing Pipeline...")

	brokers := []string{"localhost:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	config := franzgo.NewConfigBuilder().
		WithBrokers(brokers).
		WithConsumerGroup("ecommerce-pipeline").
		WithClientID("ecommerce-app").
		Build()

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start services
	log.Println("📦 Starting order service...")
	go orderService(ctx, client)

	log.Println("💳 Starting payment service...")
	go paymentService(ctx, client)

	log.Println("🚚 Starting shipment service...")
	go shipmentService(ctx, client)

	log.Println("📧 Starting notification service...")
	go notificationService(ctx, client)

	// Give services time to start
	time.Sleep(2 * time.Second)

	// Simulate orders
	log.Println("📤 Starting order simulation...")
	go simulateOrders(ctx, client)

	log.Println("✅ E-commerce pipeline running. Press Ctrl+C to stop")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 Shutting down...")
	cancel()
	time.Sleep(2 * time.Second)
	log.Println("✅ Shutdown complete")
}

// orderService processes new orders
func orderService(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)
	producer := franzgo.NewProducer(client)

	consumer.Consume(ctx, []string{"orders.created"}, func(record *kgo.Record) error {
		var order Order
		if err := json.Unmarshal(record.Value, &order); err != nil {
			return fmt.Errorf("failed to unmarshal order: %w", err)
		}

		log.Printf("📦 [Order Service] New order: %s from customer %s (total: $%.2f)",
			order.OrderID, order.CustomerID, order.Total)

		// Validate order
		if order.Total < 10 {
			log.Printf("⚠️  [Order Service] Order %s rejected: minimum order value $10", order.OrderID)
			return nil
		}

		// Confirm order
		order.Status = "confirmed"
		data, _ := json.Marshal(order)
		if err := producer.Produce(ctx, "orders.confirmed", []byte(order.OrderID), data); err != nil {
			return fmt.Errorf("failed to confirm order: %w", err)
		}

		log.Printf("✅ [Order Service] Order %s confirmed", order.OrderID)
		return nil
	})
}

// paymentService processes payments
func paymentService(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)
	producer := franzgo.NewProducer(client)

	consumer.Consume(ctx, []string{"orders.confirmed"}, func(record *kgo.Record) error {
		var order Order
		if err := json.Unmarshal(record.Value, &order); err != nil {
			return fmt.Errorf("failed to unmarshal order: %w", err)
		}

		log.Printf("💳 [Payment Service] Processing payment for order %s ($%.2f)",
			order.OrderID, order.Total)

		// Simulate payment processing
		time.Sleep(500 * time.Millisecond)

		// Create payment event
		payment := PaymentEvent{
			PaymentID: fmt.Sprintf("PAY-%s", order.OrderID),
			OrderID:   order.OrderID,
			Amount:    order.Total,
			Status:    "processed",
			Timestamp: time.Now(),
		}

		data, _ := json.Marshal(payment)
		if err := producer.Produce(ctx, "payments.processed", []byte(payment.PaymentID), data); err != nil {
			return fmt.Errorf("failed to emit payment event: %w", err)
		}

		// Update order status
		order.Status = "paid"
		orderData, _ := json.Marshal(order)
		if err := producer.Produce(ctx, "orders.paid", []byte(order.OrderID), orderData); err != nil {
			return fmt.Errorf("failed to update order: %w", err)
		}

		log.Printf("✅ [Payment Service] Payment processed for order %s", order.OrderID)
		return nil
	})
}

// shipmentService handles order fulfillment
func shipmentService(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)
	producer := franzgo.NewProducer(client)

	consumer.Consume(ctx, []string{"orders.paid"}, func(record *kgo.Record) error {
		var order Order
		if err := json.Unmarshal(record.Value, &order); err != nil {
			return fmt.Errorf("failed to unmarshal order: %w", err)
		}

		log.Printf("🚚 [Shipment Service] Preparing shipment for order %s", order.OrderID)

		// Simulate shipment preparation
		time.Sleep(500 * time.Millisecond)

		// Create shipment
		shipment := ShipmentEvent{
			ShipmentID:   fmt.Sprintf("SHIP-%s", order.OrderID),
			OrderID:      order.OrderID,
			TrackingCode: fmt.Sprintf("TRK-%d", time.Now().Unix()),
			Status:       "shipped",
			Timestamp:    time.Now(),
		}

		data, _ := json.Marshal(shipment)
		if err := producer.Produce(ctx, "shipments.shipped", []byte(shipment.ShipmentID), data); err != nil {
			return fmt.Errorf("failed to emit shipment event: %w", err)
		}

		// Update order status
		order.Status = "shipped"
		orderData, _ := json.Marshal(order)
		if err := producer.Produce(ctx, "orders.shipped", []byte(order.OrderID), orderData); err != nil {
			return fmt.Errorf("failed to update order: %w", err)
		}

		log.Printf("✅ [Shipment Service] Order %s shipped with tracking %s",
			order.OrderID, shipment.TrackingCode)
		return nil
	})
}

// notificationService sends notifications for various events
func notificationService(ctx context.Context, client *franzgo.Client) {
	consumer := franzgo.NewConsumer(client)

	topics := []string{
		"orders.confirmed",
		"payments.processed",
		"shipments.shipped",
	}

	consumer.Consume(ctx, topics, func(record *kgo.Record) error {
		var notification string

		switch record.Topic {
		case "orders.confirmed":
			var order Order
			json.Unmarshal(record.Value, &order)
			notification = fmt.Sprintf("Order %s confirmed for customer %s",
				order.OrderID, order.CustomerID)

		case "payments.processed":
			var payment PaymentEvent
			json.Unmarshal(record.Value, &payment)
			notification = fmt.Sprintf("Payment %s processed for order %s ($%.2f)",
				payment.PaymentID, payment.OrderID, payment.Amount)

		case "shipments.shipped":
			var shipment ShipmentEvent
			json.Unmarshal(record.Value, &shipment)
			notification = fmt.Sprintf("Order %s shipped with tracking %s",
				shipment.OrderID, shipment.TrackingCode)
		}

		log.Printf("📧 [Notification Service] %s", notification)
		return nil
	})
}

// simulateOrders generates sample orders
func simulateOrders(ctx context.Context, client *franzgo.Client) {
	producer := franzgo.NewProducer(client)

	orderID := 1
	customers := []string{"CUST-001", "CUST-002", "CUST-003"}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			order := Order{
				OrderID:    fmt.Sprintf("ORD-%03d", orderID),
				CustomerID: customers[orderID%len(customers)],
				Items: []OrderItem{
					{ProductID: "PROD-1", Quantity: 2, Price: 29.99},
					{ProductID: "PROD-2", Quantity: 1, Price: 49.99},
				},
				Total:     109.97,
				Status:    "pending",
				Timestamp: time.Now(),
			}

			data, _ := json.Marshal(order)
			if err := producer.Produce(ctx, "orders.created", []byte(order.OrderID), data); err != nil {
				log.Printf("❌ Failed to create order: %v", err)
			} else {
				log.Printf("📤 [Order Simulator] Created order: %s", order.OrderID)
			}

			orderID++
		}
	}
}
