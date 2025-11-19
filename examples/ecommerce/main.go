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

func main() {
	// Create logger
	logger := watermill.NewStdLogger(true, true)

	// Create configuration for ecommerce
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"ecommerce-order-service",
	)
	config.Logger = logger

	// Create Kafka client
	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Create producer and consumer
	producer := kafka.NewEcommerceProducer(client)
	consumer := kafka.NewEcommerceConsumer(client)

	// Context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumers
	go startOrderConsumer(ctx, consumer)
	go startPaymentConsumer(ctx, consumer)
	go startInventoryConsumer(ctx, consumer)

	// Simulate publishing events
	go simulateOrderFlow(ctx, producer)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down...", nil)
}

// startOrderConsumer starts the order event consumer
func startOrderConsumer(ctx context.Context, consumer *kafka.EcommerceConsumer) {
	err := consumer.SubscribeOrderCreated(ctx, func(ctx context.Context, order kafka.OrderEvent) error {
		log.Printf("Order created: %s for customer %s, total: $%.2f",
			order.OrderID, order.CustomerID, order.Total)

		// Process order (e.g., validate, reserve inventory, etc.)
		time.Sleep(100 * time.Millisecond) // Simulate processing

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe to order created events: %v", err)
	}
}

// startPaymentConsumer starts the payment event consumer
func startPaymentConsumer(ctx context.Context, consumer *kafka.EcommerceConsumer) {
	err := consumer.SubscribePaymentProcessed(ctx, func(ctx context.Context, payment kafka.PaymentEvent) error {
		log.Printf("Payment processed: %s for order %s, amount: $%.2f",
			payment.PaymentID, payment.OrderID, payment.Amount)

		// Handle successful payment (e.g., update order status, trigger fulfillment)
		time.Sleep(50 * time.Millisecond) // Simulate processing

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe to payment processed events: %v", err)
	}
}

// startInventoryConsumer starts the inventory event consumer
func startInventoryConsumer(ctx context.Context, consumer *kafka.EcommerceConsumer) {
	err := consumer.SubscribeInventoryReserved(ctx, func(ctx context.Context, inventory kafka.InventoryEvent) error {
		log.Printf("Inventory reserved: %d units of product %s",
			inventory.Quantity, inventory.ProductID)

		// Update inventory tracking
		time.Sleep(50 * time.Millisecond) // Simulate processing

		return nil
	})

	if err != nil {
		log.Printf("Failed to subscribe to inventory reserved events: %v", err)
	}
}

// simulateOrderFlow simulates an ecommerce order flow
func simulateOrderFlow(ctx context.Context, producer *kafka.EcommerceProducer) {
	time.Sleep(2 * time.Second) // Wait for consumers to be ready

	for i := 1; i <= 5; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			orderID := fmt.Sprintf("ORD-%d", i)
			customerID := fmt.Sprintf("CUST-%d", i)

			// Create order
			order := kafka.OrderEvent{
				OrderID:    orderID,
				CustomerID: customerID,
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
						Price:     49.99,
					},
				},
				Total: 109.97,
			}

			// Publish order created event
			if err := producer.PublishOrderCreated(ctx, order); err != nil {
				log.Printf("Failed to publish order created event: %v", err)
				continue
			}

			// Reserve inventory
			for _, item := range order.Items {
				if err := producer.PublishInventoryReserved(ctx, item.ProductID, item.Quantity); err != nil {
					log.Printf("Failed to publish inventory reserved event: %v", err)
				}
			}

			// Process payment
			payment := kafka.PaymentEvent{
				PaymentID:  fmt.Sprintf("PAY-%d", i),
				OrderID:    orderID,
				Amount:     order.Total,
				Status:     "completed",
				Method:     "credit_card",
				CustomerID: customerID,
			}

			if err := producer.PublishPaymentProcessed(ctx, payment); err != nil {
				log.Printf("Failed to publish payment processed event: %v", err)
			}

			log.Printf("Published order flow for order %s", orderID)

			time.Sleep(2 * time.Second) // Wait between orders
		}
	}
}
