package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"
)

func main() {
	// Create logger
	logger := watermill.NewStdLogger(true, true)

	// Create configuration
	config := kafka.EcommerceConfig(
		[]string{"localhost:9092"},
		"consumer-group",
	)
	config.Logger = logger

	// Create Kafka client
	client, err := kafka.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Create router with middleware
	router, err := client.CreateRouter()
	if err != nil {
		log.Fatalf("Failed to create router: %v", err)
	}

	// Add additional custom middleware
	router.AddMiddleware(
		kafka.NewTimeoutMiddleware(30 * time.Second),
		kafka.NewDuplicateDetectionMiddleware(5 * time.Minute),
		kafka.NewThrottleMiddleware(100), // Max 100 messages per second
	)

	// Create consumer
	consumer := kafka.NewEcommerceConsumer(client)

	// Add handlers to router
	addOrderHandlers(router, consumer)
	addPaymentHandlers(router, consumer)
	addInventoryHandlers(router, consumer)

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start router in a goroutine
	go func() {
		if err := client.RunRouter(ctx); err != nil {
			log.Printf("Router error: %v", err)
		}
	}()

	log.Println("Consumer started, waiting for messages...")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down consumer...")
	cancel()
	time.Sleep(1 * time.Second) // Give time for graceful shutdown
}

func addOrderHandlers(router *message.Router, consumer *kafka.EcommerceConsumer) {
	// Handler for order created events
	router.AddNoPublisherHandler(
		"order_created_handler",
		"orders.created",
		consumer.GetSubscriber(),
		func(msg *message.Message) error {
			var order kafka.OrderEvent
			if err := consumer.UnmarshalMessage(msg, &order); err != nil {
				return err
			}

			log.Printf("Processing order: %s for customer %s, total: $%.2f",
				order.OrderID, order.CustomerID, order.Total)

			// Business logic here
			time.Sleep(100 * time.Millisecond) // Simulate processing

			return nil
		},
	)

	// Handler for order updated events
	router.AddNoPublisherHandler(
		"order_updated_handler",
		"orders.updated",
		consumer.GetSubscriber(),
		func(msg *message.Message) error {
			var order kafka.OrderEvent
			if err := consumer.UnmarshalMessage(msg, &order); err != nil {
				return err
			}

			log.Printf("Order updated: %s, new status: %s", order.OrderID, order.Status)

			return nil
		},
	)
}

func addPaymentHandlers(router *message.Router, consumer *kafka.EcommerceConsumer) {
	// Handler for payment processed events
	router.AddNoPublisherHandler(
		"payment_processed_handler",
		"payments.processed",
		consumer.GetSubscriber(),
		func(msg *message.Message) error {
			var payment kafka.PaymentEvent
			if err := consumer.UnmarshalMessage(msg, &payment); err != nil {
				return err
			}

			log.Printf("Payment processed: %s for order %s, amount: $%.2f",
				payment.PaymentID, payment.OrderID, payment.Amount)

			// Trigger fulfillment process
			time.Sleep(50 * time.Millisecond)

			return nil
		},
	)

	// Handler for payment failed events
	router.AddNoPublisherHandler(
		"payment_failed_handler",
		"payments.failed",
		consumer.GetSubscriber(),
		func(msg *message.Message) error {
			log.Printf("Payment failed for message %s", msg.UUID)

			// Handle failed payment (e.g., notify customer, cancel order)
			return nil
		},
	)
}

func addInventoryHandlers(router *message.Router, consumer *kafka.EcommerceConsumer) {
	// Handler for inventory reserved events
	router.AddNoPublisherHandler(
		"inventory_reserved_handler",
		"inventory.reserved",
		consumer.GetSubscriber(),
		func(msg *message.Message) error {
			var inventory kafka.InventoryEvent
			if err := consumer.UnmarshalMessage(msg, &inventory); err != nil {
				return err
			}

			log.Printf("Inventory reserved: %d units of product %s",
				inventory.Quantity, inventory.ProductID)

			// Update inventory database
			time.Sleep(30 * time.Millisecond)

			return nil
		},
	)

	// Handler for inventory released events
	router.AddNoPublisherHandler(
		"inventory_released_handler",
		"inventory.released",
		consumer.GetSubscriber(),
		func(msg *message.Message) error {
			var inventory kafka.InventoryEvent
			if err := consumer.UnmarshalMessage(msg, &inventory); err != nil {
				return err
			}

			log.Printf("Inventory released: %d units of product %s",
				inventory.Quantity, inventory.ProductID)

			return nil
		},
	)
}
