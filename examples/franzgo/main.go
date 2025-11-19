package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"gitlab.cept.gov.in/it2.0common/watermill/pkg/franzgo"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	// Create configuration
	config := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("franzgo-example").
		WithClientID("franzgo-demo").
		Build()

	// Create client
	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Run examples
	fmt.Println("=== Franz-go Examples ===\n")

	// 1. Simple Producer/Consumer
	runSimpleExample(client)

	// 2. Ecommerce Example
	runEcommerceExample(client)

	// 3. Batch Processing Example
	runBatchExample(client)

	// 4. Middleware Example
	runMiddlewareExample(client)
}

func runSimpleExample(client *franzgo.Client) {
	fmt.Println("1. Simple Producer/Consumer Example")
	fmt.Println("------------------------------------")

	// Create producer
	producer := franzgo.NewProducer(client)

	// Produce some messages
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("Hello from franz-go! Message #%d", i))

		err := producer.Produce(ctx, "test-topic", key, value)
		if err != nil {
			log.Printf("Failed to produce message: %v", err)
		} else {
			fmt.Printf("Produced: %s\n", value)
		}
	}

	// Consume messages
	consumer := franzgo.NewConsumer(client)
	consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fmt.Println("\nConsuming messages...")
	err := consumer.Consume(consumeCtx, []string{"test-topic"}, func(ctx context.Context, record *kgo.Record) error {
		fmt.Printf("Consumed: %s = %s\n", record.Key, record.Value)
		return nil
	})

	if err != nil && err != context.DeadlineExceeded {
		log.Printf("Consumer error: %v", err)
	}

	fmt.Println()
}

func runEcommerceExample(client *franzgo.Client) {
	fmt.Println("2. Ecommerce Example")
	fmt.Println("--------------------")

	// Create ecommerce producer
	producer := franzgo.NewEcommerceProducer(client)

	// Publish order created event
	order := &franzgo.OrderEvent{
		OrderID:    "ORD-123",
		CustomerID: "CUST-456",
		Total:      99.99,
		Status:     "created",
		CreatedAt:  time.Now(),
		Items:      []string{"ITEM-1", "ITEM-2"},
	}

	ctx := context.Background()
	err := producer.PublishOrderCreated(ctx, order)
	if err != nil {
		log.Printf("Failed to publish order: %v", err)
	} else {
		fmt.Printf("Published order: %s\n", order.OrderID)
	}

	// Publish payment processed event
	payment := &franzgo.PaymentEvent{
		PaymentID:     "PAY-789",
		OrderID:       "ORD-123",
		Amount:        99.99,
		Currency:      "USD",
		Status:        "processed",
		PaymentMethod: "credit_card",
		ProcessedAt:   time.Now(),
	}

	err = producer.PublishPaymentProcessed(ctx, payment)
	if err != nil {
		log.Printf("Failed to publish payment: %v", err)
	} else {
		fmt.Printf("Published payment: %s\n", payment.PaymentID)
	}

	fmt.Println()
}

func runBatchExample(client *franzgo.Client) {
	fmt.Println("3. Batch Processing Example")
	fmt.Println("---------------------------")

	// Create batch processor
	batchConfig := franzgo.DefaultBatchConfig()
	batchConfig.MaxBatchSize = 10
	batchConfig.BatchTimeout = 2 * time.Second

	batchProcessor := franzgo.NewBatchProcessor(client, batchConfig)

	// Produce batch of messages
	messages := make([][]byte, 20)
	for i := 0; i < 20; i++ {
		messages[i] = []byte(fmt.Sprintf("Batch message #%d", i))
	}

	ctx := context.Background()
	err := batchProcessor.ProduceBatch(ctx, "batch-topic", messages)
	if err != nil {
		log.Printf("Failed to produce batch: %v", err)
	} else {
		fmt.Printf("Produced batch of %d messages\n", len(messages))
	}

	// Consume in batches
	consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fmt.Println("\nConsuming in batches...")
	err = batchProcessor.ConsumeBatch(consumeCtx, []string{"batch-topic"}, func(ctx context.Context, batch []*kgo.Record) error {
		fmt.Printf("Processed batch of %d messages\n", len(batch))
		for i, record := range batch {
			if i < 3 { // Show first 3
				fmt.Printf("  - %s\n", record.Value)
			}
		}
		if len(batch) > 3 {
			fmt.Printf("  ... and %d more\n", len(batch)-3)
		}
		return nil
	})

	if err != nil && err != context.DeadlineExceeded {
		log.Printf("Batch consumer error: %v", err)
	}

	fmt.Println()
}

func runMiddlewareExample(client *franzgo.Client) {
	fmt.Println("4. Middleware Example")
	fmt.Println("---------------------")

	// Create consumer with middleware
	consumer := franzgo.NewConsumer(client)
	metrics := franzgo.NewMessageMetrics()

	// Chain multiple middlewares
	middleware := franzgo.Chain(
		franzgo.RecoveryMiddleware(),
		franzgo.LoggingMiddleware(),
		franzgo.MetricsMiddleware(metrics),
		franzgo.TimeoutMiddleware(5*time.Second),
		franzgo.RetryMiddleware(2, 100*time.Millisecond),
	)

	// Wrap handler with middleware
	handler := middleware(func(ctx context.Context, record *kgo.Record) error {
		fmt.Printf("Processing: %s\n", record.Value)
		return nil
	})

	// Produce test message
	producer := franzgo.NewProducer(client)
	ctx := context.Background()
	producer.Produce(ctx, "middleware-topic", []byte("key"), []byte("Test message with middleware"))

	// Consume with middleware
	consumeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	fmt.Println("\nConsuming with middleware...")
	err := consumer.Consume(consumeCtx, []string{"middleware-topic"}, handler)

	if err != nil && err != context.DeadlineExceeded {
		log.Printf("Consumer error: %v", err)
	}

	// Show metrics
	processed, errors, avgDuration := metrics.GetStats("middleware-topic")
	fmt.Printf("\nMetrics: Processed=%d, Errors=%d, Avg Duration=%v\n", processed, errors, avgDuration)

	fmt.Println()
}
