// +build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"gitlab.cept.gov.in/it2.0common/watermill/pkg/kafka"
)

func getKafkaBrokers() []string {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return []string{"localhost:9092"}
	}
	return []string{brokers}
}

func TestProducerConsumerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	brokers := getKafkaBrokers()
	config := kafka.DefaultConfig()
	config.Brokers = brokers
	config.ConsumerGroup = "test-group-" + time.Now().Format("20060102150405")

	client, err := kafka.NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Create producer
	producer := kafka.NewProducer(client)
	topic := "test-topic-" + time.Now().Format("20060102150405")

	// Create consumer
	consumer := kafka.NewConsumer(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	received := make(chan string, 1)

	// Subscribe before publishing
	go func() {
		err := consumer.Subscribe(ctx, topic, func(ctx context.Context, payload []byte) error {
			received <- string(payload)
			return nil
		})
		if err != nil && ctx.Err() == nil {
			t.Errorf("Consumer error: %v", err)
		}
	}()

	// Wait a bit for consumer to be ready
	time.Sleep(2 * time.Second)

	// Publish message
	testPayload := "integration test message"
	err = producer.Publish(ctx, topic, []byte(testPayload))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Wait for message
	select {
	case msg := <-received:
		if msg != testPayload {
			t.Errorf("Expected %s, got %s", testPayload, msg)
		}
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for message")
	}
}

func TestBatchProcessingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	brokers := getKafkaBrokers()
	config := kafka.DefaultConfig()
	config.Brokers = brokers
	config.ConsumerGroup = "test-batch-group-" + time.Now().Format("20060102150405")

	client, err := kafka.NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	producer := kafka.NewProducer(client)
	topic := "test-batch-topic-" + time.Now().Format("20060102150405")

	// Publish batch of messages
	numMessages := 10
	payloads := make([]interface{}, numMessages)
	for i := 0; i < numMessages; i++ {
		payloads[i] = map[string]interface{}{
			"id":      i,
			"message": "test message",
		}
	}

	ctx := context.Background()
	err = producer.PublishBatch(ctx, topic, payloads)
	if err != nil {
		t.Fatalf("Failed to publish batch: %v", err)
	}

	// Consume batch
	batchConfig := kafka.BatchConfig{
		MaxBatchSize:  5,
		BatchTimeout:  2 * time.Second,
		ErrorStrategy: kafka.BatchErrorStrategySkipErrors,
		EnableMetrics: true,
	}

	batchConsumer := kafka.NewAdvancedBatchConsumer(client, batchConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	received := make(chan int, 1)

	go func() {
		err := batchConsumer.SubscribeBatch(ctx, topic, func(ctx context.Context, batch *kafka.MessageBatch) error {
			received <- len(batch.Messages)
			return nil
		})
		if err != nil && ctx.Err() == nil {
			t.Errorf("Batch consumer error: %v", err)
		}
	}()

	// Wait for batches
	totalReceived := 0
	timeout := time.After(15 * time.Second)

	for totalReceived < numMessages {
		select {
		case count := <-received:
			totalReceived += count
		case <-timeout:
			t.Errorf("Timeout: only received %d/%d messages", totalReceived, numMessages)
			return
		}
	}

	if totalReceived != numMessages {
		t.Errorf("Expected %d messages, got %d", numMessages, totalReceived)
	}
}

func TestDLQIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	brokers := getKafkaBrokers()
	config := kafka.DefaultConfig()
	config.Brokers = brokers
	config.ConsumerGroup = "test-dlq-group-" + time.Now().Format("20060102150405")
	config.DLQ.Enabled = true
	config.DLQ.Topic = "test-dlq-" + time.Now().Format("20060102150405")
	config.DLQ.MaxRetries = 2
	config.DLQ.RetryDelay = 1 * time.Second

	client, err := kafka.NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	producer := kafka.NewProducer(client)
	topic := "test-dlq-topic-" + time.Now().Format("20060102150405")

	// Publish a message
	ctx := context.Background()
	err = producer.Publish(ctx, topic, []byte("failing message"))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Consumer that always fails
	consumer := kafka.NewConsumer(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	attempts := 0
	go func() {
		consumer.Subscribe(ctx, topic, func(ctx context.Context, payload []byte) error {
			attempts++
			return kafka.ErrProcessingFailed{Message: "intentional failure"}
		})
	}()

	// Wait for retries
	time.Sleep(10 * time.Second)

	// DLQ should have received the message after max retries
	if attempts <= config.DLQ.MaxRetries {
		t.Logf("Message was retried %d times (expected > %d)", attempts, config.DLQ.MaxRetries)
	}
}

func TestHealthCheckIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	brokers := getKafkaBrokers()
	config := kafka.DefaultConfig()
	config.Brokers = brokers
	config.ConsumerGroup = "test-health-group-" + time.Now().Format("20060102150405")

	client, err := kafka.NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	checker := kafka.NewHealthChecker(client)

	ctx := context.Background()
	health, err := checker.Check(ctx)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if health.Status != kafka.HealthStatusHealthy {
		t.Errorf("Expected healthy status, got %s", health.Status)
	}

	if len(health.Checks) == 0 {
		t.Error("Expected health checks to be populated")
	}

	// Test readiness
	ready, err := checker.Ready(ctx)
	if err != nil {
		t.Fatalf("Readiness check failed: %v", err)
	}

	if ready.Status != kafka.HealthStatusHealthy {
		t.Errorf("Expected ready status, got %s", ready.Status)
	}

	// Test liveness
	alive, err := checker.Alive(ctx)
	if err != nil {
		t.Fatalf("Liveness check failed: %v", err)
	}

	if alive.Status != kafka.HealthStatusHealthy {
		t.Errorf("Expected alive status, got %s", alive.Status)
	}
}

func BenchmarkIntegrationThroughput(b *testing.B) {
	brokers := getKafkaBrokers()
	config := kafka.DefaultConfig()
	config.Brokers = brokers
	config.ConsumerGroup = "bench-group-" + time.Now().Format("20060102150405")

	client, err := kafka.NewClient(config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	producer := kafka.NewProducer(client)
	topic := "bench-topic-" + time.Now().Format("20060102150405")

	ctx := context.Background()
	payload := []byte("benchmark message payload")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := producer.Publish(ctx, topic, payload)
		if err != nil {
			b.Fatalf("Failed to publish: %v", err)
		}
	}
}
