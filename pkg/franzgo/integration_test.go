// +build integration

package franzgo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// getKafkaBrokers returns Kafka brokers from environment or default
func getKafkaBrokers() []string {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return []string{"localhost:9092"}
	}
	return []string{brokers}
}

// setupTestClient creates a test client
func setupTestClient(t *testing.T) *Client {
	config := NewConfigBuilder().
		WithBrokers(getKafkaBrokers()).
		WithConsumerGroup(fmt.Sprintf("test-group-%d", time.Now().UnixNano())).
		WithClientID(fmt.Sprintf("test-client-%d", time.Now().UnixNano())).
		Build()

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Verify connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Failed to ping Kafka: %v. Make sure Kafka is running at %v", err, getKafkaBrokers())
	}

	return client
}

// teardownTestClient closes the client
func teardownTestClient(t *testing.T, client *Client) {
	if err := client.Close(); err != nil {
		t.Logf("Warning: Failed to close client: %v", err)
	}
}

func TestIntegration_ProducerConsumer(t *testing.T) {
	client := setupTestClient(t)
	defer teardownTestClient(t, client)

	topic := fmt.Sprintf("test-produce-consume-%d", time.Now().UnixNano())

	// Create producer
	producer := NewProducer(client)

	// Produce messages
	ctx := context.Background()
	numMessages := 10

	for i := 0; i < numMessages; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))

		if err := producer.Produce(ctx, topic, key, value); err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	// Create consumer
	consumer := NewConsumer(client, nil)

	var receivedCount atomic.Int32
	receivedMessages := make(map[string]string)
	var receivedMu sync.Mutex

	handler := func(ctx context.Context, record *kgo.Record) error {
		receivedMu.Lock()
		receivedMessages[string(record.Key)] = string(record.Value)
		receivedMu.Unlock()

		count := receivedCount.Add(1)
		if int(count) >= numMessages {
			// Received all messages, can stop
			return fmt.Errorf("received all messages")
		}
		return nil
	}

	// Start consumer in background
	consumeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	go func() {
		_ = consumer.Consume(consumeCtx, []string{topic}, handler)
	}()

	// Wait for all messages
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if receivedCount.Load() >= int32(numMessages) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all messages received
	if receivedCount.Load() != int32(numMessages) {
		t.Errorf("Expected %d messages, received %d", numMessages, receivedCount.Load())
	}

	receivedMu.Lock()
	for i := 0; i < numMessages; i++ {
		key := fmt.Sprintf("key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)

		if value, exists := receivedMessages[key]; !exists {
			t.Errorf("Message with key %s not received", key)
		} else if value != expectedValue {
			t.Errorf("Message %s: expected value %s, got %s", key, expectedValue, value)
		}
	}
	receivedMu.Unlock()
}

func TestIntegration_BatchProcessing(t *testing.T) {
	client := setupTestClient(t)
	defer teardownTestClient(t, client)

	topic := fmt.Sprintf("test-batch-%d", time.Now().UnixNano())

	// Create producer and produce messages
	producer := NewProducer(client)
	ctx := context.Background()

	numMessages := 20
	for i := 0; i < numMessages; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := producer.Produce(ctx, topic, key, value); err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	// Create batch consumer
	consumer := NewConsumer(client, nil)
	batchConfig := &BatchConfig{
		MaxBatchSize:  5,
		MaxBatchBytes: 1024 * 1024,
		BatchTimeout:  2 * time.Second,
		ErrorStrategy: BatchErrorStrategySkipErrors,
	}
	batchProcessor := NewBatchProcessor(consumer, batchConfig)

	var batchCount atomic.Int32
	var messageCount atomic.Int32

	handler := func(ctx context.Context, batch []*kgo.Record) error {
		batchCount.Add(1)
		messageCount.Add(int32(len(batch)))

		if messageCount.Load() >= int32(numMessages) {
			return fmt.Errorf("received all messages")
		}
		return nil
	}

	// Start batch consumer
	consumeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	go func() {
		_ = batchProcessor.ConsumeBatch(consumeCtx, []string{topic}, handler)
	}()

	// Wait for processing
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if messageCount.Load() >= int32(numMessages) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if messageCount.Load() < int32(numMessages) {
		t.Errorf("Expected at least %d messages, got %d", numMessages, messageCount.Load())
	}

	if batchCount.Load() < 1 {
		t.Error("Expected at least one batch to be processed")
	}
}

func TestIntegration_Transactions(t *testing.T) {
	client := setupTestClient(t)
	defer teardownTestClient(t, client)

	topic := fmt.Sprintf("test-txn-%d", time.Now().UnixNano())

	// Create transactional producer
	txnConfig := &TransactionalConfig{
		TransactionalID:    fmt.Sprintf("test-txn-id-%d", time.Now().UnixNano()),
		TransactionTimeout: 60 * time.Second,
	}

	txnProducer, err := NewTransactionalProducer(client.config, txnConfig)
	if err != nil {
		t.Fatalf("Failed to create transactional producer: %v", err)
	}
	defer txnProducer.Close()

	ctx := context.Background()

	// Execute successful transaction
	err = txnProducer.ExecuteTransaction(ctx, func(ctx context.Context) error {
		for i := 0; i < 5; i++ {
			if err := txnProducer.Produce(ctx, topic, []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i))); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Execute failed transaction (should rollback)
	err = txnProducer.ExecuteTransaction(ctx, func(ctx context.Context) error {
		if err := txnProducer.Produce(ctx, topic, []byte("rollback-key"), []byte("rollback-value")); err != nil {
			return err
		}
		return fmt.Errorf("intentional failure")
	})

	if err == nil {
		t.Error("Expected transaction to fail")
	}

	// Verify only committed messages are available
	consumer := NewConsumer(client, nil)
	var messageCount atomic.Int32

	handler := func(ctx context.Context, record *kgo.Record) error {
		messageCount.Add(1)
		if messageCount.Load() >= 5 {
			return fmt.Errorf("received expected messages")
		}
		return nil
	}

	consumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	go func() {
		_ = consumer.Consume(consumeCtx, []string{topic}, handler)
	}()

	time.Sleep(5 * time.Second)

	// Should only have 5 committed messages, not the rolled back one
	if messageCount.Load() != 5 {
		t.Errorf("Expected 5 committed messages, got %d", messageCount.Load())
	}
}

func TestIntegration_HealthChecks(t *testing.T) {
	client := setupTestClient(t)
	defer teardownTestClient(t, client)

	healthConfig := DefaultHealthConfig()
	checker := NewHealthChecker(client, healthConfig)

	ctx := context.Background()

	// Perform health check
	report, err := checker.Check(ctx)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	if report.Status != HealthStatusHealthy {
		t.Errorf("Expected healthy status, got %v", report.Status)
	}

	if report.TotalChecks == 0 {
		t.Error("Expected at least one health check")
	}

	if report.HealthyChecks != report.TotalChecks {
		t.Errorf("Expected all checks to be healthy, got %d/%d", report.HealthyChecks, report.TotalChecks)
	}

	// Test readiness
	if !checker.IsReady(ctx) {
		t.Error("Service should be ready")
	}

	// Test liveness
	if !checker.IsHealthy(ctx) {
		t.Error("Service should be healthy")
	}
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	client := setupTestClient(t)

	topic := fmt.Sprintf("test-shutdown-%d", time.Now().UnixNano())

	// Setup shutdown manager
	shutdownConfig := DefaultShutdownConfig()
	shutdownConfig.Timeout = 10 * time.Second
	manager := NewShutdownManager(client, shutdownConfig)

	// Create producer and consumer
	producer := NewProducer(client)
	consumer := NewConsumer(client, nil)

	manager.SetProducer(producer)
	manager.SetConsumer(consumer)

	// Produce some messages
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = producer.Produce(ctx, topic, []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Perform graceful shutdown
	err := manager.Shutdown(ctx)
	if err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}

	// Verify client is closed
	if !client.closed {
		t.Error("Client should be closed after shutdown")
	}

	// Try to produce after shutdown (should fail gracefully)
	shutdownProducer := NewShutdownAwareProducer(producer, manager)
	err = shutdownProducer.Produce(ctx, topic, []byte("key"), []byte("value"))
	if err == nil {
		t.Error("Expected error when producing after shutdown")
	}
}

func TestIntegration_StreamTransformations(t *testing.T) {
	client := setupTestClient(t)
	defer teardownTestClient(t, client)

	inputTopic := fmt.Sprintf("test-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("test-output-%d", time.Now().UnixNano())

	// Produce test data
	producer := NewProducer(client)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		data := map[string]interface{}{
			"id":    i,
			"value": i * 10,
		}
		value, _ := json.Marshal(data)
		_ = producer.Produce(ctx, inputTopic, []byte(fmt.Sprintf("key-%d", i)), value)
	}

	// Create transformer
	transformer := NewChainableTransformer(client, inputTopic, outputTopic).
		Filter(func(record *kgo.Record) bool {
			// Filter even IDs only
			var data map[string]interface{}
			json.Unmarshal(record.Value, &data)
			id := int(data["id"].(float64))
			return id%2 == 0
		}).
		Map(func(record *kgo.Record) (*kgo.Record, error) {
			// Double the value
			var data map[string]interface{}
			json.Unmarshal(record.Value, &data)
			data["value"] = data["value"].(float64) * 2
			newValue, _ := json.Marshal(data)
			record.Value = newValue
			return record, nil
		})

	// Run transformer in background
	transformCtx, cancelTransform := context.WithTimeout(ctx, 10*time.Second)
	defer cancelTransform()

	go func() {
		_ = transformer.Run(transformCtx)
	}()

	// Consume from output topic
	consumer := NewConsumer(client, nil)
	var receivedCount atomic.Int32

	handler := func(ctx context.Context, record *kgo.Record) error {
		var data map[string]interface{}
		json.Unmarshal(record.Value, &data)

		id := int(data["id"].(float64))
		value := int(data["value"].(float64))

		// Verify transformation
		if id%2 != 0 {
			t.Errorf("Received odd ID %d, should have been filtered", id)
		}

		expectedValue := id * 10 * 2 // original * 10, then doubled
		if value != expectedValue {
			t.Errorf("ID %d: expected value %d, got %d", id, expectedValue, value)
		}

		receivedCount.Add(1)
		if receivedCount.Load() >= 5 { // 5 even numbers (0, 2, 4, 6, 8)
			return fmt.Errorf("received all transformed messages")
		}
		return nil
	}

	consumeCtx, cancelConsume := context.WithTimeout(ctx, 15*time.Second)
	defer cancelConsume()

	go func() {
		_ = consumer.Consume(consumeCtx, []string{outputTopic}, handler)
	}()

	// Wait for processing
	time.Sleep(10 * time.Second)

	if receivedCount.Load() < 5 {
		t.Errorf("Expected at least 5 transformed messages, got %d", receivedCount.Load())
	}
}

func TestIntegration_Middleware(t *testing.T) {
	client := setupTestClient(t)
	defer teardownTestClient(t, client)

	topic := fmt.Sprintf("test-middleware-%d", time.Now().UnixNano())

	// Produce messages
	producer := NewProducer(client)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_ = producer.Produce(ctx, topic, []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	// Create consumer with middleware
	consumer := NewConsumer(client, nil)
	metrics := NewMessageMetrics()

	var processedCount atomic.Int32

	baseHandler := func(ctx context.Context, record *kgo.Record) error {
		count := processedCount.Add(1)
		if count >= 5 {
			return fmt.Errorf("processed all messages")
		}
		return nil
	}

	// Apply middleware chain
	middlewareChain := Chain(
		LoggingMiddleware(),
		MetricsMiddleware(metrics),
		TimeoutMiddleware(5*time.Second),
		RecoveryMiddleware(),
	)

	wrappedHandler := middlewareChain(baseHandler)

	// Consume with middleware
	consumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	go func() {
		_ = consumer.Consume(consumeCtx, []string{topic}, wrappedHandler)
	}()

	// Wait for processing
	time.Sleep(5 * time.Second)

	// Verify metrics
	if metrics.ProcessedCount() < 5 {
		t.Errorf("Expected at least 5 processed messages, got %d", metrics.ProcessedCount())
	}
}

// Run these tests with: go test -tags=integration -v ./pkg/franzgo/
// Ensure Kafka is running: docker run -d -p 9092:9092 apache/kafka:latest
