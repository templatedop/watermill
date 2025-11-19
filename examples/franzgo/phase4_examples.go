package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.cept.gov.in/it2.0common/watermill/pkg/franzgo"
	"gitlab.cept.gov.in/it2.0common/watermill/pkg/franzgo/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
)

// Phase 4 Examples: Observability, Storage, and Schema Registry

func runPrometheusMetricsExample(client *franzgo.Client) {
	fmt.Println("\n=== Prometheus Metrics Example ===")

	// Create Prometheus metrics
	metrics := franzgo.NewPrometheusMetrics("franzgo_example")

	// Create metrics collector
	collector := franzgo.NewMetricsCollector(metrics, 60*time.Second)

	// Start collector in background
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go collector.Start(ctx)

	// Example: Record some metrics
	fmt.Println("\n--- Recording Metrics ---")

	// Simulate message production
	for i := 0; i < 10; i++ {
		metrics.RecordMessageProduced("metrics-test", 0)
		metrics.RecordProduceDuration("metrics-test", time.Duration(i*10)*time.Millisecond)
	}

	// Simulate message consumption
	for i := 0; i < 8; i++ {
		metrics.RecordMessageConsumed("metrics-test", "metrics-group")
		metrics.RecordProcessingDuration("metrics-test", "metrics-group", time.Duration(i*5)*time.Millisecond)
	}

	// Simulate some errors
	metrics.RecordMessageFailed("metrics-test", "metrics-group", "processing_error")
	metrics.RecordMessageFailed("metrics-test", "metrics-group", "timeout")
	collector.RecordError("metrics-test", "metrics-group")

	// Record batch metrics
	metrics.RecordBatchSize("metrics-test", 25)
	metrics.RecordBatchProcessingTime("metrics-test", 150*time.Millisecond)

	// Record transformation metrics
	metrics.RecordTransformation("map", "metrics-test", 5*time.Millisecond)
	metrics.RecordTransformation("filter", "metrics-test", 2*time.Millisecond)

	// Record transaction metrics
	metrics.RecordTransactionStarted("txn-1")
	metrics.RecordTransactionCommitted("txn-1", 100*time.Millisecond)

	// Record window metrics
	metrics.RecordWindowTriggered("tumbling", "user-1", 50, 200*time.Millisecond)

	// Set gauge metrics
	metrics.SetConsumerLag("metrics-test", 0, "metrics-group", 100)
	metrics.SetActiveConsumers(3)
	metrics.SetActiveProducers(2)

	fmt.Println("Metrics recorded successfully!")
	fmt.Println("\n--- Starting Metrics HTTP Server ---")
	fmt.Println("Visit http://localhost:2112/metrics to see Prometheus metrics")

	// Start HTTP server for Prometheus scraping
	http.Handle("/metrics", promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{}))
	go func() {
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)
	fmt.Println("Metrics server started (running in background)")
}

func runOpenTelemetryTracingExample(client *franzgo.Client) {
	fmt.Println("\n=== OpenTelemetry Tracing Example ===")

	// Create stdout trace exporter for demonstration
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Printf("Failed to create exporter: %v", err)
		return
	}

	// Create trace provider
	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(tp)

	// Create tracing config
	tracingConfig := franzgo.DefaultTracingConfig("franzgo-phase4-example")

	// Example 1: Producer Tracing
	fmt.Println("\n--- Producer Tracing ---")
	producerTracer := franzgo.NewProducerTracer(tracingConfig)

	ctx := context.Background()
	record := &kgo.Record{
		Topic: "tracing-test",
		Key:   []byte("trace-key-1"),
		Value: []byte(`{"message": "traced message"}`),
	}

	tracedCtx, span := producerTracer.TraceProduce(ctx, record)
	fmt.Println("Producer trace started")

	// Simulate production
	time.Sleep(10 * time.Millisecond)

	span.End()
	fmt.Println("Producer trace completed")

	// Example 2: Consumer Tracing
	fmt.Println("\n--- Consumer Tracing ---")
	consumerTracer := franzgo.NewConsumerTracer(tracingConfig)

	consumeRecord := &kgo.Record{
		Topic:     "tracing-test",
		Partition: 0,
		Offset:    123,
		Key:       []byte("trace-key-1"),
		Value:     []byte(`{"message": "traced message"}`),
	}

	tracedCtx, span = consumerTracer.TraceConsume(tracedCtx, consumeRecord)
	fmt.Println("Consumer trace started")

	// Simulate consumption
	time.Sleep(15 * time.Millisecond)

	span.End()
	fmt.Println("Consumer trace completed")

	// Example 3: Transformation Tracing
	fmt.Println("\n--- Transformation Tracing ---")
	transformTracer := franzgo.NewTransformationTracer(tracingConfig)

	tracedCtx, span = transformTracer.TraceTransformation(ctx, "map", "input-topic", "output-topic")
	fmt.Println("Transformation trace started")

	time.Sleep(5 * time.Millisecond)

	span.End()
	fmt.Println("Transformation trace completed")

	// Example 4: Window Tracing
	fmt.Println("\n--- Window Tracing ---")
	windowTracer := franzgo.NewWindowTracer(tracingConfig)

	tracedCtx, span = windowTracer.TraceWindow(ctx, "tumbling", "user-123", 50)
	fmt.Println("Window trace started")

	time.Sleep(20 * time.Millisecond)

	span.End()
	fmt.Println("Window trace completed")

	// Example 5: Transaction Tracing
	fmt.Println("\n--- Transaction Tracing ---")
	txnTracer := franzgo.NewTransactionTracer(tracingConfig)

	tracedCtx, span = txnTracer.TraceTransaction(ctx, "txn-example-1")
	fmt.Println("Transaction trace started")

	time.Sleep(30 * time.Millisecond)

	span.End()
	fmt.Println("Transaction trace completed")

	// Flush traces
	if err := tp.Shutdown(context.Background()); err != nil {
		log.Printf("Error shutting down tracer provider: %v", err)
	}

	fmt.Println("\nAll traces exported to stdout")
}

func runStorageBackendsExample() {
	fmt.Println("\n=== Storage Backends Example ===")

	// Example 1: In-Memory Storage
	fmt.Println("\n--- In-Memory Storage ---")
	runInMemoryStorageExample()

	// Example 2: LevelDB Storage
	fmt.Println("\n--- LevelDB Storage ---")
	runLevelDBStorageExample()

	// Example 3: Redis Storage
	fmt.Println("\n--- Redis Storage ---")
	runRedisStorageExample()

	// Example 4: BadgerDB Storage
	fmt.Println("\n--- BadgerDB Storage ---")
	runBadgerStorageExample()

	// Example 5: Partitioned Storage
	fmt.Println("\n--- Partitioned Storage ---")
	runPartitionedStorageExample()
}

func runInMemoryStorageExample() {
	storage := franzgo.NewMemoryStorage()
	defer storage.Close()

	// Set values
	storage.Set("user:1", []byte(`{"name": "Alice", "age": 30}`))
	storage.Set("user:2", []byte(`{"name": "Bob", "age": 25}`))
	storage.Set("user:3", []byte(`{"name": "Charlie", "age": 35}`))

	// Get value
	value, err := storage.Get("user:1")
	if err == nil {
		fmt.Printf("Retrieved: %s\n", string(value))
	}

	// Check existence
	exists, _ := storage.Has("user:2")
	fmt.Printf("user:2 exists: %v\n", exists)

	// Iterate
	fmt.Println("All users:")
	iter, _ := storage.Iterator()
	defer iter.Close()

	for iter.Next() {
		fmt.Printf("  %s: %s\n", iter.Key(), string(iter.Value()))
	}

	// Delete
	storage.Delete("user:2")
	fmt.Println("Deleted user:2")
}

func runLevelDBStorageExample() {
	storage, err := franzgo.NewLevelDBStorage("/tmp/franzgo-leveldb-example")
	if err != nil {
		log.Printf("Failed to create LevelDB storage: %v", err)
		return
	}
	defer storage.Close()

	// Set values
	storage.Set("order:1001", []byte(`{"total": 99.99, "status": "completed"}`))
	storage.Set("order:1002", []byte(`{"total": 149.99, "status": "pending"}`))

	// Get value
	value, err := storage.Get("order:1001")
	if err == nil {
		fmt.Printf("Retrieved: %s\n", string(value))
	}

	fmt.Println("LevelDB storage operations completed")
}

func runRedisStorageExample() {
	config := &franzgo.RedisConfig{
		Addr:   "localhost:6379",
		Prefix: "franzgo:example:",
		TTL:    5 * time.Minute,
	}

	storage, err := franzgo.NewRedisStorage(config)
	if err != nil {
		log.Printf("Failed to create Redis storage (is Redis running?): %v", err)
		return
	}
	defer storage.Close()

	// Set values
	storage.Set("session:abc123", []byte(`{"user_id": 42, "expires": "2025-12-31"}`))
	storage.Set("session:def456", []byte(`{"user_id": 43, "expires": "2025-12-31"}`))

	// Get value
	value, err := storage.Get("session:abc123")
	if err == nil {
		fmt.Printf("Retrieved: %s\n", string(value))
	}

	fmt.Println("Redis storage operations completed")
}

func runBadgerStorageExample() {
	storage, err := franzgo.NewBadgerStorage("/tmp/franzgo-badger-example")
	if err != nil {
		log.Printf("Failed to create BadgerDB storage: %v", err)
		return
	}
	defer storage.Close()

	// Set values
	storage.Set("state:processor1", []byte(`{"count": 1000, "last_update": "2025-01-18"}`))
	storage.Set("state:processor2", []byte(`{"count": 2500, "last_update": "2025-01-18"}`))

	// Get value
	value, err := storage.Get("state:processor1")
	if err == nil {
		fmt.Printf("Retrieved: %s\n", string(value))
	}

	fmt.Println("BadgerDB storage operations completed")
}

func runPartitionedStorageExample() {
	baseStorage := franzgo.NewMemoryStorage()
	defer baseStorage.Close()

	// Create partitioned storages
	partition0 := franzgo.NewPartitionedStorage(0, baseStorage)
	partition1 := franzgo.NewPartitionedStorage(1, baseStorage)

	// Set values in different partitions
	partition0.Set("key1", []byte("value from partition 0"))
	partition1.Set("key1", []byte("value from partition 1"))

	// Get values
	val0, _ := partition0.Get("key1")
	val1, _ := partition1.Get("key1")

	fmt.Printf("Partition 0 - key1: %s\n", string(val0))
	fmt.Printf("Partition 1 - key1: %s\n", string(val1))
	fmt.Println("Partitioned storage operations completed")
}

func runSchemaRegistryExample() {
	fmt.Println("\n=== Schema Registry Example ===")

	// Example 1: Confluent Schema Registry
	fmt.Println("\n--- Confluent Schema Registry ---")
	runConfluentSchemaRegistryExample()

	// Example 2: Avro Codec
	fmt.Println("\n--- Avro Codec ---")
	runAvroCodecExample()

	// Example 3: Avro Schema Builder
	fmt.Println("\n--- Avro Schema Builder ---")
	runAvroSchemaBuilderExample()
}

func runConfluentSchemaRegistryExample() {
	// Create schema registry client
	registry := schema.NewConfluentSchemaRegistry("http://localhost:8081")

	// Note: This requires a running Schema Registry
	// For demonstration, we'll show the API usage

	fmt.Println("Schema Registry client created")
	fmt.Println("Note: Requires Confluent Schema Registry at localhost:8081")

	// Example schema
	userSchema := `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"}
		]
	}`

	// Register schema (would work with running registry)
	_ = userSchema
	fmt.Println("Schema registration example prepared")

	// Create cached registry for better performance
	cachedRegistry := schema.NewCachedSchemaRegistry(registry, 5*time.Minute)
	_ = cachedRegistry
	fmt.Println("Cached schema registry created")
}

func runAvroCodecExample() {
	registry := schema.NewConfluentSchemaRegistry("http://localhost:8081")
	codec := schema.NewAvroCodec(registry)

	// Example data
	userData := map[string]interface{}{
		"id":    int64(123),
		"name":  "Alice",
		"email": "alice@example.com",
	}

	fmt.Printf("Example user data: %+v\n", userData)

	// In a real scenario with a running registry:
	// 1. Register schema and get schema ID
	// 2. Encode data with schema ID
	// encoded, err := codec.Encode(schemaID, userData)
	// 3. Decode data
	// decoded, schemaID, err := codec.Decode(encoded)

	fmt.Println("Avro codec example prepared")
	_ = codec
}

func runAvroSchemaBuilderExample() {
	// Build an Avro schema using the builder
	builder := schema.NewAvroSchemaBuilder("com.example", "Order")

	builder.
		AddDocumentedField("id", schema.AvroLong, "Unique order identifier").
		AddField("customer_id", schema.AvroLong).
		AddField("total", schema.AvroDouble).
		AddFieldWithDefault("status", schema.AvroString, "pending").
		AddField("items", schema.AvroArray(schema.AvroString)).
		AddField("metadata", schema.AvroMap(schema.AvroString))

	// In a real scenario:
	// schemaJSON, err := builder.Build()
	// Then register with schema registry

	fmt.Println("Avro schema builder example:")
	fmt.Println("  - Namespace: com.example")
	fmt.Println("  - Name: Order")
	fmt.Println("  - Fields: id, customer_id, total, status, items, metadata")
}

func runIntegratedObservabilityExample(client *franzgo.Client) {
	fmt.Println("\n=== Integrated Observability Example ===")
	fmt.Println("This example shows metrics + tracing + storage working together")

	// Create metrics
	metrics := franzgo.NewPrometheusMetrics("franzgo_integrated")

	// Create tracing
	tracingConfig := franzgo.DefaultTracingConfig("franzgo-integrated")

	// Create storage
	storage := franzgo.NewMemoryStorage()
	defer storage.Close()

	ctx := context.Background()
	producer := franzgo.NewProducer(client)
	producerTracer := franzgo.NewProducerTracer(tracingConfig)

	// Produce with observability
	fmt.Println("\n--- Producing with full observability ---")

	for i := 1; i <= 5; i++ {
		// Start trace
		record := &kgo.Record{
			Topic: "integrated-test",
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf(`{"id": %d, "value": %d}`, i, i*10)),
		}

		tracedCtx, span := producerTracer.TraceProduce(ctx, record)

		// Produce message
		start := time.Now()
		err := producer.Produce(tracedCtx, "integrated-test", record.Key, record.Value)
		duration := time.Since(start)

		// Record metrics
		if err == nil {
			metrics.RecordMessageProduced("integrated-test", 0)
			metrics.RecordProduceDuration("integrated-test", duration)

			// Store state
			stateKey := fmt.Sprintf("produced:%d", i)
			stateValue, _ := json.Marshal(map[string]interface{}{
				"timestamp": time.Now().Unix(),
				"key":       string(record.Key),
			})
			storage.Set(stateKey, stateValue)
		} else {
			metrics.RecordMessageFailed("integrated-test", "integrated-group", "produce_error")
		}

		span.End()

		fmt.Printf("Produced message %d with full observability\n", i)
		time.Sleep(100 * time.Millisecond)
	}

	// Check storage
	fmt.Println("\n--- Checking stored state ---")
	iter, _ := storage.Iterator()
	defer iter.Close()

	for iter.Next() {
		var state map[string]interface{}
		json.Unmarshal(iter.Value(), &state)
		fmt.Printf("  %s: timestamp=%v, key=%s\n", iter.Key(), state["timestamp"], state["key"])
	}

	fmt.Println("\nIntegrated observability example completed!")
}

func runPhase4Examples() {
	fmt.Println("==============================================")
	fmt.Println("Franz-go Phase 4: Enterprise Features Examples")
	fmt.Println("==============================================")

	// Create client
	config, err := franzgo.NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup("franzgo-phase4-examples").
		Build()

	if err != nil {
		log.Fatalf("Failed to build config: %v", err)
	}

	client, err := franzgo.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Run examples
	runPrometheusMetricsExample(client)
	runOpenTelemetryTracingExample(client)
	runStorageBackendsExample()
	runSchemaRegistryExample()
	runIntegratedObservabilityExample(client)

	fmt.Println("\n==============================================")
	fmt.Println("All Phase 4 examples completed!")
	fmt.Println("==============================================")
	fmt.Println("\nNote: Some examples require external services:")
	fmt.Println("  - Prometheus metrics available at: http://localhost:2112/metrics")
	fmt.Println("  - Redis storage requires: redis-server on localhost:6379")
	fmt.Println("  - Schema Registry requires: Confluent Schema Registry on localhost:8081")
}
