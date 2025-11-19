## Enterprise Features Documentation

This document provides comprehensive documentation for all enterprise-grade features in the Kafka Watermill client library.

## Table of Contents

1. [Testing Suite](#testing-suite)
2. [Prometheus Metrics](#prometheus-metrics)
3. [OpenTelemetry Tracing](#opentelemetry-tracing)
4. [Persistent Storage Backends](#persistent-storage-backends)
5. [Exactly-Once Semantics](#exactly-once-semantics)
6. [Windowing](#windowing)
7. [Schema Registry](#schema-registry)
8. [Stream Transformations](#stream-transformations)
9. [Enhanced Security](#enhanced-security)

---

## Testing Suite

Comprehensive testing infrastructure including unit tests, integration tests, and benchmarks.

### Unit Tests

Run unit tests for all components:

```bash
make test
```

### Integration Tests

Integration tests require a running Kafka cluster. Run with:

```bash
# Start Kafka
make kafka-up

# Run integration tests
go test -tags=integration -v ./tests/integration/...
```

### Benchmark Tests

Run performance benchmarks:

```bash
make benchmark
```

Example benchmark output:
```
BenchmarkJSONCodecEncode-8         1000000     1234 ns/op     512 B/op     10 allocs/op
BenchmarkInMemoryStorageSet-8      5000000      300 ns/op     128 B/op      3 allocs/op
BenchmarkIntegrationThroughput-8    100000    12345 ns/op    1024 B/op     15 allocs/op
```

---

## Prometheus Metrics

Production-ready Prometheus metrics for monitoring and alerting.

### Setup

```go
import (
    "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "net/http"
)

// Create metrics
metrics := kafka.NewPrometheusMetrics("my_service")

// Expose metrics endpoint
http.Handle("/metrics", promhttp.HandlerFor(
    metrics.Registry(),
    promhttp.HandlerOpts{},
))
go http.ListenAndServe(":9090", nil)
```

### Available Metrics

#### Message Metrics
- `watermill_kafka_messages_produced_total` - Total messages produced
- `watermill_kafka_messages_consumed_total` - Total messages consumed
- `watermill_kafka_messages_failed_total` - Total failed messages
- `watermill_kafka_messages_retried_total` - Total message retries
- `watermill_kafka_messages_dlq_total` - Total messages sent to DLQ

#### Processing Metrics
- `watermill_kafka_processing_duration_seconds` - Message processing duration
- `watermill_kafka_batch_size` - Batch size histogram
- `watermill_kafka_batch_processing_duration_seconds` - Batch processing duration

#### Health Metrics
- `watermill_kafka_consumer_lag` - Consumer lag by topic/partition
- `watermill_kafka_error_rate` - Current error rate
- `watermill_kafka_active_consumers` - Number of active consumers
- `watermill_kafka_active_producers` - Number of active producers

#### State Metrics
- `watermill_kafka_state_size_bytes` - Processor state size
- `watermill_kafka_storage_operations_total` - Storage operations count
- `watermill_kafka_circuit_breaker_state` - Circuit breaker state

### Example Queries

```promql
# Average processing latency
rate(watermill_kafka_processing_duration_seconds_sum[5m])
  / rate(watermill_kafka_processing_duration_seconds_count[5m])

# Error rate percentage
rate(watermill_kafka_messages_failed_total[5m])
  / rate(watermill_kafka_messages_consumed_total[5m]) * 100

# Consumer lag alert
watermill_kafka_consumer_lag > 10000
```

### Metrics Collection

Use the `MetricsCollector` for aggregated metrics:

```go
collector := kafka.NewMetricsCollector(metrics, 1*time.Minute)
go collector.Start(context.Background())

// Record errors with automatic rate calculation
collector.RecordError("orders", "order-group")
```

---

## OpenTelemetry Tracing

Distributed tracing with OpenTelemetry for full observability across microservices.

### Setup

```go
import (
    "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/jaeger"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Initialize tracer provider
exporter, _ := jaeger.New(jaeger.WithCollectorEndpoint(
    jaeger.WithEndpoint("http://localhost:14268/api/traces"),
))

tp := sdktrace.NewTracerProvider(
    sdktrace.WithBatcher(exporter),
    sdktrace.WithResource(resource.NewWithAttributes(
        semconv.SchemaURL,
        semconv.ServiceName("my-service"),
    )),
)
otel.SetTracerProvider(tp)

// Configure tracing
tracingConfig := kafka.DefaultTracingConfig("my-service")
```

### Producer Tracing

```go
tracer := kafka.NewProducerTracer(tracingConfig)

// Trace individual message
ctx, span := tracer.TracePublish(ctx, "orders", msg)
defer span.End()

err := producer.Publish(ctx, "orders", msg)
if err != nil {
    span.RecordError(err)
}
```

### Consumer Tracing

Use the tracing middleware:

```go
tracingMiddleware := kafka.NewTracingMiddleware(tracingConfig)

router.AddMiddleware(tracingMiddleware.Handler)
```

### Processor Tracing

```go
processorTracer := kafka.NewProcessorTracer(tracingConfig)

ctx, span := processorTracer.TraceProcess(ctx, "order-stats", key, msg)
defer span.End()

// Trace state operations
ctx, stateSpan := processorTracer.TraceStateOperation(ctx, "get", key)
value, _ := storage.Get(key)
stateSpan.End()
```

### View Tracing

```go
viewTracer := kafka.NewViewTracer(tracingConfig)

ctx, span := viewTracer.TraceGet(ctx, "customer-view", customerID)
defer span.End()

customer, _ := view.Get(customerID)
```

---

## Persistent Storage Backends

Three production-ready storage backends for stateful processing.

### LevelDB (Recommended for Production)

Fast embedded key-value store with excellent performance.

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka/storage"

// Create LevelDB storage
storage, err := storage.NewLevelDBStorage("/var/lib/processor/state")
if err != nil {
    log.Fatal(err)
}
defer storage.Close()

// With custom options
opts := &opt.Options{
    Compression:         opt.SnappyCompression,
    BlockCacheCapacity:  16 * 1024 * 1024, // 16MB cache
    WriteBuffer:         8 * 1024 * 1024,  // 8MB buffer
}
storage, _ := storage.NewLevelDBStorageWithOptions("/path/to/db", opts)

// Operations
storage.Set("key", []byte("value"))
value, _ := storage.Get("key")
storage.Delete("key")

// Batch operations
storage.WriteBatch([]storage.BatchOperation{
    {Type: storage.BatchOpSet, Key: "key1", Value: []byte("value1")},
    {Type: storage.BatchOpSet, Key: "key2", Value: []byte("value2")},
    {Type: storage.BatchOpDelete, Key: "key3"},
})

// Maintenance
storage.Compact() // Trigger manual compaction
stats, _ := storage.GetStats()
```

### Redis (For Distributed State)

Redis backend for sharing state across multiple instances.

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka/storage"

// Create Redis storage
config := storage.DefaultRedisConfig("localhost:6379")
config.Password = "mypassword"
config.DB = 0
config.Prefix = "processor:"
config.TTL = 24 * time.Hour // Optional TTL

storage, err := storage.NewRedisStorage(config)
if err != nil {
    log.Fatal(err)
}
defer storage.Close()

// Operations (same API as LevelDB)
storage.Set("key", []byte("value"))
value, _ := storage.Get("key")

// Redis-specific features
storage.SetTTL(1 * time.Hour)  // Update TTL for future ops
storage.Expire("key", 30*time.Minute) // Set expiration on existing key

stats, _ := storage.GetStats() // Redis INFO stats
```

### BadgerDB (High Performance)

High-performance embedded database from Dgraph.

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka/storage"

// Create BadgerDB storage
config := storage.DefaultBadgerConfig("/var/lib/processor/badger")
config.ValueLogFileSize = 128 << 20 // 128MB
config.SyncWrites = false // Async for performance

storage, err := storage.NewBadgerStorage(config)
if err != nil {
    log.Fatal(err)
}
defer storage.Close()

// Operations (same API)
storage.Set("key", []byte("value"))
value, _ := storage.Get("key")

// BadgerDB-specific features
storage.RunValueLogGC(0.5) // Run GC when 50% stale
storage.Flatten(4) // Flatten LSM tree with 4 workers

// Backup
storage.Backup("/backup/path", 0)
```

### Using with Processors

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"

// Use persistent storage with processor
leveldbStorage, _ := storage.NewLevelDBStorage("/var/lib/state")

processorConfig := &kafka.ProcessorConfig{
    Topic:      "orders",
    GroupTable: "order-stats",
    Storage:    leveldbStorage, // Use persistent storage
    Codec:      &kafka.JSONCodec{},
}

processor := kafka.NewProcessor(processorConfig, client, func(ctx *kafka.ProcessorContext) error {
    // State is now persisted to LevelDB
    stats, _ := ctx.Value()
    // ...
    return ctx.SetValue(stats)
})
```

---

## Exactly-Once Semantics

Transactional processing with exactly-once guarantees.

### Transactional Producer

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"

// Create transactional config
txnConfig := kafka.DefaultTransactionalConfig("my-app-1")

// Create transactional producer
txnProducer, err := kafka.NewTransactionalProducer(config, txnConfig)
if err != nil {
    log.Fatal(err)
}
defer txnProducer.Close()

// Use transactions
err = txnProducer.BeginTransaction()
if err != nil {
    log.Fatal(err)
}

// Publish messages
err = txnProducer.Publish("output-topic", msg1)
if err != nil {
    txnProducer.AbortTransaction()
    return err
}

err = txnProducer.Publish("output-topic", msg2)
if err != nil {
    txnProducer.AbortTransaction()
    return err
}

// Commit transaction
err = txnProducer.CommitTransaction()
```

### Exactly-Once Processor

Complete exactly-once processing with deduplication:

```go
config := &kafka.ExactlyOnceConfig{
    ConsumerGroup:       "order-processor",
    TransactionalID:     "order-processor-1",
    ProcessingTimeout:   30 * time.Second,
    DeduplicationWindow: 5 * time.Minute,
}

processor, err := kafka.NewExactlyOnceProcessor(
    client,
    config,
    func(ctx context.Context, msg *message.Message) ([]*message.Message, error) {
        // Process message
        result := processOrder(msg)

        // Create output message
        outMsg := message.NewMessage(uuid.New().String(), result)
        outMsg.Metadata.Set("output_topic", "processed-orders")

        return []*message.Message{outMsg}, nil
    },
)

// Process messages
err = processor.Process(ctx, "orders", msg)
```

### Idempotent Producer

Simple idempotence without full transactions:

```go
producer := kafka.NewProducer(client)
idempotentProducer := kafka.NewIdempotentProducer(producer, 5*time.Minute)

// Messages with same payload won't be sent twice
idempotentProducer.Publish(ctx, "orders", payload)
idempotentProducer.Publish(ctx, "orders", payload) // Skipped
```

---

## Windowing

Time-based windowing for stream aggregation.

### Tumbling Windows

Fixed-size, non-overlapping windows:

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"

// Create tumbling window (5-minute windows)
window := kafka.NewTumblingWindow(5*time.Minute, func(wc *kafka.WindowContext) error {
    aggregator := &kafka.WindowAggregator{}

    // Count messages
    count, _ := aggregator.Count(wc)

    // Calculate sum
    totalSales, _ := aggregator.Sum(wc, "amount")

    // Calculate average
    avgSales, _ := aggregator.Average(wc, "amount")

    // Emit result
    result := map[string]interface{}{
        "window_start": wc.Window.Start,
        "window_end":   wc.Window.End,
        "count":        count,
        "total_sales":  totalSales,
        "avg_sales":    avgSales,
    }

    return wc.Emit("sales-stats", result)
})

window.SetOutputTopic("windowed-stats")
window.SetEmitter(func(topic string, msg *message.Message) error {
    return producer.PublishMessage(ctx, topic, msg)
})

// Process messages
window.Process(ctx, customerID, orderMsg)
```

### Sliding Windows

Overlapping windows that slide:

```go
// Create sliding window (10-minute size, 1-minute slide)
window := kafka.NewSlidingWindow(
    10*time.Minute, // window size
    1*time.Minute,  // slide interval
    func(wc *kafka.WindowContext) error {
        aggregator := &kafka.WindowAggregator{}

        count, _ := aggregator.Count(wc)
        max, _ := aggregator.Max(wc, "price")
        min, _ := aggregator.Min(wc, "price")

        result := map[string]interface{}{
            "window_start": wc.Window.Start,
            "window_end":   wc.Window.End,
            "count":        count,
            "max_price":    max,
            "min_price":    min,
        }

        return wc.Emit("price-stats", result)
    },
)

// Process messages
window.Process(ctx, productID, priceUpdateMsg)

// Stop sliding window when done
defer window.Stop()
```

### Session Windows

Windows based on inactivity gaps:

```go
// Create session window (30-minute inactivity gap)
window := kafka.NewSessionWindow(30*time.Minute, func(wc *kafka.WindowContext) error {
    aggregator := &kafka.WindowAggregator{}

    // Session ended after 30 minutes of inactivity
    count, _ := aggregator.Count(wc)
    duration := wc.Window.End.Sub(wc.Window.Start)

    result := map[string]interface{}{
        "session_start":    wc.Window.Start,
        "session_end":      wc.Window.End,
        "session_duration": duration.Seconds(),
        "event_count":      count,
    }

    return wc.Emit("user-sessions", result)
})

// Process user activity
window.Process(ctx, userID, activityMsg)
```

### Window State Management

Windows can access persistent state:

```go
window := kafka.NewTumblingWindow(5*time.Minute, func(wc *kafka.WindowContext) error {
    // Get previous window's state
    prevTotal, _ := wc.GetState("total")

    // Calculate current window's total
    aggregator := &kafka.WindowAggregator{}
    currentTotal, _ := aggregator.Sum(wc, "amount")

    // Save state for next window
    wc.SetState("total", currentTotal)

    // Emit result
    result := map[string]interface{}{
        "current_total":  currentTotal,
        "previous_total": prevTotal,
        "change":         currentTotal.(float64) - prevTotal.(float64),
    }

    return wc.Emit("sales-trends", result)
})
```

---

## Schema Registry

Avro and Protobuf support with Confluent Schema Registry integration.

### Schema Registry Client

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka/schema"

// Create registry client
config := schema.DefaultSchemaRegistryConfig("http://localhost:8081")
registry := schema.NewRegistryClient(config)

// Register a schema
avroSchema := `{
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "total", "type": "double"}
    ]
}`

registeredSchema, err := registry.RegisterSchema(
    "orders-value",
    avroSchema,
    schema.SchemaTypeAvro,
)

// Get schema by ID
schema, _ := registry.GetSchemaByID(registeredSchema.ID)

// Get latest schema
latestSchema, _ := registry.GetLatestSchema("orders-value")

// List all subjects
subjects, _ := registry.ListSubjects()
```

### Avro Codec

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka/schema"

// Create Avro codec
codec, err := schema.NewAvroCodec(registry, "orders-value")

// Or with explicit schema
codec, err := schema.NewAvroCodecWithSchema(
    registry,
    "orders-value",
    schema.OrderEventSchema,
)

// Encode data
orderData := map[string]interface{}{
    "order_id":   "ORD-123",
    "customer_id": "CUST-456",
    "total":      99.99,
    "status":     "pending",
    "created_at": time.Now().Unix(),
}

encoded, err := codec.Encode(orderData)

// Decode data
decoded, err := codec.Decode(encoded)
decodedMap := decoded.(map[string]interface{})
```

### Protobuf Codec

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka/schema"

// Create Protobuf codec
codec, err := schema.NewProtobufCodec(registry, "orders-value")

// Encode Protobuf message
msg := &OrderEvent{
    OrderId:    "ORD-123",
    CustomerId: "CUST-456",
    Total:      99.99,
    Status:     "pending",
}

encoded, err := codec.Encode(msg)

// Decode to Protobuf message
var decodedMsg OrderEvent
err = codec.Decode(encoded, &decodedMsg)

// Or decode to map
decodedMap, err := codec.DecodeToMap(encoded)
```

### Using with Producer/Consumer

```go
// Producer with Avro
avroCodec, _ := schema.NewAvroCodec(registry, "orders-value")

orderData := map[string]interface{}{
    "order_id": "ORD-123",
    "total":    99.99,
}

// Encode with schema ID prepended
encoded, _ := avroCodec.Encode(orderData)

// Publish
producer.Publish(ctx, "orders", encoded)

// Consumer with Avro
consumer.Subscribe(ctx, "orders", func(ctx context.Context, payload []byte) error {
    // Decode with schema validation
    decoded, err := avroCodec.Decode(payload)
    if err != nil {
        return err
    }

    order := decoded.(map[string]interface{})
    fmt.Printf("Order: %s, Total: %.2f\n", order["order_id"], order["total"])

    return nil
})
```

### Schema Evolution

```go
// Update to new schema version
err := codec.UpdateSchema(2) // Use version 2

// Schema registry handles compatibility checks
// - BACKWARD: new schema can read old data
// - FORWARD: old schema can read new data
// - FULL: both backward and forward compatible
```

---

## Stream Transformations

Functional stream transformations with Map, Filter, and FlatMap operations.

### Basic Map Transformation

Transform each message using a function:

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"

// Create stream transformer
transformer := kafka.NewStreamTransformer(client, "input-topic", "output-topic")

// Map transformation - add tax to orders
err := transformer.Map(func(msg *message.Message) (*message.Message, error) {
    var order map[string]interface{}
    codec := &kafka.JSONCodec{}
    codec.Decode(msg.Payload, &order)

    // Add 10% tax
    total := order["total"].(float64)
    order["total"] = total * 1.1
    order["tax"] = total * 0.1

    payload, _ := codec.Encode(order)
    msg.Payload = payload

    return msg, nil
})

// Run transformer
transformer.Run(context.Background())
```

### Filter Transformation

Filter messages based on a predicate:

```go
transformer := kafka.NewStreamTransformer(client, "orders", "high-value-orders")

// Filter - only orders > $100
err := transformer.Filter(func(msg *message.Message) bool {
    var order map[string]interface{}
    codec := &kafka.JSONCodec{}
    codec.Decode(msg.Payload, &order)

    total := order["total"].(float64)
    return total > 100.0
})

transformer.Run(context.Background())
```

### FlatMap Transformation

Transform one message into multiple messages:

```go
transformer := kafka.NewStreamTransformer(client, "orders", "order-items")

// FlatMap - split order into individual items
err := transformer.FlatMap(func(msg *message.Message) ([]*message.Message, error) {
    var order map[string]interface{}
    codec := &kafka.JSONCodec{}
    codec.Decode(msg.Payload, &order)

    items := order["items"].([]interface{})
    messages := make([]*message.Message, 0, len(items))

    for i, item := range items {
        payload, _ := codec.Encode(item)
        newMsg := message.NewMessage(
            fmt.Sprintf("%s-%d", msg.UUID, i),
            payload,
        )
        messages = append(messages, newMsg)
    }

    return messages, nil
})

transformer.Run(context.Background())
```

### Chainable Transformations

Chain multiple transformations together:

```go
transformer := kafka.NewChainableTransformer(client, "raw-orders", "processed-orders")

// Build transformation chain
transformer.
    Filter(func(msg *message.Message) bool {
        // Filter valid orders
        return msg.Metadata.Get("status") == "valid"
    }).
    Map(func(msg *message.Message) (*message.Message, error) {
        // Add processing timestamp
        msg.Metadata.Set("processed_at", time.Now().Format(time.RFC3339))
        return msg, nil
    }).
    FlatMap(func(msg *message.Message) ([]*message.Message, error) {
        // Expand if needed
        return []*message.Message{msg}, nil
    }).
    Build()
```

### Common Transformations

Use built-in common transformations:

```go
common := &kafka.CommonTransformations{}

// Add metadata field
addTimestamp := common.AddField("processed_at", time.Now().Format(time.RFC3339))

// JSON transformation
addTax := common.JSONTransform(func(data map[string]interface{}) (map[string]interface{}, error) {
    total := data["total"].(float64)
    data["total"] = total * 1.1
    data["tax"] = total * 0.1
    return data, nil
})

// Filter by metadata
filterPremium := common.FilterByMetadata("tier", "premium")

// Split by delimiter
splitCSV := common.SplitByDelimiter(',')

// Expand JSON array
expandItems := common.ExpandArray("items")

// Enrich with lookup
enrichCustomer := common.Enrich(func(msg *message.Message) (map[string]interface{}, error) {
    customerID := msg.Metadata.Get("customer_id")
    customer, _ := customerService.Get(customerID)
    return map[string]interface{}{
        "customer_name": customer.Name,
        "customer_tier": customer.Tier,
    }, nil
})
```

### Async Transformations

Process transformations asynchronously with parallelism:

```go
// Create async transformer with 10 workers
transformer := kafka.NewAsyncTransformer(
    client,
    "input",
    "output",
    10, // worker count
    func(msg *message.Message) (*message.Message, error) {
        // Heavy transformation
        result := expensiveOperation(msg)
        return result, nil
    },
)

transformer.Run(context.Background())
```

### Example: E-commerce Order Processing

Complete example with chained transformations:

```go
transformer := kafka.NewChainableTransformer(client, "raw-orders", "enriched-orders")

common := &kafka.CommonTransformations{}

transformer.
    // 1. Filter valid orders
    Filter(func(msg *message.Message) bool {
        var order map[string]interface{}
        codec := &kafka.JSONCodec{}
        codec.Decode(msg.Payload, &order)
        return order["status"] == "confirmed"
    }).
    // 2. Add tax
    Map(common.JSONTransform(func(data map[string]interface{}) (map[string]interface{}, error) {
        total := data["total"].(float64)
        data["total"] = total * 1.1
        data["tax"] = total * 0.1
        return data, nil
    })).
    // 3. Enrich with customer data
    Map(common.Enrich(func(msg *message.Message) (map[string]interface{}, error) {
        var order map[string]interface{}
        codec := &kafka.JSONCodec{}
        codec.Decode(msg.Payload, &order)

        customerID := order["customer_id"].(string)
        customer, _ := getCustomerFromDB(customerID)

        return map[string]interface{}{
            "customer_name":  customer.Name,
            "customer_email": customer.Email,
            "customer_tier":  customer.Tier,
        }, nil
    })).
    // 4. Add processing metadata
    Map(common.AddField("processed_at", time.Now().Format(time.RFC3339))).
    Build()
```

---

## Enhanced Security

Comprehensive SASL and TLS/SSL security features for production deployments.

### SASL Authentication

#### SASL PLAIN

Simple username/password authentication:

```go
import "gitlab.cept.gov.in/it-2.0-common/watermill/pkg/kafka"

// Create SASL PLAIN config
saslConfig := kafka.NewSASLPlainConfig("username", "password")

// Create security config
securityConfig := kafka.DefaultSecurityConfig()
securityConfig.SASL = saslConfig

// Apply to Kafka config
config := kafka.DefaultConfig()
config.Brokers = []string{"localhost:9092"}

saramaConfig, _ := config.ToSaramaConfig()
securityConfig.ApplyToSaramaConfig(saramaConfig)

client, _ := kafka.NewClient(config)
```

#### SASL SCRAM (Recommended for Production)

More secure than PLAIN with SHA-256 or SHA-512:

```go
// SCRAM-SHA-256
saslConfig := kafka.NewSASLSCRAMConfig(
    kafka.SASLTypeSCRAMSHA256,
    "username",
    "password",
)

// Or SCRAM-SHA-512 for higher security
saslConfig := kafka.NewSASLSCRAMConfig(
    kafka.SASLTypeSCRAMSHA512,
    "username",
    "password",
)

securityConfig := kafka.DefaultSecurityConfig()
securityConfig.SASL = saslConfig
```

#### SASL GSSAPI (Kerberos)

Enterprise authentication with Kerberos:

```go
saslConfig := kafka.NewSASLGSSAPIConfig(
    "kafka",                        // service name
    "EXAMPLE.COM",                  // realm
    "user@EXAMPLE.COM",             // username
    "password",                     // password
    "/etc/security/user.keytab",    // keytab path
    "/etc/krb5.conf",               // kerberos config
)

securityConfig := kafka.DefaultSecurityConfig()
securityConfig.SASL = saslConfig
```

#### SASL OAuth Bearer

Modern token-based authentication:

```go
saslConfig := kafka.NewSASLOAuthConfig(func() (string, error) {
    // Get token from your OAuth provider
    token, err := oauthClient.GetAccessToken()
    return token, err
})

// Optional: Add extensions
saslConfig.OAuth.Extensions = map[string]string{
    "logicalCluster": "cluster-1",
    "identityPoolId": "pool-123",
}

securityConfig := kafka.DefaultSecurityConfig()
securityConfig.SASL = saslConfig
```

### TLS/SSL Encryption

#### Basic TLS

Server certificate validation:

```go
tlsConfig := kafka.NewTLSConfig(
    "",                    // no client cert
    "",                    // no client key
    "/path/to/ca.crt",     // CA certificate
)

securityConfig := kafka.DefaultSecurityConfig()
securityConfig.TLS = tlsConfig
```

#### Mutual TLS (mTLS)

Client and server authentication:

```go
tlsConfig := kafka.NewMutualTLSConfig(
    "/path/to/client.crt",
    "/path/to/client.key",
    "/path/to/ca.crt",
)

// Optional: Set server name for SNI
tlsConfig.ServerName = "kafka.example.com"

// Optional: Set minimum TLS version
tlsConfig.MinVersion = tls.VersionTLS13

securityConfig := kafka.DefaultSecurityConfig()
securityConfig.TLS = tlsConfig
```

#### TLS with In-Memory Certificates

Use certificate data instead of files:

```go
certData, _ := os.ReadFile("/path/to/client.crt")
keyData, _ := os.ReadFile("/path/to/client.key")
caData, _ := os.ReadFile("/path/to/ca.crt")

tlsConfig := &kafka.TLSConfig{
    Enabled:  true,
    CertData: certData,
    KeyData:  keyData,
    CAData:   caData,
}

securityConfig := kafka.DefaultSecurityConfig()
securityConfig.TLS = tlsConfig
```

### Secure Config Builder

Fluent API for building secure configurations:

```go
config, err := kafka.NewSecureConfigBuilder().
    WithBrokers([]string{"kafka1:9093", "kafka2:9093", "kafka3:9093"}).
    WithConsumerGroup("secure-group").
    WithSASLSCRAM(kafka.SASLTypeSCRAMSHA256, "username", "password").
    WithMutualTLS("/certs/client.crt", "/certs/client.key", "/certs/ca.crt").
    Build()

client, _ := kafka.NewClient(config)
```

### Environment Variable Configuration

Load security config from environment variables:

```bash
# Set environment variables
export KAFKA_SASL_ENABLED=true
export KAFKA_SASL_MECHANISM=SCRAM-SHA-256
export KAFKA_SASL_USERNAME=myuser
export KAFKA_SASL_PASSWORD=mypassword
export KAFKA_TLS_ENABLED=true
export KAFKA_TLS_CERT_FILE=/certs/client.crt
export KAFKA_TLS_KEY_FILE=/certs/client.key
export KAFKA_TLS_CA_FILE=/certs/ca.crt
```

```go
// Load from environment
loader := &kafka.EnvConfigLoader{}
securityConfig := loader.LoadFromEnv()

// Apply to config
config := kafka.DefaultConfig()
saramaConfig, _ := config.ToSaramaConfig()
securityConfig.ApplyToSaramaConfig(saramaConfig)
```

### Complete Secure Setup Example

Production-ready secure Kafka connection:

```go
// Build secure configuration
config, err := kafka.NewSecureConfigBuilder().
    WithBrokers([]string{
        "kafka1.prod.example.com:9093",
        "kafka2.prod.example.com:9093",
        "kafka3.prod.example.com:9093",
    }).
    WithConsumerGroup("payment-processor").
    // SCRAM-SHA-512 authentication
    WithSASLSCRAM(
        kafka.SASLTypeSCRAMSHA512,
        os.Getenv("KAFKA_USERNAME"),
        os.Getenv("KAFKA_PASSWORD"),
    ).
    // Mutual TLS
    WithMutualTLS(
        "/etc/kafka/certs/client.crt",
        "/etc/kafka/certs/client.key",
        "/etc/kafka/certs/ca.crt",
    ).
    Build()

if err != nil {
    log.Fatal(err)
}

// Create client
client, err := kafka.NewClient(config)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Use client securely
producer := kafka.NewProducer(client)
producer.Publish(ctx, "secure-topic", payload)
```

### Security Best Practices

1. **Always use TLS in production**
   - Minimum TLS 1.2, prefer TLS 1.3
   - Use mutual TLS for highest security

2. **Use SCRAM over PLAIN**
   - SCRAM-SHA-256 minimum
   - SCRAM-SHA-512 for sensitive data

3. **Rotate credentials regularly**
   - Implement automated credential rotation
   - Use OAuth for dynamic tokens

4. **Validate certificates**
   - Never use `InsecureSkipVerify` in production
   - Keep CA certificates updated

5. **Use Kerberos for enterprise**
   - GSSAPI integration with AD/LDAP
   - Centralized authentication management

6. **Secure credential storage**
   - Use secrets management (Vault, AWS Secrets Manager)
   - Never commit credentials to code

7. **Network security**
   - Use VPN or private networks
   - Implement network policies
   - Enable broker-level ACLs

---

## Best Practices

### Monitoring
1. Always expose Prometheus metrics on `/metrics`
2. Set up alerts for consumer lag, error rates, and processing latency
3. Use distributed tracing in production for debugging

### Storage
1. Use LevelDB for single-instance stateful processing
2. Use Redis for multi-instance shared state
3. Use BadgerDB for highest performance requirements
4. Always implement backup strategies for persistent storage

### Exactly-Once
1. Use transactional processing for critical data flows
2. Enable idempotence even when not using transactions
3. Set appropriate deduplication windows based on your use case

### Windowing
1. Choose window type based on use case:
   - Tumbling: Regular reports, fixed intervals
   - Sliding: Moving averages, trend analysis
   - Session: User activity, behavioral analysis
2. Store window state for trend analysis
3. Handle late-arriving data appropriately

### Schema Registry
1. Always version your schemas
2. Use FULL compatibility mode when possible
3. Test schema evolution before production deployment
4. Cache schemas to reduce registry load

---

## Performance Tips

1. **Batch Processing**: Use batch consumers for high-throughput scenarios
2. **Compression**: Enable Snappy or LZ4 compression for large messages
3. **Partitioning**: Use partition keys for ordered processing
4. **Storage**: Tune storage backend parameters based on workload
5. **Monitoring**: Use metrics to identify bottlenecks
6. **Tracing**: Sample traces in high-volume scenarios (e.g., 1% sampling)

---

For more examples, see the `examples/` directory in the repository.
