# Kafka Watermill Client Library

A comprehensive Kafka client library for Go, built on top of [Watermill](https://github.com/ThreeDotsLabs/watermill), designed specifically for ecommerce microservices with **Goka-inspired stateful stream processing**.

## Features

### Core Messaging
- **Easy-to-use API** for Kafka producers and consumers
- **Comprehensive configuration** with heartbeat, timeouts, and all Kafka settings
- **Dead Letter Queue (DLQ)** support with automatic retry logic
- **Middleware** for logging, metrics, retry, timeout, circuit breaker, and more
- **Ecommerce-specific** event types and handlers
- **Batch processing** capabilities
- **Graceful shutdown** handling
- **Type-safe** message handling with Go generics

### Stateful Stream Processing (Goka-Inspired)
- **Stateful Processors** with Kafka-backed persistent state
- **Group Tables** for storing state in compacted topics
- **Views** for read-only access to state (perfect for APIs)
- **Context-based State Management** with `Value()` and `SetValue()`
- **Codec System** for flexible serialization (JSON, String, Bytes, Int64)
- **Stream-Table Joins** for enriching streams with reference data
- **Stream-Stream Joins** with time windowing
- **Pluggable Storage** for local caching (in-memory, extensible)
- **Loopback Topics** for self-referencing flows

📖 See [GOKA_FEATURES.md](GOKA_FEATURES.md) for detailed documentation on stateful processing features.
📖 See [BATCH_PROCESSING.md](BATCH_PROCESSING.md) for detailed documentation on batch processing.

## Installation

```bash
go get github.com/templatedop/watermill
```

## Quick Start

### Basic Producer

```go
package main

import (
    "context"
    "log"

    "github.com/templatedop/watermill/pkg/kafka"
)

func main() {
    // Create configuration
    config := kafka.EcommerceConfig(
        []string{"localhost:9092"},
        "my-consumer-group",
    )

    // Create client
    client, err := kafka.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Create producer
    producer := kafka.NewEcommerceProducer(client)

    // Publish an event
    order := kafka.OrderEvent{
        OrderID:    "ORD-123",
        CustomerID: "CUST-456",
        Status:     "pending",
        Total:      99.99,
    }

    ctx := context.Background()
    if err := producer.PublishOrderCreated(ctx, order); err != nil {
        log.Fatal(err)
    }
}
```

### Basic Consumer

```go
package main

import (
    "context"
    "log"

    "github.com/templatedop/watermill/pkg/kafka"
)

func main() {
    // Create configuration
    config := kafka.EcommerceConfig(
        []string{"localhost:9092"},
        "my-consumer-group",
    )

    // Create client
    client, err := kafka.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Create consumer
    consumer := kafka.NewEcommerceConsumer(client)

    ctx := context.Background()

    // Subscribe to events
    err = consumer.SubscribeOrderCreated(ctx, func(ctx context.Context, order kafka.OrderEvent) error {
        log.Printf("Order created: %s", order.OrderID)
        // Process order...
        return nil
    })

    if err != nil {
        log.Fatal(err)
    }
}
```

## Configuration

### Default Configuration

```go
config := kafka.DefaultConfig()
config.Brokers = []string{"localhost:9092"}
config.ConsumerGroup = "my-group"
```

### Ecommerce Configuration

Pre-configured settings optimized for ecommerce workloads:

```go
config := kafka.EcommerceConfig(
    []string{"localhost:9092"},
    "order-service-group",
)
```

### Custom Configuration

```go
config := &kafka.Config{
    Brokers:       []string{"localhost:9092"},
    ConsumerGroup: "my-group",
    ClientID:      "my-client",
    Version:       "3.6.0",

    Producer: kafka.ProducerConfig{
        RequiredAcks:    sarama.WaitForAll,
        Compression:     sarama.CompressionSnappy,
        MaxRetries:      5,
        Idempotent:      true,
        Timeout:         10 * time.Second,
    },

    Consumer: kafka.ConsumerConfig{
        SessionTimeout:    20 * time.Second,
        HeartbeatInterval: 3 * time.Second,
        MaxProcessingTime: 30 * time.Second,
        AutoCommit:        true,
    },

    DLQ: kafka.DLQConfig{
        Enabled:            true,
        Topic:              "dlq",
        MaxRetries:         3,
        RetryDelay:         5 * time.Second,
        ExponentialBackoff: true,
    },
}
```

## Dead Letter Queue (DLQ)

### Automatic DLQ Handling

Messages that fail after max retries are automatically sent to the DLQ:

```go
config := kafka.EcommerceConfig(brokers, group)
config.DLQ.Enabled = true
config.DLQ.MaxRetries = 3
config.DLQ.RetryDelay = 5 * time.Second
config.DLQ.ExponentialBackoff = true

client, _ := kafka.NewClient(config)
```

### Processing DLQ Messages

```go
dlqConsumer := kafka.NewDLQConsumer(client)

err := dlqConsumer.Subscribe(ctx, func(ctx context.Context, dlqMsg kafka.DLQMessage) error {
    log.Printf("Failed message from topic: %s", dlqMsg.OriginalTopic)
    log.Printf("Error: %s", dlqMsg.Error)
    log.Printf("Retry count: %d", dlqMsg.RetryCount)

    // Option 1: Archive for later analysis
    dlqConsumer.Archive(ctx, dlqMsg, "failed-orders-archive")

    // Option 2: Retry the message
    if shouldRetry(dlqMsg) {
        dlqConsumer.Retry(ctx, dlqMsg)
    }

    return nil
})
```

## Middleware

The library includes several useful middleware:

### Retry Middleware

```go
router.AddMiddleware(
    kafka.NewRetryMiddleware(3, 1*time.Second, true),
)
```

### Timeout Middleware

```go
router.AddMiddleware(
    kafka.NewTimeoutMiddleware(30 * time.Second),
)
```

### Duplicate Detection

```go
router.AddMiddleware(
    kafka.NewDuplicateDetectionMiddleware(5 * time.Minute),
)
```

### Throttling

```go
router.AddMiddleware(
    kafka.NewThrottleMiddleware(100), // Max 100 msg/sec
)
```

### Circuit Breaker

```go
router.AddMiddleware(
    kafka.NewCircuitBreakerMiddleware(10, 1*time.Minute),
)
```

### Logging

```go
router.AddMiddleware(
    kafka.NewLoggingMiddleware(logger),
)
```

### Metrics

```go
router.AddMiddleware(
    kafka.NewMetricsMiddleware(),
)
```

## Ecommerce Event Types

### Order Events

```go
// Publish
producer.PublishOrderCreated(ctx, order)
producer.PublishOrderUpdated(ctx, order)
producer.PublishOrderCancelled(ctx, orderID, reason)

// Subscribe
consumer.SubscribeOrderCreated(ctx, handler)
consumer.SubscribeOrderUpdated(ctx, handler)
```

### Payment Events

```go
// Publish
producer.PublishPaymentProcessed(ctx, payment)
producer.PublishPaymentFailed(ctx, payment, reason)

// Subscribe
consumer.SubscribePaymentProcessed(ctx, handler)
```

### Inventory Events

```go
// Publish
producer.PublishInventoryReserved(ctx, productID, quantity)
producer.PublishInventoryReleased(ctx, productID, quantity)

// Subscribe
consumer.SubscribeInventoryReserved(ctx, handler)
```

### Shipment Events

```go
// Publish
producer.PublishShipmentCreated(ctx, shipment)
producer.PublishShipmentDelivered(ctx, shipment)

// Subscribe
consumer.SubscribeShipmentCreated(ctx, handler)
```

## Advanced Usage

### Router with Multiple Handlers

```go
router, _ := client.CreateRouter()

// Add handlers
consumer := kafka.NewEcommerceConsumer(client)

router.AddNoPublisherHandler(
    "order_handler",
    "orders.created",
    client.GetSubscriber(),
    func(msg *message.Message) error {
        // Process message
        return nil
    },
)

router.AddHandler(
    "order_to_inventory",
    "orders.created",
    client.GetSubscriber(),
    "inventory.reserved",
    client.GetPublisher(),
    func(msg *message.Message) ([]*message.Message, error) {
        // Transform and forward message
        return []*message.Message{newMsg}, nil
    },
)

// Run router
ctx := context.Background()
router.Run(ctx)
```

### Batch Processing

```go
batchConsumer := kafka.NewBatchConsumer(
    client,
    100,              // batch size
    5 * time.Second,  // batch timeout
)

batchConsumer.SubscribeBatch(ctx, "orders.created", func(ctx context.Context, messages []*message.Message) error {
    // Process batch
    log.Printf("Processing batch of %d messages", len(messages))
    return nil
})
```

### Partition Keys

```go
// All messages with same key go to same partition
producer.PublishWithKey(ctx, "orders.created", customerID, order)
```

## Configuration Details

### Heartbeat Configuration

The consumer heartbeat is configured through:

```go
config.Consumer.HeartbeatInterval = 3 * time.Second   // How often to send heartbeats
config.Consumer.SessionTimeout = 20 * time.Second     // Max time without heartbeat before rebalance
config.Consumer.RebalanceTimeout = 60 * time.Second   // Max time for rebalance operation
```

**Important**: `HeartbeatInterval` must be less than `SessionTimeout`

### Producer Reliability

```go
config.Producer.RequiredAcks = sarama.WaitForAll  // Wait for all replicas
config.Producer.Idempotent = true                  // Prevent duplicates
config.Producer.MaxRetries = 5                     // Retry failed sends
config.Producer.RetryBackoff = 100 * time.Millisecond
```

### Consumer Processing

```go
config.Consumer.MaxProcessingTime = 30 * time.Second  // Max time to process one message
config.Consumer.FetchMin = 1                          // Min bytes per fetch
config.Consumer.FetchDefault = 1024 * 1024           // Default bytes (1MB)
config.Consumer.FetchMax = 10 * 1024 * 1024          // Max bytes (10MB)
config.Consumer.MaxWaitTime = 500 * time.Millisecond // Max wait for fetch
```

### Offset Management

```go
// Auto-commit
config.Consumer.AutoCommit = true
config.Consumer.OffsetCommitInterval = 1 * time.Second

// Manual commit (for exactly-once processing)
config.Consumer.AutoCommit = false
// Then manually ack/nack messages
msg.Ack()  // or msg.Nack()
```

## Examples

Check the `examples/` directory for complete working examples:

### Basic Examples
- `examples/ecommerce/` - Full ecommerce microservice example
- `examples/producer/` - Producer-only example
- `examples/consumer/` - Consumer with router and middleware
- `examples/dlq/` - Dead letter queue handling

### Stateful Processing Examples (Goka-Inspired)
- `examples/stateful-processor/` - Stateful order statistics aggregation
- `examples/view/` - HTTP API for querying state from group tables
- `examples/join/` - Stream-table join for order enrichment

### Advanced Processing Examples
- `examples/batch-consumer/` - Advanced batch processing with metrics and error handling

## Running Examples

```bash
# Start Kafka (using Docker)
docker-compose up -d

# Run producer example
go run examples/producer/main.go

# Run consumer example
go run examples/consumer/main.go

# Run full ecommerce example
go run examples/ecommerce/main.go

# Run DLQ example
go run examples/dlq/main.go
```

## Best Practices

1. **Use consumer groups** for horizontal scaling
2. **Enable idempotent producer** for exactly-once semantics
3. **Configure DLQ** for failed message handling
4. **Use partition keys** for ordered processing
5. **Set appropriate timeouts** based on your workload
6. **Monitor heartbeats** and session timeouts
7. **Use middleware** for cross-cutting concerns
8. **Handle errors gracefully** with retry logic
9. **Implement graceful shutdown** for clean consumer rebalancing
10. **Use structured logging** for better observability

## Error Handling

All errors are typed for easy handling:

```go
err := client.Publish(ctx, topic, msg)
switch e := err.(type) {
case kafka.ErrPublishFailed:
    log.Printf("Failed to publish to %s: %v", e.Topic, e.Err)
case kafka.ErrMaxRetriesExceeded:
    log.Printf("Max retries exceeded: %d attempts", e.Attempts)
case kafka.ErrClientClosed:
    log.Println("Client is closed")
}
```

## Performance Considerations

- **Batch size**: Larger batches improve throughput but increase latency
- **Compression**: Use Snappy or LZ4 for good balance of speed and compression
- **Fetch size**: Tune based on message size and network bandwidth
- **Connection pooling**: Watermill handles connection pooling internally
- **Partition count**: More partitions = more parallelism but more overhead

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.
