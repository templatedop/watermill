# Watermill Kafka Client Library

This repository contains **TWO separate Kafka client implementations** for Go:

## 🚀 Franz-go Standalone Implementation (NEW - Recommended)

**Location:** `pkg/franzgo/`

A **standalone high-performance Kafka client** using [franz-go](https://github.com/twmb/franz-go) - **does NOT use Watermill**.

### Why Franz-go?
- ✅ **5-10x faster** than sarama
- ✅ **50-70% lower memory** usage
- ✅ **Modern API** with better error handling
- ✅ **All enterprise features** built-in
- ✅ **Production-ready** with comprehensive testing

### Quick Start (Franz-go)

```go
import "github.com/templatedop/watermill/pkg/franzgo"

// Create config
config := franzgo.NewConfigBuilder().
    WithBrokers([]string{"localhost:9092"}).
    WithConsumerGroup("my-group").
    WithClientID("my-client").
    Build()

// Create client
client, _ := franzgo.NewClient(config)
defer client.Close()

// Produce
producer := franzgo.NewProducer(client)
producer.Produce(ctx, "my-topic", []byte("key"), []byte("value"))

// Consume
consumer := franzgo.NewConsumer(client, nil)
handler := func(ctx context.Context, record *kgo.Record) error {
    log.Printf("Received: %s = %s", record.Key, record.Value)
    return nil
}
consumer.Consume(ctx, []string{"my-topic"}, handler)
```

### Franz-go Features

**Complete documentation:** See [FRANZ_GO_README.md](FRANZ_GO_README.md)

#### Core Features (Phase 1-2) ✅
- Configuration with builder pattern & SASL/TLS
- High-performance producer (sync/async)
- Consumer with built-in DLQ support
- Batch processing with 4 error strategies
- 9 middleware types (logging, metrics, retry, timeout, circuit breaker, etc.)

#### Stream Processing (Phase 3) ✅
- Stream transformations (map/filter/flatMap)
- Exactly-once semantics with transactions
- Windowing operations (tumbling/sliding/session)
- Async transformers with worker pools

#### Enterprise Observability (Phase 4) ✅
- Prometheus metrics (30+ metrics)
- OpenTelemetry distributed tracing
- 4 storage backends (Memory, LevelDB, Redis, BadgerDB)
- Schema registry (Avro/Protobuf with Confluent)

#### Production Readiness (Phase 5) ✅
- Health checks & Kubernetes probes (liveness/readiness)
- Graceful shutdown with signal handling
- Shutdown-aware components
- **100+ unit tests**
- **8 integration tests**
- **30+ benchmark tests**

#### Advanced Features (Phase 6) ✅
- **Stateful processing** - Goka-inspired state management
- **Admin API** - Complete cluster administration (15+ operations)
- **Rebalance listeners** - 8 listener types for rebalance events
- **Performance benchmarks** - Comprehensive performance test suite
- **Migration guide** - Complete guide for migrating from sarama

### Franz-go Examples

See `examples/franzgo/`:
- `main.go` - Phase 2 examples (producer, consumer, batch, middleware)
- `phase3_examples.go` - Stream processing examples
- `phase4_examples.go` - Observability and storage examples
- `phase5_examples.go` - Health checks and graceful shutdown

### Franz-go Migration

**Migrating from Sarama?** See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for a complete guide with side-by-side code comparisons.

---

## 📦 Original Watermill + Sarama Implementation

**Location:** `pkg/kafka/`

The **original implementation** built on top of [Watermill](https://github.com/ThreeDotsLabs/watermill) using Shopify/sarama.

### When to Use Watermill Implementation?

- ✅ You're already using Watermill in your project
- ✅ You need Watermill's router and middleware system
- ✅ You have existing Watermill code to maintain

### Quick Start (Watermill)

```go
import "github.com/templatedop/watermill/pkg/kafka"

// Create config
config := kafka.EcommerceConfig(
    []string{"localhost:9092"},
    "my-consumer-group",
)

// Create client
client, _ := kafka.NewClient(config)
defer client.Close()

// Create producer
producer := kafka.NewEcommerceProducer(client)

// Publish event
order := kafka.OrderEvent{
    OrderID:    "ORD-123",
    CustomerID: "CUST-456",
    Status:     "pending",
    Total:      99.99,
}
producer.PublishOrderCreated(ctx, order)

// Create consumer
consumer := kafka.NewEcommerceConsumer(client)
consumer.SubscribeOrderCreated(ctx, func(ctx context.Context, order kafka.OrderEvent) error {
    log.Printf("Order created: %s", order.OrderID)
    return nil
})
```

### Watermill Features

- Easy-to-use Watermill API
- Ecommerce-specific event types
- DLQ support with retry logic
- Middleware (retry, timeout, throttle, circuit breaker)
- Goka-inspired stateful processing
- Batch processing

**Documentation:** See original README sections below for detailed Watermill documentation.

---

## 📊 Comparison: Franz-go vs Watermill

| Feature | Franz-go (pkg/franzgo) | Watermill (pkg/kafka) |
|---------|------------------------|----------------------|
| **Performance** | 5-10x faster | Baseline |
| **Memory** | 50-70% lower | Baseline |
| **API** | Standalone, franz-go native | Watermill interfaces |
| **Dependencies** | Only franz-go | Watermill + sarama |
| **Testing** | 138+ tests | Limited |
| **Health Checks** | ✅ Built-in | ❌ Manual |
| **Graceful Shutdown** | ✅ Built-in | ⚠️ Basic |
| **Admin API** | ✅ Complete (15+ ops) | ❌ None |
| **Stateful Processing** | ✅ Changelog-based | ✅ Goka-style |
| **Observability** | ✅ Prometheus + OTel | ⚠️ Basic |
| **Migration Guide** | ✅ From sarama | N/A |

## 🎯 Which Should You Choose?

### Choose Franz-go (`pkg/franzgo/`) if:
- ✅ Starting a new project
- ✅ Need maximum performance
- ✅ Want enterprise features (health checks, admin API, stateful processing)
- ✅ Don't need Watermill compatibility

### Choose Watermill (`pkg/kafka/`) if:
- ✅ Already using Watermill
- ✅ Need Watermill router and middleware
- ✅ Have existing Watermill code

## 📚 Documentation

- **Franz-go:** [FRANZ_GO_README.md](FRANZ_GO_README.md) - Complete documentation
- **Migration:** [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) - Sarama to franz-go migration
- **Watermill:** See sections below for original Watermill documentation

## 🚀 Installation

```bash
go get github.com/templatedop/watermill
```

**Franz-go only:**
```go
import "github.com/templatedop/watermill/pkg/franzgo"
```

**Watermill only:**
```go
import "github.com/templatedop/watermill/pkg/kafka"
```

## 📦 Repository Structure

```
watermill/
├── pkg/
│   ├── franzgo/           # Standalone franz-go implementation (NEW)
│   │   ├── config.go
│   │   ├── client.go
│   │   ├── producer.go
│   │   ├── consumer.go
│   │   ├── batch.go
│   │   ├── middleware.go
│   │   ├── transform.go
│   │   ├── window.go
│   │   ├── transaction.go
│   │   ├── metrics.go
│   │   ├── tracing.go
│   │   ├── storage.go
│   │   ├── health.go
│   │   ├── shutdown.go
│   │   ├── stateful.go
│   │   ├── admin.go
│   │   ├── rebalance.go
│   │   └── schema/
│   │       ├── registry.go
│   │       ├── avro.go
│   │       └── protobuf.go
│   │
│   └── kafka/             # Original Watermill implementation
│       ├── config.go
│       ├── client.go
│       ├── producer.go
│       └── ...
│
├── examples/
│   ├── franzgo/           # Franz-go examples
│   │   ├── main.go
│   │   ├── phase3_examples.go
│   │   ├── phase4_examples.go
│   │   └── phase5_examples.go
│   │
│   └── ecommerce/         # Watermill examples
│
├── FRANZ_GO_README.md     # Franz-go documentation
├── MIGRATION_GUIDE.md     # Sarama → franz-go migration guide
└── README.md              # This file
```

## 🧪 Testing

### Franz-go Tests

```bash
# Unit tests
go test -v ./pkg/franzgo/

# Integration tests (requires Kafka)
go test -tags=integration -v ./pkg/franzgo/

# Benchmarks
go test -bench=. -benchmem ./pkg/franzgo/

# Performance tests
go test -tags=performance -bench=BenchmarkPerformance -benchtime=30s ./pkg/franzgo/
```

---

# Original Watermill Documentation

The sections below document the original Watermill-based implementation in `pkg/kafka/`.

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

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.
