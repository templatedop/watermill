# Goka-Inspired Features

This document describes the Goka-inspired stateful stream processing features added to the Watermill Kafka client library.

## Overview

[Goka](https://github.com/lovoo/goka) is a powerful stream processing library that extends Kafka consumer groups with persistent state tables. We've implemented similar features in our Watermill-based library while maintaining compatibility with Watermill's ecosystem.

## Comparison: Goka vs This Implementation

| Feature | Goka | This Library | Status |
|---------|------|--------------|--------|
| **Stateful Processors** | ✅ | ✅ | Implemented |
| **Group Tables** | ✅ | ✅ | Implemented |
| **Views** | ✅ | ✅ | Implemented |
| **Context with State** | ✅ | ✅ | Implemented |
| **Codec System** | ✅ | ✅ | Implemented |
| **Join Operations** | ✅ | ✅ | Implemented |
| **Local Storage** | ✅ LevelDB | ✅ In-Memory (Pluggable) | Implemented |
| **Loopback Topics** | ✅ | ✅ | Implemented |
| **Stream-Table Join** | ✅ | ✅ | Implemented |
| **Stream-Stream Join** | ✅ | ✅ | Implemented |
| **Web Monitoring** | ✅ | ❌ | Not yet |
| **Typed Generics** | ❌ | ✅ | Enhanced! |

## Key Components

### 1. Codec System

Flexible serialization/deserialization for different data types.

```go
// Built-in codecs
jsonCodec := kafka.NewJSONCodec()
stringCodec := kafka.NewStringCodec()
bytesCodec := kafka.NewBytesCodec()
int64Codec := kafka.NewInt64Codec()

// Codec registry
registry := kafka.DefaultCodecRegistry()
registry.Register("custom", myCodec)
```

### 2. Storage Abstraction

Pluggable local storage for caching state.

```go
// In-memory storage (default)
storage := kafka.NewMemoryStorage()

// Partitioned storage
partStorage := kafka.NewPartitionedStorage(partitionID, storage)

// Storage operations
storage.Set("key", []byte("value"))
value, _ := storage.Get("key")
exists, _ := storage.Has("key")
```

### 3. Processor Context

Access to state and message within processors.

```go
type ProcessorContext struct {
    // State management
    Value() (interface{}, error)
    SetValue(value interface{}) error
    Delete() error

    // Messaging
    Emit(topic, key string, value interface{}) error
    Loopback(key string, value interface{}) error

    // Joins
    Join(table string) (interface{}, error)
}

// Typed version (generics)
type TypedProcessorContext[T any] struct {
    Value() (*T, error)
    SetValue(value T) error
}
```

### 4. Stateful Processors

Process streams with persistent state backed by Kafka.

```go
processor, err := kafka.NewTypedProcessor[OrderStats](
    client,
    &kafka.ProcessorConfig{
        Topic:      "orders.created",
        GroupTable: "order-stats-table",  // Compacted topic
        Codec:      kafka.NewJSONCodec(),
        Storage:    kafka.NewMemoryStorage(),
    },
    func(ctx *kafka.TypedProcessorContext[OrderStats]) error {
        // Get current state
        stats, _ := ctx.Value()

        // Update state
        stats.TotalOrders++

        // Save state
        return ctx.SetValue(*stats)
    },
)

processor.Start(ctx)
```

### 5. Views

Read-only access to group tables for querying state.

```go
view, err := kafka.NewTypedView[OrderStats](
    client,
    "order-stats-table",
    kafka.NewJSONCodec(),
    kafka.NewMemoryStorage(),
)

view.Start(ctx)
view.WaitReady()

// Query state
stats, _ := view.Get("CUST-123")

// Iterate all
it, _ := view.Iterator()
for it.Next() {
    key := it.Key()
    value := it.Value()
}
```

### 6. Join Operations

#### Stream-Table Join

Join streaming events with a table of reference data.

```go
joiner, err := kafka.NewStreamJoiner(
    client,
    &kafka.JoinConfig{
        LeftTopic:   "orders.created",      // Stream
        RightTopic:  "customers-table",     // Table
        OutputTopic: "orders.enriched",
        JoinType:    kafka.LeftJoin,
        Codec:       kafka.NewJSONCodec(),
    },
    func(ctx context.Context, key string, order, customer interface{}) (interface{}, error) {
        // Enrich order with customer data
        enriched := EnrichOrder(order, customer)
        return enriched, nil
    },
)

joiner.Start(ctx)
```

#### Stream-Stream Join

Join two streams with time windowing.

```go
joiner, err := kafka.NewStreamStreamJoiner(
    client,
    &kafka.JoinConfig{
        LeftTopic:   "clicks",
        RightTopic:  "impressions",
        OutputTopic: "click-through-rate",
        JoinType:    kafka.InnerJoin,
        Window:      5 * time.Minute,  // Time window
        Codec:       kafka.NewJSONCodec(),
    },
    joinFunc,
)
```

## Use Cases

### 1. Customer Lifetime Value Tracking

Track aggregate statistics per customer:

```go
processor, _ := kafka.NewTypedProcessor[CustomerStats](
    client,
    &kafka.ProcessorConfig{
        Topic:      "orders.created",
        GroupTable: "customer-ltv-table",
    },
    func(ctx *kafka.TypedProcessorContext[CustomerStats]) error {
        stats, _ := ctx.Value()
        if stats == nil {
            stats = &CustomerStats{CustomerID: ctx.Key()}
        }

        // Decode order
        var order Order
        kafka.NewJSONCodec().Decode(ctx.Message().Payload, &order)

        // Update stats
        stats.TotalOrders++
        stats.TotalSpent += order.Total

        return ctx.SetValue(*stats)
    },
)
```

### 2. Order Enrichment with Customer Data

Join orders with customer information:

```go
joiner, _ := kafka.NewStreamJoiner(
    client,
    &kafka.JoinConfig{
        LeftTopic:   "orders",
        RightTopic:  "customers",
        OutputTopic: "orders-enriched",
        JoinType:    kafka.LeftJoin,
    },
    func(ctx context.Context, key string, order, customer interface{}) (interface{}, error) {
        enriched := EnrichedOrder{
            Order:        order,
            CustomerName: customer.Name,
            CustomerTier: customer.Tier,
        }

        // Apply tier discounts
        enriched.ApplyDiscount()

        return enriched, nil
    },
)
```

### 3. Real-time Analytics API

Expose aggregated state via HTTP API:

```go
view, _ := kafka.NewTypedView[Analytics](client, "analytics-table", ...)
view.Start(ctx)

http.HandleFunc("/analytics/{id}", func(w http.ResponseWriter, r *http.Request) {
    id := extractID(r)
    analytics, _ := view.Get(id)
    json.NewEncoder(w).Encode(analytics)
})
```

### 4. Session Aggregation

Aggregate user sessions:

```go
processor, _ := kafka.NewTypedProcessor[Session](
    client,
    &kafka.ProcessorConfig{
        Topic:      "user-events",
        GroupTable: "sessions-table",
    },
    func(ctx *kafka.TypedProcessorContext[Session]) error {
        session, _ := ctx.Value()

        var event UserEvent
        kafka.NewJSONCodec().Decode(ctx.Message().Payload, &event)

        // Update session
        session.Events = append(session.Events, event)
        session.LastActivity = time.Now()

        // Emit session end if inactive
        if session.IsInactive() {
            ctx.Emit("sessions-ended", ctx.Key(), session)
        }

        return ctx.SetValue(*session)
    },
)
```

### 5. Fraud Detection with Pattern Matching

Detect fraud patterns across events:

```go
processor, _ := kafka.NewTypedProcessor[UserActivity](
    client,
    &kafka.ProcessorConfig{
        Topic:      "transactions",
        GroupTable: "user-activity-table",
        Joins: map[string]string{
            "user-profiles": "user-profiles-table",
        },
    },
    func(ctx *kafka.TypedProcessorContext[UserActivity]) error {
        activity, _ := ctx.Value()
        profile, _ := ctx.Join("user-profiles")

        // Analyze transaction pattern
        if activity.IsSuspicious(profile) {
            alert := FraudAlert{...}
            ctx.Emit("fraud-alerts", ctx.Key(), alert)
        }

        return ctx.SetValue(*activity)
    },
)
```

## Configuration

### Group Table Setup

Group tables must use Kafka's log compaction:

```bash
kafka-topics --create \
  --topic order-stats-table \
  --partitions 10 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=100
```

### Processor Configuration

```go
config := &kafka.ProcessorConfig{
    Topic:      "input-topic",
    GroupTable: "state-table",
    Codec:      kafka.NewJSONCodec(),
    Storage:    kafka.NewMemoryStorage(),

    // Optional: Join other tables
    Joins: map[string]string{
        "customers": "customers-table",
        "products":  "products-table",
    },

    // Optional: Loopback for iterative processing
    LoopbackTopic: "input-topic-loopback",

    // Optional: Output topics
    OutputTopics: []string{"output-topic-1", "output-topic-2"},
}
```

## Best Practices

### 1. State Size Management

- Keep state small and relevant
- Use TTL or cleanup logic for old data
- Consider partitioning strategy

```go
// Cleanup old sessions
if time.Since(session.LastActivity) > 24*time.Hour {
    return ctx.Delete()
}
```

### 2. Idempotency

Processors should be idempotent:

```go
func processOrder(ctx *kafka.TypedProcessorContext[OrderState]) error {
    // Check if already processed
    state, _ := ctx.Value()
    if state != nil && state.Processed {
        return nil  // Skip
    }

    // Process...
    state.Processed = true
    return ctx.SetValue(*state)
}
```

### 3. Error Handling

```go
func processor(ctx *kafka.TypedProcessorContext[State]) error {
    // Transient errors: return error to retry
    if isTransient(err) {
        return err
    }

    // Permanent errors: send to DLQ and continue
    if isPermanent(err) {
        ctx.Emit("dlq", ctx.Key(), ctx.Message())
        return nil
    }

    return nil
}
```

### 4. Testing

```go
func TestProcessor(t *testing.T) {
    storage := kafka.NewMemoryStorage()
    codec := kafka.NewJSONCodec()

    ctx := kafka.NewProcessorContext(
        context.Background(),
        msg,
        storage,
        codec,
        "test-key",
    )

    // Test processor logic
    err := myProcessor(ctx)
    assert.NoError(t, err)

    // Verify state
    value, _ := ctx.Value()
    assert.Equal(t, expected, value)
}
```

## Performance Considerations

1. **Partition Count**: More partitions = better parallelism
2. **State Size**: Keep state small, use compression
3. **Compaction**: Tune `min.cleanable.dirty.ratio`
4. **Local Storage**: Consider persistent storage for large state
5. **Joins**: Stream-table joins are more efficient than stream-stream

## Migrating from Goka

If you're coming from Goka, here are the equivalents:

| Goka | This Library |
|------|--------------|
| `goka.Processor` | `kafka.Processor` |
| `goka.View` | `kafka.View` |
| `goka.Context` | `kafka.ProcessorContext` |
| `goka.Codec` | `kafka.Codec` |
| `context.Value()` | `ctx.Value()` |
| `context.SetValue()` | `ctx.SetValue()` |
| `context.Join()` | `ctx.Join()` |
| `context.Emit()` | `ctx.Emit()` |
| `context.Loopback()` | `ctx.Loopback()` |

## Examples

See the `examples/` directory:

- `examples/stateful-processor/` - Stateful order statistics
- `examples/view/` - HTTP API for querying state
- `examples/join/` - Stream-table join for order enrichment

## Future Enhancements

- [ ] LevelDB storage backend
- [ ] Redis storage backend
- [ ] Web monitoring dashboard
- [ ] Query API for views
- [ ] State snapshots and restore
- [ ] Windowed aggregations
- [ ] Exactly-once semantics
