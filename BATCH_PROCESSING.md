# Batch Processing

This library provides advanced batch processing capabilities for Kafka consumers, allowing you to efficiently process messages in batches with various error handling strategies, metrics, and partitioning options.

## Overview

Batch processing is essential for:
- **High throughput** - Process multiple messages at once
- **Database efficiency** - Batch inserts/updates
- **API rate limiting** - Group API calls
- **Cost optimization** - Reduce per-operation costs
- **Latency vs throughput tradeoff** - Control processing delays

## Features

### 1. **Simple Batch Consumer** (Basic)

Located in `pkg/kafka/consumer.go`, provides basic batching:

```go
batchConsumer := kafka.NewBatchConsumer(client, 100, 5*time.Second)

batchConsumer.SubscribeBatch(ctx, "orders", func(ctx context.Context, messages []*message.Message) error {
    // Process batch
    for _, msg := range messages {
        // Process message
    }
    return nil
})
```

### 2. **Advanced Batch Consumer** (Recommended)

Located in `pkg/kafka/batch.go`, provides advanced features:

```go
config := kafka.DefaultBatchConfig()
config.MaxBatchSize = 100
config.BatchTimeout = 5 * time.Second
config.ErrorStrategy = kafka.BatchErrorStrategySkipErrors
config.EnableMetrics = true

batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)

batchConsumer.SubscribeBatch(ctx, "orders", func(ctx context.Context, batch *kafka.MessageBatch) error {
    log.Printf("Processing %d messages (%d bytes)", len(batch.Messages), batch.TotalBytes)

    // Process batch
    return bulkInsert(batch.Messages)
})
```

## Configuration Options

### BatchConfig

```go
type BatchConfig struct {
    // MaxBatchSize is the maximum number of messages per batch
    MaxBatchSize int  // Default: 100

    // MaxBatchBytes is the maximum total bytes per batch
    MaxBatchBytes int64  // Default: 10MB

    // BatchTimeout triggers batch processing after this duration
    BatchTimeout time.Duration  // Default: 5s

    // ErrorStrategy determines how to handle errors
    ErrorStrategy BatchErrorStrategy

    // PartitionByKey groups messages by key for ordered processing
    PartitionByKey bool  // Default: false

    // MaxRetries for individual messages (with BatchErrorStrategyRetryFailed)
    MaxRetries int  // Default: 3

    // RetryDelay between retries
    RetryDelay time.Duration  // Default: 1s

    // EnableMetrics enables batch metrics collection
    EnableMetrics bool  // Default: true
}
```

## Error Handling Strategies

### 1. **Fail Fast** (Default for Simple)

Stops on first error and nacks all messages in batch:

```go
config.ErrorStrategy = kafka.BatchErrorStrategyFailFast
```

**Use when**: All messages must succeed together (transactions)

### 2. **Skip Errors**

Continues processing and acks only successful messages:

```go
config.ErrorStrategy = kafka.BatchErrorStrategySkipErrors
```

**Use when**: Independent messages, some failures acceptable

### 3. **All or Nothing**

Processes all, but only acks if all succeed:

```go
config.ErrorStrategy = kafka.BatchErrorStrategyAllOrNothing
```

**Use when**: Need to try all messages but atomic commit required

### 4. **Retry Failed** (Recommended)

Retries failed messages individually with backoff:

```go
config.ErrorStrategy = kafka.BatchErrorStrategyRetryFailed
config.MaxRetries = 3
config.RetryDelay = 1 * time.Second
```

**Use when**: Transient errors expected, want automatic retry

## Batching Strategies

### Time-based Batching

Process after timeout, even if batch not full:

```go
config.MaxBatchSize = 100
config.BatchTimeout = 5 * time.Second
// Processes every 5s OR when 100 messages accumulated
```

### Size-based Batching

Process when message count or bytes limit reached:

```go
config.MaxBatchSize = 1000
config.MaxBatchBytes = 10 * 1024 * 1024  // 10MB
// Processes at 1000 messages OR 10MB, whichever first
```

### Partitioned Batching

Group messages by partition key for ordered processing:

```go
config.PartitionByKey = true
// Messages with same key are batched together
```

This ensures:
- Ordered processing per key
- Parallel processing across keys
- Useful for aggregations per customer/user/session

## Metrics

Enable metrics to monitor batch performance:

```go
config.EnableMetrics = true

batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)

// Get metrics periodically
metrics := batchConsumer.GetMetrics()
stats := metrics.GetStats()

fmt.Printf("Total Batches: %v\n", stats["total_batches"])
fmt.Printf("Avg Batch Size: %.2f\n", stats["avg_batch_size"])
fmt.Printf("Success Rate: %.2f%%\n", stats["success_rate_percent"])
fmt.Printf("Throughput: %.2f msg/sec\n", stats["throughput_messages_sec"])
```

Available metrics:
- `total_batches` - Total number of batches processed
- `total_messages` - Total messages processed
- `total_bytes` - Total bytes processed
- `avg_batch_size` - Average messages per batch
- `avg_processing_time_ms` - Average batch processing time
- `successful_batches` - Number of successful batches
- `failed_batches` - Number of failed batches
- `success_rate_percent` - Success rate percentage
- `throughput_messages_sec` - Messages per second

## Use Cases

### 1. Bulk Database Inserts

```go
batchConsumer.SubscribeBatch(ctx, "user-events", func(ctx context.Context, batch *kafka.MessageBatch) error {
    // Prepare bulk insert
    events := make([]Event, len(batch.Messages))
    for i, msg := range batch.Messages {
        json.Unmarshal(msg.Payload, &events[i])
    }

    // Bulk insert to database
    return db.BulkInsert("events", events)
})
```

### 2. Batch API Calls

```go
batchConsumer.SubscribeBatch(ctx, "email-notifications", func(ctx context.Context, batch *kafka.MessageBatch) error {
    emails := make([]Email, len(batch.Messages))
    for i, msg := range batch.Messages {
        json.Unmarshal(msg.Payload, &emails[i])
    }

    // Send batch of emails via API
    return emailService.SendBatch(emails)
})
```

### 3. Aggregations per Customer

```go
config := kafka.DefaultBatchConfig()
config.PartitionByKey = true  // Group by customer ID

batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)

batchConsumer.SubscribeBatch(ctx, "purchases", func(ctx context.Context, batch *kafka.MessageBatch) error {
    // Process each customer's purchases together
    for customerID, messages := range batch.Keys {
        total := calculateTotal(messages)
        updateCustomerStats(customerID, total)
    }
    return nil
})
```

### 4. Log Aggregation

```go
batchConsumer.SubscribeBatch(ctx, "application-logs", func(ctx context.Context, batch *kafka.MessageBatch) error {
    // Aggregate logs before writing to storage
    aggregated := aggregateLogs(batch.Messages)

    // Write batch to S3/BigQuery/Elasticsearch
    return storage.WriteBatch(aggregated)
})
```

### 5. ML Feature Engineering

```go
batchConsumer.SubscribeBatch(ctx, "user-clicks", func(ctx context.Context, batch *kafka.MessageBatch) error {
    // Extract features from batch
    features := extractFeatures(batch.Messages)

    // Update feature store in batch
    return featureStore.UpdateBatch(features)
})
```

## Performance Tuning

### Batch Size

**Larger batches**:
- ✅ Higher throughput
- ✅ More efficient DB operations
- ❌ Higher latency
- ❌ More memory usage

**Smaller batches**:
- ✅ Lower latency
- ✅ Less memory usage
- ❌ Lower throughput
- ❌ More DB round trips

**Recommendation**: Start with 100-500 messages, tune based on metrics

### Batch Timeout

**Longer timeout**:
- ✅ Larger batches (better throughput)
- ❌ Higher latency

**Shorter timeout**:
- ✅ Lower latency
- ❌ Smaller batches (lower throughput)

**Recommendation**: 5-30 seconds for most use cases

### Byte Limits

Set `MaxBatchBytes` to prevent out-of-memory:

```go
config.MaxBatchBytes = 50 * 1024 * 1024  // 50MB max
```

**Recommendation**: 10-100MB depending on available memory

### Partitioned Batching

Enable when:
- Order matters per key (customer, session, etc.)
- Want parallel processing across keys
- Doing aggregations per entity

Disable when:
- Order doesn't matter
- Want maximum batch sizes
- Single key dominates traffic

## Comparison with Other Approaches

| Approach | Throughput | Latency | Complexity | Use Case |
|----------|-----------|---------|------------|----------|
| **Single Message** | Low | Lowest | Low | Real-time critical |
| **Simple Batch** | Medium | Medium | Low | Basic batching |
| **Advanced Batch** | High | Medium | Medium | Production systems |
| **Partitioned Batch** | High | Medium | Medium | Ordered processing |
| **Stream Processing** | Highest | Higher | High | Complex transformations |

## Best Practices

### 1. Choose Appropriate Batch Size

```go
// For database inserts
config.MaxBatchSize = 500
config.BatchTimeout = 10 * time.Second

// For API calls with rate limits
config.MaxBatchSize = 100
config.BatchTimeout = 1 * time.Second

// For analytics/logging
config.MaxBatchSize = 10000
config.BatchTimeout = 60 * time.Second
```

### 2. Handle Partial Failures

Use `BatchErrorStrategySkipErrors` or `BatchErrorStrategyRetryFailed`:

```go
config.ErrorStrategy = kafka.BatchErrorStrategyRetryFailed
config.MaxRetries = 3
```

### 3. Monitor Metrics

```go
go func() {
    ticker := time.NewTicker(60 * time.Second)
    for range ticker.C {
        stats := batchConsumer.GetMetrics().GetStats()

        // Alert if success rate drops
        if stats["success_rate_percent"].(float64) < 95.0 {
            alertOps("Batch success rate dropped")
        }

        // Alert if throughput too low
        if stats["throughput_messages_sec"].(float64) < 100 {
            alertOps("Batch throughput too low")
        }
    }
}()
```

### 4. Set Byte Limits

Always set `MaxBatchBytes` to prevent OOM:

```go
config.MaxBatchBytes = int64(runtime.MemoryLimit() / 10)  // 10% of memory
```

### 5. Use Partitioned Batching for Ordering

```go
config.PartitionByKey = true

// Ensure messages have partition key
producer.PublishWithKey(ctx, topic, customerID, order)
```

## Example

See `examples/batch-consumer/` for a complete example demonstrating:
- Simple batch processing
- Error handling strategies
- Partitioned batching
- Metrics collection
- Performance monitoring

## Running the Example

```bash
# Start Kafka
make kafka-up

# Run batch consumer example
make run-batch

# Or directly
go run examples/batch-consumer/main.go
```

## Migration Guide

### From Simple Batch Consumer

```go
// Old
batchConsumer := kafka.NewBatchConsumer(client, 100, 5*time.Second)

// New
config := kafka.DefaultBatchConfig()
config.MaxBatchSize = 100
config.BatchTimeout = 5 * time.Second
batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)
```

### From Single Message Processing

```go
// Old - Process one at a time
consumer.Subscribe(ctx, topic, func(ctx context.Context, msg *message.Message) error {
    return processMessage(msg)
})

// New - Process in batches
config := kafka.DefaultBatchConfig()
batchConsumer := kafka.NewAdvancedBatchConsumer(client, config)
batchConsumer.SubscribeBatch(ctx, topic, func(ctx context.Context, batch *kafka.MessageBatch) error {
    for _, msg := range batch.Messages {
        if err := processMessage(msg); err != nil {
            return err
        }
    }
    return nil
})
```

## Troubleshooting

### Batches Too Small

- Increase `MaxBatchSize`
- Increase `BatchTimeout`
- Check message production rate

### High Memory Usage

- Decrease `MaxBatchSize`
- Set `MaxBatchBytes` limit
- Process batches faster

### High Latency

- Decrease `BatchTimeout`
- Decrease `MaxBatchSize`
- Use `PartitionByKey` for parallelism

### Low Throughput

- Increase `MaxBatchSize`
- Increase `BatchTimeout`
- Optimize batch processing logic
- Check metrics for bottlenecks

## Advanced Topics

### Custom Batching Logic

Extend `AdvancedBatchConsumer` for custom logic:

```go
type CustomBatchConsumer struct {
    *kafka.AdvancedBatchConsumer
}

func (c *CustomBatchConsumer) processWithPriority(batch *kafka.MessageBatch) {
    // Custom priority-based processing
}
```

### Integration with Metrics Systems

Export metrics to Prometheus, DataDog, etc.:

```go
metrics := batchConsumer.GetMetrics()
stats := metrics.GetStats()

prometheus.Gauge("batch_size").Set(stats["avg_batch_size"].(float64))
prometheus.Gauge("batch_success_rate").Set(stats["success_rate_percent"].(float64))
```

### Testing Batch Consumers

```go
func TestBatchProcessing(t *testing.T) {
    config := kafka.DefaultBatchConfig()
    config.MaxBatchSize = 10

    // Use in-memory Kafka or mock
    batchConsumer := kafka.NewAdvancedBatchConsumer(mockClient, config)

    // Test batch processing
    // ...
}
```
