# Franz-go Examples

This directory contains comprehensive examples demonstrating various Kafka patterns and use cases using the high-performance franz-go client library.

## Overview

These examples showcase enterprise-grade Kafka patterns implemented with franz-go, which offers:
- ⚡ High performance and low latency
- 🔒 Built-in support for all Kafka features
- 📊 Production-ready with metrics and health checks
- 🛡️ Strong type safety
- 🚀 Zero-copy reads and efficient memory usage

## Prerequisites

- Go 1.21 or higher
- Kafka 2.1+ (tested with 3.x)
- Running Kafka broker (default: localhost:9092)

## Environment Variables

All examples support the following environment variable:

- `KAFKA_BROKERS`: Comma-separated list of Kafka brokers (default: `localhost:9092`)

Example:
```bash
export KAFKA_BROKERS="kafka1:9092,kafka2:9092,kafka3:9092"
```

## Examples

### 1. Consumer Example (`consumer/`)

**Purpose**: Demonstrates consuming messages from multiple topics with different message types.

**Key Concepts**:
- Multi-topic consumption
- Message routing by topic
- Type-safe message handling
- Error handling
- Graceful shutdown

**Run**:
```bash
cd consumer
go run main.go
```

**Topics**:
- `orders.created` - New order events
- `orders.updated` - Order updates
- `payments.processed` - Payment confirmations
- `payments.failed` - Payment failures
- `inventory.reserved` - Inventory reservations
- `inventory.released` - Inventory releases

### 2. Producer Example (`producer/`)

**Purpose**: Demonstrates producing messages with various strategies.

**Key Concepts**:
- Simple message production
- Batch production
- Partition key usage (for ordering)
- Different event types
- Message flushing
- Compression (Zstandard)

**Run**:
```bash
cd producer
go run main.go
```

**Features**:
- Single message publishing
- Batch publishing with flush
- Partition key for ordering guarantees
- Multiple event types (orders, payments, inventory)

### 3. DLQ (Dead Letter Queue) Example (`dlq/`)

**Purpose**: Demonstrates error handling with the DLQ pattern.

**Key Concepts**:
- Dead letter queue pattern
- Retry logic with exponential backoff
- Error categorization
- Failed message tracking
- DLQ consumer for monitoring

**Run**:
```bash
cd dlq
go run main.go
```

**Topics**:
- `orders.test` - Main processing topic
- `ecommerce-dlq` - Dead letter queue for failed messages

**Failure Types**:
- `processing_error` - Business logic failures
- `validation_error` - Data validation failures
- `unmarshal_error` - Invalid message format
- `max_retries_exceeded` - Persistent failures

### 4. Stateful Processor Example (`stateful-processor/`)

**Purpose**: Demonstrates stateful stream processing with state management.

**Key Concepts**:
- Stateful processing
- Local state store (memory/LevelDB)
- State changelog topic (compacted)
- Aggregations and transformations
- State recovery

**Run**:
```bash
cd stateful-processor
go run main.go
```

**Topics**:
- `orders.created` - Input stream
- `customer-stats-changelog` - State changelog (compacted)

**Use Case**: Aggregates customer statistics (total orders, total spent, average order value) from order events.

### 5. View Example (`view/`)

**Purpose**: Demonstrates querying stateful data via HTTP API.

**Key Concepts**:
- Materialized views
- State store querying
- HTTP API for state access
- Real-time state updates
- Changelog consumption

**Run**:
```bash
cd view
go run main.go
```

**Endpoints**:
- `GET /stats/:customer_id` - Get stats for specific customer
- `GET /stats` - List all customer stats

**Example**:
```bash
curl http://localhost:8080/stats/CUST-1
curl http://localhost:8080/stats
```

### 6. Join Example (`join/`)

**Purpose**: Demonstrates stream-table joins for data enrichment.

**Key Concepts**:
- Stream-table join
- Compacted topics as tables
- Data enrichment
- Join processing
- Discount calculation based on customer tier

**Run**:
```bash
cd join
go run main.go
```

**Topics**:
- `orders.created` - Order stream (input)
- `customers-table` - Customer table (compacted)
- `orders.enriched` - Enriched orders (output)

**Use Case**: Enriches orders with customer information and applies tier-based discounts.

### 7. Batch Consumer Example (`batch-consumer/`)

**Purpose**: Demonstrates batch processing for improved throughput.

**Key Concepts**:
- Batch consumption
- Configurable batch size and timeout
- Bulk processing
- Batch statistics
- Throughput optimization

**Run**:
```bash
cd batch-consumer
go run main.go
```

**Configuration**:
- Batch size: 50 messages
- Batch timeout: 3 seconds
- Process timeout: 30 seconds

**Use Case**: Processes orders in batches for bulk database inserts or bulk API calls.

### 8. Production-Ready Example (`production-ready/`)

**Purpose**: Demonstrates production-ready features for enterprise deployment.

**Key Concepts**:
- Health checks (liveness/readiness)
- Prometheus metrics
- Graceful shutdown
- Application lifecycle management
- Kubernetes-compatible probes
- Observability endpoints

**Run**:
```bash
cd production-ready
go run main.go
```

**Endpoints**:
- `GET /health` - Detailed health check
- `GET /health/live` - Kubernetes liveness probe
- `GET /health/ready` - Kubernetes readiness probe
- `GET /metrics` - Prometheus metrics
- `GET /stats` - Application statistics

**Example**:
```bash
curl http://localhost:8080/health
curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready
curl http://localhost:8080/metrics
curl http://localhost:8080/stats
```

### 9. E-commerce Example (`ecommerce/`)

**Purpose**: Demonstrates a complete e-commerce order processing pipeline.

**Key Concepts**:
- Microservices architecture
- Event-driven workflow
- Service orchestration
- Multi-stage processing
- Cross-service communication

**Run**:
```bash
cd ecommerce
go run main.go
```

**Services**:
1. **Order Service** - Validates and confirms orders
2. **Payment Service** - Processes payments
3. **Shipment Service** - Handles order fulfillment
4. **Notification Service** - Sends customer notifications

**Topics**:
- `orders.created` → `orders.confirmed` → `orders.paid` → `orders.shipped`
- `payments.processed`
- `shipments.shipped`

**Flow**:
```
Order Created → Order Confirmed → Payment Processed → Shipment Prepared → Customer Notified
```

## Common Patterns

### Basic Consumer
```go
consumer := franzgo.NewConsumer(client)
err := consumer.Consume(ctx, []string{"my-topic"}, func(record *kgo.Record) error {
    // Process message
    return nil
})
```

### Basic Producer
```go
producer := franzgo.NewProducer(client)
err := producer.Produce(ctx, "my-topic", []byte("key"), []byte("value"))
```

### Stateful Processing
```go
processor, err := franzgo.NewStatefulProcessor(client, config, func(ctx *franzgo.StateProcessorContext) error {
    // Get state
    var state MyState
    ctx.GetJSON("key", &state)

    // Update state
    state.Counter++

    // Save state
    ctx.SetJSON("key", state)
    return nil
})
```

### Batch Processing
```go
batchConsumer := franzgo.NewBatchConsumer(client, &franzgo.BatchConfig{
    MaxBatchSize: 50,
    BatchTimeout: 3 * time.Second,
})

err := batchConsumer.ConsumeBatch(ctx, []string{"my-topic"}, func(records []*kgo.Record) error {
    // Process batch
    return nil
})
```

## Performance Tips

1. **Batch Size**: Adjust `ProducerBatchSize` and `BatchTimeout` for optimal throughput
2. **Compression**: Use `CompressionZstd` for best compression ratio
3. **Fetch Size**: Increase `FetchMaxBytes` for batch consumers
4. **Acks**: Use `AllISRAcks` for production, `LeaderAck` for higher throughput
5. **Parallelism**: Use multiple consumer instances with the same consumer group

## Production Checklist

- [ ] Configure appropriate batch sizes
- [ ] Enable compression (Zstandard recommended)
- [ ] Set up health checks
- [ ] Configure Prometheus metrics
- [ ] Implement graceful shutdown
- [ ] Set appropriate timeouts
- [ ] Configure retry logic and DLQ
- [ ] Enable TLS/SASL if required
- [ ] Monitor consumer lag
- [ ] Set up alerting

## Troubleshooting

### Consumer Lag
Check health endpoint:
```bash
curl http://localhost:8080/health
```

### Connection Issues
Verify broker connectivity:
```bash
KAFKA_BROKERS="your-broker:9092" go run main.go
```

### Performance Issues
- Check batch sizes and timeouts
- Monitor metrics endpoint
- Review consumer group rebalancing

## Additional Resources

- [Franz-go Documentation](https://github.com/twmb/franz-go)
- [Watermill Documentation](https://github.com/ThreeDotsLabs/watermill)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

## License

These examples are part of the Watermill project and are provided as educational resources.
