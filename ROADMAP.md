# Roadmap & Improvement Suggestions

This document outlines suggested improvements and future enhancements for the Watermill Kafka client library.

## ✅ Completed Features

- [x] Core producer/consumer functionality
- [x] DLQ support with retry logic
- [x] Comprehensive middleware (logging, metrics, retry, timeout, circuit breaker, throttling)
- [x] Ecommerce-specific event types
- [x] Goka-inspired stateful processing (processors, views, joins)
- [x] Codec system (JSON, String, Bytes, Int64)
- [x] Storage abstraction (in-memory)
- [x] Stream-table and stream-stream joins
- [x] Advanced batch processing with metrics
- [x] Comprehensive documentation

## 🔥 High Priority

### 1. Testing & Quality Assurance

**Why**: Critical for production reliability and maintainability

```go
// Unit tests for all components
pkg/kafka/
  ├── client_test.go
  ├── producer_test.go
  ├── consumer_test.go
  ├── processor_test.go
  ├── view_test.go
  ├── join_test.go
  ├── batch_test.go
  └── middleware_test.go

// Integration tests
tests/integration/
  ├── producer_consumer_test.go
  ├── stateful_processing_test.go
  ├── batch_processing_test.go
  └── dlq_test.go

// Benchmark tests
tests/benchmarks/
  ├── throughput_test.go
  ├── latency_test.go
  └── memory_test.go
```

**Implementation**:
- Unit tests with 80%+ coverage
- Integration tests using testcontainers
- Property-based testing for codecs
- Chaos testing for resilience
- Performance benchmarks

### 2. Observability & Monitoring

**Why**: Essential for production debugging and optimization

**Prometheus Metrics**:
```go
// pkg/kafka/metrics.go
type PrometheusMetrics struct {
    messagesProduced    prometheus.Counter
    messagesConsumed    prometheus.Counter
    processingDuration  prometheus.Histogram
    batchSize          prometheus.Histogram
    errorRate          prometheus.Gauge
    lagGauge           prometheus.Gauge
}

// Expose metrics endpoint
http.Handle("/metrics", promhttp.Handler())
```

**OpenTelemetry Integration**:
```go
// Distributed tracing
func (p *Producer) PublishWithTrace(ctx context.Context, topic string, msg *message.Message) error {
    ctx, span := otel.Tracer("kafka").Start(ctx, "kafka.publish")
    defer span.End()

    span.SetAttributes(
        attribute.String("topic", topic),
        attribute.String("message.id", msg.UUID),
    )

    return p.publish(ctx, topic, msg)
}
```

**Health Checks**:
```go
// pkg/kafka/health.go
type HealthChecker struct {
    client *Client
}

func (h *HealthChecker) Check(ctx context.Context) error {
    // Check Kafka connectivity
    // Check consumer lag
    // Check processor state
    return nil
}

// HTTP endpoint
http.HandleFunc("/health", healthHandler)
http.HandleFunc("/ready", readinessHandler)
```

### 3. Persistent Storage Backends

**Why**: Critical for production stateful processing

**LevelDB Implementation**:
```go
// pkg/kafka/storage/leveldb.go
type LevelDBStorage struct {
    db *leveldb.DB
}

func NewLevelDBStorage(path string) (*LevelDBStorage, error) {
    db, err := leveldb.OpenFile(path, nil)
    if err != nil {
        return nil, err
    }
    return &LevelDBStorage{db: db}, nil
}
```

**Redis Implementation**:
```go
// pkg/kafka/storage/redis.go
type RedisStorage struct {
    client *redis.Client
    prefix string
}

func NewRedisStorage(addr, prefix string) *RedisStorage {
    return &RedisStorage{
        client: redis.NewClient(&redis.Options{Addr: addr}),
        prefix: prefix,
    }
}
```

**BadgerDB Implementation**:
```go
// pkg/kafka/storage/badger.go
type BadgerStorage struct {
    db *badger.DB
}
```

## 🚀 Medium Priority

### 4. Advanced Windowing

**Tumbling Windows**:
```go
// pkg/kafka/window.go
type TumblingWindow struct {
    size     time.Duration
    processor ProcessorFunc
}

func NewTumblingWindow(size time.Duration) *TumblingWindow {
    return &TumblingWindow{size: size}
}

// Example: 5-minute tumbling windows
window := kafka.NewTumblingWindow(5 * time.Minute)
window.Process(func(ctx *kafka.WindowContext) error {
    // Aggregate all events in 5-minute window
    stats := aggregateWindow(ctx.Messages)
    return ctx.Emit("windowed-stats", stats)
})
```

**Sliding Windows**:
```go
type SlidingWindow struct {
    size   time.Duration
    slide  time.Duration
}

// Example: 10-minute window, 1-minute slide
window := kafka.NewSlidingWindow(10*time.Minute, 1*time.Minute)
```

**Session Windows**:
```go
type SessionWindow struct {
    gap time.Duration // Inactivity gap
}

// Example: Session ends after 30 minutes of inactivity
window := kafka.NewSessionWindow(30 * time.Minute)
```

### 5. Schema Registry Integration

**Avro Support**:
```go
// pkg/kafka/codec/avro.go
type AvroCodec struct {
    registry *schemaregistry.Client
    schema   *avro.Schema
}

func NewAvroCodec(registryURL, subject string) (*AvroCodec, error) {
    client := schemaregistry.NewClient(registryURL)
    schema, err := client.GetLatestSchema(subject)
    return &AvroCodec{registry: client, schema: schema}, err
}
```

**Protobuf Support**:
```go
// pkg/kafka/codec/protobuf.go
type ProtobufCodec struct {
    messageType proto.Message
}

func NewProtobufCodec(msg proto.Message) *ProtobufCodec {
    return &ProtobufCodec{messageType: msg}
}
```

### 6. Exactly-Once Semantics

**Transactional Producer**:
```go
// pkg/kafka/transaction.go
type TransactionalProducer struct {
    producer *kafka.Producer
    txnID    string
}

func (p *TransactionalProducer) BeginTransaction() error {
    return p.producer.BeginTxn()
}

func (p *TransactionalProducer) CommitTransaction() error {
    return p.producer.CommitTxn()
}

func (p *TransactionalProducer) AbortTransaction() error {
    return p.producer.AbortTxn()
}

// Usage
txnProducer.BeginTransaction()
txnProducer.Publish(ctx, topic, msg)
txnProducer.CommitTransaction()
```

**Idempotent Processing**:
```go
// pkg/kafka/idempotent.go
type IdempotentProcessor struct {
    deduplicator *Deduplicator
    processor    ProcessorFunc
}
```

### 7. Stream Transformations

**Map/Filter/FlatMap**:
```go
// pkg/kafka/transform.go
type StreamTransformer struct {
    client *Client
}

func (t *StreamTransformer) Map(topic string, mapper func(msg *message.Message) (*message.Message, error)) {
    // Transform each message
}

func (t *StreamTransformer) Filter(topic string, predicate func(msg *message.Message) bool) {
    // Filter messages
}

func (t *StreamTransformer) FlatMap(topic string, mapper func(msg *message.Message) ([]*message.Message, error)) {
    // Emit multiple messages per input
}

// Example
transformer.Map("orders", func(msg *message.Message) (*message.Message, error) {
    var order Order
    json.Unmarshal(msg.Payload, &order)
    order.Total = order.Total * 1.1 // Add 10% tax
    payload, _ := json.Marshal(order)
    return message.NewMessage(msg.UUID, payload), nil
})
```

### 8. CLI Tool

**Management CLI**:
```bash
# watermill-cli tool
watermill topic list
watermill topic create my-topic --partitions 10
watermill consumer list
watermill consumer lag my-group
watermill processor status
watermill view query customer-stats CUST-123
watermill metrics export
watermill config validate config.yaml
```

**Implementation**:
```go
// cmd/watermill-cli/main.go
package main

import (
    "github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "watermill",
    Short: "Watermill Kafka CLI",
}

func init() {
    rootCmd.AddCommand(topicCmd)
    rootCmd.AddCommand(consumerCmd)
    rootCmd.AddCommand(processorCmd)
}
```

## 💡 Nice to Have

### 9. Web UI Dashboard

**Monitoring Dashboard**:
- Real-time metrics visualization
- Consumer lag monitoring
- Processor state inspection
- View queries (like Goka's web UI)
- Topic management
- Message browser

**Implementation**:
```go
// pkg/kafka/ui/server.go
type DashboardServer struct {
    client *Client
    port   int
}

func (s *DashboardServer) Start() error {
    http.HandleFunc("/", s.handleDashboard)
    http.HandleFunc("/api/metrics", s.handleMetrics)
    http.HandleFunc("/api/consumers", s.handleConsumers)
    http.HandleFunc("/api/processors", s.handleProcessors)
    return http.ListenAndServe(fmt.Sprintf(":%d", s.port), nil)
}
```

### 10. Complex Event Processing (CEP)

**Pattern Matching**:
```go
// pkg/kafka/cep.go
type Pattern struct {
    events    []EventMatcher
    timeframe time.Duration
}

// Example: Detect fraud pattern
pattern := kafka.NewPattern().
    Match("login", func(e Event) bool { return e.Location != "US" }).
    FollowedBy("purchase", func(e Event) bool { return e.Amount > 1000 }).
    Within(5 * time.Minute)

pattern.OnMatch(func(events []Event) {
    alert := FraudAlert{Events: events}
    notify(alert)
})
```

### 11. State Snapshots

**Periodic Snapshots**:
```go
// pkg/kafka/snapshot.go
type SnapshotManager struct {
    processor *Processor
    interval  time.Duration
    storage   Storage
}

func (s *SnapshotManager) TakeSnapshot() error {
    // Save current processor state
    state := s.processor.GetState()
    return s.storage.SaveSnapshot(state)
}

func (s *SnapshotManager) Restore() error {
    // Restore from latest snapshot
    state, err := s.storage.LoadSnapshot()
    if err != nil {
        return err
    }
    return s.processor.SetState(state)
}
```

### 12. Auto-scaling Support

**Dynamic Consumer Scaling**:
```go
// pkg/kafka/autoscale.go
type AutoScaler struct {
    client      *Client
    minReplicas int
    maxReplicas int
    targetLag   int64
}

func (a *AutoScaler) Scale() error {
    lag := a.getCurrentLag()
    if lag > a.targetLag {
        return a.scaleUp()
    } else if lag < a.targetLag/2 {
        return a.scaleDown()
    }
    return nil
}
```

### 13. Multi-DC Replication

**Cross-DC Support**:
```go
// pkg/kafka/replication.go
type MultiDCClient struct {
    primary   *Client
    secondary *Client
}

func (m *MultiDCClient) PublishToAll(ctx context.Context, topic string, msg *message.Message) error {
    // Publish to all datacenters
    errPrimary := m.primary.Publish(ctx, topic, msg)
    errSecondary := m.secondary.Publish(ctx, topic, msg)
    return errors.Join(errPrimary, errSecondary)
}
```

## 🔧 Developer Experience

### 14. Code Generation

**Generate from Schema**:
```bash
# Generate Go types from Avro schema
watermill generate --schema order.avsc --output order.go

# Generate producer/consumer from config
watermill generate --config events.yaml --output handlers/
```

### 15. Interactive Debugger

**Debug Console**:
```go
// pkg/kafka/debug.go
type Debugger struct {
    client *Client
}

func (d *Debugger) Inspect(topic string) {
    // Interactive message browser
    // Live tail
    // Message replay
}

// Usage
debugger := kafka.NewDebugger(client)
debugger.Tail("orders.created")
debugger.Replay("orders.created", startOffset, endOffset)
```

### 16. Docker Images

**Official Docker Images**:
```dockerfile
# Dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o watermill-processor ./cmd/processor

FROM alpine:latest
COPY --from=builder /app/watermill-processor /usr/local/bin/
ENTRYPOINT ["watermill-processor"]
```

### 17. Kubernetes Support

**Operator**:
```yaml
# CRD for Kafka processor
apiVersion: watermill.io/v1
kind: Processor
metadata:
  name: order-stats
spec:
  topic: orders.created
  groupTable: order-stats-table
  replicas: 3
  resources:
    requests:
      memory: "256Mi"
      cpu: "100m"
```

**Helm Chart**:
```yaml
# helm/watermill/values.yaml
processor:
  name: order-processor
  replicas: 3
  topic: orders.created
  groupTable: order-stats

kafka:
  brokers:
    - kafka-1:9092
    - kafka-2:9092
```

## 📊 Performance Optimizations

### 18. Zero-Copy & Memory Pooling

```go
// pkg/kafka/pool.go
type MessagePool struct {
    pool sync.Pool
}

func (p *MessagePool) Get() *message.Message {
    if msg := p.pool.Get(); msg != nil {
        return msg.(*message.Message)
    }
    return &message.Message{}
}

func (p *MessagePool) Put(msg *message.Message) {
    msg.Payload = msg.Payload[:0]
    msg.Metadata = make(message.Metadata)
    p.pool.Put(msg)
}
```

### 19. Compression Strategies

```go
// pkg/kafka/compression.go
type CompressionStrategy interface {
    Compress(data []byte) ([]byte, error)
    Decompress(data []byte) ([]byte, error)
}

// Built-in strategies
type GzipStrategy struct{}
type ZstdStrategy struct{}
type LZ4Strategy struct{}
```

## 🔒 Security Enhancements

### 20. Enhanced SASL/SSL

```go
// pkg/kafka/security.go
type SecurityConfig struct {
    SASL struct {
        Mechanism string // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
        Username  string
        Password  string
    }
    TLS struct {
        Enabled            bool
        CertFile          string
        KeyFile           string
        CAFile            string
        InsecureSkipVerify bool
    }
}
```

### 21. Audit Logging

```go
// pkg/kafka/audit.go
type AuditLogger struct {
    logger Logger
}

func (a *AuditLogger) LogProduced(topic string, key string, user string) {
    a.logger.Info("message_produced", Fields{
        "topic": topic,
        "key":   key,
        "user":  user,
        "time":  time.Now(),
    })
}
```

## 📈 Priority Matrix

| Feature | Impact | Effort | Priority |
|---------|--------|--------|----------|
| Testing Suite | 🔴 High | 🟡 Medium | **P0** |
| Prometheus Metrics | 🔴 High | 🟢 Low | **P0** |
| Health Checks | 🔴 High | 🟢 Low | **P0** |
| LevelDB Storage | 🔴 High | 🟡 Medium | **P1** |
| Windowing | 🟡 Medium | 🔴 High | **P2** |
| Schema Registry | 🟡 Medium | 🟡 Medium | **P2** |
| CLI Tool | 🟡 Medium | 🟡 Medium | **P2** |
| Web UI | 🟢 Low | 🔴 High | **P3** |
| CEP | 🟢 Low | 🔴 High | **P3** |

## 🎯 Recommended Next Steps

1. **Week 1-2**: Testing suite (unit + integration tests)
2. **Week 3**: Prometheus metrics + health checks
3. **Week 4**: LevelDB storage backend
4. **Week 5-6**: Documentation improvements + examples
5. **Week 7**: CLI tool (basic version)
6. **Week 8**: Performance benchmarks + optimization

## 📝 Contributing

These improvements are suggestions. Priority should be based on:
- Production needs
- User feedback
- Resource availability
- Technology trends

Each feature should include:
- Design document
- Implementation plan
- Tests
- Documentation
- Migration guide (if breaking)
