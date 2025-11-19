package franzgo

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "gitlab.cept.gov.in/it2.0common/watermill/pkg/franzgo"
)

// TracingConfig configures OpenTelemetry tracing
type TracingConfig struct {
	Enabled        bool
	ServiceName    string
	TracerProvider trace.TracerProvider
	Propagator     propagation.TextMapPropagator
}

// DefaultTracingConfig returns default tracing configuration
func DefaultTracingConfig(serviceName string) *TracingConfig {
	return &TracingConfig{
		Enabled:        true,
		ServiceName:    serviceName,
		TracerProvider: otel.GetTracerProvider(),
		Propagator:     otel.GetTextMapPropagator(),
	}
}

// TracingMiddleware provides distributed tracing middleware
type TracingMiddleware struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewTracingMiddleware creates a new tracing middleware
func NewTracingMiddleware(config *TracingConfig) *TracingMiddleware {
	if config == nil {
		config = DefaultTracingConfig("franzgo-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &TracingMiddleware{
		config: config,
		tracer: tracer,
	}
}

// Middleware returns a middleware function that adds tracing
func (tm *TracingMiddleware) Middleware() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			if !tm.config.Enabled {
				return next(ctx, record)
			}

			// Extract trace context from record headers
			ctx = tm.extractTraceContext(ctx, record)

			// Start a new span
			ctx, span := tm.tracer.Start(ctx, "kafka.consume",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "kafka"),
					attribute.String("messaging.operation", "consume"),
					attribute.String("messaging.destination", record.Topic),
					attribute.Int("messaging.kafka.partition", int(record.Partition)),
					attribute.Int64("messaging.kafka.offset", record.Offset),
				),
			)
			defer span.End()

			// Add key if present
			if len(record.Key) > 0 {
				span.SetAttributes(attribute.String("messaging.kafka.message_key", string(record.Key)))
			}

			// Call the handler with traced context
			err := next(ctx, record)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}

			span.SetStatus(codes.Ok, "")
			return nil
		}
	}
}

// ProducerTracer provides tracing for producers
type ProducerTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewProducerTracer creates a new producer tracer
func NewProducerTracer(config *TracingConfig) *ProducerTracer {
	if config == nil {
		config = DefaultTracingConfig("franzgo-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &ProducerTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceProduce traces a message publication
func (pt *ProducerTracer) TraceProduce(ctx context.Context, record *kgo.Record) (context.Context, trace.Span) {
	if !pt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := pt.tracer.Start(ctx, "kafka.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "publish"),
			attribute.String("messaging.destination", record.Topic),
		),
	)

	// Add key if present
	if len(record.Key) > 0 {
		span.SetAttributes(attribute.String("messaging.kafka.message_key", string(record.Key)))
	}

	// Inject trace context into record headers
	pt.injectTraceContext(ctx, record)

	return ctx, span
}

// TraceProduceBatch traces a batch publication
func (pt *ProducerTracer) TraceProduceBatch(ctx context.Context, topic string, records []*kgo.Record) (context.Context, trace.Span) {
	if !pt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := pt.tracer.Start(ctx, "kafka.publish_batch",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "publish_batch"),
			attribute.String("messaging.destination", topic),
			attribute.Int("messaging.batch_size", len(records)),
		),
	)

	// Inject trace context into all records
	for _, record := range records {
		pt.injectTraceContext(ctx, record)
	}

	return ctx, span
}

func (pt *ProducerTracer) injectTraceContext(ctx context.Context, record *kgo.Record) {
	carrier := NewRecordCarrier(record)
	pt.config.Propagator.Inject(ctx, carrier)
}

// ConsumerTracer provides tracing for consumers
type ConsumerTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewConsumerTracer creates a new consumer tracer
func NewConsumerTracer(config *TracingConfig) *ConsumerTracer {
	if config == nil {
		config = DefaultTracingConfig("franzgo-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &ConsumerTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceConsume traces message consumption
func (ct *ConsumerTracer) TraceConsume(ctx context.Context, record *kgo.Record) (context.Context, trace.Span) {
	if !ct.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	// Extract parent trace context from record
	parentCtx := ct.extractTraceContext(ctx, record)

	ctx, span := ct.tracer.Start(parentCtx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "consume"),
			attribute.String("messaging.destination", record.Topic),
			attribute.Int("messaging.kafka.partition", int(record.Partition)),
			attribute.Int64("messaging.kafka.offset", record.Offset),
		),
	)

	// Add key if present
	if len(record.Key) > 0 {
		span.SetAttributes(attribute.String("messaging.kafka.message_key", string(record.Key)))
	}

	return ctx, span
}

func (ct *ConsumerTracer) extractTraceContext(ctx context.Context, record *kgo.Record) context.Context {
	carrier := NewRecordCarrier(record)
	return ct.config.Propagator.Extract(ctx, carrier)
}

func (tm *TracingMiddleware) extractTraceContext(ctx context.Context, record *kgo.Record) context.Context {
	carrier := NewRecordCarrier(record)
	return tm.config.Propagator.Extract(ctx, carrier)
}

// RecordCarrier implements propagation.TextMapCarrier for franz-go records
type RecordCarrier struct {
	record *kgo.Record
}

// NewRecordCarrier creates a new record carrier
func NewRecordCarrier(record *kgo.Record) *RecordCarrier {
	return &RecordCarrier{record: record}
}

// Get retrieves a value from record headers
func (rc *RecordCarrier) Get(key string) string {
	for _, header := range rc.record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

// Set stores a value in record headers
func (rc *RecordCarrier) Set(key, value string) {
	// Check if header already exists
	for i, header := range rc.record.Headers {
		if header.Key == key {
			rc.record.Headers[i].Value = []byte(value)
			return
		}
	}

	// Add new header
	rc.record.Headers = append(rc.record.Headers, kgo.RecordHeader{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys returns all header keys
func (rc *RecordCarrier) Keys() []string {
	keys := make([]string, 0, len(rc.record.Headers))
	for _, header := range rc.record.Headers {
		keys = append(keys, header.Key)
	}
	return keys
}

// TransformationTracer provides tracing for stream transformations
type TransformationTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewTransformationTracer creates a new transformation tracer
func NewTransformationTracer(config *TracingConfig) *TransformationTracer {
	if config == nil {
		config = DefaultTracingConfig("franzgo-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &TransformationTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceTransformation traces a transformation operation
func (tt *TransformationTracer) TraceTransformation(ctx context.Context, transformType, inputTopic, outputTopic string) (context.Context, trace.Span) {
	if !tt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := tt.tracer.Start(ctx, fmt.Sprintf("transformation.%s", transformType),
		trace.WithAttributes(
			attribute.String("transformation.type", transformType),
			attribute.String("transformation.input_topic", inputTopic),
			attribute.String("transformation.output_topic", outputTopic),
		),
	)

	return ctx, span
}

// WindowTracer provides tracing for windowing operations
type WindowTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewWindowTracer creates a new window tracer
func NewWindowTracer(config *TracingConfig) *WindowTracer {
	if config == nil {
		config = DefaultTracingConfig("franzgo-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &WindowTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceWindow traces a window operation
func (wt *WindowTracer) TraceWindow(ctx context.Context, windowType, key string, recordCount int) (context.Context, trace.Span) {
	if !wt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := wt.tracer.Start(ctx, fmt.Sprintf("window.%s", windowType),
		trace.WithAttributes(
			attribute.String("window.type", windowType),
			attribute.String("window.key", key),
			attribute.Int("window.record_count", recordCount),
		),
	)

	return ctx, span
}

// TransactionTracer provides tracing for transactions
type TransactionTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewTransactionTracer creates a new transaction tracer
func NewTransactionTracer(config *TracingConfig) *TransactionTracer {
	if config == nil {
		config = DefaultTracingConfig("franzgo-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &TransactionTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceTransaction traces a transaction
func (tt *TransactionTracer) TraceTransaction(ctx context.Context, transactionalID string) (context.Context, trace.Span) {
	if !tt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := tt.tracer.Start(ctx, "kafka.transaction",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "transaction"),
			attribute.String("messaging.kafka.transaction_id", transactionalID),
		),
	)

	return ctx, span
}

// ProcessorTracer provides tracing for processors
type ProcessorTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewProcessorTracer creates a new processor tracer
func NewProcessorTracer(config *TracingConfig) *ProcessorTracer {
	if config == nil {
		config = DefaultTracingConfig("franzgo-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &ProcessorTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceProcess traces processor execution
func (pt *ProcessorTracer) TraceProcess(ctx context.Context, processorName, key string, record *kgo.Record) (context.Context, trace.Span) {
	if !pt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	// Extract parent trace context
	carrier := NewRecordCarrier(record)
	parentCtx := pt.config.Propagator.Extract(ctx, carrier)

	ctx, span := pt.tracer.Start(parentCtx, fmt.Sprintf("processor.%s", processorName),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("processor.name", processorName),
			attribute.String("processor.key", key),
			attribute.String("messaging.destination", record.Topic),
			attribute.Int("messaging.kafka.partition", int(record.Partition)),
		),
	)

	return ctx, span
}

// TraceStateOperation traces state operations
func (pt *ProcessorTracer) TraceStateOperation(ctx context.Context, operation, key string) (context.Context, trace.Span) {
	if !pt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := pt.tracer.Start(ctx, fmt.Sprintf("state.%s", operation),
		trace.WithAttributes(
			attribute.String("state.operation", operation),
			attribute.String("state.key", key),
		),
	)

	return ctx, span
}

// BatchTracer provides tracing for batch operations
type BatchTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewBatchTracer creates a new batch tracer
func NewBatchTracer(config *TracingConfig) *BatchTracer {
	if config == nil {
		config = DefaultTracingConfig("franzgo-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &BatchTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceBatch traces batch processing
func (bt *BatchTracer) TraceBatch(ctx context.Context, topic string, batchSize int) (context.Context, trace.Span) {
	if !bt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := bt.tracer.Start(ctx, "kafka.batch_process",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "batch_process"),
			attribute.String("messaging.destination", topic),
			attribute.Int("messaging.batch_size", batchSize),
		),
	)

	return ctx, span
}
