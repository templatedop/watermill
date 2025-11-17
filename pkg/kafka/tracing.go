package kafka

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	tracerName = "github.com/templatedop/watermill/pkg/kafka"
)

// TracingConfig configures OpenTelemetry tracing
type TracingConfig struct {
	Enabled     bool
	ServiceName string
	TracerProvider trace.TracerProvider
	Propagator  propagation.TextMapPropagator
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
		config = DefaultTracingConfig("watermill-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &TracingMiddleware{
		config: config,
		tracer: tracer,
	}
}

// Handler returns the middleware handler function
func (tm *TracingMiddleware) Handler(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		if !tm.config.Enabled {
			return h(msg)
		}

		// Extract trace context from message metadata
		ctx := tm.extractTraceContext(msg)

		// Start a new span
		ctx, span := tm.tracer.Start(ctx, "kafka.consume",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.operation", "consume"),
				attribute.String("messaging.message_id", msg.UUID),
			),
		)
		defer span.End()

		// Add topic information if available
		if topic := msg.Metadata.Get("topic"); topic != "" {
			span.SetAttributes(attribute.String("messaging.destination", topic))
		}

		// Add partition information if available
		if partition := msg.Metadata.Get("partition"); partition != "" {
			span.SetAttributes(attribute.String("messaging.kafka.partition", partition))
		}

		// Add consumer group if available
		if group := msg.Metadata.Get("consumer_group"); group != "" {
			span.SetAttributes(attribute.String("messaging.consumer_group", group))
		}

		// Call the handler with traced context
		msgs, err := h(msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return msgs, err
		}

		span.SetStatus(codes.Ok, "")

		// Inject trace context into produced messages
		for _, outMsg := range msgs {
			tm.injectTraceContext(ctx, outMsg)
		}

		return msgs, nil
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
		config = DefaultTracingConfig("watermill-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &ProducerTracer{
		config: config,
		tracer: tracer,
	}
}

// TracePublish traces a message publication
func (pt *ProducerTracer) TracePublish(ctx context.Context, topic string, msg *message.Message) (context.Context, trace.Span) {
	if !pt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := pt.tracer.Start(ctx, "kafka.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "publish"),
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.message_id", msg.UUID),
		),
	)

	// Inject trace context into message
	pt.injectTraceContext(ctx, msg)

	return ctx, span
}

// TracePublishBatch traces a batch publication
func (pt *ProducerTracer) TracePublishBatch(ctx context.Context, topic string, messages []*message.Message) (context.Context, trace.Span) {
	if !pt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := pt.tracer.Start(ctx, "kafka.publish_batch",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "publish_batch"),
			attribute.String("messaging.destination", topic),
			attribute.Int("messaging.batch_size", len(messages)),
		),
	)

	// Inject trace context into all messages
	for _, msg := range messages {
		pt.injectTraceContext(ctx, msg)
	}

	return ctx, span
}

func (pt *ProducerTracer) injectTraceContext(ctx context.Context, msg *message.Message) {
	carrier := NewMessageCarrier(msg)
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
		config = DefaultTracingConfig("watermill-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &ConsumerTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceConsume traces message consumption
func (ct *ConsumerTracer) TraceConsume(ctx context.Context, topic string, msg *message.Message) (context.Context, trace.Span) {
	if !ct.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	// Extract parent trace context from message
	parentCtx := ct.extractTraceContext(msg)

	ctx, span := ct.tracer.Start(parentCtx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "consume"),
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.message_id", msg.UUID),
		),
	)

	return ctx, span
}

func (ct *ConsumerTracer) extractTraceContext(msg *message.Message) context.Context {
	carrier := NewMessageCarrier(msg)
	return ct.config.Propagator.Extract(context.Background(), carrier)
}

func (tm *TracingMiddleware) extractTraceContext(msg *message.Message) context.Context {
	carrier := NewMessageCarrier(msg)
	return tm.config.Propagator.Extract(context.Background(), carrier)
}

func (tm *TracingMiddleware) injectTraceContext(ctx context.Context, msg *message.Message) {
	carrier := NewMessageCarrier(msg)
	tm.config.Propagator.Inject(ctx, carrier)
}

// MessageCarrier implements propagation.TextMapCarrier for Watermill messages
type MessageCarrier struct {
	msg *message.Message
}

// NewMessageCarrier creates a new message carrier
func NewMessageCarrier(msg *message.Message) *MessageCarrier {
	return &MessageCarrier{msg: msg}
}

// Get retrieves a value from message metadata
func (mc *MessageCarrier) Get(key string) string {
	return mc.msg.Metadata.Get(key)
}

// Set stores a value in message metadata
func (mc *MessageCarrier) Set(key, value string) {
	mc.msg.Metadata.Set(key, value)
}

// Keys returns all metadata keys
func (mc *MessageCarrier) Keys() []string {
	keys := make([]string, 0, len(mc.msg.Metadata))
	for k := range mc.msg.Metadata {
		keys = append(keys, k)
	}
	return keys
}

// ProcessorTracer provides tracing for stateful processors
type ProcessorTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewProcessorTracer creates a new processor tracer
func NewProcessorTracer(config *TracingConfig) *ProcessorTracer {
	if config == nil {
		config = DefaultTracingConfig("watermill-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &ProcessorTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceProcess traces processor execution
func (pt *ProcessorTracer) TraceProcess(ctx context.Context, processorName, key string, msg *message.Message) (context.Context, trace.Span) {
	if !pt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	// Extract parent trace context
	carrier := NewMessageCarrier(msg)
	parentCtx := pt.config.Propagator.Extract(ctx, carrier)

	ctx, span := pt.tracer.Start(parentCtx, fmt.Sprintf("processor.%s", processorName),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("processor.name", processorName),
			attribute.String("processor.key", key),
			attribute.String("messaging.message_id", msg.UUID),
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

// ViewTracer provides tracing for views
type ViewTracer struct {
	config *TracingConfig
	tracer trace.Tracer
}

// NewViewTracer creates a new view tracer
func NewViewTracer(config *TracingConfig) *ViewTracer {
	if config == nil {
		config = DefaultTracingConfig("watermill-kafka")
	}

	tracer := config.TracerProvider.Tracer(tracerName)

	return &ViewTracer{
		config: config,
		tracer: tracer,
	}
}

// TraceGet traces view get operations
func (vt *ViewTracer) TraceGet(ctx context.Context, viewName, key string) (context.Context, trace.Span) {
	if !vt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := vt.tracer.Start(ctx, fmt.Sprintf("view.%s.get", viewName),
		trace.WithAttributes(
			attribute.String("view.name", viewName),
			attribute.String("view.key", key),
			attribute.String("view.operation", "get"),
		),
	)

	return ctx, span
}

// TraceIterator traces view iterator operations
func (vt *ViewTracer) TraceIterator(ctx context.Context, viewName string) (context.Context, trace.Span) {
	if !vt.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	ctx, span := vt.tracer.Start(ctx, fmt.Sprintf("view.%s.iterator", viewName),
		trace.WithAttributes(
			attribute.String("view.name", viewName),
			attribute.String("view.operation", "iterator"),
		),
	)

	return ctx, span
}
