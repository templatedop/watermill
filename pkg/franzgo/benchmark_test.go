package franzgo

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Benchmark configuration
func BenchmarkConfigCreation(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = DefaultConfig()
	}
}

func BenchmarkConfigBuilder(b *testing.B) {
	brokers := []string{"localhost:9092"}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewConfigBuilder().
			WithBrokers(brokers).
			WithConsumerGroup("test-group").
			WithClientID("test-client").
			Build()
	}
}

// Benchmark middleware performance
func BenchmarkMiddleware_Logging(b *testing.B) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	middleware := LoggingMiddleware()
	wrappedHandler := middleware(handler)
	record := &kgo.Record{
		Topic: "test",
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = wrappedHandler(ctx, record)
	}
}

func BenchmarkMiddleware_Metrics(b *testing.B) {
	metrics := NewMessageMetrics()
	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	middleware := MetricsMiddleware(metrics)
	wrappedHandler := middleware(handler)
	record := &kgo.Record{Topic: "test"}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = wrappedHandler(ctx, record)
	}
}

func BenchmarkMiddleware_Chain(b *testing.B) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	chain := Chain(
		LoggingMiddleware(),
		MetricsMiddleware(NewMessageMetrics()),
		RecoveryMiddleware(),
	)

	wrappedHandler := chain(handler)
	record := &kgo.Record{Topic: "test"}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = wrappedHandler(ctx, record)
	}
}

func BenchmarkMiddleware_ChainLong(b *testing.B) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	chain := Chain(
		LoggingMiddleware(),
		MetricsMiddleware(NewMessageMetrics()),
		TimeoutMiddleware(5*time.Second),
		RecoveryMiddleware(),
		HeadersMiddleware(map[string]string{"test": "value"}),
		ThrottleMiddleware(1000),
	)

	wrappedHandler := chain(handler)
	record := &kgo.Record{Topic: "test"}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = wrappedHandler(ctx, record)
	}
}

// Benchmark transformations
func BenchmarkTransform_AddHeader(b *testing.B) {
	ct := &CommonTransformations{}
	transformer := ct.AddHeader("test-key", "test-value")
	record := &kgo.Record{
		Topic: "test",
		Value: []byte("value"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

func BenchmarkTransform_JSONSimple(b *testing.B) {
	ct := &CommonTransformations{}
	transformer := ct.JSONTransform(func(data map[string]interface{}) (map[string]interface{}, error) {
		data["processed"] = true
		return data, nil
	})

	data := map[string]interface{}{
		"field1": "value1",
		"field2": 123,
	}
	value, _ := json.Marshal(data)
	record := &kgo.Record{
		Topic: "test",
		Value: value,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

func BenchmarkTransform_JSONComplex(b *testing.B) {
	ct := &CommonTransformations{}
	transformer := ct.JSONTransform(func(data map[string]interface{}) (map[string]interface{}, error) {
		data["processed"] = true
		data["timestamp"] = time.Now().Unix()
		data["count"] = float64(data["count"].(float64)) * 2
		return data, nil
	})

	data := map[string]interface{}{
		"field1": "value1",
		"field2": 123,
		"count":  float64(10),
		"nested": map[string]interface{}{
			"a": "b",
			"c": "d",
		},
	}
	value, _ := json.Marshal(data)
	record := &kgo.Record{
		Topic: "test",
		Value: value,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

func BenchmarkTransform_Enrich(b *testing.B) {
	ct := &CommonTransformations{}

	lookupFunc := func(record *kgo.Record) (map[string]interface{}, error) {
		return map[string]interface{}{
			"enriched_field": "enriched_value",
			"timestamp":      time.Now().Unix(),
		}, nil
	}

	transformer := ct.Enrich(lookupFunc)

	data := map[string]interface{}{
		"id":    "123",
		"value": "test",
	}
	value, _ := json.Marshal(data)
	record := &kgo.Record{
		Topic: "test",
		Value: value,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

func BenchmarkTransform_SplitByDelimiter(b *testing.B) {
	ct := &CommonTransformations{}
	transformer := ct.SplitByDelimiter('\n')

	value := []byte("line1\nline2\nline3\nline4\nline5")
	record := &kgo.Record{
		Topic: "test",
		Key:   []byte("key"),
		Value: value,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

func BenchmarkTransform_ExpandArray(b *testing.B) {
	ct := &CommonTransformations{}
	transformer := ct.ExpandArray("items")

	data := map[string]interface{}{
		"order_id": "123",
		"items": []interface{}{
			map[string]interface{}{"product": "A"},
			map[string]interface{}{"product": "B"},
			map[string]interface{}{"product": "C"},
		},
	}
	value, _ := json.Marshal(data)
	record := &kgo.Record{
		Topic: "test",
		Value: value,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

func BenchmarkTransform_FilterByHeader(b *testing.B) {
	ct := &CommonTransformations{}
	predicate := ct.FilterByHeader("type", "important")

	record := &kgo.Record{
		Topic: "test",
		Headers: []kgo.RecordHeader{
			{Key: "type", Value: []byte("important")},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = predicate(record)
	}
}

func BenchmarkTransform_ToUpperCase(b *testing.B) {
	ct := &CommonTransformations{}
	transformer := ct.ToUpperCase()

	record := &kgo.Record{
		Topic: "test",
		Value: []byte("this is a test message that will be converted to uppercase"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

// Benchmark async transformation
func BenchmarkAsyncTransformer_Sequential(b *testing.B) {
	transformer := func(record *kgo.Record) (*kgo.Record, error) {
		var data map[string]interface{}
		json.Unmarshal(record.Value, &data)
		data["processed"] = true
		newValue, _ := json.Marshal(data)
		record.Value = newValue
		return record, nil
	}

	data := map[string]interface{}{"id": 1}
	value, _ := json.Marshal(data)
	record := &kgo.Record{Topic: "test", Value: value}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = transformer(record)
	}
}

func BenchmarkAsyncTransformer_Parallel(b *testing.B) {
	transformer := func(record *kgo.Record) (*kgo.Record, error) {
		var data map[string]interface{}
		json.Unmarshal(record.Value, &data)
		data["processed"] = true
		newValue, _ := json.Marshal(data)
		record.Value = newValue
		return record, nil
	}

	asyncTransformer := NewAsyncTransformer(transformer, 4)

	// Create batch of records
	batchSize := 100
	records := make([]*kgo.Record, batchSize)
	for i := 0; i < batchSize; i++ {
		data := map[string]interface{}{"id": i}
		value, _ := json.Marshal(data)
		records[i] = &kgo.Record{Topic: "test", Value: value}
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = asyncTransformer.TransformBatch(ctx, records)
	}
}

// Benchmark message metrics
func BenchmarkMessageMetrics_Operations(b *testing.B) {
	metrics := NewMessageMetrics()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		metrics.IncrementProcessed()
		metrics.IncrementSuccess()
		metrics.RecordDuration(time.Millisecond)
	}
}

func BenchmarkMessageMetrics_Concurrent(b *testing.B) {
	metrics := NewMessageMetrics()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.IncrementProcessed()
			metrics.IncrementSuccess()
			metrics.RecordDuration(time.Millisecond)
		}
	})
}

// Benchmark deduplication
func BenchmarkDeduplication(b *testing.B) {
	dedup := &Deduplicator{
		processed: make(map[string]time.Time),
		window:    1 * time.Minute,
	}

	record := &kgo.Record{
		Topic: "test",
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = dedup.IsDuplicate(record)
	}
}

// Benchmark circuit breaker
func BenchmarkCircuitBreaker_Closed(b *testing.B) {
	cb := &circuitBreaker{
		threshold: 5,
		timeout:   1 * time.Minute,
		state:     circuitStateClosed,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = cb.allow()
	}
}

// Benchmark window aggregator
func BenchmarkWindowAggregator_Count(b *testing.B) {
	aggregator := &WindowAggregator{}

	records := make([]*kgo.Record, 100)
	for i := 0; i < 100; i++ {
		records[i] = &kgo.Record{
			Topic: "test",
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}

	wc := &WindowContext{
		Records: records,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = aggregator.Count(wc)
	}
}

func BenchmarkWindowAggregator_Sum(b *testing.B) {
	aggregator := &WindowAggregator{}

	records := make([]*kgo.Record, 100)
	for i := 0; i < 100; i++ {
		data := map[string]interface{}{
			"amount": float64(i * 10),
		}
		value, _ := json.Marshal(data)
		records[i] = &kgo.Record{
			Topic: "test",
			Value: value,
		}
	}

	wc := &WindowContext{
		Records: records,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = aggregator.Sum(wc, "amount")
	}
}

func BenchmarkWindowAggregator_Average(b *testing.B) {
	aggregator := &WindowAggregator{}

	records := make([]*kgo.Record, 100)
	for i := 0; i < 100; i++ {
		data := map[string]interface{}{
			"value": float64(i),
		}
		value, _ := json.Marshal(data)
		records[i] = &kgo.Record{
			Topic: "test",
			Value: value,
		}
	}

	wc := &WindowContext{
		Records: records,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = aggregator.Average(wc, "value")
	}
}

// Comparative benchmarks
func BenchmarkComparison_WithMiddleware(b *testing.B) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		// Simulate processing
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			return err
		}
		data["processed"] = true
		_, err := json.Marshal(data)
		return err
	}

	chain := Chain(
		LoggingMiddleware(),
		MetricsMiddleware(NewMessageMetrics()),
		RecoveryMiddleware(),
	)

	wrappedHandler := chain(handler)

	data := map[string]interface{}{"id": 1, "value": "test"}
	value, _ := json.Marshal(data)
	record := &kgo.Record{Topic: "test", Value: value}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = wrappedHandler(ctx, record)
	}
}

func BenchmarkComparison_WithoutMiddleware(b *testing.B) {
	handler := func(ctx context.Context, record *kgo.Record) error {
		// Simulate processing
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			return err
		}
		data["processed"] = true
		_, err := json.Marshal(data)
		return err
	}

	data := map[string]interface{}{"id": 1, "value": "test"}
	value, _ := json.Marshal(data)
	record := &kgo.Record{Topic: "test", Value: value}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = handler(ctx, record)
	}
}

// Run benchmarks with: go test -bench=. -benchmem -benchtime=10s ./pkg/franzgo/
