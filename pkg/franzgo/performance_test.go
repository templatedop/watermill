// +build performance

package franzgo

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Performance benchmarks comparing franz-go implementation
// Run with: go test -tags=performance -bench=BenchmarkPerformance -benchtime=30s -benchmem ./pkg/franzgo/

// BenchmarkPerformance_ProducerThroughput measures producer throughput
func BenchmarkPerformance_ProducerThroughput(b *testing.B) {
	config := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithClientID("perf-producer").
		WithProducerBatchSize(16384).
		WithProducerLinger(10 * time.Millisecond).
		Build()

	client, err := NewClient(config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	producer := NewProducer(client)
	topic := fmt.Sprintf("perf-throughput-%d", time.Now().Unix())

	ctx := context.Background()
	value := []byte("test message value for throughput benchmark")

	b.ResetTimer()
	b.ReportAllocs()

	var successCount atomic.Int64
	var errorCount atomic.Int64

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := producer.Produce(ctx, topic, key, value); err != nil {
			errorCount.Add(1)
		} else {
			successCount.Add(1)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(successCount.Load()), "messages/sent")
	b.ReportMetric(float64(errorCount.Load()), "errors")
	b.ReportMetric(float64(successCount.Load())/b.Elapsed().Seconds(), "messages/sec")
}

// BenchmarkPerformance_ProducerLatency measures producer latency
func BenchmarkPerformance_ProducerLatency(b *testing.B) {
	config := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithClientID("perf-latency").
		WithProducerBatchSize(1).  // Disable batching for latency test
		WithProducerLinger(0).
		Build()

	client, err := NewClient(config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	producer := NewProducer(client)
	topic := fmt.Sprintf("perf-latency-%d", time.Now().Unix())

	ctx := context.Background()
	value := []byte("latency test message")

	var totalLatency atomic.Int64
	var count atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		key := []byte(fmt.Sprintf("key-%d", i))

		if err := producer.Produce(ctx, topic, key, value); err != nil {
			b.Logf("Produce error: %v", err)
			continue
		}

		latency := time.Since(start)
		totalLatency.Add(int64(latency))
		count.Add(1)
	}

	b.StopTimer()

	avgLatency := time.Duration(totalLatency.Load() / count.Load())
	b.ReportMetric(float64(avgLatency.Microseconds()), "µs/op-avg")
}

// BenchmarkPerformance_ConsumerThroughput measures consumer throughput
func BenchmarkPerformance_ConsumerThroughput(b *testing.B) {
	topic := fmt.Sprintf("perf-consumer-%d", time.Now().Unix())

	// Setup: Produce messages first
	config := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithClientID("perf-producer-setup").
		Build()

	client, err := NewClient(config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}

	producer := NewProducer(client)
	ctx := context.Background()

	// Produce N messages
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		_ = producer.Produce(ctx, topic, key, value)
	}

	client.Close()

	// Now measure consumption
	consumerConfig := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup(fmt.Sprintf("perf-group-%d", time.Now().Unix())).
		WithClientID("perf-consumer").
		Build()

	consumerClient, err := NewClient(consumerConfig)
	if err != nil {
		b.Fatalf("Failed to create consumer client: %v", err)
	}
	defer consumerClient.Close()

	consumer := NewConsumer(consumerClient, nil)

	var consumed atomic.Int64
	handler := func(ctx context.Context, record *kgo.Record) error {
		consumed.Add(1)
		if consumed.Load() >= int64(b.N) {
			return fmt.Errorf("done")
		}
		return nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	go func() {
		_ = consumer.Consume(ctx, []string{topic}, handler)
	}()

	// Wait for consumption
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && consumed.Load() < int64(b.N) {
		time.Sleep(100 * time.Millisecond)
	}

	b.StopTimer()
	b.ReportMetric(float64(consumed.Load()), "messages/consumed")
	b.ReportMetric(float64(consumed.Load())/b.Elapsed().Seconds(), "messages/sec")
}

// BenchmarkPerformance_BatchProcessing measures batch processing performance
func BenchmarkPerformance_BatchProcessing(b *testing.B) {
	topic := fmt.Sprintf("perf-batch-%d", time.Now().Unix())

	// Setup: Produce messages
	config := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithClientID("perf-batch-setup").
		Build()

	client, err := NewClient(config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}

	producer := NewProducer(client)
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		_ = producer.Produce(ctx, topic, []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}

	client.Close()

	// Measure batch consumption
	consumerConfig := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup(fmt.Sprintf("perf-batch-group-%d", time.Now().Unix())).
		Build()

	consumerClient, err := NewClient(consumerConfig)
	if err != nil {
		b.Fatalf("Failed to create consumer client: %v", err)
	}
	defer consumerClient.Close()

	consumer := NewConsumer(consumerClient, nil)
	batchConfig := &BatchConfig{
		MaxBatchSize:  100,
		MaxBatchBytes: 1024 * 1024,
		BatchTimeout:  1 * time.Second,
		ErrorStrategy: BatchErrorStrategySkipErrors,
	}

	batchProcessor := NewBatchProcessor(consumer, batchConfig)

	var consumed atomic.Int64
	var batchCount atomic.Int64

	handler := func(ctx context.Context, batch []*kgo.Record) error {
		consumed.Add(int64(len(batch)))
		batchCount.Add(1)
		if consumed.Load() >= int64(b.N) {
			return fmt.Errorf("done")
		}
		return nil
	}

	b.ResetTimer()
	b.ReportAllocs()

	go func() {
		_ = batchProcessor.ConsumeBatch(ctx, []string{topic}, handler)
	}()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && consumed.Load() < int64(b.N) {
		time.Sleep(100 * time.Millisecond)
	}

	b.StopTimer()
	b.ReportMetric(float64(consumed.Load()), "messages/consumed")
	b.ReportMetric(float64(batchCount.Load()), "batches")
	b.ReportMetric(float64(consumed.Load())/float64(batchCount.Load()), "messages/batch-avg")
}

// BenchmarkPerformance_JSONSerialization measures JSON processing performance
func BenchmarkPerformance_JSONSerialization(b *testing.B) {
	type TestMessage struct {
		ID        string                 `json:"id"`
		Timestamp int64                  `json:"timestamp"`
		Type      string                 `json:"type"`
		Data      map[string]interface{} `json:"data"`
	}

	record := &kgo.Record{
		Topic: "test",
	}

	b.Run("Marshal", func(b *testing.B) {
		msg := TestMessage{
			ID:        "msg-123",
			Timestamp: time.Now().Unix(),
			Type:      "test",
			Data: map[string]interface{}{
				"field1": "value1",
				"field2": 123,
				"field3": true,
			},
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data, _ := json.Marshal(msg)
			record.Value = data
		}
	})

	b.Run("Unmarshal", func(b *testing.B) {
		msg := TestMessage{
			ID:        "msg-123",
			Timestamp: time.Now().Unix(),
			Type:      "test",
			Data: map[string]interface{}{
				"field1": "value1",
				"field2": 123,
			},
		}
		data, _ := json.Marshal(msg)
		record.Value = data

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var result TestMessage
			_ = json.Unmarshal(record.Value, &result)
		}
	})
}

// BenchmarkPerformance_ConcurrentProducers measures concurrent producer performance
func BenchmarkPerformance_ConcurrentProducers(b *testing.B) {
	config := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithClientID("perf-concurrent").
		WithProducerBatchSize(16384).
		WithProducerLinger(10 * time.Millisecond).
		Build()

	client, err := NewClient(config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	producer := NewProducer(client)
	topic := fmt.Sprintf("perf-concurrent-%d", time.Now().Unix())

	concurrency := []int{1, 4, 8, 16, 32}

	for _, numWorkers := range concurrency {
		b.Run(fmt.Sprintf("Workers-%d", numWorkers), func(b *testing.B) {
			ctx := context.Background()
			value := []byte("concurrent test message")

			b.ResetTimer()
			b.ReportAllocs()

			var wg sync.WaitGroup
			messagesPerWorker := b.N / numWorkers

			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					for i := 0; i < messagesPerWorker; i++ {
						key := []byte(fmt.Sprintf("worker-%d-key-%d", workerID, i))
						_ = producer.Produce(ctx, topic, key, value)
					}
				}(w)
			}

			wg.Wait()
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "messages/sec")
		})
	}
}

// BenchmarkPerformance_MessageSizes measures performance with different message sizes
func BenchmarkPerformance_MessageSizes(b *testing.B) {
	config := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithClientID("perf-sizes").
		Build()

	client, err := NewClient(config)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	producer := NewProducer(client)
	topic := fmt.Sprintf("perf-sizes-%d", time.Now().Unix())

	sizes := []int{100, 1024, 10240, 102400, 1048576} // 100B, 1KB, 10KB, 100KB, 1MB

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size-%dB", size), func(b *testing.B) {
			ctx := context.Background()
			value := make([]byte, size)
			for i := range value {
				value[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				_ = producer.Produce(ctx, topic, key, value)
			}

			b.StopTimer()
			b.ReportMetric(float64(b.N*size)/b.Elapsed().Seconds(), "bytes/sec")
		})
	}
}

// BenchmarkPerformance_MiddlewareOverhead measures middleware overhead
func BenchmarkPerformance_MiddlewareOverhead(b *testing.B) {
	record := &kgo.Record{
		Topic: "test",
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	ctx := context.Background()

	baseHandler := func(ctx context.Context, record *kgo.Record) error {
		return nil
	}

	b.Run("NoMiddleware", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = baseHandler(ctx, record)
		}
	})

	b.Run("LoggingMiddleware", func(b *testing.B) {
		handler := LoggingMiddleware()(baseHandler)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = handler(ctx, record)
		}
	})

	b.Run("MetricsMiddleware", func(b *testing.B) {
		metrics := NewMessageMetrics()
		handler := MetricsMiddleware(metrics)(baseHandler)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = handler(ctx, record)
		}
	})

	b.Run("FullMiddlewareChain", func(b *testing.B) {
		chain := Chain(
			LoggingMiddleware(),
			MetricsMiddleware(NewMessageMetrics()),
			TimeoutMiddleware(30*time.Second),
			RecoveryMiddleware(),
		)
		handler := chain(baseHandler)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = handler(ctx, record)
		}
	})
}

// BenchmarkPerformance_TransformationOverhead measures transformation performance
func BenchmarkPerformance_TransformationOverhead(b *testing.B) {
	ct := &CommonTransformations{}

	data := map[string]interface{}{
		"id":    "123",
		"value": "test",
		"count": float64(42),
	}
	value, _ := json.Marshal(data)

	record := &kgo.Record{
		Topic: "test",
		Value: value,
	}

	b.Run("JSONTransform", func(b *testing.B) {
		transformer := ct.JSONTransform(func(data map[string]interface{}) (map[string]interface{}, error) {
			data["processed"] = true
			return data, nil
		})

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _ = transformer(record)
		}
	})

	b.Run("AddHeader", func(b *testing.B) {
		transformer := ct.AddHeader("test", "value")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _ = transformer(record)
		}
	})

	b.Run("Enrich", func(b *testing.B) {
		transformer := ct.Enrich(func(record *kgo.Record) (map[string]interface{}, error) {
			return map[string]interface{}{"enriched": true}, nil
		})

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _ = transformer(record)
		}
	})
}

// BenchmarkPerformance_StateStore measures state store performance
func BenchmarkPerformance_StateStore(b *testing.B) {
	b.Run("MemoryStorage", func(b *testing.B) {
		storage := NewMemoryStorage()
		store := NewPartitionedStateStore(storage, 0)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := []byte(fmt.Sprintf("value-%d", i))

			_ = store.Set(key, value)
			_, _ = store.Get(key)
		}
	})

	b.Run("MemoryStorage-Concurrent", func(b *testing.B) {
		storage := NewMemoryStorage()
		store := NewPartitionedStateStore(storage, 0)

		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("key-%d", i)
				value := []byte(fmt.Sprintf("value-%d", i))

				_ = store.Set(key, value)
				_, _ = store.Get(key)
				i++
			}
		})
	})
}

// BenchmarkPerformance_EndToEnd measures end-to-end throughput
func BenchmarkPerformance_EndToEnd(b *testing.B) {
	topic := fmt.Sprintf("perf-e2e-%d", time.Now().Unix())

	// Producer setup
	producerConfig := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithClientID("perf-e2e-producer").
		WithProducerBatchSize(16384).
		WithProducerLinger(10 * time.Millisecond).
		WithCompression(CompressionZstd).
		Build()

	producerClient, err := NewClient(producerConfig)
	if err != nil {
		b.Fatalf("Failed to create producer client: %v", err)
	}
	defer producerClient.Close()

	producer := NewProducer(producerClient)

	// Consumer setup
	consumerConfig := NewConfigBuilder().
		WithBrokers([]string{"localhost:9092"}).
		WithConsumerGroup(fmt.Sprintf("perf-e2e-group-%d", time.Now().Unix())).
		WithClientID("perf-e2e-consumer").
		Build()

	consumerClient, err := NewClient(consumerConfig)
	if err != nil {
		b.Fatalf("Failed to create consumer client: %v", err)
	}
	defer consumerClient.Close()

	consumer := NewConsumer(consumerClient, nil)

	// Start consumer
	var consumed atomic.Int64
	handler := func(ctx context.Context, record *kgo.Record) error {
		consumed.Add(1)
		return nil
	}

	ctx := context.Background()
	go func() {
		_ = consumer.Consume(ctx, []string{topic}, handler)
	}()

	// Give consumer time to subscribe
	time.Sleep(2 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	// Produce messages
	value := []byte("end-to-end test message")
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_ = producer.Produce(ctx, topic, key, value)
	}

	// Wait for consumption
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && consumed.Load() < int64(b.N) {
		time.Sleep(100 * time.Millisecond)
	}

	b.StopTimer()
	b.ReportMetric(float64(consumed.Load()), "messages/processed")
	b.ReportMetric(float64(consumed.Load())/b.Elapsed().Seconds(), "messages/sec")
}
