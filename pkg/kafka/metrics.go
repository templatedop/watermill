package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PrometheusMetrics provides Prometheus metrics for Kafka operations
type PrometheusMetrics struct {
	// Message counters
	messagesProduced    *prometheus.CounterVec
	messagesConsumed    *prometheus.CounterVec
	messagesFailed      *prometheus.CounterVec
	messagesRetried     *prometheus.CounterVec
	messagesSentToDLQ   *prometheus.CounterVec

	// Processing metrics
	processingDuration  *prometheus.HistogramVec
	batchSize          *prometheus.HistogramVec
	batchProcessingTime *prometheus.HistogramVec

	// Lag and health
	consumerLag         *prometheus.GaugeVec
	errorRate           *prometheus.GaugeVec
	activeConsumers     prometheus.Gauge
	activeProducers     prometheus.Gauge

	// State and storage
	stateSize           *prometheus.GaugeVec
	storageOperations   *prometheus.CounterVec

	// Circuit breaker
	circuitBreakerState *prometheus.GaugeVec

	registry *prometheus.Registry
	mu       sync.RWMutex
}

// NewPrometheusMetrics creates a new Prometheus metrics instance
func NewPrometheusMetrics(namespace string) *PrometheusMetrics {
	if namespace == "" {
		namespace = "watermill_kafka"
	}

	registry := prometheus.NewRegistry()

	m := &PrometheusMetrics{
		registry: registry,
	}

	m.messagesProduced = promauto.With(registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_produced_total",
			Help:      "Total number of messages produced",
		},
		[]string{"topic", "partition"},
	)

	m.messagesConsumed = promauto.With(registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_consumed_total",
			Help:      "Total number of messages consumed",
		},
		[]string{"topic", "consumer_group"},
	)

	m.messagesFailed = promauto.With(registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_failed_total",
			Help:      "Total number of messages that failed processing",
		},
		[]string{"topic", "consumer_group", "error_type"},
	)

	m.messagesRetried = promauto.With(registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_retried_total",
			Help:      "Total number of message retries",
		},
		[]string{"topic", "consumer_group"},
	)

	m.messagesSentToDLQ = promauto.With(registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_dlq_total",
			Help:      "Total number of messages sent to DLQ",
		},
		[]string{"topic", "consumer_group"},
	)

	m.processingDuration = promauto.With(registry).NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "processing_duration_seconds",
			Help:      "Message processing duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"topic", "consumer_group"},
	)

	m.batchSize = promauto.With(registry).NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "batch_size",
			Help:      "Number of messages in a batch",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"topic"},
	)

	m.batchProcessingTime = promauto.With(registry).NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "batch_processing_duration_seconds",
			Help:      "Batch processing duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"topic"},
	)

	m.consumerLag = promauto.With(registry).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "consumer_lag",
			Help:      "Current consumer lag",
		},
		[]string{"topic", "partition", "consumer_group"},
	)

	m.errorRate = promauto.With(registry).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "error_rate",
			Help:      "Current error rate (errors per second)",
		},
		[]string{"topic", "consumer_group"},
	)

	m.activeConsumers = promauto.With(registry).NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_consumers",
			Help:      "Number of active consumers",
		},
	)

	m.activeProducers = promauto.With(registry).NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_producers",
			Help:      "Number of active producers",
		},
	)

	m.stateSize = promauto.With(registry).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "state_size_bytes",
			Help:      "Size of processor state in bytes",
		},
		[]string{"processor", "partition"},
	)

	m.storageOperations = promauto.With(registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "storage_operations_total",
			Help:      "Total number of storage operations",
		},
		[]string{"operation", "storage_type"},
	)

	m.circuitBreakerState = promauto.With(registry).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (0=closed, 1=open, 2=half-open)",
		},
		[]string{"handler"},
	)

	return m
}

// RecordMessageProduced records a message production
func (m *PrometheusMetrics) RecordMessageProduced(topic string, partition int32) {
	m.messagesProduced.WithLabelValues(topic, string(rune(partition))).Inc()
}

// RecordMessageConsumed records a message consumption
func (m *PrometheusMetrics) RecordMessageConsumed(topic, consumerGroup string) {
	m.messagesConsumed.WithLabelValues(topic, consumerGroup).Inc()
}

// RecordMessageFailed records a failed message
func (m *PrometheusMetrics) RecordMessageFailed(topic, consumerGroup, errorType string) {
	m.messagesFailed.WithLabelValues(topic, consumerGroup, errorType).Inc()
}

// RecordMessageRetried records a message retry
func (m *PrometheusMetrics) RecordMessageRetried(topic, consumerGroup string) {
	m.messagesRetried.WithLabelValues(topic, consumerGroup).Inc()
}

// RecordMessageSentToDLQ records a message sent to DLQ
func (m *PrometheusMetrics) RecordMessageSentToDLQ(topic, consumerGroup string) {
	m.messagesSentToDLQ.WithLabelValues(topic, consumerGroup).Inc()
}

// RecordProcessingDuration records message processing duration
func (m *PrometheusMetrics) RecordProcessingDuration(topic, consumerGroup string, duration time.Duration) {
	m.processingDuration.WithLabelValues(topic, consumerGroup).Observe(duration.Seconds())
}

// RecordBatchSize records batch size
func (m *PrometheusMetrics) RecordBatchSize(topic string, size int) {
	m.batchSize.WithLabelValues(topic).Observe(float64(size))
}

// RecordBatchProcessingTime records batch processing time
func (m *PrometheusMetrics) RecordBatchProcessingTime(topic string, duration time.Duration) {
	m.batchProcessingTime.WithLabelValues(topic).Observe(duration.Seconds())
}

// SetConsumerLag sets the current consumer lag
func (m *PrometheusMetrics) SetConsumerLag(topic string, partition int32, consumerGroup string, lag int64) {
	m.consumerLag.WithLabelValues(topic, string(rune(partition)), consumerGroup).Set(float64(lag))
}

// SetErrorRate sets the current error rate
func (m *PrometheusMetrics) SetErrorRate(topic, consumerGroup string, rate float64) {
	m.errorRate.WithLabelValues(topic, consumerGroup).Set(rate)
}

// SetActiveConsumers sets the number of active consumers
func (m *PrometheusMetrics) SetActiveConsumers(count int) {
	m.activeConsumers.Set(float64(count))
}

// SetActiveProducers sets the number of active producers
func (m *PrometheusMetrics) SetActiveProducers(count int) {
	m.activeProducers.Set(float64(count))
}

// SetStateSize sets the processor state size
func (m *PrometheusMetrics) SetStateSize(processor string, partition int32, sizeBytes int64) {
	m.stateSize.WithLabelValues(processor, string(rune(partition))).Set(float64(sizeBytes))
}

// RecordStorageOperation records a storage operation
func (m *PrometheusMetrics) RecordStorageOperation(operation, storageType string) {
	m.storageOperations.WithLabelValues(operation, storageType).Inc()
}

// SetCircuitBreakerState sets the circuit breaker state
func (m *PrometheusMetrics) SetCircuitBreakerState(handler string, state int) {
	m.circuitBreakerState.WithLabelValues(handler).Set(float64(state))
}

// Registry returns the Prometheus registry
func (m *PrometheusMetrics) Registry() *prometheus.Registry {
	return m.registry
}

// MetricsCollector aggregates metrics over time windows
type MetricsCollector struct {
	metrics     *PrometheusMetrics
	windowSize  time.Duration
	errorCounts map[string]*windowedCounter
	mu          sync.RWMutex
}

type windowedCounter struct {
	counts    []int
	timestamps []time.Time
	windowSize time.Duration
	mu         sync.RWMutex
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(metrics *PrometheusMetrics, windowSize time.Duration) *MetricsCollector {
	return &MetricsCollector{
		metrics:     metrics,
		windowSize:  windowSize,
		errorCounts: make(map[string]*windowedCounter),
	}
}

// RecordError records an error and updates error rate
func (mc *MetricsCollector) RecordError(topic, consumerGroup string) {
	key := topic + ":" + consumerGroup

	mc.mu.Lock()
	counter, exists := mc.errorCounts[key]
	if !exists {
		counter = &windowedCounter{
			windowSize: mc.windowSize,
		}
		mc.errorCounts[key] = counter
	}
	mc.mu.Unlock()

	counter.mu.Lock()
	now := time.Now()
	counter.counts = append(counter.counts, 1)
	counter.timestamps = append(counter.timestamps, now)

	// Clean old entries
	cutoff := now.Add(-mc.windowSize)
	validIdx := 0
	for i, ts := range counter.timestamps {
		if ts.After(cutoff) {
			validIdx = i
			break
		}
	}
	if validIdx > 0 {
		counter.counts = counter.counts[validIdx:]
		counter.timestamps = counter.timestamps[validIdx:]
	}

	// Calculate rate
	total := 0
	for _, count := range counter.counts {
		total += count
	}
	rate := float64(total) / mc.windowSize.Seconds()
	counter.mu.Unlock()

	// Update Prometheus metric
	mc.metrics.SetErrorRate(topic, consumerGroup, rate)
}

// Start starts the metrics collector background tasks
func (mc *MetricsCollector) Start(ctx context.Context) {
	ticker := time.NewTicker(mc.windowSize / 10)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mc.cleanupOldMetrics()
		}
	}
}

func (mc *MetricsCollector) cleanupOldMetrics() {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	now := time.Now()
	cutoff := now.Add(-mc.windowSize * 2)

	for _, counter := range mc.errorCounts {
		counter.mu.Lock()
		validIdx := 0
		for i, ts := range counter.timestamps {
			if ts.After(cutoff) {
				validIdx = i
				break
			}
		}
		if validIdx > 0 {
			counter.counts = counter.counts[validIdx:]
			counter.timestamps = counter.timestamps[validIdx:]
		}
		counter.mu.Unlock()
	}
}
