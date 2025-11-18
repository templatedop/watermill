package franzgo

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TransformFunc transforms a single record
type TransformFunc func(record *kgo.Record) (*kgo.Record, error)

// FilterFunc determines if a record should be passed through
type FilterFunc func(record *kgo.Record) bool

// FlatMapFunc transforms a single record into multiple records
type FlatMapFunc func(record *kgo.Record) ([]*kgo.Record, error)

// StreamTransformer provides functional stream transformations
type StreamTransformer struct {
	client      *Client
	consumer    *Consumer
	producer    *Producer
	inputTopic  string
	outputTopic string
	mu          sync.RWMutex
	running     bool
	stopChan    chan struct{}
}

// NewStreamTransformer creates a new stream transformer
func NewStreamTransformer(client *Client, inputTopic, outputTopic string) *StreamTransformer {
	return &StreamTransformer{
		client:      client,
		consumer:    NewConsumer(client),
		producer:    NewProducer(client),
		inputTopic:  inputTopic,
		outputTopic: outputTopic,
		stopChan:    make(chan struct{}),
	}
}

// Map transforms each record using the provided function
func (st *StreamTransformer) Map(ctx context.Context, transformer TransformFunc) error {
	return st.consumer.Consume(ctx, []string{st.inputTopic}, func(ctx context.Context, record *kgo.Record) error {
		transformed, err := transformer(record)
		if err != nil {
			return fmt.Errorf("transformation failed: %w", err)
		}

		if transformed == nil {
			return nil // Skip record
		}

		// Produce to output topic
		return st.producer.Produce(ctx, st.outputTopic, transformed.Key, transformed.Value)
	})
}

// Filter filters records based on the provided predicate
func (st *StreamTransformer) Filter(ctx context.Context, predicate FilterFunc) error {
	return st.consumer.Consume(ctx, []string{st.inputTopic}, func(ctx context.Context, record *kgo.Record) error {
		if predicate(record) {
			// Produce to output topic
			return st.producer.Produce(ctx, st.outputTopic, record.Key, record.Value)
		}
		return nil // Filter out record
	})
}

// FlatMap transforms each record into zero or more records
func (st *StreamTransformer) FlatMap(ctx context.Context, transformer FlatMapFunc) error {
	return st.consumer.Consume(ctx, []string{st.inputTopic}, func(ctx context.Context, record *kgo.Record) error {
		records, err := transformer(record)
		if err != nil {
			return fmt.Errorf("flat map transformation failed: %w", err)
		}

		// Produce all transformed records
		for _, r := range records {
			if err := st.producer.Produce(ctx, st.outputTopic, r.Key, r.Value); err != nil {
				return fmt.Errorf("failed to produce transformed record: %w", err)
			}
		}

		return nil
	})
}

// Close closes the stream transformer
func (st *StreamTransformer) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.running {
		close(st.stopChan)
		st.running = false
	}

	return nil
}

// ChainableTransformer allows chaining multiple transformations
type ChainableTransformer struct {
	client       *Client
	consumer     *Consumer
	producer     *Producer
	inputTopic   string
	outputTopic  string
	transformers []transformStep
	mu           sync.RWMutex
}

type transformStep struct {
	stepType    string // "map", "filter", "flatmap"
	mapFunc     TransformFunc
	filterFunc  FilterFunc
	flatMapFunc FlatMapFunc
}

// NewChainableTransformer creates a new chainable transformer
func NewChainableTransformer(client *Client, inputTopic, outputTopic string) *ChainableTransformer {
	return &ChainableTransformer{
		client:       client,
		consumer:     NewConsumer(client),
		producer:     NewProducer(client),
		inputTopic:   inputTopic,
		outputTopic:  outputTopic,
		transformers: make([]transformStep, 0),
	}
}

// Map adds a map transformation to the chain
func (ct *ChainableTransformer) Map(transformer TransformFunc) *ChainableTransformer {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ct.transformers = append(ct.transformers, transformStep{
		stepType: "map",
		mapFunc:  transformer,
	})

	return ct
}

// Filter adds a filter transformation to the chain
func (ct *ChainableTransformer) Filter(predicate FilterFunc) *ChainableTransformer {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ct.transformers = append(ct.transformers, transformStep{
		stepType:   "filter",
		filterFunc: predicate,
	})

	return ct
}

// FlatMap adds a flat map transformation to the chain
func (ct *ChainableTransformer) FlatMap(transformer FlatMapFunc) *ChainableTransformer {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	ct.transformers = append(ct.transformers, transformStep{
		stepType:    "flatmap",
		flatMapFunc: transformer,
	})

	return ct
}

// Run runs the transformation chain
func (ct *ChainableTransformer) Run(ctx context.Context) error {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	if len(ct.transformers) == 0 {
		return fmt.Errorf("no transformations configured")
	}

	return ct.consumer.Consume(ctx, []string{ct.inputTopic}, func(ctx context.Context, record *kgo.Record) error {
		records, err := ct.applyChain(record)
		if err != nil {
			return err
		}

		// Produce all resulting records
		for _, r := range records {
			if err := ct.producer.Produce(ctx, ct.outputTopic, r.Key, r.Value); err != nil {
				return fmt.Errorf("failed to produce transformed record: %w", err)
			}
		}

		return nil
	})
}

func (ct *ChainableTransformer) applyChain(record *kgo.Record) ([]*kgo.Record, error) {
	records := []*kgo.Record{record}

	for _, step := range ct.transformers {
		var nextRecords []*kgo.Record

		for _, r := range records {
			switch step.stepType {
			case "map":
				transformed, err := step.mapFunc(r)
				if err != nil {
					return nil, err
				}
				if transformed != nil {
					nextRecords = append(nextRecords, transformed)
				}

			case "filter":
				if step.filterFunc(r) {
					nextRecords = append(nextRecords, r)
				}

			case "flatmap":
				expanded, err := step.flatMapFunc(r)
				if err != nil {
					return nil, err
				}
				nextRecords = append(nextRecords, expanded...)
			}
		}

		records = nextRecords

		// If no records left, stop processing
		if len(records) == 0 {
			return nil, nil
		}
	}

	return records, nil
}

// CommonTransformations provides commonly used transformation functions
type CommonTransformations struct{}

// AddHeader adds a header to the record
func (ct *CommonTransformations) AddHeader(key, value string) TransformFunc {
	return func(record *kgo.Record) (*kgo.Record, error) {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   key,
			Value: []byte(value),
		})
		return record, nil
	}
}

// RemoveHeader removes a header from the record
func (ct *CommonTransformations) RemoveHeader(key string) TransformFunc {
	return func(record *kgo.Record) (*kgo.Record, error) {
		filtered := make([]kgo.RecordHeader, 0, len(record.Headers))
		for _, h := range record.Headers {
			if h.Key != key {
				filtered = append(filtered, h)
			}
		}
		record.Headers = filtered
		return record, nil
	}
}

// RenameHeader renames a header
func (ct *CommonTransformations) RenameHeader(oldKey, newKey string) TransformFunc {
	return func(record *kgo.Record) (*kgo.Record, error) {
		for i, h := range record.Headers {
			if h.Key == oldKey {
				record.Headers[i].Key = newKey
			}
		}
		return record, nil
	}
}

// JSONTransform transforms the record payload using a JSON transformation
func (ct *CommonTransformations) JSONTransform(transformer func(data map[string]interface{}) (map[string]interface{}, error)) TransformFunc {
	return func(record *kgo.Record) (*kgo.Record, error) {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %w", err)
		}

		transformed, err := transformer(data)
		if err != nil {
			return nil, fmt.Errorf("transformation failed: %w", err)
		}

		payload, err := json.Marshal(transformed)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON: %w", err)
		}

		record.Value = payload
		return record, nil
	}
}

// FilterByHeader filters records by header value
func (ct *CommonTransformations) FilterByHeader(key, value string) FilterFunc {
	return func(record *kgo.Record) bool {
		for _, h := range record.Headers {
			if h.Key == key && string(h.Value) == value {
				return true
			}
		}
		return false
	}
}

// FilterByPayloadSize filters records by payload size
func (ct *CommonTransformations) FilterByPayloadSize(minSize, maxSize int) FilterFunc {
	return func(record *kgo.Record) bool {
		size := len(record.Value)
		return size >= minSize && size <= maxSize
	}
}

// SplitByDelimiter splits a record into multiple records by a delimiter
func (ct *CommonTransformations) SplitByDelimiter(delimiter byte) FlatMapFunc {
	return func(record *kgo.Record) ([]*kgo.Record, error) {
		parts := splitBytes(record.Value, delimiter)
		records := make([]*kgo.Record, 0, len(parts))

		for i, part := range parts {
			newRecord := &kgo.Record{
				Topic: record.Topic,
				Key:   record.Key,
				Value: part,
			}

			// Copy headers
			newRecord.Headers = make([]kgo.RecordHeader, len(record.Headers))
			copy(newRecord.Headers, record.Headers)

			// Add split metadata
			newRecord.Headers = append(newRecord.Headers,
				kgo.RecordHeader{Key: "split_index", Value: []byte(fmt.Sprintf("%d", i))},
				kgo.RecordHeader{Key: "split_total", Value: []byte(fmt.Sprintf("%d", len(parts)))},
			)

			records = append(records, newRecord)
		}

		return records, nil
	}
}

// ExpandArray expands a JSON array into multiple records
func (ct *CommonTransformations) ExpandArray(arrayField string) FlatMapFunc {
	return func(record *kgo.Record) ([]*kgo.Record, error) {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %w", err)
		}

		arrayData, ok := data[arrayField].([]interface{})
		if !ok {
			return nil, fmt.Errorf("field %s is not an array", arrayField)
		}

		records := make([]*kgo.Record, 0, len(arrayData))

		for i, item := range arrayData {
			payload, err := json.Marshal(item)
			if err != nil {
				return nil, fmt.Errorf("failed to encode item %d: %w", i, err)
			}

			newRecord := &kgo.Record{
				Topic: record.Topic,
				Key:   record.Key,
				Value: payload,
			}

			// Copy headers
			newRecord.Headers = make([]kgo.RecordHeader, len(record.Headers))
			copy(newRecord.Headers, record.Headers)

			// Add array metadata
			newRecord.Headers = append(newRecord.Headers,
				kgo.RecordHeader{Key: "array_index", Value: []byte(fmt.Sprintf("%d", i))},
				kgo.RecordHeader{Key: "array_total", Value: []byte(fmt.Sprintf("%d", len(arrayData)))},
			)

			records = append(records, newRecord)
		}

		return records, nil
	}
}

// Enrich enriches a record by adding data from a lookup function
func (ct *CommonTransformations) Enrich(lookupFunc func(record *kgo.Record) (map[string]interface{}, error)) TransformFunc {
	return func(record *kgo.Record) (*kgo.Record, error) {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %w", err)
		}

		enrichmentData, err := lookupFunc(record)
		if err != nil {
			return nil, fmt.Errorf("enrichment lookup failed: %w", err)
		}

		// Merge enrichment data
		for k, v := range enrichmentData {
			data[k] = v
		}

		payload, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON: %w", err)
		}

		record.Value = payload
		return record, nil
	}
}

// Helper function to split bytes by delimiter
func splitBytes(data []byte, delimiter byte) [][]byte {
	var result [][]byte
	var current []byte

	for _, b := range data {
		if b == delimiter {
			if len(current) > 0 {
				result = append(result, current)
				current = nil
			}
		} else {
			current = append(current, b)
		}
	}

	if len(current) > 0 {
		result = append(result, current)
	}

	return result
}

// AsyncTransformer provides asynchronous transformations with parallelism
type AsyncTransformer struct {
	client      *Client
	consumer    *Consumer
	producer    *Producer
	inputTopic  string
	outputTopic string
	workers     int
	transformer TransformFunc
}

// NewAsyncTransformer creates a new async transformer
func NewAsyncTransformer(client *Client, inputTopic, outputTopic string, workers int, transformer TransformFunc) *AsyncTransformer {
	return &AsyncTransformer{
		client:      client,
		consumer:    NewConsumer(client),
		producer:    NewProducer(client),
		inputTopic:  inputTopic,
		outputTopic: outputTopic,
		workers:     workers,
		transformer: transformer,
	}
}

// Run starts the async transformer with worker pool
func (at *AsyncTransformer) Run(ctx context.Context) error {
	// Create worker pool
	recordChan := make(chan *kgo.Record, at.workers*2)
	resultChan := make(chan *kgo.Record, at.workers*2)
	errorChan := make(chan error, at.workers)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < at.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for record := range recordChan {
				transformed, err := at.transformer(record)
				if err != nil {
					errorChan <- err
					continue
				}
				if transformed != nil {
					resultChan <- transformed
				}
			}
		}()
	}

	// Start result producer
	go func() {
		for record := range resultChan {
			if err := at.producer.Produce(ctx, at.outputTopic, record.Key, record.Value); err != nil {
				errorChan <- err
			}
		}
	}()

	// Close channels when workers are done
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// Consume and distribute to workers
	return at.consumer.Consume(ctx, []string{at.inputTopic}, func(ctx context.Context, record *kgo.Record) error {
		select {
		case recordChan <- record:
			return nil
		case err := <-errorChan:
			return err
		case <-ctx.Done():
			close(recordChan)
			return ctx.Err()
		}
	})
}
