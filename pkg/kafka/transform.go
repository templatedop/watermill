package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
)

// TransformFunc transforms a single message
type TransformFunc func(msg *message.Message) (*message.Message, error)

// FilterFunc determines if a message should be passed through
type FilterFunc func(msg *message.Message) bool

// FlatMapFunc transforms a single message into multiple messages
type FlatMapFunc func(msg *message.Message) ([]*message.Message, error)

// StreamTransformer provides functional stream transformations
type StreamTransformer struct {
	client    *Client
	inputTopic string
	outputTopic string
	router     *message.Router
	mu         sync.RWMutex
}

// NewStreamTransformer creates a new stream transformer
func NewStreamTransformer(client *Client, inputTopic, outputTopic string) *StreamTransformer {
	return &StreamTransformer{
		client:      client,
		inputTopic:  inputTopic,
		outputTopic: outputTopic,
	}
}

// Map transforms each message using the provided function
func (st *StreamTransformer) Map(transformer TransformFunc) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	router, err := st.client.CreateRouter()
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
	}

	router.AddHandler(
		"map_handler",
		st.inputTopic,
		st.client.GetSubscriber(),
		st.outputTopic,
		st.client.GetPublisher(),
		func(msg *message.Message) ([]*message.Message, error) {
			transformed, err := transformer(msg)
			if err != nil {
				return nil, fmt.Errorf("transformation failed: %w", err)
			}

			if transformed == nil {
				return nil, nil // Skip message
			}

			return []*message.Message{transformed}, nil
		},
	)

	st.router = router
	return nil
}

// Filter filters messages based on the provided predicate
func (st *StreamTransformer) Filter(predicate FilterFunc) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	router, err := st.client.CreateRouter()
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
	}

	router.AddHandler(
		"filter_handler",
		st.inputTopic,
		st.client.GetSubscriber(),
		st.outputTopic,
		st.client.GetPublisher(),
		func(msg *message.Message) ([]*message.Message, error) {
			if predicate(msg) {
				return []*message.Message{msg}, nil
			}
			return nil, nil // Filter out message
		},
	)

	st.router = router
	return nil
}

// FlatMap transforms each message into zero or more messages
func (st *StreamTransformer) FlatMap(transformer FlatMapFunc) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	router, err := st.client.CreateRouter()
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
	}

	router.AddHandler(
		"flatmap_handler",
		st.inputTopic,
		st.client.GetSubscriber(),
		st.outputTopic,
		st.client.GetPublisher(),
		func(msg *message.Message) ([]*message.Message, error) {
			messages, err := transformer(msg)
			if err != nil {
				return nil, fmt.Errorf("flat map transformation failed: %w", err)
			}

			return messages, nil
		},
	)

	st.router = router
	return nil
}

// Run starts the stream transformer
func (st *StreamTransformer) Run(ctx context.Context) error {
	st.mu.RLock()
	defer st.mu.RUnlock()

	if st.router == nil {
		return fmt.Errorf("no transformation configured")
	}

	return st.router.Run(ctx)
}

// Close closes the stream transformer
func (st *StreamTransformer) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.router != nil {
		return st.router.Close()
	}

	return nil
}

// ChainableTransformer allows chaining multiple transformations
type ChainableTransformer struct {
	client       *Client
	inputTopic   string
	outputTopic  string
	transformers []transformStep
	mu           sync.RWMutex
}

type transformStep struct {
	stepType     string // "map", "filter", "flatmap"
	mapFunc      TransformFunc
	filterFunc   FilterFunc
	flatMapFunc  FlatMapFunc
}

// NewChainableTransformer creates a new chainable transformer
func NewChainableTransformer(client *Client, inputTopic, outputTopic string) *ChainableTransformer {
	return &ChainableTransformer{
		client:       client,
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

// Build builds and runs the transformation chain
func (ct *ChainableTransformer) Build() error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if len(ct.transformers) == 0 {
		return fmt.Errorf("no transformations configured")
	}

	router, err := ct.client.CreateRouter()
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
	}

	router.AddHandler(
		"chain_handler",
		ct.inputTopic,
		ct.client.GetSubscriber(),
		ct.outputTopic,
		ct.client.GetPublisher(),
		func(msg *message.Message) ([]*message.Message, error) {
			return ct.applyChain(msg)
		},
	)

	return router.Run(context.Background())
}

func (ct *ChainableTransformer) applyChain(msg *message.Message) ([]*message.Message, error) {
	messages := []*message.Message{msg}

	for _, step := range ct.transformers {
		var nextMessages []*message.Message

		for _, m := range messages {
			switch step.stepType {
			case "map":
				transformed, err := step.mapFunc(m)
				if err != nil {
					return nil, err
				}
				if transformed != nil {
					nextMessages = append(nextMessages, transformed)
				}

			case "filter":
				if step.filterFunc(m) {
					nextMessages = append(nextMessages, m)
				}

			case "flatmap":
				expanded, err := step.flatMapFunc(m)
				if err != nil {
					return nil, err
				}
				nextMessages = append(nextMessages, expanded...)
			}
		}

		messages = nextMessages

		// If no messages left, stop processing
		if len(messages) == 0 {
			return nil, nil
		}
	}

	return messages, nil
}

// CommonTransformations provides commonly used transformation functions
type CommonTransformations struct{}

// AddField adds a field to the message metadata
func (ct *CommonTransformations) AddField(key, value string) TransformFunc {
	return func(msg *message.Message) (*message.Message, error) {
		msg.Metadata.Set(key, value)
		return msg, nil
	}
}

// RemoveField removes a field from the message metadata
func (ct *CommonTransformations) RemoveField(key string) TransformFunc {
	return func(msg *message.Message) (*message.Message, error) {
		delete(msg.Metadata, key)
		return msg, nil
	}
}

// RenameField renames a metadata field
func (ct *CommonTransformations) RenameField(oldKey, newKey string) TransformFunc {
	return func(msg *message.Message) (*message.Message, error) {
		if value := msg.Metadata.Get(oldKey); value != "" {
			msg.Metadata.Set(newKey, value)
			delete(msg.Metadata, oldKey)
		}
		return msg, nil
	}
}

// JSONTransform transforms the message payload using a JSON transformation
func (ct *CommonTransformations) JSONTransform(transformer func(data map[string]interface{}) (map[string]interface{}, error)) TransformFunc {
	codec := &JSONCodec{}

	return func(msg *message.Message) (*message.Message, error) {
		var data map[string]interface{}
		if err := codec.Decode(msg.Payload, &data); err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %w", err)
		}

		transformed, err := transformer(data)
		if err != nil {
			return nil, fmt.Errorf("transformation failed: %w", err)
		}

		payload, err := codec.Encode(transformed)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON: %w", err)
		}

		msg.Payload = payload
		return msg, nil
	}
}

// FilterByMetadata filters messages by metadata field value
func (ct *CommonTransformations) FilterByMetadata(key, value string) FilterFunc {
	return func(msg *message.Message) bool {
		return msg.Metadata.Get(key) == value
	}
}

// FilterByPayloadSize filters messages by payload size
func (ct *CommonTransformations) FilterByPayloadSize(minSize, maxSize int) FilterFunc {
	return func(msg *message.Message) bool {
		size := len(msg.Payload)
		return size >= minSize && size <= maxSize
	}
}

// SplitByDelimiter splits a message into multiple messages by a delimiter
func (ct *CommonTransformations) SplitByDelimiter(delimiter byte) FlatMapFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		parts := splitBytes(msg.Payload, delimiter)
		messages := make([]*message.Message, 0, len(parts))

		for i, part := range parts {
			newMsg := message.NewMessage(
				fmt.Sprintf("%s-%d", msg.UUID, i),
				part,
			)

			// Copy metadata
			for k, v := range msg.Metadata {
				newMsg.Metadata.Set(k, v)
			}

			newMsg.Metadata.Set("split_index", fmt.Sprintf("%d", i))
			newMsg.Metadata.Set("split_total", fmt.Sprintf("%d", len(parts)))

			messages = append(messages, newMsg)
		}

		return messages, nil
	}
}

// ExpandArray expands a JSON array into multiple messages
func (ct *CommonTransformations) ExpandArray(arrayField string) FlatMapFunc {
	codec := &JSONCodec{}

	return func(msg *message.Message) ([]*message.Message, error) {
		var data map[string]interface{}
		if err := codec.Decode(msg.Payload, &data); err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %w", err)
		}

		arrayData, ok := data[arrayField].([]interface{})
		if !ok {
			return nil, fmt.Errorf("field %s is not an array", arrayField)
		}

		messages := make([]*message.Message, 0, len(arrayData))

		for i, item := range arrayData {
			payload, err := codec.Encode(item)
			if err != nil {
				return nil, fmt.Errorf("failed to encode item %d: %w", i, err)
			}

			newMsg := message.NewMessage(
				fmt.Sprintf("%s-%d", msg.UUID, i),
				payload,
			)

			// Copy metadata
			for k, v := range msg.Metadata {
				newMsg.Metadata.Set(k, v)
			}

			newMsg.Metadata.Set("array_index", fmt.Sprintf("%d", i))
			newMsg.Metadata.Set("array_total", fmt.Sprintf("%d", len(arrayData)))

			messages = append(messages, newMsg)
		}

		return messages, nil
	}
}

// Enrich enriches a message by adding data from a lookup function
func (ct *CommonTransformations) Enrich(lookupFunc func(msg *message.Message) (map[string]interface{}, error)) TransformFunc {
	codec := &JSONCodec{}

	return func(msg *message.Message) (*message.Message, error) {
		var data map[string]interface{}
		if err := codec.Decode(msg.Payload, &data); err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %w", err)
		}

		enrichmentData, err := lookupFunc(msg)
		if err != nil {
			return nil, fmt.Errorf("enrichment lookup failed: %w", err)
		}

		// Merge enrichment data
		for k, v := range enrichmentData {
			data[k] = v
		}

		payload, err := codec.Encode(data)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON: %w", err)
		}

		msg.Payload = payload
		return msg, nil
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
	inputTopic  string
	outputTopic string
	workers     int
	transformer TransformFunc
}

// NewAsyncTransformer creates a new async transformer
func NewAsyncTransformer(client *Client, inputTopic, outputTopic string, workers int, transformer TransformFunc) *AsyncTransformer {
	return &AsyncTransformer{
		client:      client,
		inputTopic:  inputTopic,
		outputTopic: outputTopic,
		workers:     workers,
		transformer: transformer,
	}
}

// Run starts the async transformer
func (at *AsyncTransformer) Run(ctx context.Context) error {
	router, err := at.client.CreateRouter()
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
	}

	// Create worker pool
	messageChan := make(chan *message.Message, at.workers*2)
	resultChan := make(chan *message.Message, at.workers*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < at.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range messageChan {
				transformed, err := at.transformer(msg)
				if err != nil {
					// Log error, skip message
					continue
				}
				if transformed != nil {
					resultChan <- transformed
				}
			}
		}()
	}

	// Close result channel when workers are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	router.AddHandler(
		"async_handler",
		at.inputTopic,
		at.client.GetSubscriber(),
		at.outputTopic,
		at.client.GetPublisher(),
		func(msg *message.Message) ([]*message.Message, error) {
			messageChan <- msg

			// Non-blocking send
			select {
			case result := <-resultChan:
				return []*message.Message{result}, nil
			default:
				return nil, nil
			}
		},
	)

	return router.Run(ctx)
}
