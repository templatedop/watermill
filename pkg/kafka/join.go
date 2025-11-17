package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

// JoinType represents the type of join operation
type JoinType int

const (
	// InnerJoin only outputs when both sides have values
	InnerJoin JoinType = iota
	// LeftJoin outputs all left side values, with optional right values
	LeftJoin
	// OuterJoin outputs all values from both sides
	OuterJoin
)

// JoinConfig configures a join operation
type JoinConfig struct {
	// LeftTopic is the left side of the join
	LeftTopic string

	// RightTopic is the right side of the join (typically a group table)
	RightTopic string

	// OutputTopic where joined results are published
	OutputTopic string

	// JoinType determines the join semantics
	JoinType JoinType

	// Codec for serialization
	Codec Codec

	// Storage for caching
	Storage Storage

	// Window for time-based joins (optional)
	Window time.Duration
}

// JoinFunc is a callback for join operations
type JoinFunc func(ctx context.Context, leftKey string, leftValue interface{}, rightValue interface{}) (interface{}, error)

// StreamJoiner performs stream-table joins
type StreamJoiner struct {
	config     *JoinConfig
	client     *Client
	logger     watermill.LoggerAdapter
	joinFunc   JoinFunc
	rightView  *View
	cancelFunc context.CancelFunc
}

// NewStreamJoiner creates a new stream joiner
func NewStreamJoiner(client *Client, config *JoinConfig, joinFunc JoinFunc) (*StreamJoiner, error) {
	if config.LeftTopic == "" || config.RightTopic == "" {
		return nil, fmt.Errorf("both topics are required")
	}

	if config.Codec == nil {
		config.Codec = NewJSONCodec()
	}

	if config.Storage == nil {
		config.Storage = NewMemoryStorage()
	}

	// Create view for right side (table)
	rightView, err := NewView(client, config.RightTopic, config.Codec, NewMemoryStorage())
	if err != nil {
		return nil, fmt.Errorf("failed to create right view: %w", err)
	}

	return &StreamJoiner{
		config:    config,
		client:    client,
		logger:    client.logger,
		joinFunc:  joinFunc,
		rightView: rightView,
	}, nil
}

// Start starts the joiner
func (j *StreamJoiner) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	j.cancelFunc = cancel

	// Start right view
	if err := j.rightView.Start(ctx); err != nil {
		return fmt.Errorf("failed to start right view: %w", err)
	}

	// Wait for view to be ready
	j.rightView.WaitReady()

	// Subscribe to left topic
	messages, err := j.client.subscriber.Subscribe(ctx, j.config.LeftTopic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to left topic: %w", err)
	}

	// Start processing joins
	go j.processJoins(ctx, messages)

	j.logger.Info("Stream joiner started", watermill.LogFields{
		"left_topic":  j.config.LeftTopic,
		"right_topic": j.config.RightTopic,
	})

	return nil
}

// Stop stops the joiner
func (j *StreamJoiner) Stop() error {
	if j.cancelFunc != nil {
		j.cancelFunc()
	}
	return j.rightView.Stop()
}

// processJoins processes join operations
func (j *StreamJoiner) processJoins(ctx context.Context, messages <-chan *message.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			if err := j.processJoin(ctx, msg); err != nil {
				j.logger.Error("Join processing failed", err, watermill.LogFields{
					"message_uuid": msg.UUID,
				})
				msg.Nack()
			} else {
				msg.Ack()
			}
		}
	}
}

// processJoin processes a single join
func (j *StreamJoiner) processJoin(ctx context.Context, msg *message.Message) error {
	// Extract key
	key := msg.Metadata.Get("partition_key")
	if key == "" {
		key = msg.UUID
	}

	// Decode left value
	var leftValue interface{}
	if err := j.config.Codec.Decode(msg.Payload, &leftValue); err != nil {
		return fmt.Errorf("failed to decode left value: %w", err)
	}

	// Lookup right value
	rightValue, err := j.rightView.Get(key)
	if err != nil {
		return fmt.Errorf("failed to get right value: %w", err)
	}

	// Apply join semantics
	shouldOutput := false
	switch j.config.JoinType {
	case InnerJoin:
		shouldOutput = rightValue != nil
	case LeftJoin:
		shouldOutput = true
	case OuterJoin:
		shouldOutput = true
	}

	if !shouldOutput {
		return nil
	}

	// Call join function
	result, err := j.joinFunc(ctx, key, leftValue, rightValue)
	if err != nil {
		return fmt.Errorf("join function failed: %w", err)
	}

	// Encode and publish result
	if result != nil && j.config.OutputTopic != "" {
		data, err := j.config.Codec.Encode(result)
		if err != nil {
			return fmt.Errorf("failed to encode result: %w", err)
		}

		outputMsg := message.NewMessage(uuid.New().String(), data)
		outputMsg.Metadata.Set("partition_key", key)

		if err := j.client.publisher.Publish(j.config.OutputTopic, outputMsg); err != nil {
			return fmt.Errorf("failed to publish result: %w", err)
		}
	}

	return nil
}

// StreamStreamJoiner performs stream-stream joins with windowing
type StreamStreamJoiner struct {
	config     *JoinConfig
	client     *Client
	logger     watermill.LoggerAdapter
	joinFunc   JoinFunc
	leftCache  Storage
	rightCache Storage
	cancelFunc context.CancelFunc
}

// NewStreamStreamJoiner creates a stream-stream joiner
func NewStreamStreamJoiner(client *Client, config *JoinConfig, joinFunc JoinFunc) (*StreamStreamJoiner, error) {
	if config.LeftTopic == "" || config.RightTopic == "" {
		return nil, fmt.Errorf("both topics are required")
	}

	if config.Window == 0 {
		config.Window = 5 * time.Minute // Default window
	}

	if config.Codec == nil {
		config.Codec = NewJSONCodec()
	}

	return &StreamStreamJoiner{
		config:     config,
		client:     client,
		logger:     client.logger,
		joinFunc:   joinFunc,
		leftCache:  NewMemoryStorage(),
		rightCache: NewMemoryStorage(),
	}, nil
}

// Start starts the stream-stream joiner
func (j *StreamStreamJoiner) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	j.cancelFunc = cancel

	// Subscribe to both topics
	leftMessages, err := j.client.subscriber.Subscribe(ctx, j.config.LeftTopic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to left topic: %w", err)
	}

	rightMessages, err := j.client.subscriber.Subscribe(ctx, j.config.RightTopic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to right topic: %w", err)
	}

	// Start processing both streams
	go j.processLeftStream(ctx, leftMessages)
	go j.processRightStream(ctx, rightMessages)

	j.logger.Info("Stream-stream joiner started", watermill.LogFields{
		"left_topic":  j.config.LeftTopic,
		"right_topic": j.config.RightTopic,
		"window":      j.config.Window.String(),
	})

	return nil
}

// Stop stops the joiner
func (j *StreamStreamJoiner) Stop() error {
	if j.cancelFunc != nil {
		j.cancelFunc()
	}
	return nil
}

// processLeftStream processes the left stream
func (j *StreamStreamJoiner) processLeftStream(ctx context.Context, messages <-chan *message.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			key := msg.Metadata.Get("partition_key")
			if key == "" {
				key = msg.UUID
			}

			// Store in left cache
			j.leftCache.Set(key, msg.Payload)

			// Try to join with right
			if err := j.tryJoin(ctx, key, msg.Payload, true); err != nil {
				j.logger.Error("Join failed", err, watermill.LogFields{
					"key": key,
				})
			}

			msg.Ack()
		}
	}
}

// processRightStream processes the right stream
func (j *StreamStreamJoiner) processRightStream(ctx context.Context, messages <-chan *message.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			key := msg.Metadata.Get("partition_key")
			if key == "" {
				key = msg.UUID
			}

			// Store in right cache
			j.rightCache.Set(key, msg.Payload)

			// Try to join with left
			if err := j.tryJoin(ctx, key, nil, false); err != nil {
				j.logger.Error("Join failed", err, watermill.LogFields{
					"key": key,
				})
			}

			msg.Ack()
		}
	}
}

// tryJoin attempts to join values
func (j *StreamStreamJoiner) tryJoin(ctx context.Context, key string, leftData []byte, fromLeft bool) error {
	var leftValue, rightValue interface{}

	// Get left value
	if fromLeft {
		if err := j.config.Codec.Decode(leftData, &leftValue); err != nil {
			return err
		}
	} else {
		data, err := j.leftCache.Get(key)
		if err != nil {
			return nil // No left value yet
		}
		if err := j.config.Codec.Decode(data, &leftValue); err != nil {
			return err
		}
	}

	// Get right value
	rightData, err := j.rightCache.Get(key)
	if err != nil {
		// No right value yet
		if j.config.JoinType == InnerJoin {
			return nil
		}
	} else {
		if err := j.config.Codec.Decode(rightData, &rightValue); err != nil {
			return err
		}
	}

	// Call join function
	result, err := j.joinFunc(ctx, key, leftValue, rightValue)
	if err != nil {
		return err
	}

	// Publish result
	if result != nil && j.config.OutputTopic != "" {
		data, err := j.config.Codec.Encode(result)
		if err != nil {
			return err
		}

		msg := message.NewMessage(uuid.New().String(), data)
		msg.Metadata.Set("partition_key", key)

		return j.client.publisher.Publish(j.config.OutputTopic, msg)
	}

	return nil
}
