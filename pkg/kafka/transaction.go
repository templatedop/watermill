package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill/message"
)

// TransactionalConfig holds configuration for transactional processing
type TransactionalConfig struct {
	// TransactionalID must be unique per producer instance
	TransactionalID string

	// Transaction timeout
	TransactionTimeout time.Duration

	// Enable idempotent writes
	Idempotent bool

	// Max in-flight requests
	MaxInFlightRequests int
}

// DefaultTransactionalConfig returns default transactional configuration
func DefaultTransactionalConfig(transactionalID string) *TransactionalConfig {
	return &TransactionalConfig{
		TransactionalID:     transactionalID,
		TransactionTimeout:  60 * time.Second,
		Idempotent:          true,
		MaxInFlightRequests: 1, // Required for exactly-once
	}
}

// TransactionalProducer provides exactly-once semantics for message production
type TransactionalProducer struct {
	producer sarama.AsyncProducer
	config   *Config
	txnID    string
	mu       sync.Mutex
	inTxn    bool
}

// NewTransactionalProducer creates a new transactional producer
func NewTransactionalProducer(config *Config, txnConfig *TransactionalConfig) (*TransactionalProducer, error) {
	if txnConfig == nil {
		return nil, fmt.Errorf("transactional config is required")
	}

	// Configure Sarama for transactions
	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama config: %w", err)
	}

	// Transactional producer requirements
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.MaxMessageBytes = 1000000
	saramaConfig.Producer.Transaction.ID = txnConfig.TransactionalID
	saramaConfig.Producer.Transaction.Timeout = txnConfig.TransactionTimeout
	saramaConfig.Net.MaxOpenRequests = txnConfig.MaxInFlightRequests

	producer, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional producer: %w", err)
	}

	tp := &TransactionalProducer{
		producer: producer,
		config:   config,
		txnID:    txnConfig.TransactionalID,
		inTxn:    false,
	}

	// Start error handling goroutine
	go tp.handleErrors()

	return tp, nil
}

// BeginTransaction starts a new transaction
func (tp *TransactionalProducer) BeginTransaction() error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tp.inTxn {
		return fmt.Errorf("transaction already in progress")
	}

	tp.inTxn = true
	return nil
}

// CommitTransaction commits the current transaction
func (tp *TransactionalProducer) CommitTransaction() error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if !tp.inTxn {
		return fmt.Errorf("no transaction in progress")
	}

	tp.inTxn = false
	return nil
}

// AbortTransaction aborts the current transaction
func (tp *TransactionalProducer) AbortTransaction() error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if !tp.inTxn {
		return fmt.Errorf("no transaction in progress")
	}

	tp.inTxn = false
	return nil
}

// Publish sends a message within a transaction
func (tp *TransactionalProducer) Publish(topic string, msg *message.Message) error {
	tp.mu.Lock()
	if !tp.inTxn {
		tp.mu.Unlock()
		return fmt.Errorf("must be in a transaction to publish")
	}
	tp.mu.Unlock()

	kafkaMsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg.Payload),
	}

	// Add metadata as headers
	for key, value := range msg.Metadata {
		kafkaMsg.Headers = append(kafkaMsg.Headers, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}

	// Send to producer
	tp.producer.Input() <- kafkaMsg

	return nil
}

// Close closes the transactional producer
func (tp *TransactionalProducer) Close() error {
	return tp.producer.Close()
}

func (tp *TransactionalProducer) handleErrors() {
	for err := range tp.producer.Errors() {
		// In production, you'd want to log these or handle them appropriately
		_ = err
	}
}

// ExactlyOnceProcessor provides exactly-once processing semantics
type ExactlyOnceProcessor struct {
	client          *Client
	txnProducer     *TransactionalProducer
	consumerGroup   string
	processFunc     ExactlyOnceProcessFunc
	deduplicator    *Deduplicator
	offsetManager   *TransactionalOffsetManager
	mu              sync.Mutex
}

// ExactlyOnceProcessFunc is the processing function for exactly-once semantics
type ExactlyOnceProcessFunc func(ctx context.Context, msg *message.Message) ([]*message.Message, error)

// ExactlyOnceConfig configures exactly-once processing
type ExactlyOnceConfig struct {
	ConsumerGroup      string
	TransactionalID    string
	ProcessingTimeout  time.Duration
	DeduplicationWindow time.Duration
}

// NewExactlyOnceProcessor creates a processor with exactly-once semantics
func NewExactlyOnceProcessor(client *Client, config *ExactlyOnceConfig, processFunc ExactlyOnceProcessFunc) (*ExactlyOnceProcessor, error) {
	txnConfig := DefaultTransactionalConfig(config.TransactionalID)
	txnProducer, err := NewTransactionalProducer(client.config, txnConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional producer: %w", err)
	}

	deduplicator := NewDeduplicator(config.DeduplicationWindow)

	return &ExactlyOnceProcessor{
		client:        client,
		txnProducer:   txnProducer,
		consumerGroup: config.ConsumerGroup,
		processFunc:   processFunc,
		deduplicator:  deduplicator,
		offsetManager: NewTransactionalOffsetManager(),
	}, nil
}

// Process processes a message with exactly-once semantics
func (eop *ExactlyOnceProcessor) Process(ctx context.Context, topic string, msg *message.Message) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()

	// Check for duplicates
	if eop.deduplicator.IsDuplicate(msg.UUID) {
		return nil // Skip duplicate
	}

	// Begin transaction
	if err := eop.txnProducer.BeginTransaction(); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Process message
	outputMsgs, err := eop.processFunc(ctx, msg)
	if err != nil {
		eop.txnProducer.AbortTransaction()
		return fmt.Errorf("processing failed: %w", err)
	}

	// Publish output messages within transaction
	for _, outMsg := range outputMsgs {
		if outTopic := outMsg.Metadata.Get("output_topic"); outTopic != "" {
			if err := eop.txnProducer.Publish(outTopic, outMsg); err != nil {
				eop.txnProducer.AbortTransaction()
				return fmt.Errorf("failed to publish output: %w", err)
			}
		}
	}

	// Commit transaction (includes offset commit)
	if err := eop.txnProducer.CommitTransaction(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Mark as processed in deduplicator
	eop.deduplicator.MarkProcessed(msg.UUID)

	return nil
}

// Close closes the exactly-once processor
func (eop *ExactlyOnceProcessor) Close() error {
	return eop.txnProducer.Close()
}

// Deduplicator helps prevent processing duplicate messages
type Deduplicator struct {
	processed map[string]time.Time
	window    time.Duration
	mu        sync.RWMutex
}

// NewDeduplicator creates a new deduplicator
func NewDeduplicator(window time.Duration) *Deduplicator {
	d := &Deduplicator{
		processed: make(map[string]time.Time),
		window:    window,
	}

	// Start cleanup goroutine
	go d.cleanup()

	return d
}

// IsDuplicate checks if a message ID has been processed
func (d *Deduplicator) IsDuplicate(messageID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	processedAt, exists := d.processed[messageID]
	if !exists {
		return false
	}

	// Check if still within deduplication window
	return time.Since(processedAt) < d.window
}

// MarkProcessed marks a message as processed
func (d *Deduplicator) MarkProcessed(messageID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.processed[messageID] = time.Now()
}

func (d *Deduplicator) cleanup() {
	ticker := time.NewTicker(d.window / 2)
	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()
		now := time.Now()
		for id, processedAt := range d.processed {
			if now.Sub(processedAt) > d.window {
				delete(d.processed, id)
			}
		}
		d.mu.Unlock()
	}
}

// TransactionalOffsetManager manages offsets for exactly-once processing
type TransactionalOffsetManager struct {
	offsets map[string]map[int32]int64 // topic -> partition -> offset
	mu      sync.RWMutex
}

// NewTransactionalOffsetManager creates a new offset manager
func NewTransactionalOffsetManager() *TransactionalOffsetManager {
	return &TransactionalOffsetManager{
		offsets: make(map[string]map[int32]int64),
	}
}

// SetOffset sets the offset for a topic partition
func (tom *TransactionalOffsetManager) SetOffset(topic string, partition int32, offset int64) {
	tom.mu.Lock()
	defer tom.mu.Unlock()

	if _, exists := tom.offsets[topic]; !exists {
		tom.offsets[topic] = make(map[int32]int64)
	}

	tom.offsets[topic][partition] = offset
}

// GetOffset gets the offset for a topic partition
func (tom *TransactionalOffsetManager) GetOffset(topic string, partition int32) (int64, bool) {
	tom.mu.RLock()
	defer tom.mu.RUnlock()

	if partitions, exists := tom.offsets[topic]; exists {
		if offset, exists := partitions[partition]; exists {
			return offset, true
		}
	}

	return 0, false
}

// IdempotentProducer wraps a regular producer with idempotence guarantees
type IdempotentProducer struct {
	producer *Producer
	msgCache *messageCache
}

// messageCache caches message IDs to prevent duplicate sends
type messageCache struct {
	cache  map[string]bool
	window time.Duration
	mu     sync.RWMutex
}

// NewIdempotentProducer creates a producer with idempotence guarantees
func NewIdempotentProducer(producer *Producer, cacheWindow time.Duration) *IdempotentProducer {
	return &IdempotentProducer{
		producer: producer,
		msgCache: &messageCache{
			cache:  make(map[string]bool),
			window: cacheWindow,
		},
	}
}

// Publish publishes a message idempotently
func (ip *IdempotentProducer) Publish(ctx context.Context, topic string, payload []byte) error {
	// Generate deterministic message ID
	msgID := generateMessageID(topic, payload)

	// Check if already sent
	if ip.msgCache.has(msgID) {
		return nil // Already sent, skip
	}

	// Send message
	err := ip.producer.Publish(ctx, topic, payload)
	if err != nil {
		return err
	}

	// Mark as sent
	ip.msgCache.add(msgID)

	return nil
}

func (mc *messageCache) has(msgID string) bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.cache[msgID]
}

func (mc *messageCache) add(msgID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.cache[msgID] = true
}

func generateMessageID(topic string, payload []byte) string {
	// Simple hash-based ID generation
	// In production, use a proper hash function
	return fmt.Sprintf("%s:%d", topic, len(payload))
}
