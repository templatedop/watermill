package franzgo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TransactionalConfig holds configuration for transactional processing
type TransactionalConfig struct {
	// TransactionalID must be unique per producer instance
	TransactionalID string

	// Transaction timeout
	TransactionTimeout time.Duration

	// Enable idempotent writes (required for transactions)
	Idempotent bool

	// Deduplication window
	DeduplicationWindow time.Duration
}

// DefaultTransactionalConfig returns default transactional configuration
func DefaultTransactionalConfig(transactionalID string) *TransactionalConfig {
	return &TransactionalConfig{
		TransactionalID:     transactionalID,
		TransactionTimeout:  60 * time.Second,
		Idempotent:          true,
		DeduplicationWindow: 5 * time.Minute,
	}
}

// TransactionalProducer provides exactly-once semantics for message production
// Franz-go makes transactions much simpler than sarama!
type TransactionalProducer struct {
	client *kgo.Client
	config *TransactionalConfig
	mu     sync.Mutex
}

// NewTransactionalProducer creates a new transactional producer
func NewTransactionalProducer(config *Config, txnConfig *TransactionalConfig) (*TransactionalProducer, error) {
	if txnConfig == nil {
		return nil, fmt.Errorf("transactional config is required")
	}

	// Create franz-go options with transactional support
	opts, err := config.ToKgoOpts()
	if err != nil {
		return nil, fmt.Errorf("failed to create options: %w", err)
	}

	// Add transactional options
	opts = append(opts,
		kgo.TransactionalID(txnConfig.TransactionalID),
		kgo.TransactionTimeout(txnConfig.TransactionTimeout),
		kgo.RequiredAcks(kgo.AllISRAcks()), // Required for transactions
		kgo.DisableIdempotentWrite(false),  // Idempotent writes required
	)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional client: %w", err)
	}

	return &TransactionalProducer{
		client: client,
		config: txnConfig,
	}, nil
}

// ExecuteTransaction executes a function within a transaction
// This is franz-go's simple transaction API - much cleaner than sarama!
func (tp *TransactionalProducer) ExecuteTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	// Begin transaction
	if err := tp.client.BeginTransaction(); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute user function
	err := fn(ctx)
	if err != nil {
		// Abort on error
		if abortErr := tp.client.AbortBufferedRecords(ctx); abortErr != nil {
			return fmt.Errorf("transaction failed and abort failed: %w (original error: %v)", abortErr, err)
		}
		return fmt.Errorf("transaction aborted: %w", err)
	}

	// Commit transaction
	if err := tp.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Produce sends a message within the current transaction
func (tp *TransactionalProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	// Franz-go handles buffering internally
	results := tp.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

// ProduceWithHeaders sends a message with headers within the current transaction
func (tp *TransactionalProducer) ProduceWithHeaders(ctx context.Context, topic string, key, value []byte, headers map[string]string) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	for k, v := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   k,
			Value: []byte(v),
		})
	}

	results := tp.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

// Close closes the transactional producer
func (tp *TransactionalProducer) Close() error {
	tp.client.Close()
	return nil
}

// ExactlyOnceProcessor provides exactly-once processing semantics
type ExactlyOnceProcessor struct {
	client       *Client
	txnProducer  *TransactionalProducer
	processFunc  ExactlyOnceProcessFunc
	deduplicator *Deduplicator
	mu           sync.Mutex
}

// ExactlyOnceProcessFunc is the processing function for exactly-once semantics
type ExactlyOnceProcessFunc func(ctx context.Context, record *kgo.Record) ([]*ProduceMessage, error)

// ProduceMessage represents a message to be produced
type ProduceMessage struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// ExactlyOnceConfig configures exactly-once processing
type ExactlyOnceConfig struct {
	ConsumerGroup       string
	TransactionalID     string
	ProcessingTimeout   time.Duration
	DeduplicationWindow time.Duration
}

// NewExactlyOnceProcessor creates a processor with exactly-once semantics
func NewExactlyOnceProcessor(config *Config, eoConfig *ExactlyOnceConfig, processFunc ExactlyOnceProcessFunc) (*ExactlyOnceProcessor, error) {
	client, err := NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	txnConfig := DefaultTransactionalConfig(eoConfig.TransactionalID)
	txnConfig.DeduplicationWindow = eoConfig.DeduplicationWindow

	txnProducer, err := NewTransactionalProducer(config, txnConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional producer: %w", err)
	}

	deduplicator := NewDeduplicator(eoConfig.DeduplicationWindow)

	return &ExactlyOnceProcessor{
		client:       client,
		txnProducer:  txnProducer,
		processFunc:  processFunc,
		deduplicator: deduplicator,
	}, nil
}

// Process processes a record with exactly-once semantics
func (eop *ExactlyOnceProcessor) Process(ctx context.Context, record *kgo.Record) error {
	eop.mu.Lock()
	defer eop.mu.Unlock()

	// Generate deterministic message ID
	messageID := generateRecordID(record)

	// Check for duplicates
	if eop.deduplicator.IsDuplicate(messageID) {
		return nil // Skip duplicate
	}

	// Execute within transaction
	err := eop.txnProducer.ExecuteTransaction(ctx, func(txnCtx context.Context) error {
		// Process record
		outputMsgs, err := eop.processFunc(txnCtx, record)
		if err != nil {
			return fmt.Errorf("processing failed: %w", err)
		}

		// Produce all output messages within transaction
		for _, msg := range outputMsgs {
			if msg.Headers != nil {
				if err := eop.txnProducer.ProduceWithHeaders(txnCtx, msg.Topic, msg.Key, msg.Value, msg.Headers); err != nil {
					return fmt.Errorf("failed to produce output: %w", err)
				}
			} else {
				if err := eop.txnProducer.Produce(txnCtx, msg.Topic, msg.Key, msg.Value); err != nil {
					return fmt.Errorf("failed to produce output: %w", err)
				}
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Mark as processed in deduplicator
	eop.deduplicator.MarkProcessed(messageID)

	return nil
}

// Run starts the exactly-once processor
func (eop *ExactlyOnceProcessor) Run(ctx context.Context, topics []string) error {
	consumer := NewConsumer(eop.client)

	return consumer.Consume(ctx, topics, func(ctx context.Context, record *kgo.Record) error {
		return eop.Process(ctx, record)
	})
}

// Close closes the exactly-once processor
func (eop *ExactlyOnceProcessor) Close() error {
	if err := eop.txnProducer.Close(); err != nil {
		return err
	}
	return eop.client.Close()
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

// IdempotentProducer wraps a regular producer with idempotence guarantees
type IdempotentProducer struct {
	producer *Producer
	msgCache *messageCache
}

// messageCache caches message IDs to prevent duplicate sends
type messageCache struct {
	cache  map[string]time.Time
	window time.Duration
	mu     sync.RWMutex
}

// NewIdempotentProducer creates a producer with idempotence guarantees
func NewIdempotentProducer(producer *Producer, cacheWindow time.Duration) *IdempotentProducer {
	mc := &messageCache{
		cache:  make(map[string]time.Time),
		window: cacheWindow,
	}

	// Start cleanup goroutine
	go mc.cleanup()

	return &IdempotentProducer{
		producer: producer,
		msgCache: mc,
	}
}

// Produce publishes a message idempotently
func (ip *IdempotentProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	// Generate deterministic message ID
	msgID := generateMessageID(topic, key, value)

	// Check if already sent
	if ip.msgCache.has(msgID) {
		return nil // Already sent, skip
	}

	// Send message
	err := ip.producer.Produce(ctx, topic, key, value)
	if err != nil {
		return err
	}

	// Mark as sent
	ip.msgCache.add(msgID)

	return nil
}

// ProduceWithHeaders publishes a message with headers idempotently
func (ip *IdempotentProducer) ProduceWithHeaders(ctx context.Context, topic string, key, value []byte, headers map[string]string) error {
	// Generate deterministic message ID
	msgID := generateMessageID(topic, key, value)

	// Check if already sent
	if ip.msgCache.has(msgID) {
		return nil // Already sent, skip
	}

	// Send message
	err := ip.producer.ProduceWithHeaders(ctx, topic, key, value, headers)
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

	processedAt, exists := mc.cache[msgID]
	if !exists {
		return false
	}

	// Check if still within cache window
	return time.Since(processedAt) < mc.window
}

func (mc *messageCache) add(msgID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.cache[msgID] = time.Now()
}

func (mc *messageCache) cleanup() {
	ticker := time.NewTicker(mc.window / 2)
	defer ticker.Stop()

	for range ticker.C {
		mc.mu.Lock()
		now := time.Now()
		for id, processedAt := range mc.cache {
			if now.Sub(processedAt) > mc.window {
				delete(mc.cache, id)
			}
		}
		mc.mu.Unlock()
	}
}

// Helper function to generate a deterministic message ID
func generateMessageID(topic string, key, value []byte) string {
	h := sha256.New()
	h.Write([]byte(topic))
	h.Write(key)
	h.Write(value)
	return hex.EncodeToString(h.Sum(nil))
}

// Helper function to generate a deterministic record ID
func generateRecordID(record *kgo.Record) string {
	h := sha256.New()
	h.Write([]byte(record.Topic))
	h.Write([]byte(fmt.Sprintf("%d", record.Partition)))
	h.Write([]byte(fmt.Sprintf("%d", record.Offset)))
	return hex.EncodeToString(h.Sum(nil))
}

// TransactionManager provides higher-level transaction management
type TransactionManager struct {
	producer *TransactionalProducer
	mu       sync.Mutex
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(config *Config, txnConfig *TransactionalConfig) (*TransactionManager, error) {
	producer, err := NewTransactionalProducer(config, txnConfig)
	if err != nil {
		return nil, err
	}

	return &TransactionManager{
		producer: producer,
	}, nil
}

// ExecuteInTransaction executes multiple operations in a single transaction
func (tm *TransactionManager) ExecuteInTransaction(ctx context.Context, operations []func(ctx context.Context) error) error {
	return tm.producer.ExecuteTransaction(ctx, func(txnCtx context.Context) error {
		for i, op := range operations {
			if err := op(txnCtx); err != nil {
				return fmt.Errorf("operation %d failed: %w", i, err)
			}
		}
		return nil
	})
}

// Close closes the transaction manager
func (tm *TransactionManager) Close() error {
	return tm.producer.Close()
}
