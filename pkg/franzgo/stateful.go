package franzgo

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// StateStore represents a partitioned state store
type StateStore interface {
	// Get retrieves a value by key for the current partition
	Get(key string) ([]byte, error)

	// Set stores a value by key for the current partition
	Set(key string, value []byte) error

	// Delete removes a value by key
	Delete(key string) error

	// Has checks if a key exists
	Has(key string) (bool, error)

	// Iterator returns an iterator over all keys in the partition
	Iterator() (StorageIterator, error)

	// Flush persists any pending changes
	Flush() error

	// Close closes the state store
	Close() error

	// GetPartition returns the partition this store manages
	GetPartition() int32
}

// PartitionedStateStore wraps a Storage backend with partition awareness
type PartitionedStateStore struct {
	storage   Storage
	partition int32
	prefix    string
	mu        sync.RWMutex
}

// NewPartitionedStateStore creates a new partitioned state store
func NewPartitionedStateStore(storage Storage, partition int32) *PartitionedStateStore {
	return &PartitionedStateStore{
		storage:   storage,
		partition: partition,
		prefix:    fmt.Sprintf("partition-%d:", partition),
	}
}

func (pss *PartitionedStateStore) Get(key string) ([]byte, error) {
	pss.mu.RLock()
	defer pss.mu.RUnlock()
	return pss.storage.Get(pss.prefix + key)
}

func (pss *PartitionedStateStore) Set(key string, value []byte) error {
	pss.mu.Lock()
	defer pss.mu.Unlock()
	return pss.storage.Set(pss.prefix+key, value)
}

func (pss *PartitionedStateStore) Delete(key string) error {
	pss.mu.Lock()
	defer pss.mu.Unlock()
	return pss.storage.Delete(pss.prefix + key)
}

func (pss *PartitionedStateStore) Has(key string) (bool, error) {
	pss.mu.RLock()
	defer pss.mu.RUnlock()
	return pss.storage.Has(pss.prefix + key)
}

func (pss *PartitionedStateStore) Iterator() (StorageIterator, error) {
	return pss.storage.Iterator()
}

func (pss *PartitionedStateStore) Flush() error {
	// Most storage backends auto-flush, but this can be implemented if needed
	return nil
}

func (pss *PartitionedStateStore) Close() error {
	return pss.storage.Close()
}

func (pss *PartitionedStateStore) GetPartition() int32 {
	return pss.partition
}

// StateProcessorContext provides context for stateful processing
type StateProcessorContext struct {
	ctx       context.Context
	record    *kgo.Record
	state     StateStore
	changelog *ChangelogProducer
}

// Get retrieves state
func (spc *StateProcessorContext) Get(key string) ([]byte, error) {
	return spc.state.Get(key)
}

// Set updates state and optionally emits to changelog
func (spc *StateProcessorContext) Set(key string, value []byte) error {
	if err := spc.state.Set(key, value); err != nil {
		return err
	}

	// Emit to changelog if configured
	if spc.changelog != nil {
		return spc.changelog.EmitChange(spc.ctx, key, value)
	}

	return nil
}

// Delete removes state
func (spc *StateProcessorContext) Delete(key string) error {
	if err := spc.state.Delete(key); err != nil {
		return err
	}

	// Emit deletion to changelog
	if spc.changelog != nil {
		return spc.changelog.EmitChange(spc.ctx, key, nil)
	}

	return nil
}

// GetJSON retrieves and unmarshals JSON state
func (spc *StateProcessorContext) GetJSON(key string, v interface{}) error {
	data, err := spc.Get(key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// SetJSON marshals and stores JSON state
func (spc *StateProcessorContext) SetJSON(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return spc.Set(key, data)
}

// Record returns the current Kafka record being processed
func (spc *StateProcessorContext) Record() *kgo.Record {
	return spc.record
}

// Partition returns the partition being processed
func (spc *StateProcessorContext) Partition() int32 {
	return spc.record.Partition
}

// StateProcessorFunc is a function that processes a record with state
type StateProcessorFunc func(ctx *StateProcessorContext) error

// StatefulProcessor processes records with state management
type StatefulProcessor struct {
	client         *Client
	consumer       *Consumer
	storageFactory func(partition int32) (Storage, error)
	stateStores    map[int32]StateStore
	changelogTopic string
	changelog      *ChangelogProducer
	processor      StateProcessorFunc
	mu             sync.RWMutex
}

// StatefulProcessorConfig configures stateful processing
type StatefulProcessorConfig struct {
	// Storage factory creates storage for each partition
	StorageFactory func(partition int32) (Storage, error)

	// Changelog topic for state persistence (optional)
	ChangelogTopic string

	// Whether to compact changelog (recommended for state)
	ChangelogCompacted bool

	// Recovery timeout for rebuilding state from changelog
	RecoveryTimeout time.Duration
}

// NewStatefulProcessor creates a new stateful processor
func NewStatefulProcessor(client *Client, config *StatefulProcessorConfig, processor StateProcessorFunc) (*StatefulProcessor, error) {
	if config.StorageFactory == nil {
		return nil, fmt.Errorf("storage factory is required")
	}

	sp := &StatefulProcessor{
		client:         client,
		consumer:       NewConsumer(client, nil),
		storageFactory: config.StorageFactory,
		stateStores:    make(map[int32]StateStore),
		changelogTopic: config.ChangelogTopic,
		processor:      processor,
	}

	// Create changelog producer if configured
	if config.ChangelogTopic != "" {
		sp.changelog = NewChangelogProducer(NewProducer(client), config.ChangelogTopic)
	}

	return sp, nil
}

// Process starts stateful processing
func (sp *StatefulProcessor) Process(ctx context.Context, topics []string) error {
	// Handler that manages state per partition
	handler := func(handlerCtx context.Context, record *kgo.Record) error {
		// Get or create state store for this partition
		stateStore, err := sp.getOrCreateStateStore(record.Partition)
		if err != nil {
			return fmt.Errorf("failed to get state store: %w", err)
		}

		// Create processor context
		processorCtx := &StateProcessorContext{
			ctx:       handlerCtx,
			record:    record,
			state:     stateStore,
			changelog: sp.changelog,
		}

		// Execute processor
		return sp.processor(processorCtx)
	}

	return sp.consumer.Consume(ctx, topics, handler)
}

// getOrCreateStateStore gets or creates a state store for a partition
func (sp *StatefulProcessor) getOrCreateStateStore(partition int32) (StateStore, error) {
	sp.mu.RLock()
	store, exists := sp.stateStores[partition]
	sp.mu.RUnlock()

	if exists {
		return store, nil
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()

	// Double-check after acquiring write lock
	if store, exists := sp.stateStores[partition]; exists {
		return store, nil
	}

	// Create storage for this partition
	storage, err := sp.storageFactory(partition)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage for partition %d: %w", partition, err)
	}

	// Create partitioned state store
	stateStore := NewPartitionedStateStore(storage, partition)
	sp.stateStores[partition] = stateStore

	return stateStore, nil
}

// Close closes all state stores
func (sp *StatefulProcessor) Close() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var errs []error
	for partition, store := range sp.stateStores {
		if err := store.Close(); err != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", partition, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing state stores: %v", errs)
	}

	return nil
}

// GetStateStore returns the state store for a specific partition
func (sp *StatefulProcessor) GetStateStore(partition int32) (StateStore, bool) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	store, exists := sp.stateStores[partition]
	return store, exists
}

// ChangelogProducer produces changelog events for state updates
type ChangelogProducer struct {
	producer *Producer
	topic    string
}

// NewChangelogProducer creates a changelog producer
func NewChangelogProducer(producer *Producer, topic string) *ChangelogProducer {
	return &ChangelogProducer{
		producer: producer,
		topic:    topic,
	}
}

// EmitChange emits a state change to the changelog topic
func (cp *ChangelogProducer) EmitChange(ctx context.Context, key string, value []byte) error {
	return cp.producer.Produce(ctx, cp.topic, []byte(key), value)
}

// StateRecovery handles state recovery from changelog
type StateRecovery struct {
	client    *Client
	stateStore StateStore
	changelog  string
	partition  int32
}

// NewStateRecovery creates a state recovery manager
func NewStateRecovery(client *Client, stateStore StateStore, changelog string, partition int32) *StateRecovery {
	return &StateRecovery{
		client:     client,
		stateStore: stateStore,
		changelog:  changelog,
		partition:  partition,
	}
}

// Recover rebuilds state from the changelog topic
func (sr *StateRecovery) Recover(ctx context.Context) error {
	// Create a temporary consumer for recovery
	consumer := NewConsumer(sr.client, nil)

	var recoveredKeys int64
	handler := func(handlerCtx context.Context, record *kgo.Record) error {
		// Only process records for our partition
		if record.Partition != sr.partition {
			return nil
		}

		key := string(record.Key)

		if record.Value == nil {
			// Deletion (tombstone)
			if err := sr.stateStore.Delete(key); err != nil {
				return fmt.Errorf("failed to delete key during recovery: %w", err)
			}
		} else {
			// Update
			if err := sr.stateStore.Set(key, record.Value); err != nil {
				return fmt.Errorf("failed to set key during recovery: %w", err)
			}
		}

		recoveredKeys++
		return nil
	}

	// Consume from beginning to end
	if err := consumer.Consume(ctx, []string{sr.changelog}, handler); err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}

	return nil
}

// StateSnapshot represents a point-in-time snapshot of state
type StateSnapshot struct {
	Partition int32                  `json:"partition"`
	Timestamp time.Time              `json:"timestamp"`
	Keys      int                    `json:"keys"`
	Data      map[string][]byte      `json:"data"`
	Metadata  map[string]interface{} `json:"metadata"`
}

// CreateSnapshot creates a snapshot of the state store
func CreateSnapshot(store StateStore) (*StateSnapshot, error) {
	snapshot := &StateSnapshot{
		Partition: store.GetPartition(),
		Timestamp: time.Now(),
		Data:      make(map[string][]byte),
		Metadata:  make(map[string]interface{}),
	}

	// Iterate over all keys
	iter, err := store.Iterator()
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		snapshot.Data[key] = value
		snapshot.Keys++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return snapshot, nil
}

// RestoreSnapshot restores a state store from a snapshot
func RestoreSnapshot(store StateStore, snapshot *StateSnapshot) error {
	if store.GetPartition() != snapshot.Partition {
		return fmt.Errorf("partition mismatch: store is partition %d, snapshot is partition %d",
			store.GetPartition(), snapshot.Partition)
	}

	for key, value := range snapshot.Data {
		if err := store.Set(key, value); err != nil {
			return fmt.Errorf("failed to restore key %s: %w", key, err)
		}
	}

	return store.Flush()
}

// AggregatingStateProcessor provides common aggregation operations
type AggregatingStateProcessor struct {
	processor *StatefulProcessor
}

// NewAggregatingStateProcessor creates an aggregating processor
func NewAggregatingStateProcessor(client *Client, config *StatefulProcessorConfig) (*AggregatingStateProcessor, error) {
	// Default processor that does nothing (users override via With methods)
	processor, err := NewStatefulProcessor(client, config, func(ctx *StateProcessorContext) error {
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &AggregatingStateProcessor{
		processor: processor,
	}, nil
}

// WithCountAggregator creates a processor that counts occurrences by key
func (asp *AggregatingStateProcessor) WithCountAggregator() *StatefulProcessor {
	asp.processor.processor = func(ctx *StateProcessorContext) error {
		key := string(ctx.Record().Key)

		// Get current count
		var count int64
		data, err := ctx.Get(key)
		if err == nil {
			json.Unmarshal(data, &count)
		}

		// Increment
		count++

		// Store
		return ctx.SetJSON(key, count)
	}

	return asp.processor
}

// WithSumAggregator creates a processor that sums numeric values by key
func (asp *AggregatingStateProcessor) WithSumAggregator(valueField string) *StatefulProcessor {
	asp.processor.processor = func(ctx *StateProcessorContext) error {
		key := string(ctx.Record().Key)

		// Parse incoming value
		var incoming map[string]interface{}
		if err := json.Unmarshal(ctx.Record().Value, &incoming); err != nil {
			return err
		}

		value, ok := incoming[valueField].(float64)
		if !ok {
			return fmt.Errorf("field %s not found or not numeric", valueField)
		}

		// Get current sum
		var sum float64
		data, err := ctx.Get(key)
		if err == nil {
			json.Unmarshal(data, &sum)
		}

		// Add
		sum += value

		// Store
		return ctx.SetJSON(key, sum)
	}

	return asp.processor
}

// WithAverageAggregator creates a processor that computes average by key
func (asp *AggregatingStateProcessor) WithAverageAggregator(valueField string) *StatefulProcessor {
	type avgState struct {
		Sum   float64 `json:"sum"`
		Count int64   `json:"count"`
		Avg   float64 `json:"avg"`
	}

	asp.processor.processor = func(ctx *StateProcessorContext) error {
		key := string(ctx.Record().Key)

		// Parse incoming value
		var incoming map[string]interface{}
		if err := json.Unmarshal(ctx.Record().Value, &incoming); err != nil {
			return err
		}

		value, ok := incoming[valueField].(float64)
		if !ok {
			return fmt.Errorf("field %s not found or not numeric", valueField)
		}

		// Get current state
		var state avgState
		ctx.GetJSON(key, &state)

		// Update
		state.Sum += value
		state.Count++
		state.Avg = state.Sum / float64(state.Count)

		// Store
		return ctx.SetJSON(key, state)
	}

	return asp.processor
}

// WithJoinProcessor creates a processor that joins two streams via state
func WithJoinProcessor(client *Client, config *StatefulProcessorConfig, leftTopic, rightTopic string, joinFunc func(left, right []byte) ([]byte, error)) (*StatefulProcessor, error) {
	processor := func(ctx *StateProcessorContext) error {
		key := string(ctx.Record().Key)
		value := ctx.Record().Value
		topic := ctx.Record().Topic

		if topic == leftTopic {
			// Store left side
			if err := ctx.Set("left:"+key, value); err != nil {
				return err
			}

			// Try to join with right side
			rightValue, err := ctx.Get("right:" + key)
			if err == nil {
				// Join available
				joined, err := joinFunc(value, rightValue)
				if err != nil {
					return err
				}

				// Store joined result
				return ctx.Set("joined:"+key, joined)
			}
		} else if topic == rightTopic {
			// Store right side
			if err := ctx.Set("right:"+key, value); err != nil {
				return err
			}

			// Try to join with left side
			leftValue, err := ctx.Get("left:" + key)
			if err == nil {
				// Join available
				joined, err := joinFunc(leftValue, value)
				if err != nil {
					return err
				}

				// Store joined result
				return ctx.Set("joined:"+key, joined)
			}
		}

		return nil
	}

	return NewStatefulProcessor(client, config, processor)
}

// StateMigration handles state migration between versions
type StateMigration struct {
	version int
	migrate func(key string, oldValue []byte) ([]byte, error)
}

// StateVersionManager manages state schema versions
type StateVersionManager struct {
	currentVersion int
	migrations     []StateMigration
}

// NewStateVersionManager creates a version manager
func NewStateVersionManager(currentVersion int) *StateVersionManager {
	return &StateVersionManager{
		currentVersion: currentVersion,
		migrations:     make([]StateMigration, 0),
	}
}

// AddMigration adds a migration step
func (svm *StateVersionManager) AddMigration(version int, migrate func(key string, oldValue []byte) ([]byte, error)) {
	svm.migrations = append(svm.migrations, StateMigration{
		version: version,
		migrate: migrate,
	})
}

// Migrate migrates state from old version to current version
func (svm *StateVersionManager) Migrate(store StateStore, fromVersion int) error {
	iter, err := store.Iterator()
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Apply migrations in sequence
		for _, migration := range svm.migrations {
			if migration.version > fromVersion && migration.version <= svm.currentVersion {
				newValue, err := migration.migrate(key, value)
				if err != nil {
					return fmt.Errorf("migration to version %d failed for key %s: %w",
						migration.version, key, err)
				}
				value = newValue
			}
		}

		// Store migrated value
		if err := store.Set(key, value); err != nil {
			return fmt.Errorf("failed to store migrated value for key %s: %w", key, err)
		}
	}

	return iter.Error()
}
