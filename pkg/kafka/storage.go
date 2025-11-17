package kafka

import (
	"fmt"
	"sync"
)

// Storage is an interface for local state storage
// Similar to Goka's pluggable storage (LevelDB, Redis, in-memory)
type Storage interface {
	// Get retrieves a value by key
	Get(key string) ([]byte, error)
	// Set stores a key-value pair
	Set(key string, value []byte) error
	// Delete removes a key
	Delete(key string) error
	// Has checks if a key exists
	Has(key string) (bool, error)
	// Iterator returns an iterator for all keys
	Iterator() (StorageIterator, error)
	// Close closes the storage
	Close() error
}

// StorageIterator iterates over storage keys
type StorageIterator interface {
	// Next moves to the next key-value pair
	Next() bool
	// Key returns the current key
	Key() string
	// Value returns the current value
	Value() []byte
	// Error returns any iteration error
	Error() error
	// Close closes the iterator
	Close() error
}

// MemoryStorage implements in-memory storage
type MemoryStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStorage creates a new in-memory storage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string][]byte),
	}
}

// Get retrieves a value
func (s *MemoryStorage) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Return a copy to prevent modifications
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Set stores a key-value pair
func (s *MemoryStorage) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store a copy
	stored := make([]byte, len(value))
	copy(stored, value)
	s.data[key] = stored
	return nil
}

// Delete removes a key
func (s *MemoryStorage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}

// Has checks if a key exists
func (s *MemoryStorage) Has(key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.data[key]
	return exists, nil
}

// Iterator returns an iterator
func (s *MemoryStorage) Iterator() (StorageIterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create a snapshot of keys
	keys := make([]string, 0, len(s.data))
	values := make(map[string][]byte)

	for k, v := range s.data {
		keys = append(keys, k)
		// Copy value
		vcopy := make([]byte, len(v))
		copy(vcopy, v)
		values[k] = vcopy
	}

	return &memoryIterator{
		keys:   keys,
		values: values,
		index:  -1,
	}, nil
}

// Close closes the storage (no-op for memory)
func (s *MemoryStorage) Close() error {
	return nil
}

// memoryIterator implements StorageIterator for in-memory storage
type memoryIterator struct {
	keys   []string
	values map[string][]byte
	index  int
	err    error
}

// Next moves to the next item
func (it *memoryIterator) Next() bool {
	it.index++
	return it.index < len(it.keys)
}

// Key returns the current key
func (it *memoryIterator) Key() string {
	if it.index < 0 || it.index >= len(it.keys) {
		return ""
	}
	return it.keys[it.index]
}

// Value returns the current value
func (it *memoryIterator) Value() []byte {
	key := it.Key()
	if key == "" {
		return nil
	}
	return it.values[key]
}

// Error returns any error
func (it *memoryIterator) Error() error {
	return it.err
}

// Close closes the iterator
func (it *memoryIterator) Close() error {
	return nil
}

// PartitionedStorage wraps storage with partition awareness
type PartitionedStorage struct {
	partition int32
	storage   Storage
	prefix    string
}

// NewPartitionedStorage creates a partitioned storage
func NewPartitionedStorage(partition int32, storage Storage) *PartitionedStorage {
	return &PartitionedStorage{
		partition: partition,
		storage:   storage,
		prefix:    fmt.Sprintf("p%d:", partition),
	}
}

// Get retrieves a value with partition prefix
func (s *PartitionedStorage) Get(key string) ([]byte, error) {
	return s.storage.Get(s.prefix + key)
}

// Set stores a value with partition prefix
func (s *PartitionedStorage) Set(key string, value []byte) error {
	return s.storage.Set(s.prefix+key, value)
}

// Delete removes a key with partition prefix
func (s *PartitionedStorage) Delete(key string) error {
	return s.storage.Delete(s.prefix + key)
}

// Has checks if a key exists with partition prefix
func (s *PartitionedStorage) Has(key string) (bool, error) {
	return s.storage.Has(s.prefix + key)
}

// Iterator returns an iterator for this partition
func (s *PartitionedStorage) Iterator() (StorageIterator, error) {
	it, err := s.storage.Iterator()
	if err != nil {
		return nil, err
	}
	return &partitionedIterator{
		inner:  it,
		prefix: s.prefix,
	}, nil
}

// Close closes the underlying storage
func (s *PartitionedStorage) Close() error {
	return s.storage.Close()
}

// GetPartition returns the partition number
func (s *PartitionedStorage) GetPartition() int32 {
	return s.partition
}

// partitionedIterator filters keys by partition prefix
type partitionedIterator struct {
	inner  StorageIterator
	prefix string
}

// Next moves to the next item in this partition
func (it *partitionedIterator) Next() bool {
	for it.inner.Next() {
		key := it.inner.Key()
		if len(key) > len(it.prefix) && key[:len(it.prefix)] == it.prefix {
			return true
		}
	}
	return false
}

// Key returns the current key without partition prefix
func (it *partitionedIterator) Key() string {
	key := it.inner.Key()
	if len(key) > len(it.prefix) {
		return key[len(it.prefix):]
	}
	return key
}

// Value returns the current value
func (it *partitionedIterator) Value() []byte {
	return it.inner.Value()
}

// Error returns any error
func (it *partitionedIterator) Error() error {
	return it.inner.Error()
}

// Close closes the iterator
func (it *partitionedIterator) Close() error {
	return it.inner.Close()
}
