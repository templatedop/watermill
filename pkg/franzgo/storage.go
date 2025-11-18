package franzgo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/redis/go-redis/v9"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Storage is an interface for local state storage
// Similar to Goka's pluggable storage (LevelDB, Redis, BadgerDB, in-memory)
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

// =========================================================================
// In-Memory Storage
// =========================================================================

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

// =========================================================================
// LevelDB Storage
// =========================================================================

// LevelDBStorage implements LevelDB-based storage
type LevelDBStorage struct {
	db   *leveldb.DB
	path string
}

// NewLevelDBStorage creates a new LevelDB storage
func NewLevelDBStorage(path string) (*LevelDBStorage, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}

	return &LevelDBStorage{
		db:   db,
		path: path,
	}, nil
}

// Get retrieves a value
func (s *LevelDBStorage) Get(key string) ([]byte, error) {
	value, err := s.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, err
	}

	// Return a copy
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Set stores a key-value pair
func (s *LevelDBStorage) Set(key string, value []byte) error {
	return s.db.Put([]byte(key), value, nil)
}

// Delete removes a key
func (s *LevelDBStorage) Delete(key string) error {
	return s.db.Delete([]byte(key), nil)
}

// Has checks if a key exists
func (s *LevelDBStorage) Has(key string) (bool, error) {
	return s.db.Has([]byte(key), nil)
}

// Iterator returns an iterator
func (s *LevelDBStorage) Iterator() (StorageIterator, error) {
	iter := s.db.NewIterator(nil, nil)
	return &levelDBIterator{iter: iter}, nil
}

// Close closes the storage
func (s *LevelDBStorage) Close() error {
	return s.db.Close()
}

// levelDBIterator implements StorageIterator for LevelDB
type levelDBIterator struct {
	iter iterator.Iterator
}

// Next moves to the next item
func (it *levelDBIterator) Next() bool {
	return it.iter.Next()
}

// Key returns the current key
func (it *levelDBIterator) Key() string {
	return string(it.iter.Key())
}

// Value returns the current value
func (it *levelDBIterator) Value() []byte {
	value := it.iter.Value()
	// Return a copy
	result := make([]byte, len(value))
	copy(result, value)
	return result
}

// Error returns any error
func (it *levelDBIterator) Error() error {
	return it.iter.Error()
}

// Close closes the iterator
func (it *levelDBIterator) Close() error {
	it.iter.Release()
	return nil
}

// =========================================================================
// Redis Storage
// =========================================================================

// RedisStorage implements Redis-based storage
type RedisStorage struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

// RedisConfig holds Redis storage configuration
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	Prefix   string
	TTL      time.Duration // 0 means no expiration
}

// NewRedisStorage creates a new Redis storage
func NewRedisStorage(config *RedisConfig) (*RedisStorage, error) {
	if config == nil {
		config = &RedisConfig{
			Addr:   "localhost:6379",
			Prefix: "franzgo:",
		}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisStorage{
		client: client,
		prefix: config.Prefix,
		ttl:    config.TTL,
	}, nil
}

// Get retrieves a value
func (s *RedisStorage) Get(key string) ([]byte, error) {
	ctx := context.Background()
	value, err := s.client.Get(ctx, s.prefix+key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, err
	}
	return value, nil
}

// Set stores a key-value pair
func (s *RedisStorage) Set(key string, value []byte) error {
	ctx := context.Background()
	return s.client.Set(ctx, s.prefix+key, value, s.ttl).Err()
}

// Delete removes a key
func (s *RedisStorage) Delete(key string) error {
	ctx := context.Background()
	return s.client.Del(ctx, s.prefix+key).Err()
}

// Has checks if a key exists
func (s *RedisStorage) Has(key string) (bool, error) {
	ctx := context.Background()
	count, err := s.client.Exists(ctx, s.prefix+key).Result()
	return count > 0, err
}

// Iterator returns an iterator
func (s *RedisStorage) Iterator() (StorageIterator, error) {
	ctx := context.Background()

	// Scan all keys with prefix
	var keys []string
	iter := s.client.Scan(ctx, 0, s.prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return &redisIterator{
		client: s.client,
		keys:   keys,
		prefix: s.prefix,
		index:  -1,
	}, nil
}

// Close closes the storage
func (s *RedisStorage) Close() error {
	return s.client.Close()
}

// redisIterator implements StorageIterator for Redis
type redisIterator struct {
	client *redis.Client
	keys   []string
	prefix string
	index  int
	value  []byte
	err    error
}

// Next moves to the next item
func (it *redisIterator) Next() bool {
	it.index++
	if it.index >= len(it.keys) {
		return false
	}

	// Fetch value for current key
	ctx := context.Background()
	value, err := it.client.Get(ctx, it.keys[it.index]).Bytes()
	if err != nil {
		it.err = err
		return false
	}

	it.value = value
	return true
}

// Key returns the current key (without prefix)
func (it *redisIterator) Key() string {
	if it.index < 0 || it.index >= len(it.keys) {
		return ""
	}

	key := it.keys[it.index]
	if len(key) > len(it.prefix) {
		return key[len(it.prefix):]
	}
	return key
}

// Value returns the current value
func (it *redisIterator) Value() []byte {
	return it.value
}

// Error returns any error
func (it *redisIterator) Error() error {
	return it.err
}

// Close closes the iterator
func (it *redisIterator) Close() error {
	return nil
}

// =========================================================================
// BadgerDB Storage
// =========================================================================

// BadgerStorage implements BadgerDB-based storage
type BadgerStorage struct {
	db   *badger.DB
	path string
}

// NewBadgerStorage creates a new BadgerDB storage
func NewBadgerStorage(path string) (*BadgerStorage, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // Disable logging by default

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badgerdb: %w", err)
	}

	return &BadgerStorage{
		db:   db,
		path: path,
	}, nil
}

// Get retrieves a value
func (s *BadgerStorage) Get(key string) ([]byte, error) {
	var value []byte

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		value, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, err
	}

	return value, nil
}

// Set stores a key-value pair
func (s *BadgerStorage) Set(key string, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

// Delete removes a key
func (s *BadgerStorage) Delete(key string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// Has checks if a key exists
func (s *BadgerStorage) Has(key string) (bool, error) {
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		return err
	})

	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Iterator returns an iterator
func (s *BadgerStorage) Iterator() (StorageIterator, error) {
	txn := s.db.NewTransaction(false)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	iter.Rewind()

	return &badgerIterator{
		txn:  txn,
		iter: iter,
	}, nil
}

// Close closes the storage
func (s *BadgerStorage) Close() error {
	return s.db.Close()
}

// badgerIterator implements StorageIterator for BadgerDB
type badgerIterator struct {
	txn   *badger.Txn
	iter  *badger.Iterator
	value []byte
	err   error
}

// Next moves to the next item
func (it *badgerIterator) Next() bool {
	if !it.iter.Valid() {
		return false
	}

	item := it.iter.Item()

	// Get value
	value, err := item.ValueCopy(nil)
	if err != nil {
		it.err = err
		return false
	}

	it.value = value
	it.iter.Next()

	return true
}

// Key returns the current key
func (it *badgerIterator) Key() string {
	if !it.iter.Valid() {
		return ""
	}
	return string(it.iter.Item().Key())
}

// Value returns the current value
func (it *badgerIterator) Value() []byte {
	return it.value
}

// Error returns any error
func (it *badgerIterator) Error() error {
	return it.err
}

// Close closes the iterator
func (it *badgerIterator) Close() error {
	it.iter.Close()
	it.txn.Discard()
	return nil
}

// =========================================================================
// Partitioned Storage
// =========================================================================

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
