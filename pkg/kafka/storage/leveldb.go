package storage

import (
	"fmt"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// LevelDBStorage implements Storage interface using LevelDB
type LevelDBStorage struct {
	db   *leveldb.DB
	path string
	mu   sync.RWMutex
}

// NewLevelDBStorage creates a new LevelDB storage
func NewLevelDBStorage(path string) (*LevelDBStorage, error) {
	return NewLevelDBStorageWithOptions(path, nil)
}

// NewLevelDBStorageWithOptions creates a new LevelDB storage with custom options
func NewLevelDBStorageWithOptions(path string, opts *opt.Options) (*LevelDBStorage, error) {
	if opts == nil {
		opts = &opt.Options{
			Compression:            opt.SnappyCompression,
			BlockCacheCapacity:     8 * 1024 * 1024, // 8MB cache
			WriteBuffer:            4 * 1024 * 1024, // 4MB write buffer
			CompactionTableSize:    2 * 1024 * 1024, // 2MB table size
			CompactionTotalSize:    10 * 1024 * 1024, // 10MB total size
		}
	}

	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb at %s: %w", path, err)
	}

	return &LevelDBStorage{
		db:   db,
		path: path,
	}, nil
}

// Get retrieves a value by key
func (s *LevelDBStorage) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	// Return a copy to avoid issues with LevelDB's internal buffers
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Set stores a key-value pair
func (s *LevelDBStorage) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Put([]byte(key), value, nil)
	if err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}
	return nil
}

// Delete removes a key
func (s *LevelDBStorage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Delete([]byte(key), nil)
	if err != nil && err != leveldb.ErrNotFound {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

// Has checks if a key exists
func (s *LevelDBStorage) Has(key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exists, err := s.db.Has([]byte(key), nil)
	if err != nil {
		return false, fmt.Errorf("failed to check key %s: %w", key, err)
	}
	return exists, nil
}

// Iterator returns an iterator over all key-value pairs
func (s *LevelDBStorage) Iterator() (Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter := s.db.NewIterator(nil, nil)
	return &levelDBIterator{
		iter:    iter,
		storage: s,
	}, nil
}

// IteratorWithPrefix returns an iterator over keys with a specific prefix
func (s *LevelDBStorage) IteratorWithPrefix(prefix string) (Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	return &levelDBIterator{
		iter:    iter,
		storage: s,
	}, nil
}

// Clear removes all keys from storage
func (s *LevelDBStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	batch := new(leveldb.Batch)
	for iter.Next() {
		batch.Delete(iter.Key())
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during clear: %w", err)
	}

	err := s.db.Write(batch, nil)
	if err != nil {
		return fmt.Errorf("failed to clear storage: %w", err)
	}

	return nil
}

// Close closes the LevelDB database
func (s *LevelDBStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		err := s.db.Close()
		if err != nil {
			return fmt.Errorf("failed to close leveldb: %w", err)
		}
		s.db = nil
	}
	return nil
}

// Compact triggers manual compaction
func (s *LevelDBStorage) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.CompactRange(util.Range{})
	if err != nil {
		return fmt.Errorf("failed to compact: %w", err)
	}
	return nil
}

// GetStats returns database statistics
func (s *LevelDBStorage) GetStats() (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats, err := s.db.GetProperty("leveldb.stats")
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}

	return map[string]string{
		"stats": stats,
		"path":  s.path,
	}, nil
}

// WriteBatch writes multiple operations atomically
func (s *LevelDBStorage) WriteBatch(operations []BatchOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch := new(leveldb.Batch)
	for _, op := range operations {
		switch op.Type {
		case BatchOpSet:
			batch.Put([]byte(op.Key), op.Value)
		case BatchOpDelete:
			batch.Delete([]byte(op.Key))
		default:
			return fmt.Errorf("unknown batch operation type: %d", op.Type)
		}
	}

	err := s.db.Write(batch, nil)
	if err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	return nil
}

// levelDBIterator implements Iterator for LevelDB
type levelDBIterator struct {
	iter    iterator.Iterator
	storage *LevelDBStorage
}

func (it *levelDBIterator) Next() bool {
	return it.iter.Next()
}

func (it *levelDBIterator) Key() string {
	return string(it.iter.Key())
}

func (it *levelDBIterator) Value() []byte {
	// Return a copy to avoid issues with LevelDB's internal buffers
	value := it.iter.Value()
	result := make([]byte, len(value))
	copy(result, value)
	return result
}

func (it *levelDBIterator) Err() error {
	return it.iter.Error()
}

func (it *levelDBIterator) Close() error {
	it.iter.Release()
	return it.iter.Error()
}

// Storage interface types
type Iterator interface {
	Next() bool
	Key() string
	Value() []byte
	Err() error
	Close() error
}

type BatchOpType int

const (
	BatchOpSet BatchOpType = iota
	BatchOpDelete
)

type BatchOperation struct {
	Type  BatchOpType
	Key   string
	Value []byte
}
