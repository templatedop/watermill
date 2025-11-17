package storage

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// BadgerStorage implements Storage interface using BadgerDB
type BadgerStorage struct {
	db   *badger.DB
	path string
	mu   sync.RWMutex
}

// BadgerConfig holds BadgerDB configuration
type BadgerConfig struct {
	Path string

	// Performance tuning
	ValueLogFileSize   int64
	MemTableSize       int64
	NumMemtables       int
	NumLevelZeroTables int
	NumCompactors      int

	// Storage options
	SyncWrites      bool
	ValueThreshold  int
	NumVersionsToKeep int
}

// DefaultBadgerConfig returns default BadgerDB configuration
func DefaultBadgerConfig(path string) *BadgerConfig {
	return &BadgerConfig{
		Path:               path,
		ValueLogFileSize:   64 << 20, // 64MB
		MemTableSize:       64 << 20, // 64MB
		NumMemtables:       3,
		NumLevelZeroTables: 3,
		NumCompactors:      2,
		SyncWrites:         false, // Async writes for performance
		ValueThreshold:     1024,  // 1KB - values larger than this go to value log
		NumVersionsToKeep:  1,     // MVCC versions
	}
}

// NewBadgerStorage creates a new BadgerDB storage
func NewBadgerStorage(config *BadgerConfig) (*BadgerStorage, error) {
	if config == nil {
		config = DefaultBadgerConfig("/tmp/badger")
	}

	opts := badger.DefaultOptions(config.Path).
		WithValueLogFileSize(config.ValueLogFileSize).
		WithMemTableSize(config.MemTableSize).
		WithNumMemtables(config.NumMemtables).
		WithNumLevelZeroTables(config.NumLevelZeroTables).
		WithNumCompactors(config.NumCompactors).
		WithSyncWrites(config.SyncWrites).
		WithValueThreshold(config.ValueThreshold).
		WithNumVersionsToKeep(config.NumVersionsToKeep).
		WithLogger(nil) // Disable logging

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger at %s: %w", config.Path, err)
	}

	return &BadgerStorage{
		db:   db,
		path: config.Path,
	}, nil
}

// Get retrieves a value by key
func (s *BadgerStorage) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		value, err = item.ValueCopy(nil)
		return err
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	return value, nil
}

// Set stores a key-value pair
func (s *BadgerStorage) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})

	if err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}
	return nil
}

// Delete removes a key
func (s *BadgerStorage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err != nil && err != badger.ErrKeyNotFound {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

// Has checks if a key exists
func (s *BadgerStorage) Has(key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})

	if err != nil {
		return false, fmt.Errorf("failed to check key %s: %w", key, err)
	}
	return exists, nil
}

// Iterator returns an iterator over all key-value pairs
func (s *BadgerStorage) Iterator() (Iterator, error) {
	return s.IteratorWithPrefix("")
}

// IteratorWithPrefix returns an iterator over keys with a specific prefix
func (s *BadgerStorage) IteratorWithPrefix(prefix string) (Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	txn := s.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte(prefix)

	iter := txn.NewIterator(opts)

	return &badgerIterator{
		txn:    txn,
		iter:   iter,
		prefix: []byte(prefix),
	}, nil
}

// Clear removes all keys from storage
func (s *BadgerStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.DropAll()
	if err != nil {
		return fmt.Errorf("failed to clear storage: %w", err)
	}

	return nil
}

// Close closes the BadgerDB database
func (s *BadgerStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		err := s.db.Close()
		if err != nil {
			return fmt.Errorf("failed to close badger: %w", err)
		}
		s.db = nil
	}
	return nil
}

// RunValueLogGC runs the value log garbage collector
func (s *BadgerStorage) RunValueLogGC(discardRatio float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.RunValueLogGC(discardRatio)
	if err != nil && err != badger.ErrNoRewrite {
		return fmt.Errorf("failed to run value log GC: %w", err)
	}
	return nil
}

// Flatten flattens the LSM tree
func (s *BadgerStorage) Flatten(workers int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Flatten(workers)
	if err != nil {
		return fmt.Errorf("failed to flatten: %w", err)
	}
	return nil
}

// WriteBatch writes multiple operations atomically
func (s *BadgerStorage) WriteBatch(operations []BatchOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, op := range operations {
		switch op.Type {
		case BatchOpSet:
			if err := wb.Set([]byte(op.Key), op.Value); err != nil {
				return fmt.Errorf("failed to add set operation: %w", err)
			}
		case BatchOpDelete:
			if err := wb.Delete([]byte(op.Key)); err != nil {
				return fmt.Errorf("failed to add delete operation: %w", err)
			}
		default:
			return fmt.Errorf("unknown batch operation type: %d", op.Type)
		}
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch: %w", err)
	}

	return nil
}

// GetStats returns database statistics
func (s *BadgerStorage) GetStats() (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lsm, vlog := s.db.Size()

	stats := map[string]string{
		"path":      s.path,
		"lsm_size":  fmt.Sprintf("%d", lsm),
		"vlog_size": fmt.Sprintf("%d", vlog),
		"total_size": fmt.Sprintf("%d", lsm+vlog),
	}

	return stats, nil
}

// Backup creates a backup of the database
func (s *BadgerStorage) Backup(path string, since uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	f, err := badger.DefaultOptions(path).Open()
	if err != nil {
		return fmt.Errorf("failed to open backup database: %w", err)
	}
	defer f.Close()

	_, err = s.db.Backup(f, since)
	if err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	return nil
}

// badgerIterator implements Iterator for BadgerDB
type badgerIterator struct {
	txn    *badger.Txn
	iter   *badger.Iterator
	prefix []byte
}

func (it *badgerIterator) Next() bool {
	if !it.iter.Valid() {
		it.iter.Rewind()
		if !it.iter.Valid() {
			return false
		}
	} else {
		it.iter.Next()
	}

	return it.iter.ValidForPrefix(it.prefix)
}

func (it *badgerIterator) Key() string {
	return string(it.iter.Item().Key())
}

func (it *badgerIterator) Value() []byte {
	var value []byte
	err := it.iter.Item().Value(func(val []byte) error {
		value = append([]byte{}, val...)
		return nil
	})
	if err != nil {
		return nil
	}
	return value
}

func (it *badgerIterator) Err() error {
	// BadgerDB iterator doesn't have a separate error method
	return nil
}

func (it *badgerIterator) Close() error {
	it.iter.Close()
	it.txn.Discard()
	return nil
}
