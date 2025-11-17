package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage implements Storage interface using Redis
type RedisStorage struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
	ctx    context.Context
	mu     sync.RWMutex
}

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	Prefix   string
	TTL      time.Duration

	// Advanced options
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DefaultRedisConfig returns default Redis configuration
func DefaultRedisConfig(addr string) *RedisConfig {
	return &RedisConfig{
		Addr:         addr,
		DB:           0,
		Prefix:       "watermill:",
		TTL:          0, // No expiration
		PoolSize:     10,
		MinIdleConns: 2,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// NewRedisStorage creates a new Redis storage
func NewRedisStorage(config *RedisConfig) (*RedisStorage, error) {
	if config == nil {
		config = DefaultRedisConfig("localhost:6379")
	}

	client := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
		MaxRetries:   config.MaxRetries,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisStorage{
		client: client,
		prefix: config.Prefix,
		ttl:    config.TTL,
		ctx:    context.Background(),
	}, nil
}

func (s *RedisStorage) makeKey(key string) string {
	return s.prefix + key
}

// Get retrieves a value by key
func (s *RedisStorage) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, err := s.client.Get(s.ctx, s.makeKey(key)).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	return value, nil
}

// Set stores a key-value pair
func (s *RedisStorage) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.client.Set(s.ctx, s.makeKey(key), value, s.ttl).Err()
	if err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}
	return nil
}

// Delete removes a key
func (s *RedisStorage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.client.Del(s.ctx, s.makeKey(key)).Err()
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

// Has checks if a key exists
func (s *RedisStorage) Has(key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count, err := s.client.Exists(s.ctx, s.makeKey(key)).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check key %s: %w", key, err)
	}
	return count > 0, nil
}

// Iterator returns an iterator over all key-value pairs
func (s *RedisStorage) Iterator() (Iterator, error) {
	return s.IteratorWithPrefix("")
}

// IteratorWithPrefix returns an iterator over keys with a specific prefix
func (s *RedisStorage) IteratorWithPrefix(userPrefix string) (Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pattern := s.makeKey(userPrefix + "*")
	cursor := uint64(0)
	keys := []string{}

	// Scan all keys matching the pattern
	for {
		var batch []string
		var err error
		batch, cursor, err = s.client.Scan(s.ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan keys: %w", err)
		}

		keys = append(keys, batch...)

		if cursor == 0 {
			break
		}
	}

	return &redisIterator{
		storage: s,
		keys:    keys,
		index:   -1,
	}, nil
}

// Clear removes all keys with the storage prefix
func (s *RedisStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pattern := s.makeKey("*")
	cursor := uint64(0)

	for {
		var keys []string
		var err error
		keys, cursor, err = s.client.Scan(s.ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("failed to scan keys during clear: %w", err)
		}

		if len(keys) > 0 {
			if err := s.client.Del(s.ctx, keys...).Err(); err != nil {
				return fmt.Errorf("failed to delete keys: %w", err)
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}

// Close closes the Redis connection
func (s *RedisStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		err := s.client.Close()
		if err != nil {
			return fmt.Errorf("failed to close redis: %w", err)
		}
		s.client = nil
	}
	return nil
}

// WriteBatch writes multiple operations atomically using pipeline
func (s *RedisStorage) WriteBatch(operations []BatchOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pipe := s.client.Pipeline()

	for _, op := range operations {
		switch op.Type {
		case BatchOpSet:
			pipe.Set(s.ctx, s.makeKey(op.Key), op.Value, s.ttl)
		case BatchOpDelete:
			pipe.Del(s.ctx, s.makeKey(op.Key))
		default:
			return fmt.Errorf("unknown batch operation type: %d", op.Type)
		}
	}

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to execute batch: %w", err)
	}

	return nil
}

// GetStats returns Redis INFO stats
func (s *RedisStorage) GetStats() (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, err := s.client.Info(s.ctx, "stats", "memory").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}

	// Parse INFO output
	stats := make(map[string]string)
	lines := strings.Split(info, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			stats[parts[0]] = parts[1]
		}
	}

	return stats, nil
}

// SetTTL updates the TTL for future operations
func (s *RedisStorage) SetTTL(ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ttl = ttl
}

// Expire sets expiration on an existing key
func (s *RedisStorage) Expire(key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.client.Expire(s.ctx, s.makeKey(key), ttl).Err()
	if err != nil {
		return fmt.Errorf("failed to set expiration on key %s: %w", key, err)
	}
	return nil
}

// redisIterator implements Iterator for Redis
type redisIterator struct {
	storage      *RedisStorage
	keys         []string
	index        int
	currentKey   string
	currentValue []byte
	err          error
}

func (it *redisIterator) Next() bool {
	it.index++
	if it.index >= len(it.keys) {
		return false
	}

	it.currentKey = it.keys[it.index]

	// Get the value
	value, err := it.storage.client.Get(it.storage.ctx, it.currentKey).Bytes()
	if err != nil {
		if err != redis.Nil {
			it.err = err
		}
		// Key might have been deleted, continue to next
		return it.Next()
	}

	it.currentValue = value

	return true
}

func (it *redisIterator) Key() string {
	// Remove the storage prefix
	return strings.TrimPrefix(it.currentKey, it.storage.prefix)
}

func (it *redisIterator) Value() []byte {
	return it.currentValue
}

func (it *redisIterator) Err() error {
	return it.err
}

func (it *redisIterator) Close() error {
	// Nothing to close for Redis iterator
	return nil
}
