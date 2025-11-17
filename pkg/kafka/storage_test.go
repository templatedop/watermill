package kafka

import (
	"bytes"
	"sync"
	"testing"
)

func TestInMemoryStorage(t *testing.T) {
	storage := NewInMemoryStorage()

	// Test Set and Get
	key := "test-key"
	value := []byte("test-value")

	err := storage.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set: %v", err)
	}

	retrieved, err := storage.Get(key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if !bytes.Equal(retrieved, value) {
		t.Errorf("Expected %s, got %s", value, retrieved)
	}

	// Test Has
	exists, err := storage.Has(key)
	if err != nil {
		t.Fatalf("Failed to check existence: %v", err)
	}
	if !exists {
		t.Error("Expected key to exist")
	}

	// Test non-existent key
	_, err = storage.Get("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent key")
	}

	// Test Delete
	err = storage.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	exists, err = storage.Has(key)
	if err != nil {
		t.Fatalf("Failed to check existence after delete: %v", err)
	}
	if exists {
		t.Error("Expected key to not exist after delete")
	}
}

func TestInMemoryStorageIterator(t *testing.T) {
	storage := NewInMemoryStorage()

	// Add multiple key-value pairs
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err := storage.Set(k, []byte(v))
		if err != nil {
			t.Fatalf("Failed to set %s: %v", k, err)
		}
	}

	// Test iterator
	iter, err := storage.Iterator()
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		expectedValue, exists := testData[key]
		if !exists {
			t.Errorf("Unexpected key: %s", key)
		}

		if string(value) != expectedValue {
			t.Errorf("Expected %s, got %s for key %s", expectedValue, value, key)
		}

		count++
	}

	if count != len(testData) {
		t.Errorf("Expected %d items, got %d", len(testData), count)
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}
}

func TestInMemoryStorageClear(t *testing.T) {
	storage := NewInMemoryStorage()

	// Add some data
	storage.Set("key1", []byte("value1"))
	storage.Set("key2", []byte("value2"))

	// Clear
	err := storage.Clear()
	if err != nil {
		t.Fatalf("Failed to clear: %v", err)
	}

	// Verify empty
	exists, _ := storage.Has("key1")
	if exists {
		t.Error("Expected storage to be empty after clear")
	}
}

func TestPartitionedStorage(t *testing.T) {
	baseStorage := NewInMemoryStorage()
	partition := int32(5)
	prefix := "group-"

	storage := NewPartitionedStorage(partition, baseStorage, prefix)

	// Test Set and Get with partition prefix
	key := "test-key"
	value := []byte("test-value")

	err := storage.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set: %v", err)
	}

	retrieved, err := storage.Get(key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if !bytes.Equal(retrieved, value) {
		t.Errorf("Expected %s, got %s", value, retrieved)
	}

	// Verify partition info
	if storage.Partition() != partition {
		t.Errorf("Expected partition %d, got %d", partition, storage.Partition())
	}

	if storage.Prefix() != prefix {
		t.Errorf("Expected prefix %s, got %s", prefix, storage.Prefix())
	}

	// Test that keys are prefixed correctly
	fullKey := storage.makeKey(key)
	expectedKey := "group-5:test-key"
	if fullKey != expectedKey {
		t.Errorf("Expected key %s, got %s", expectedKey, fullKey)
	}
}

func TestPartitionedStorageIsolation(t *testing.T) {
	baseStorage := NewInMemoryStorage()

	storage1 := NewPartitionedStorage(1, baseStorage, "group-")
	storage2 := NewPartitionedStorage(2, baseStorage, "group-")

	key := "same-key"
	value1 := []byte("value-partition-1")
	value2 := []byte("value-partition-2")

	// Set same key in different partitions
	storage1.Set(key, value1)
	storage2.Set(key, value2)

	// Verify isolation
	retrieved1, _ := storage1.Get(key)
	retrieved2, _ := storage2.Get(key)

	if !bytes.Equal(retrieved1, value1) {
		t.Error("Partition 1 value incorrect")
	}

	if !bytes.Equal(retrieved2, value2) {
		t.Error("Partition 2 value incorrect")
	}

	if bytes.Equal(retrieved1, retrieved2) {
		t.Error("Partitions should be isolated")
	}
}

func TestStorageConcurrency(t *testing.T) {
	storage := NewInMemoryStorage()
	var wg sync.WaitGroup

	// Concurrent writes
	numGoroutines := 100
	numOps := 100

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := string(rune('a' + (id+j)%26))
				value := []byte{byte(id), byte(j)}
				storage.Set(key, value)
			}
		}(i)
	}

	wg.Wait()

	// Concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := string(rune('a' + (id+j)%26))
				_, _ = storage.Get(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestStorageIteratorConcurrency(t *testing.T) {
	storage := NewInMemoryStorage()

	// Add some initial data
	for i := 0; i < 100; i++ {
		key := string(rune('a' + i%26))
		storage.Set(key, []byte{byte(i)})
	}

	var wg sync.WaitGroup
	numReaders := 10

	// Multiple concurrent iterators
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			iter, err := storage.Iterator()
			if err != nil {
				t.Errorf("Failed to create iterator: %v", err)
				return
			}
			defer iter.Close()

			for iter.Next() {
				_ = iter.Key()
				_ = iter.Value()
			}
		}()
	}

	wg.Wait()
}

func BenchmarkInMemoryStorageSet(b *testing.B) {
	storage := NewInMemoryStorage()
	value := []byte("benchmark value")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := string(rune('a' + i%26))
		_ = storage.Set(key, value)
	}
}

func BenchmarkInMemoryStorageGet(b *testing.B) {
	storage := NewInMemoryStorage()
	value := []byte("benchmark value")

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := string(rune('a' + i%26))
		storage.Set(key, value)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := string(rune('a' + i%26))
		_, _ = storage.Get(key)
	}
}

func BenchmarkInMemoryStorageIterator(b *testing.B) {
	storage := NewInMemoryStorage()

	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		key := string(rune(i))
		storage.Set(key, []byte{byte(i % 256)})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		iter, _ := storage.Iterator()
		for iter.Next() {
			_ = iter.Key()
			_ = iter.Value()
		}
		iter.Close()
	}
}

func BenchmarkPartitionedStorage(b *testing.B) {
	baseStorage := NewInMemoryStorage()
	storage := NewPartitionedStorage(1, baseStorage, "group-")
	value := []byte("benchmark value")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := string(rune('a' + i%26))
		_ = storage.Set(key, value)
		_, _ = storage.Get(key)
	}
}
